package archiver

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/filechunker"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/test"
	"golang.org/x/sync/errgroup"
)

func treeSaveHelper(_ context.Context, _ restic.BlobType, chunk filechunker.ChunkI, _ string, cb func(res saveBlobResponse)) {
	cb(saveBlobResponse{
		id:         restic.NewRandomID(),
		known:      false,
		length:     len(chunk.Data()),
		sizeInRepo: len(chunk.Data()),
	})
}

func setupTreeSaver() (context.Context, context.CancelFunc, *treeSaver, func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx)

	errFn := func(snPath string, err error) error {
		return err
	}

	b := newTreeSaver(ctx, wg, uint(runtime.NumCPU()), treeSaveHelper, errFn)

	shutdown := func() error {
		b.TriggerShutdown()
		return wg.Wait()
	}

	return ctx, cancel, b, shutdown
}

func TestTreeSaver(t *testing.T) {
	ctx, cancel, b, shutdown := setupTreeSaver()
	defer cancel()

	var results []futureNode

	for i := 0; i < 20; i++ {
		node := &restic.Node{
			Name: fmt.Sprintf("file-%d", i),
		}

		fb := b.Save(ctx, join("/", node.Name), node.Name, node, nil, nil)
		results = append(results, fb)
	}

	for _, tree := range results {
		tree.take(ctx)
	}

	err := shutdown()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTreeSaverError(t *testing.T) {
	var tests = []struct {
		trees  int
		failAt int
	}{
		{1, 1},
		{20, 2},
		{20, 5},
		{20, 15},
		{200, 150},
	}

	errTest := errors.New("test error")

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx, cancel, b, shutdown := setupTreeSaver()
			defer cancel()

			var results []futureNode

			for i := 0; i < test.trees; i++ {
				node := &restic.Node{
					Name: fmt.Sprintf("file-%d", i),
				}
				nodes := []futureNode{
					newFutureNodeWithResult(futureNodeResult{node: &restic.Node{
						Name: fmt.Sprintf("child-%d", i),
					}}),
				}
				if (i + 1) == test.failAt {
					nodes = append(nodes, newFutureNodeWithResult(futureNodeResult{
						err: errTest,
					}))
				}

				fb := b.Save(ctx, join("/", node.Name), node.Name, node, nodes, nil)
				results = append(results, fb)
			}

			for _, tree := range results {
				tree.take(ctx)
			}

			err := shutdown()
			if err == nil {
				t.Errorf("expected error not found")
			}
			if err != errTest {
				t.Fatalf("unexpected error found: %v", err)
			}
		})
	}
}

func TestTreeSaverDuplicates(t *testing.T) {
	for _, identicalNodes := range []bool{true, false} {
		t.Run("", func(t *testing.T) {
			ctx, cancel, b, shutdown := setupTreeSaver()
			defer cancel()

			node := &restic.Node{
				Name: "file",
			}
			nodes := []futureNode{
				newFutureNodeWithResult(futureNodeResult{node: &restic.Node{
					Name: "child",
				}}),
			}
			if identicalNodes {
				nodes = append(nodes, newFutureNodeWithResult(futureNodeResult{node: &restic.Node{
					Name: "child",
				}}))
			} else {
				nodes = append(nodes, newFutureNodeWithResult(futureNodeResult{node: &restic.Node{
					Name: "child",
					Size: 42,
				}}))
			}

			fb := b.Save(ctx, join("/", node.Name), node.Name, node, nodes, nil)
			fb.take(ctx)

			err := shutdown()
			if identicalNodes {
				test.Assert(t, err == nil, "unexpected error found: %v", err)
			} else {
				test.Assert(t, err != nil, "expected error not found")
			}
		})
	}
}
