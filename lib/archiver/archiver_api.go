package archiver

import (
	"context"

	cm_main "github.com/restic/restic/cmd/restic"
	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/restic"
)

type EasyArchiver struct {
	writer *archiver.SnapshotWriter
	unlock func()
}

func NewEasyArchiver(ctx context.Context) (*EasyArchiver, error) {
	lockCtx, repo, unlock, err := cm_main.OpenWithAppendLock(ctx, cm_main.GlobalOptions{}, false)
	if err != nil {
		return nil, err
	}

	return &EasyArchiver{
		writer: archiver.NewSnapshotWriter(
			lockCtx,
			repo,
			archiver.Options{},
			archiver.SnapshotOptions{},
			func(bytes uint64) {},
			func(file string, err error) error { return err },
			func(snPath, filename string, meta archiver.ToNoder, ignoreXattrListError bool) (*restic.Node, error) {
				return nil, nil
			},
		),
		unlock: unlock,
	}, nil
}

func (a *EasyArchiver) Close() {
	a.unlock()
}
