package archiver

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/filechunker"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

var ErrFileCompletedWithNullID = errors.New("completed file with null ID")

// saveBlobFn saves a blob to a repo.
type saveBlobFn func(context.Context, restic.BlobType, filechunker.ChunkI, string, func(res saveBlobResponse))

// fileSaver concurrently saves incoming files to the repo.
type fileSaver struct {
	saveFilePool *bufferPool
	saveBlob     saveBlobFn

	pol chunker.Pol

	ch chan<- saveFileJob

	CompleteBlob func(bytes uint64)

	NodeFromFileInfo func(snPath, filename string, meta ToNoder, ignoreXattrListError bool) (*restic.Node, error)
}

// newFileSaver returns a new file saver. A worker pool with fileWorkers is
// started, it is stopped when ctx is cancelled.
func newFileSaver(ctx context.Context, wg *errgroup.Group, save saveBlobFn, pol chunker.Pol, fileWorkers, blobWorkers uint) *fileSaver {
	ch := make(chan saveFileJob)

	debug.Log("new file saver with %v file workers and %v blob workers", fileWorkers, blobWorkers)

	poolSize := fileWorkers + blobWorkers

	s := &fileSaver{
		saveBlob:     save,
		saveFilePool: newBufferPool(int(poolSize), chunker.MaxSize),
		pol:          pol,
		ch:           ch,

		CompleteBlob: func(uint64) {},
	}

	for i := uint(0); i < fileWorkers; i++ {
		wg.Go(func() error {
			s.worker(ctx, ch)
			return nil
		})
	}

	return s
}

func (s *fileSaver) TriggerShutdown() {
	close(s.ch)
}

// fileCompleteFunc is called when the file has been saved.
type fileCompleteFunc func(*restic.Node, ItemStats)

// Save stores the file f and returns the data once it has been completed. The
// file is closed by Save. completeReading is only called if the file was read
// successfully. complete is always called. If completeReading is called, then
// this will always happen before calling complete.
func (s *fileSaver) Save(ctx context.Context, snPath string, target string, file fs.File, start func(), completeReading func(), complete fileCompleteFunc) futureNode {
	fn, ch := newFutureNode()
	job := saveFileJob{
		snPath: snPath,
		target: target,
		file:   file,
		ch:     ch,

		start:           start,
		completeReading: completeReading,
		complete:        complete,
	}

	select {
	case s.ch <- job:
	case <-ctx.Done():
		debug.Log("not sending job, context is cancelled: %v", ctx.Err())
		_ = file.Close()
		close(ch)
	}

	return fn
}

type saveFileJob struct {
	snPath string
	target string
	file   fs.File
	ch     chan<- futureNodeResult

	start           func()
	completeReading func()
	complete        fileCompleteFunc
}

type CloseAndToNoder interface {
	io.Closer
	ToNoder
}

// saveFile stores the file f in the repo, then closes it.
func (s *fileSaver) saveFile(ctx context.Context, chnker *chunker.Chunker, snPath string, target string, fr fs.File, start func(), finishReading func(), finish func(res futureNodeResult)) {

	fch := &NormalFileChunker{
		s:        s,
		chnker:   chnker,
		f:        fr,
		initDone: false,
	}

	s.saveFileGeneric(ctx, fch, snPath, target, fr, start, finishReading, finish)
}

func (s *fileSaver) SaveFileGeneric(
	ctx context.Context,
	fch filechunker.ChunkerI,
	snPath string,
	target string,
	f CloseAndToNoder,
	start func(),
	finishReading func(),
	finish func(snPath, target string, stats ItemStats, err error),
) {
	s.saveFileGeneric(ctx, fch, snPath, target, f, start, finishReading, func(res futureNodeResult) {
		finish(res.snPath, res.target, res.stats, res.err)
	})
}

func (s *fileSaver) saveFileGeneric(ctx context.Context, fch filechunker.ChunkerI, snPath string, target string, f CloseAndToNoder, start func(), finishReading func(), finish func(res futureNodeResult)) {

	start()

	fnr := futureNodeResult{
		snPath: snPath,
		target: target,
	}
	var lock sync.Mutex
	// require one additional completeFuture() call to ensure that the future only completes
	// after reaching the end of this method
	remaining := 1
	isCompleted := false

	completeBlob := func() {
		lock.Lock()
		defer lock.Unlock()

		remaining--
		if remaining == 0 && fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			for _, id := range fnr.node.Content {
				if id.IsNull() {
					fnr.err = ErrFileCompletedWithNullID
					break
				}
			}
			isCompleted = true
			finish(fnr)
		}
	}
	completeError := func(err error) {
		lock.Lock()
		defer lock.Unlock()

		if fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			isCompleted = true
			fnr.err = fmt.Errorf("failed to save %v: %w", target, err)
			fnr.node = nil
			fnr.stats = ItemStats{}
			finish(fnr)
		}
	}

	debug.Log("%v", snPath)

	node, err := s.NodeFromFileInfo(snPath, target, f, false)
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	if node.Type != restic.NodeTypeFile {
		_ = f.Close()
		completeError(errors.Errorf("node type %q is wrong", node.Type))
		return
	}

	node.Content = []restic.ID{}
	node.Size = 0
	var idx int
	for {
		chunk, err := fch.Next()

		if err == io.EOF {
			break
		}

		node.Size += chunk.Size()

		if err != nil {
			_ = f.Close()
			completeError(err)
			return
		}
		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = f.Close()
			completeError(ctx.Err())
			return
		}

		// add a place to store the saveBlob result
		pos := idx

		lock.Lock()
		node.Content = append(node.Content, restic.ID{})
		remaining += 1
		lock.Unlock()

		s.saveBlob(ctx, restic.DataBlob, chunk, target, func(sbr saveBlobResponse) {

			defer completeBlob()
			lock.Lock()
			defer lock.Unlock()

			if sbr.err != nil {
				node.Content[pos] = restic.ID{}
				return
			}

			if !sbr.known {
				fnr.stats.DataBlobs++
				fnr.stats.DataSize += uint64(sbr.length)
				fnr.stats.DataSizeInRepo += uint64(sbr.sizeInRepo)
			}

			node.Content[pos] = sbr.id
		})
		idx++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = f.Close()
			completeError(ctx.Err())
			return
		}

		s.CompleteBlob(chunk.Size())
	}

	err = f.Close()
	if err != nil {
		completeError(err)
		return
	}

	fnr.node = node
	finishReading()
	completeBlob()
}

type NormalFileChunker struct {
	s        *fileSaver
	chnker   *chunker.Chunker
	f        fs.File
	initDone bool
}

func (c *NormalFileChunker) Next() (filechunker.ChunkI, error) {

	if !c.initDone {
		c.initDone = true
		// reuse the chunker
		c.chnker.Reset(c.f, c.s.pol)
	}

	buf := c.s.saveFilePool.Get()
	chunk, err := c.chnker.Next(buf.Data)

	if err == io.EOF {
		buf.Release()
	}

	if err != io.EOF {
		buf.Data = chunk.Data
	}

	return filechunker.NewRawDataChunk(chunk.Data), err
}

func (s *fileSaver) worker(ctx context.Context, jobs <-chan saveFileJob) {
	// a worker has one chunker which is reused for each file (because it contains a rather large buffer)
	chnker := chunker.New(nil, s.pol)

	for {
		var job saveFileJob
		var ok bool
		select {
		case <-ctx.Done():
			return
		case job, ok = <-jobs:
			if !ok {
				return
			}
		}

		s.saveFile(ctx, chnker, job.snPath, job.target, job.file, job.start, func() {
			if job.completeReading != nil {
				job.completeReading()
			}
		}, func(res futureNodeResult) {
			if job.complete != nil {
				job.complete(res.node, res.stats)
			}
			job.ch <- res
			close(job.ch)
		})
	}
}
