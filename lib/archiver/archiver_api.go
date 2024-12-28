package archiver

import (
	"context"
	"fmt"

	cm_main "github.com/restic/restic/cmd/restic"
	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/filechunker"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/lib/model"
	"golang.org/x/sync/errgroup"
)

type EasyArchiver struct {
	writer *archiver.SnapshotWriter
	unlock func()
}

func NewEasyArchiver(ctx context.Context, workCb func(context.Context, *errgroup.Group) error) (*EasyArchiver, error) {
	lockCtx, repo, unlock, err := cm_main.OpenWithAppendLock(ctx, cm_main.GlobalOptions{}, false)
	if err != nil {
		return nil, err
	}

	writer := archiver.NewSnapshotWriter(
		lockCtx,
		repo,
		archiver.Options{},
		archiver.SnapshotOptions{},
		func(bytes uint64) {},
		func(file string, err error) error { return err },
		func(snPath, filename string, meta archiver.ToNoder, ignoreXattrListError bool) (*restic.Node, error) {
			return nil, nil
		},
	)

	writer.StartPackUploader()

	go writer.StartWorker(workCb)

	return &EasyArchiver{
		writer: writer,
		unlock: unlock,
	}, nil
}

func (a *EasyArchiver) Close() {
	a.unlock()
}

type EasyFileChunker struct {
	blockSize           uint64
	hashList            [][32]byte
	currentIdx          uint
	downloadBlockDataCb func(hash []byte) ([]byte, error)
}

// Next implements filechunker.ChunkerI.
func (e *EasyFileChunker) Next() (filechunker.ChunkI, error) {
	nextChunk := &EasyFileChunk{
		blockSize:           e.blockSize,
		hash:                e.hashList[e.currentIdx],
		currentIdx:          e.currentIdx,
		downloadBlockDataCb: e.downloadBlockDataCb,
		data:                nil,
	}

	e.currentIdx += 1

	return nextChunk, nil
}

type EasyFileChunk struct {
	blockSize           uint64
	currentIdx          uint
	hash                [32]byte
	downloadBlockDataCb func(hash []byte) ([]byte, error)
	data                []byte
}

// Data implements filechunker.ChunkI.
func (e *EasyFileChunk) Data() []byte {
	if e.data == nil {
		data, err := e.downloadBlockDataCb(e.hash[:])
		if err != nil {
			panic(fmt.Sprintf("EasyFileChunk(): error downloading block data: %v", err))
		}
		e.data = data
	}
	return e.data
}

// PcHash implements filechunker.ChunkI.
func (e *EasyFileChunk) PcHash() [32]byte {
	return e.hash
}

// Release implements filechunker.ChunkI.
func (e *EasyFileChunk) Release() {
	// nothing to do
}

// Size implements filechunker.ChunkI.
func (e *EasyFileChunk) Size() uint64 {
	return e.blockSize
}

type EasyFile struct {
	meta *model.Node
}

// Close implements archiver.CloseAndToNoder.
func (e *EasyFile) Close() error {
	// nothing to do
	return nil
}

// ToNode implements archiver.CloseAndToNoder.
func (e *EasyFile) ToNode(ignoreXattrListError bool) (*restic.Node, error) {
	return e.meta, nil
}

func (a *EasyArchiver) UpdateFile(
	ctx context.Context,
	path string,
	meta *model.Node,
	blockSize uint64,
	hashList [][32]byte,
	downloadBlockDataCb func(hash []byte) ([]byte, error),
) error {
	_, fileSaver, _ := a.writer.GetSavers()
	fch := &EasyFileChunker{
		blockSize:  blockSize,
		hashList:   hashList,
		currentIdx: 0,
	}
	f := &EasyFile{meta: meta}
	fileSaver.SaveFileGeneric(ctx, fch, path, path, f, func() {
		// start
	}, func() {
		// completeReading
	}, func(snPath, target string, stats archiver.ItemStats, err error) {
		// finish
	})

	return nil
}
