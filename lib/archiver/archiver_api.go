package archiver

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	cm_main "github.com/restic/restic/cmd/restic"
	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/filechunker"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/walker"
	"github.com/restic/restic/lib/model"
	"golang.org/x/sync/errgroup"
)

var ErrNoRepository = cm_main.ErrNoRepository

type EasyArchiverOptions = cm_main.GlobalOptions
type EasyArchiverInitOptions = cm_main.InitOptions
type TagLists = restic.TagLists

func GetDefaultEasyArchiverOptions() EasyArchiverOptions {
	return cm_main.GetGlobalOptions()
}

type EasyArchiveReader struct {
	options EasyArchiverOptions
	ctx     context.Context
	repo    *repository.Repository
	unlock  func()
}

type EasyArchiveWriter struct {
	options EasyArchiverOptions
	writer  *archiver.SnapshotWriter
	wg      *errgroup.Group
	unlock  func()
	root    *restic.Tree
}

func InitNewRepository(
	ctx context.Context,
	opts cm_main.InitOptions,
	gopts cm_main.GlobalOptions,
) error {
	err := cm_main.RunInit(ctx, opts, gopts)
	if err != nil {
		return err
	}

	return nil
}

func NewEasyArchiveWriter(
	ctx context.Context,
	targets []string,
	options EasyArchiverOptions,
	workCb func(ctx context.Context, eaw *EasyArchiveWriter) error,
) (*EasyArchiveWriter, error) {
	lockCtx, repo, unlock, err := cm_main.OpenWithAppendLock(ctx, options, false)
	if err != nil {
		return nil, err
	}

	saveOptions := archiver.Options{}.ApplyDefaults()

	writer := archiver.NewSnapshotWriter(
		lockCtx,
		repo,
		saveOptions,
		archiver.SnapshotOptions{},
		func(bytes uint64) {},
		func(file string, err error) error { return err },
		func(snPath, filename string, meta archiver.ToNoder, ignoreXattrListError bool) (*restic.Node, error) {
			return meta.ToNode(ignoreXattrListError)
		},
	)

	wg, _ := errgroup.WithContext(lockCtx)

	eaw := &EasyArchiveWriter{
		options: options,
		writer:  writer,
		wg:      wg,
		unlock:  unlock,
		root:    nil,
	}

	ch := make(chan error)

	wg.Go(func() error {

		summary := &archiver.Summary{
			BackupStart: time.Now(),
		}

		writer.StartPackUploader()
		err := writer.StartWorker(func(ctx context.Context, g *errgroup.Group) error {
			ch <- nil
			return workCb(ctx, eaw)
		})

		if err != nil {
			return err
		}

		snap, err := writer.PrepareSnapshot(targets)
		if err != nil {
			return err
		}

		rootTreeID, err := restic.SaveTree(lockCtx, repo, eaw.root)
		if err != nil {
			return err
		}

		snap.Tree = &rootTreeID
		summary.BackupEnd = time.Now()
		snap.Summary = &restic.SnapshotSummary{
			BackupStart: summary.BackupStart,
			BackupEnd:   summary.BackupEnd,

			FilesNew:            summary.Files.New,
			FilesChanged:        summary.Files.Changed,
			FilesUnmodified:     summary.Files.Unchanged,
			DirsNew:             summary.Dirs.New,
			DirsChanged:         summary.Dirs.Changed,
			DirsUnmodified:      summary.Dirs.Unchanged,
			DataBlobs:           summary.ItemStats.DataBlobs,
			TreeBlobs:           summary.ItemStats.TreeBlobs,
			DataAdded:           summary.ItemStats.DataSize + summary.ItemStats.TreeSize,
			DataAddedPacked:     summary.ItemStats.DataSizeInRepo + summary.ItemStats.TreeSizeInRepo,
			TotalFilesProcessed: summary.Files.New + summary.Files.Changed + summary.Files.Unchanged,
			TotalBytesProcessed: summary.ProcessedBytes,
		}

		_, err = restic.SaveSnapshot(lockCtx, repo, snap)
		if err != nil {
			return err
		}

		return nil
	})

	<-ch

	return eaw, nil
}

func (a *EasyArchiveWriter) Close() {
	a.wg.Wait()
	a.unlock()
}

type DownloadBlockDataCallback func(blockIdx uint64, hash []byte) ([]byte, error)

type EasyFileChunker struct {
	blockSize           uint64
	hashList            model.IDs
	currentIdx          uint
	downloadBlockDataCb DownloadBlockDataCallback
}

// Next implements filechunker.ChunkerI.
func (e *EasyFileChunker) Next() (filechunker.ChunkI, error) {

	if e.currentIdx >= uint(len(e.hashList)) {
		return nil, io.EOF
	}

	nextChunk := &EasyFileChunk{
		blockSize:           e.blockSize,
		hash:                e.hashList[e.currentIdx],
		blockIdx:            e.currentIdx,
		downloadBlockDataCb: e.downloadBlockDataCb,
		data:                nil,
	}

	e.currentIdx += 1

	return nextChunk, nil
}

type EasyFileChunk struct {
	blockSize           uint64
	blockIdx            uint
	hash                [32]byte
	downloadBlockDataCb DownloadBlockDataCallback
	data                []byte
}

// Data implements filechunker.ChunkI.
func (e *EasyFileChunk) Data() []byte {
	if e.data == nil {
		data, err := e.downloadBlockDataCb(uint64(e.blockIdx), e.hash[:])
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

func (a *EasyArchiveWriter) UpdateFile(
	ctx context.Context,
	path string,
	meta *model.Node,
	blockSize uint64,
	downloadBlockDataCb DownloadBlockDataCallback,
) error {

	if a.root == nil {
		a.root = &restic.Tree{}
	}

	_, fileSaver, _ := a.writer.GetSavers()
	fch := &EasyFileChunker{
		blockSize:           blockSize,
		hashList:            meta.Content,
		currentIdx:          0,
		downloadBlockDataCb: downloadBlockDataCb,
	}
	f := &EasyFile{meta: meta}
	fileSaver.SaveFileGeneric(ctx, fch, path, path, f, func() {
		// start
	}, func() {
		// completeReading
	}, func(snPath, target string, stats archiver.ItemStats, err error) {
		// finish
	})

	a.root.Nodes = append(a.root.Nodes, meta)

	return nil
}

func (a *EasyArchiveReader) LoadDataBlob(ctx context.Context, id model.ID) ([]byte, error) {
	return a.repo.LoadBlob(ctx, restic.DataBlob, id, nil)
}

func NewEasyArchiveReader(ctx context.Context, options EasyArchiverOptions) (*EasyArchiveReader, error) {
	ctx, repo, unlock, err := cm_main.OpenWithReadLock(ctx, options, options.NoLock)
	if err != nil {
		return nil, err
	}

	return &EasyArchiveReader{
		options: options,
		ctx:     ctx,
		repo:    repo,
		unlock:  unlock,
	}, nil
}

func (a *EasyArchiveReader) Close() {
	a.unlock()
}

func commonPrefixLen(s1, s2 string) uint {
	i := 0
	for i < len(s1) && i < len(s2) && s1[i] == s2[i] {
		i++
	}
	return uint(i)
}

func (a *EasyArchiveReader) ReadFile(
	ctx context.Context,
	Hosts []string,
	Tags TagLists,
	Paths []string,
	SnapshotID string,
	Filename string,
) ([]byte, error) {

	snapshotLister, err := restic.MemorizeList(ctx, a.repo, restic.SnapshotFile)
	if err != nil {
		return nil, err
	}

	sn, _, err := (&restic.SnapshotFilter{
		Hosts: Hosts,
		Paths: Paths,
		Tags:  restic.TagLists(Tags),
	}).FindLatest(ctx, snapshotLister, a.repo, SnapshotID)
	if err != nil {
		return nil, err
	}

	ErrFoundFile := fmt.Errorf("found file")
	var resultNode *restic.Node = nil

	processNode := func(_ restic.ID, nodepath string, node *restic.Node, err error) error {
		if err != nil {
			return err
		}
		if node == nil {
			return nil
		}

		cpl := commonPrefixLen(nodepath, Filename)
		log.Default().Printf("commonPrefixLen(%v, %v) = %v", nodepath, Filename, cpl)
		if cpl == uint(len(nodepath)) {
			log.Default().Printf("commonPrefixLen(%v, %v) = %v A", nodepath, Filename, cpl)
			if cpl == uint(len(Filename)) {
				log.Default().Printf("commonPrefixLen(%v, %v) = %v B", nodepath, Filename, cpl)
				resultNode = node
				return ErrFoundFile // stop walking
			}
			return nil // continue walking
		} else {
			return walker.ErrSkipNode
		}
	}

	err = walker.Walk(ctx, a.repo, *sn.Tree, walker.WalkVisitor{
		ProcessNode: processNode,
		LeaveDir: func(path string) error {
			return nil
		},
	})

	if err != nil {
		return nil, err
	}

	log.Default().Printf("resultNode: %v", resultNode)

	buffer := make([]byte, resultNode.Size)
	offset := uint64(0)
	for _, id := range resultNode.Content {
		data, err := a.repo.LoadBlob(ctx, restic.DataBlob, id, buffer[offset:])
		log.Default().Printf("LoadBlob(%v) = %v, %v", id, len(data), err)
		if err != nil {
			return nil, err
		}
		if &buffer[offset] != &data[0] { // Check if src and dst are the same
			offset += uint64(copy(buffer[offset:], data))
		} else {
			offset += uint64(len(data)) // Skip copy if they are the same
		}
	}

	return buffer, nil
}
