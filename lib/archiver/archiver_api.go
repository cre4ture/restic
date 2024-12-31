package archiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
type EasyArchiverSnapshotOptions = archiver.SnapshotOptions
type TagLists = restic.TagLists

type BlockUpdateStatus int

const (
	BlockUpdateStatusError BlockUpdateStatus = iota
	BlockUpdateStatusCached
	BlockUpdateStatusDownloaded
)

func GetDefaultEasyArchiverOptions() EasyArchiverOptions {
	return cm_main.GetGlobalOptions()
}

type EasyArchiveReader struct {
	options EasyArchiverOptions
	ctx     context.Context
	repo    *repository.Repository
	unlock  func()
}

type InMemoryTreeNode struct {
	node         *model.Node
	childsLoaded bool
	childs       []*InMemoryTreeNode
}

func (n *InMemoryTreeNode) LoadDirData(ctx context.Context, repo restic.BlobLoader) error {
	if n.node.Type != model.NodeTypeDir {
		return fmt.Errorf("LoadDirData() called on non-dir node")
	}

	if n.node.Subtree.IsNull() {
		// new node
		n.childsLoaded = true
		return nil
	}

	if n.childsLoaded {
		return nil
	}

	tree, err := restic.LoadTree(ctx, repo, *n.node.Subtree)
	if err != nil {
		return err
	}

	for _, node := range tree.Nodes {
		treeNode := &InMemoryTreeNode{
			node: node,
		}
		n.childs = append(n.childs, treeNode)
	}

	n.childsLoaded = true

	return nil
}

func (n *InMemoryTreeNode) SaveDirTree(ctx context.Context, r restic.BlobSaver) (*restic.ID, error) {

	if n.node.Type != model.NodeTypeDir {
		return nil, fmt.Errorf("SaveDirTree() called on non-dir node")
	}

	if !n.childsLoaded {
		// node was not touched - no changes - no need to save
		log.Printf("SaveDirTree(%v, %v, %v) - skip %v", n.node.Name, n.node.Path, n.node.Type, n.node.Subtree)
		return n.node.Subtree, nil
	}

	newTree := &restic.Tree{
		Nodes: make([]*restic.Node, len(n.childs)),
	}
	for i, child := range n.childs {
		if child.node.Type == model.NodeTypeDir {
			_, err := child.SaveDirTree(ctx, r)
			if err != nil {
				return nil, err
			}
		}
		newTree.Nodes[i] = child.node
	}

	log.Printf("SaveDirTree(%v, %v, %v, %v) - before %v", n.node.Name, n.node.Path, n.node.Type, len(newTree.Nodes), n.node.Subtree)

	newTreeID, err := restic.SaveTree(ctx, r, newTree)
	if err != nil {
		return nil, err
	}
	n.node.Subtree = &newTreeID
	log.Printf("SaveDirTree(%v, %v, %v, %v) - after %v", n.node.Name, n.node.Path, n.node.Type, len(newTree.Nodes), n.node.Subtree)
	return &newTreeID, nil
}

type EasyArchiveWriter struct {
	options EasyArchiverOptions
	writer  *archiver.SnapshotWriter
	wg      *errgroup.Group
	unlock  func()

	rootMutex sync.Mutex
	root      *InMemoryTreeNode
}

func InitNewRepository(
	ctx context.Context,
	opts cm_main.InitOptions,
	gopts cm_main.GlobalOptions,
) error {
	cm_main.ResolvePassword(&gopts)
	err := cm_main.RunInit(ctx, opts, gopts)
	if err != nil {
		return err
	}

	return nil
}

func NewEasyArchiveWriter(
	ctx context.Context,
	hostname string,
	targets []string,
	options EasyArchiverOptions,
	workCb func(ctx context.Context, eaw *EasyArchiveWriter) error,
) (*EasyArchiveWriter, error) {

	cm_main.ResolvePassword(&options)

	lockCtx, repo, unlock, err := cm_main.OpenWithAppendLock(ctx, options, false)
	if err != nil {
		return nil, err
	}

	err = repo.LoadIndex(ctx, nil)
	if err != nil {
		return nil, err
	}

	snOptions := EasyArchiverSnapshotOptions{
		Tags:            nil,
		Hostname:        hostname,
		Excludes:        nil,
		BackupStart:     time.Now(),
		Time:            time.Now(),
		ProgramVersion:  "syncthing",
		SkipIfUnchanged: true,
	}

	saveOptions := archiver.Options{}.ApplyDefaults()

	writer := archiver.NewSnapshotWriter(
		lockCtx,
		repo,
		saveOptions,
		snOptions,
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

	snapshotLister, err := restic.MemorizeList(ctx, repo, restic.SnapshotFile)
	if err != nil {
		return nil, err
	}

	eaw.root = &InMemoryTreeNode{
		node: &model.Node{Name: "", Path: "/", Type: model.NodeTypeDir, Size: 0, Subtree: &restic.ID{}},
	}

	sn, _, err := (&restic.SnapshotFilter{
		Hosts: []string{hostname},
		Paths: []string{},
		Tags:  restic.TagLists(nil),
	}).FindLatest(ctx, snapshotLister, repo, "latest")
	if (err != nil) && (!errors.Is(err, restic.ErrNoSnapshotFound)) {
		return nil, err
	}

	if sn != nil {
		eaw.root.node.Subtree = sn.Tree
	}

	ch := make(chan error)

	wg.Go(func() error {

		summary := &archiver.Summary{
			BackupStart: time.Now(),
		}

		writer.StartPackUploader()
		err := writer.StartWorker(func(ctx context.Context, g *errgroup.Group) error {
			ch <- nil
			err := workCb(ctx, eaw)
			if err != nil {
				return err
			}

			cleanupCtx := context.Background()

			var rootTreeID *restic.ID
			func() {
				eaw.rootMutex.Lock()
				defer eaw.rootMutex.Unlock()
				rootTreeID, err = eaw.root.SaveDirTree(cleanupCtx, writer.GetRepo())
			}()
			if err != nil {
				return err
			}

			snap, err := writer.PrepareSnapshot(targets)
			if err != nil {
				return err
			}

			snap.Tree = rootTreeID
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

			_, err = restic.SaveSnapshot(cleanupCtx, repo, snap)
			log.Printf("SaveSnapshot() = %v", err)
			if err != nil {
				return err
			}

			return nil
		})

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
	fileSize            uint64
	hashList            model.IDs
	currentIdx          uint
	blockStatusCb       func(offset uint64, blockSize uint64, status BlockUpdateStatus)
	downloadBlockDataCb DownloadBlockDataCallback
}

// Next implements filechunker.ChunkerI.
func (e *EasyFileChunker) Next() (filechunker.ChunkI, error) {

	if e.currentIdx >= uint(len(e.hashList)) {
		return nil, io.EOF
	}

	b_offset := uint64(e.currentIdx) * e.blockSize
	b_end := b_offset + e.blockSize
	if b_end > e.fileSize {
		b_end = e.fileSize
	}

	nextChunk := &EasyFileChunk{
		offset:              b_offset,
		blockSize:           b_end - b_offset,
		hash:                e.hashList[e.currentIdx],
		blockIdx:            e.currentIdx,
		blockStatusCb:       e.blockStatusCb,
		downloadBlockDataCb: e.downloadBlockDataCb,
		data:                nil,
	}

	e.currentIdx += 1

	return nextChunk, nil
}

type EasyFileChunk struct {
	offset              uint64
	blockSize           uint64
	blockIdx            uint
	hash                [32]byte
	blockStatusCb       func(offset uint64, blockSize uint64, status BlockUpdateStatus)
	downloadBlockDataCb DownloadBlockDataCallback
	data                []byte
}

// Data implements filechunker.ChunkI.
func (e *EasyFileChunk) Data() ([]byte, error) {
	if e.data == nil {
		data, err := e.downloadBlockDataCb(uint64(e.blockIdx), e.hash[:])
		if err != nil {
			return nil, err
		}
		e.data = data
	}
	return e.data, nil
}

// PcHash implements filechunker.ChunkI.
func (e *EasyFileChunk) PcHash() [32]byte {
	return e.hash
}

// Release implements filechunker.ChunkI.
func (e *EasyFileChunk) Release() {
	status := BlockUpdateStatusCached
	if e.data != nil {
		status = BlockUpdateStatusDownloaded
	}
	e.blockStatusCb(e.offset, e.blockSize, status)
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
	blockStatusCb func(offset uint64, blockSize uint64, status BlockUpdateStatus),
	downloadBlockDataCb DownloadBlockDataCallback,
) error {

	_, fileSaver, _ := a.writer.GetSavers()
	fch := &EasyFileChunker{
		blockSize:           blockSize,
		fileSize:            meta.Size,
		hashList:            meta.Content,
		currentIdx:          0,
		blockStatusCb:       blockStatusCb,
		downloadBlockDataCb: downloadBlockDataCb,
	}
	f := &EasyFile{meta: meta}
	ch := make(chan error)
	fileSaver.SaveFileGeneric(ctx, fch, path, path, f, func() {
		// start
	}, func() {
		// completeReading
	}, func(snPath, target string, stats archiver.ItemStats, err error) {
		// finish
		close(ch)
	})

	<-ch

	a.updateTree(ctx, meta)

	return nil
}

func (a *EasyArchiveWriter) updateTree(ctx context.Context, node *model.Node) {

	a.rootMutex.Lock()
	defer a.rootMutex.Unlock()

	log.Printf("updateTree(%v, %v, %v, %v)", node.Name, node.Type, node.Size, node.Path)

	err := a.root.LoadDirData(ctx, a.writer.GetRepo())
	if err != nil {
		log.Printf("updateTree(%v, %v, %v, %v) resetting root - error: %v", node.Name, node.Type, node.Size, node.Path, err)
		a.root = &InMemoryTreeNode{
			node:         &model.Node{Name: "", Path: "/", Type: model.NodeTypeDir, Size: 0, Subtree: &restic.ID{}},
			childsLoaded: true,
			childs:       []*InMemoryTreeNode{},
		}
		return
	}
	currDir := a.root
	log.Printf("updateTree(%v, %v, %v, %v) - parent: %v, %v, %v", node.Name, node.Type, node.Size, node.Path,
		currDir.node.Name, currDir.node.Path, currDir.node.Subtree)

	pathElements := strings.Split(node.Path, "/")
	if pathElements[0] == "" {
		pathElements = pathElements[1:]
	}
	if len(pathElements) >= 1 && pathElements[len(pathElements)-1] == "" {
		pathElements = pathElements[:len(pathElements)-1]
	}
	for _, part := range pathElements {
		pos := slices.IndexFunc(currDir.childs, func(c *InMemoryTreeNode) bool {
			return c.node.Name == part
		})

		if pos >= 0 {
			currDir = currDir.childs[pos]
		} else {
			newDir := &InMemoryTreeNode{
				node: &model.Node{Name: part, Path: filepath.Join(currDir.node.Path, currDir.node.Name), Type: model.NodeTypeDir, Size: 0, Subtree: &restic.ID{}},
			}
			currDir.childs = append(currDir.childs, newDir)
			currDir = newDir
		}
		err = currDir.LoadDirData(ctx, a.writer.GetRepo())
		if err != nil {
			log.Printf("updateTree(%v, %v, %v, %v) resetting currDir - error: %v", currDir.node.Name, currDir.node.Type, currDir.node.Size, currDir.node.Path, err)
			currDir.node = &model.Node{Name: "", Path: "/", Type: model.NodeTypeDir, Size: 0, Subtree: &restic.ID{}}
			currDir.childsLoaded = true
			currDir.childs = []*InMemoryTreeNode{}
			return
		}

		log.Printf("updateTree(%v, %v, %v, %v) - parent: %v, %v, %v", node.Name, node.Type, node.Size, node.Path,
			currDir.node.Name, currDir.node.Path, currDir.node.Subtree)
	}

	// add file to found or created dir
	currDir.childs = append(currDir.childs, &InMemoryTreeNode{
		node: node,
	})
}

func (a *EasyArchiveReader) LoadDataBlob(ctx context.Context, id model.ID) ([]byte, error) {
	return a.repo.LoadBlob(ctx, restic.DataBlob, id, nil)
}

func NewEasyArchiveReader(ctx context.Context, options EasyArchiverOptions) (*EasyArchiveReader, error) {

	cm_main.ResolvePassword(&options)

	ctx, repo, unlock, err := cm_main.OpenWithReadLock(ctx, options, options.NoLock)
	if err != nil {
		return nil, err
	}

	err = repo.LoadIndex(ctx, nil)
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

	processNode := func(nodeId restic.ID, nodepath string, node *restic.Node, err error) error {
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
			if node.Type == restic.NodeTypeDir {
				return walker.ErrSkipNode
			} else {
				return nil
			}
		}
	}

	err = walker.Walk(ctx, a.repo, *sn.Tree, walker.WalkVisitor{
		ProcessNode: processNode,
		LeaveDir: func(path string) error {
			return nil
		},
	})

	if errors.Is(err, ErrFoundFile) {
		err = nil
	} else {
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("file not found")
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

	return buffer[:offset], nil
}
