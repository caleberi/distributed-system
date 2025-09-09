package namespacemanager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/utils"
)

// SerializedNsTreeNode represents a serialized node in the namespace tree, used for persistence.
type SerializedNsTreeNode struct {
	IsDir    bool           // Indicates if the node is a directory
	Path     common.Path    // The path of the node
	Children map[string]int // Map of child node names to their indices in the serialized slice
	Chunks   int64          // Number of chunks for file nodes
}

// NsTree represents a node in the namespace tree, which can be a file or directory.
type NsTree struct {
	sync.RWMutex             // Embedded mutex for thread-safe access
	Path         common.Path // The path of the node

	// File-related fields
	Length int64 // Length of the file (for file nodes)
	Chunks int64 // Number of chunks in the file (for file nodes)

	// Directory-related fields
	IsDir         bool               // Indicates if the node is a directory
	childrenNodes map[string]*NsTree // Map of child nodes (for directory nodes)
}

// NamespaceManager manages the namespace tree for a distributed file system.
// It supports operations like file/directory creation, deletion, renaming, and listing,
// with thread-safe access using read-write locks. It also handles serialization for persistence
// and periodic cleanup of deleted nodes.
//
// Since the namespace can have many nodes, read-write lock
// objects are allocated lazily and deleted once they are not in
// use. Also, locks are acquired in a consistent total order
// to prevent deadlock: they are first ordered by level in the
// namespace tree and lexicographically within the same level.
//
// The read lock on the directory name suffices to prevent the directory from
// being deleted, renamed, or snapshotted. The write locks on
// file names serialize attempts to create a file with the same (ensure linear-lity)
// name twice.
type NamespaceManager struct {
	root                 *NsTree             // Root of the namespace tree
	serializationCount   int                 // Counter for serialization operations
	deserializationCount int                 // Counter for deserialization operations
	cleanUpInterval      time.Duration       // Interval for periodic cleanup of deleted nodes
	deleteCache          map[string]struct{} // Cache for tracking deleted nodes
	ctx                  context.Context
	cancel               context.CancelFunc
}

// NewNameSpaceManager creates a new NamespaceManager with the specified cleanup interval.
// It initializes the root node as a directory and starts a background goroutine for periodic cleanup.
// The cleanup removes nodes marked as deleted from the namespace tree.
func NewNameSpaceManager(ctx context.Context, cleanUpInterval time.Duration) *NamespaceManager {
	nctx, cancelFunc := context.WithCancel(ctx)
	nm := &NamespaceManager{
		ctx:    nctx,
		cancel: cancelFunc,
		root: &NsTree{
			IsDir:         true,
			Path:          "*",
			childrenNodes: make(map[string]*NsTree),
		},
		cleanUpInterval: cleanUpInterval,
		deleteCache:     make(map[string]struct{}),
	}

	go func(nm *NamespaceManager) {
		cleanup := time.NewTicker(nm.cleanUpInterval)
		for {
			select {
			case <-nm.ctx.Done():
				cleanup.Stop()
				return
			case <-cleanup.C:
				queue := utils.Deque[*NsTree]{}
				queue.PushBack(nm.root)
				for queue.Length() != 0 {
					if curr := queue.PopFront(); curr != nil {
						if pf, _, err := nm.lockParents(curr.Path, true); err == nil {
							for key, node := range curr.childrenNodes {
								if !strings.HasPrefix(key, common.DeletedNamespaceFilePrefix) {
									queue.PushBack(node)
									continue
								}
								delete(curr.childrenNodes, key)
							}
							nm.unlockParents(pf, true)
						}
					}
				}
			}
		}
	}(nm)
	return nm
}

// SliceToNsTree converts a slice of serialized nodes into an NsTree, starting from the specified index.
// It recursively reconstructs the tree structure during deserialization.
func (nm *NamespaceManager) SliceToNsTree(r []SerializedNsTreeNode, id int) *NsTree {

	n := &NsTree{
		Path:   r[id].Path,
		Chunks: r[id].Chunks,
		IsDir:  r[id].IsDir,
	}
	if r[id].IsDir {
		n.childrenNodes = make(map[string]*NsTree)
		for k, child := range r[id].Children {
			n.childrenNodes[k] = nm.SliceToNsTree(r, child)
		}
	} else {
		nextId := id - 1
		if nextId < 0 {
			return n
		}
		parent := nm.SliceToNsTree(r, nextId)
		if parent != nil {
			if parent.childrenNodes != nil {
				parent.childrenNodes[string(r[id].Path)] = n
			} else {
				parent.childrenNodes = make(map[string]*NsTree)
				parent.childrenNodes[string(r[id].Path)] = n
			}

		}
	}
	nm.deserializationCount++
	return n
}

// Deserialize reconstructs the namespace tree from a slice of serialized nodes.
// It locks the root node for thread safety and updates it with the deserialized tree.
func (nm *NamespaceManager) Deserialize(nodes []SerializedNsTreeNode) *NsTree {
	nm.root.RLock()
	defer nm.root.RUnlock()
	nm.root = nm.SliceToNsTree(nodes, len(nodes)-1)
	return nm.root
}

// Serialize converts the namespace tree into a slice of SerializedNsTreeNode for persistence.
// It locks the root node for thread safety and builds the serialized representation.
func (nm *NamespaceManager) Serialize() []SerializedNsTreeNode {
	nm.root.RLock()
	defer nm.root.RUnlock()

	nm.serializationCount = 0
	ret := []SerializedNsTreeNode{}
	nm.nsTreeToSlice(&ret, nm.root)
	return ret
}

// nsTreeToSlice recursively converts an NsTree node into a SerializedNsTreeNode and appends it to the result slice.
// It returns the index of the node in the serialized slice.
func (nm *NamespaceManager) nsTreeToSlice(r *[]SerializedNsTreeNode, node *NsTree) int {
	n := SerializedNsTreeNode{
		IsDir:  node.IsDir,
		Chunks: node.Chunks,
		Path:   node.Path,
	}
	if n.IsDir {
		n.Children = make(map[string]int)
		for k, child := range node.childrenNodes {
			// use the idx to store the location in the list
			n.Children[k] = nm.nsTreeToSlice(r, child)
		}
	}
	*r = append(*r, n)
	ret := nm.serializationCount
	nm.serializationCount++
	return ret
}

// lockParents acquires locks on the parent nodes of the given path.
// If lock is true, it acquires a write lock on the immediate parent; otherwise, it uses read locks.
func (nm *NamespaceManager) lockParents(p common.Path, lock bool) ([]string, *NsTree, error) {
	pf := strings.Split(string(p), "/")
	return lockParentHelper(nm.root, pf, lock)
}

// lockParentHelper is a helper function to traverse and lock parent nodes in the namespace tree.
// It returns the list of parent node names, the target node, and any error encountered.
func lockParentHelper(cwd *NsTree, pf []string, lock bool) ([]string, *NsTree, error) {
	var parents []string
	for i := 1; i < len(pf); i++ {
		if cwd == nil {
			return parents, nil, fmt.Errorf("path %s is not found", strings.Join(pf, "/"))
		}
		if lock && i == len(parents)-1 {
			cwd.Lock()
			parents = append(parents, pf[i-1])
			cwd = cwd.childrenNodes[pf[i]]
		}
		cwd.RLock()
		parents = append(parents, pf[i-1])
		cwd = cwd.childrenNodes[pf[i]]
	}
	if cwd == nil {
		return parents, nil, fmt.Errorf("path %s is not found", strings.Join(pf, "/"))
	}
	cwd.RLock()
	return parents, cwd, nil
}

// unlockParents releases locks on the parent nodes in the namespace tree.
// If lock is true, it releases the write lock on the immediate parent; otherwise, it releases read locks.
func (nm *NamespaceManager) unlockParents(parents []string, lock bool) {
	cwd := nm.root
	for i, parent := range parents {
		cwd = cwd.childrenNodes[parent]
		if cwd == nil {
			return
		}
		if lock && i == len(parents)-1 {
			cwd.Unlock()
		}
		cwd.RUnlock()
	}
}

// Create adds a new file node to the namespace at the specified path.
// It validates the filename and ensures the path does not already exist
func (nm *NamespaceManager) Create(p common.Path) error {
	var (
		filename string
		dirpath  string
	)
	dirpath, filename = nm.RetrievePartitionFromPath(p)
	_, err := utils.ValidateFilenameStr(filename, p)
	if err != nil {
		return err
	}

	pf, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(pf, true) // to ensure unlocking of read locks
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[filename]; ok {
		return fmt.Errorf("path %s exist before", p)
	}

	cwd.childrenNodes[filename] = &NsTree{Path: p}
	return nil
}

// Delete marks a file or directory at the specified path as deleted by prefixing its name.
// The actual removal is handled by the periodic cleanup goroutine.
func (nm *NamespaceManager) Delete(p common.Path) error {
	var (
		filename string
		dirpath  string
	)
	dirpath, filename = nm.RetrievePartitionFromPath(p)
	_, err := utils.ValidateFilenameStr(filename, p)
	if err != nil {
		return err
	}

	pf, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(pf, true) // to ensure unlocking of read locks
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[filename]; !ok {
		return fmt.Errorf("path does not %s exist", p)
	}

	node := cwd.childrenNodes[filename]
	//??
	//namespace is only deleted via go routine after the chunk servers has successfully
	// deleted all associated chunks [ NOPE - I found a workaround check the master removes the file chunk
	//  from the available in-memory map ]
	delete(cwd.childrenNodes, filename)
	deletedNodeKey := fmt.Sprintf("%s%s", common.DeletedNamespaceFilePrefix, filename)
	node.Path = common.Path(deletedNodeKey)
	cwd.childrenNodes[deletedNodeKey] = node

	return nil
}

// MkDir creates a new directory node at the specified path if it does not already exist.
func (nm *NamespaceManager) MkDir(p common.Path) error {
	// each nodetree is a directory
	var (
		dirpath string
		dirname string
	)
	dirpath, dirname = nm.RetrievePartitionFromPath(p)
	parents, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(parents, true)
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[dirname]; ok {
		return nil
	}
	cwd.childrenNodes[dirname] = &NsTree{
		IsDir:         true,
		childrenNodes: make(map[string]*NsTree),
		Path:          p,
	}
	return nil
}

// MkDirAll creates all necessary parent directories for the specified path, similar to `mkdir -p`.
// It ensures the entire path exists by creating directories as needed.
func (nm *NamespaceManager) MkDirAll(p common.Path) error {
	// I don't give a fuck whether it is not optimized lol
	fragments := []string{}
	sfrags := strings.Split(string(p), "/")
	fragments = append(fragments, sfrags[1:]...)
	currPath := "/"
	for idx, fragment := range fragments {
		if idx > 0 {
			currPath += "/"
		}
		currPath += fragment
		_, err := nm.Get(common.Path(currPath))
		if err == nil {
			continue
		}
		err = nm.MkDir(common.Path(currPath))
		if err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves the NsTree node at the specified path, returning an error if it does not exist.
func (nm *NamespaceManager) Get(p common.Path) (*NsTree, error) {
	dirpath, filenameOrDirname := nm.RetrievePartitionFromPath(p)
	parents, cwd, err := nm.lockParents(common.Path(dirpath), false)
	defer nm.unlockParents(parents, false)
	if err != nil {
		return nil, err
	}
	if dir, ok := cwd.childrenNodes[filenameOrDirname]; ok {
		return dir, nil
	}
	return nil, fmt.Errorf("node with path %s does not exist for %s", dirpath, filenameOrDirname)
}

// Rename moves a file or directory from the source path to the target path.
// It updates the node's path and ensures the source exists and the target does not.
func (nm *NamespaceManager) Rename(source, target common.Path) error {
	var (
		srcDirnameOrFilename    string
		srcDirpath              string
		targetDirnameOrFilename string
		targetDirpath           string
	)
	srcDirpath, srcDirnameOrFilename = nm.RetrievePartitionFromPath(source)
	parents, cwd, err := nm.lockParents(common.Path(srcDirpath), true)
	defer nm.unlockParents(parents, true)

	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[srcDirnameOrFilename]; !ok {
		return fmt.Errorf("filename/dirname %s does not seem exist", srcDirnameOrFilename)
	}

	targetDirpath, targetDirnameOrFilename = nm.RetrievePartitionFromPath(target)
	_ = targetDirpath

	node := cwd.childrenNodes[srcDirnameOrFilename]
	delete(cwd.childrenNodes, srcDirnameOrFilename)
	cwd.childrenNodes[targetDirnameOrFilename] = node
	node.Path = target
	return nil
}

// List returns a list of PathInfo for all nodes under the specified path.
// It traverses the directory tree and includes both files and directories.
func (nm *NamespaceManager) List(p common.Path) ([]common.PathInfo, error) {
	var dir *NsTree
	if p == common.Path("/") {
		dir = nm.root
	} else {
		parents, cwd, err := nm.lockParents(p, false)
		defer nm.unlockParents(parents, false)
		if err != nil {
			return nil, err
		}
		dir = cwd
	}

	info := []common.PathInfo{}
	queue := utils.Deque[*NsTree]{}
	queue.PushBack(dir)
	for queue.Length() > 0 {
		current := queue.PopFront()
		for name, child := range current.childrenNodes {
			if child.IsDir && len(child.childrenNodes) != 0 {
				queue.PushBack(child)
			}
			info = append(info, common.PathInfo{
				Path:   string(child.Path),
				IsDir:  child.IsDir,
				Length: child.Length,
				Chunk:  child.Chunks,
				Name:   name,
			})
		}
	}

	return info, nil
}

// RetrievePartitionFromPath splits a path into its directory path and filename or directory name.
func (nm *NamespaceManager) RetrievePartitionFromPath(p common.Path) (string, string) {
	ps := string(p)
	pf := strings.Split(ps, "/")
	pf1, pf2 := pf[:len(pf)-1], pf[len(pf)-1]
	return strings.Join(pf1, "/"), pf2
}
