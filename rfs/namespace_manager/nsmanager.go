package namespacemanager

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type SerializedNsTreeNode struct {
	IsDir    bool
	Path     common.Path
	Children map[string]int
	Chunks   int64
}

type NsTree struct {
	sync.RWMutex // for locking mechanism on file namespace
	Path         common.Path

	// file related
	Length int64 // the length of a current file
	Chunks int64 // monitor chunk per level in tree representation

	// directory related
	IsDir         bool               // register as a directory if the path is not the basefile
	childrenNodes map[string]*NsTree // child subfolder /folder
}

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
	root *NsTree
	// in order to figure out when to serialize for persistence
	serializationCount int
	// find out if the right number of trees node were deserialized
	deserializationCount int
	cleanUpInterval      time.Duration
	deleteCache          map[string]struct{}
	shutdown             chan bool
}

func NewNameSpaceManager(cleanup time.Duration) *NamespaceManager {
	nm := &NamespaceManager{
		root: &NsTree{
			IsDir:         true,
			Path:          "*",
			childrenNodes: make(map[string]*NsTree),
		},
		cleanUpInterval: cleanup,
		deleteCache:     make(map[string]struct{}),
		shutdown:        make(chan bool),
	}

	go func(nm *NamespaceManager) {
		cleanup := time.NewTicker(nm.cleanUpInterval)
		for {
			select {
			case <-nm.shutdown:
				cleanup.Stop()
				return
			case <-cleanup.C:
				queue := utils.Deque[*NsTree]{}
				queue.PushBack(nm.root)
				for queue.Length() != 0 {
					curr := queue.PopFront() // BFS
					if curr == nil {         // defensive
						continue
					}
					pf, _, err := nm.lockParents(curr.Path, true)
					if err != nil {
						log.Err(err).Stack().Msg(err.Error())
						continue
					}
					for key, node := range curr.childrenNodes {
						if strings.HasPrefix(key, common.DeletedNamespaceFilePrefix) { // delete directory or file
							log.Info().Msg(fmt.Sprintf("clean up ntree: %v", key))

							delete(curr.childrenNodes, key)
						} else {
							queue.PushBack(node)
						}
					}
					nm.unlockParents(pf, true)
				}
			}
		}
	}(nm)
	return nm
}

func (nm *NamespaceManager) Shutdown() {
	nm.shutdown <- true
}

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

func (nm *NamespaceManager) Deserialize(nodes []SerializedNsTreeNode) *NsTree {
	nm.root.RLock()
	defer nm.root.RUnlock()
	nm.root = nm.SliceToNsTree(nodes, len(nodes)-1)
	return nm.root
}

// Serialize helps to create a storable tree data structure on disk with gob
// package
func (nm *NamespaceManager) Serialize() []SerializedNsTreeNode {
	nm.root.RLock()
	defer nm.root.RUnlock()

	nm.serializationCount = 0
	ret := []SerializedNsTreeNode{}
	nm.nsTreeToSlice(&ret, nm.root)
	return ret
}

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

func (nm *NamespaceManager) lockParents(p common.Path, lock bool) ([]string, *NsTree, error) {
	pf := strings.Split(string(p), "/")
	return lockParentHelper(nm.root, pf, lock)
}

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
			log.Info().Msg("current path -> " + currPath)
			return err
		}
	}
	return nil
}

func (nm *NamespaceManager) Get(p common.Path) (*NsTree, error) {
	dirpath, filenameOrDirname := nm.RetrievePartitionFromPath(p)
	log.Info().Msg(fmt.Sprintf("dirpath [%v] & filenameOrDirname [%v]", dirpath, filenameOrDirname))
	parents, cwd, err := nm.lockParents(common.Path(dirpath), false)
	defer nm.unlockParents(parents, false)
	if err != nil {
		return nil, err
	}
	log.Info().Msg(fmt.Sprintf("cwd [%v] & parents [%v]", cwd, parents))
	if dir, ok := cwd.childrenNodes[filenameOrDirname]; ok {
		return dir, nil
	}
	return nil, fmt.Errorf("node with path %s does not exist for %s", dirpath, filenameOrDirname)
}

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

func (nm *NamespaceManager) RetrievePartitionFromPath(p common.Path) (string, string) {
	ps := string(p)
	pf := strings.Split(ps, "/")
	pf1, pf2 := pf[:len(pf)-1], pf[len(pf)-1]
	return strings.Join(pf1, "/"), pf2
}
