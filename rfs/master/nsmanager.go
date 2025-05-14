package master_server

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type serializedNsTreeNode struct {
	IsDir    bool
	Path     common.Path
	Children map[string]int
	Chunks   int64
}

type nsTree struct {
	sync.RWMutex // for locking mechanism on file namespace
	Path         common.Path
	// directory related
	isDir         bool               // register as a directory if the path is not the basefile
	childrenNodes map[string]*nsTree // child subfolder /folder

	// file related
	length int64 // the length of a current file
	chunks int64 // monitor chunk per level in tree representation
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
type namespaceManager struct {
	root *nsTree
	// in order to figure out when to serialize for persistence
	serializationCount int
	// find out if the right number of trees node were deserialized
	deserializationCount int
	cleanUpInterval      time.Duration
	deleteCache          map[string]struct{}
	shutdown             chan bool
}

func NewNameSpaceManager(cleanup time.Duration) *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{
			isDir:         true,
			Path:          "*",
			childrenNodes: make(map[string]*nsTree),
		},
		cleanUpInterval: cleanup,
		deleteCache:     make(map[string]struct{}),
		shutdown:        make(chan bool),
	}

	go func(nm *namespaceManager) {
		cleanup := time.NewTicker(nm.cleanUpInterval)
		for {
			select {
			case <-nm.shutdown:
				cleanup.Stop()
				return
			case <-cleanup.C:
				queue := utils.Deque[*nsTree]{}
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

func (nm *namespaceManager) Shutdown() {
	nm.shutdown <- true
}

func (nm *namespaceManager) SliceToNsTree(r []serializedNsTreeNode, id int) *nsTree {

	n := &nsTree{
		Path:   r[id].Path,
		isDir:  r[id].IsDir,
		chunks: r[id].Chunks,
	}
	if r[id].IsDir {
		n.childrenNodes = make(map[string]*nsTree)
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
				parent.childrenNodes = make(map[string]*nsTree)
				parent.childrenNodes[string(r[id].Path)] = n
			}

		}
	}
	nm.deserializationCount++
	return n
}

func (nm *namespaceManager) Deserialize(nodes []serializedNsTreeNode) *nsTree {
	nm.root.RLock()
	defer nm.root.RUnlock()
	nm.root = nm.SliceToNsTree(nodes, len(nodes)-1)
	return nm.root
}

// Serialize helps to create a storable tree data structure on disk with gob
// package
func (nm *namespaceManager) Serialize() []serializedNsTreeNode {
	nm.root.RLock()
	defer nm.root.RUnlock()

	nm.serializationCount = 0
	ret := []serializedNsTreeNode{}
	nm.nsTreeToSlice(&ret, nm.root)
	return ret
}

func (nm *namespaceManager) nsTreeToSlice(r *[]serializedNsTreeNode, node *nsTree) int {
	n := serializedNsTreeNode{
		IsDir:  node.isDir,
		Chunks: node.chunks,
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

func (nm *namespaceManager) lockParents(p common.Path, lock bool) ([]string, *nsTree, error) {
	pf := strings.Split(string(p), "/")
	return lockParentHelper(nm.root, pf, lock)
}

func lockParentHelper(cwd *nsTree, pf []string, lock bool) ([]string, *nsTree, error) {
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

func (nm *namespaceManager) unlockParents(parents []string, lock bool) {
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

func (nm *namespaceManager) Create(p common.Path) error {
	var (
		filename string
		dirpath  string
	)
	dirpath, filename = nm.retrievePartitionFromPath(p)
	_, err := validateFilenameStr(filename, p)
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

	cwd.childrenNodes[filename] = &nsTree{Path: p}
	return nil
}

func (nm *namespaceManager) Delete(p common.Path) error {
	var (
		filename string
		dirpath  string
	)
	dirpath, filename = nm.retrievePartitionFromPath(p)
	_, err := validateFilenameStr(filename, p)
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

func (nm *namespaceManager) MkDir(p common.Path) error {
	// each nodetree is a directory
	var (
		dirpath string
		dirname string
	)
	dirpath, dirname = nm.retrievePartitionFromPath(p)
	parents, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(parents, true)
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[dirname]; ok {
		return nil
	}
	cwd.childrenNodes[dirname] = &nsTree{
		isDir:         true,
		childrenNodes: make(map[string]*nsTree),
		Path:          p,
	}
	return nil
}

func (nm *namespaceManager) MkDirAll(p common.Path) error {
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

func (nm *namespaceManager) Get(p common.Path) (*nsTree, error) {
	dirpath, filenameOrDirname := nm.retrievePartitionFromPath(p)
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

func (nm *namespaceManager) Rename(source, target common.Path) error {
	var (
		srcDirnameOrFilename    string
		srcDirpath              string
		targetDirnameOrFilename string
		targetDirpath           string
	)
	srcDirpath, srcDirnameOrFilename = nm.retrievePartitionFromPath(source)
	parents, cwd, err := nm.lockParents(common.Path(srcDirpath), true)
	defer nm.unlockParents(parents, true)

	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[srcDirnameOrFilename]; !ok {
		return fmt.Errorf("filename/dirname %s does not seem exist", srcDirnameOrFilename)
	}

	targetDirpath, targetDirnameOrFilename = nm.retrievePartitionFromPath(target)
	_ = targetDirpath

	node := cwd.childrenNodes[srcDirnameOrFilename]
	delete(cwd.childrenNodes, srcDirnameOrFilename)
	cwd.childrenNodes[targetDirnameOrFilename] = node
	node.Path = target
	return nil
}

func (nm *namespaceManager) List(p common.Path) ([]common.PathInfo, error) {
	var dir *nsTree
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
	queue := utils.Deque[*nsTree]{}
	queue.PushBack(dir)
	for queue.Length() > 0 {
		current := queue.PopFront()
		for name, child := range current.childrenNodes {
			if child.isDir && len(child.childrenNodes) != 0 {
				queue.PushBack(child)
			}
			info = append(info, common.PathInfo{
				Path:   string(child.Path),
				IsDir:  child.isDir,
				Length: child.length,
				Chunk:  child.chunks,
				Name:   name,
			})
		}
	}

	return info, nil
}

func (nm *namespaceManager) retrievePartitionFromPath(p common.Path) (string, string) {
	ps := string(p)
	pf := strings.Split(ps, "/")
	pf1, pf2 := pf[:len(pf)-1], pf[len(pf)-1]
	return strings.Join(pf1, "/"), pf2
}
