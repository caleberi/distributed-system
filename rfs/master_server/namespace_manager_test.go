package master_server

import (
	"testing"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/stretchr/testify/assert"
)

func treesAreEqual(tree1, tree2 *nsTree) bool {
	if tree1 == nil && tree2 == nil {
		return true
	}
	if tree1 == nil || tree2 == nil {
		return false
	}

	if tree1.Path != tree2.Path || tree1.isDir != tree2.isDir || tree1.chunks != tree2.chunks {
		return false
	}

	if tree1.isDir {
		if len(tree1.childrenNodes) != len(tree2.childrenNodes) {
			return false
		}
		for k, v := range tree1.childrenNodes {
			if _, ok := tree2.childrenNodes[k]; !ok || !treesAreEqual(v, tree2.childrenNodes[k]) {
				return false
			}
		}
	}
	return true
}

func TestMkDir(t *testing.T) {
	nm := NewNameSpaceManager(3 * time.Second)
	defer nm.Shutdown()

	type testcase struct {
		path        string
		shouldThrow bool
	}

	testCases := []testcase{
		{path: "/home", shouldThrow: false},
		{path: "/everest", shouldThrow: false},
		{path: "/home/usr/lib", shouldThrow: true},
		{path: "/home/bin", shouldThrow: false},
	}

	for _, tc := range testCases {
		err := nm.MkDir(common.Path(tc.path))
		if tc.shouldThrow {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
	}
}

func TestCreateFile(t *testing.T) {
	nm := NewNameSpaceManager(3 * time.Second)
	defer nm.Shutdown()
	err := nm.MkDirAll(common.Path("/home/usr"))
	assert.NoError(t, err)
	err = nm.MkDir(common.Path("/home/usr"))
	assert.NoError(t, err)
	err = nm.Create("/home/usr/crab.txt")
	assert.NoError(t, err)
}

func TestDeleteFile(t *testing.T) {
	nm := NewNameSpaceManager(3 * time.Second)
	err := nm.MkDir(common.Path("/home"))
	assert.NoError(t, err)
	err = nm.MkDir(common.Path("/home/usr"))
	assert.NoError(t, err)
	err = nm.Create("/home/usr/crab.txt")
	assert.NoError(t, err)
	err = nm.Delete("/home/usr/crab.txt")
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	defer nm.Shutdown()
}

func TestMkDirAll(t *testing.T) {
	nm := NewNameSpaceManager(3 * time.Second)
	err := nm.MkDirAll(common.Path("/home/usr"))
	assert.NoError(t, err)
	err = nm.MkDirAll(common.Path("/home/usr/local"))
	assert.NoError(t, err)
	err = nm.Create(common.Path("/home/usr/local/foo.go"))
	assert.NoError(t, err)
	err = nm.Create(common.Path("/home/usr/local/crab.txt"))
	assert.NoError(t, err)
}
func TestRenameFile(t *testing.T) {
	nm := NewNameSpaceManager(3 * time.Second)
	defer nm.Shutdown()
	err := nm.MkDir(common.Path("/home"))
	assert.NoError(t, err)
	err = nm.MkDir(common.Path("/home/usr")) // TODO: use recursuve dir creation
	assert.NoError(t, err)
	err = nm.Rename(common.Path("/home/usr"), common.Path("/home/local"))
	assert.NoError(t, err)
	err = nm.Create("/home/local/crab.txt")
	assert.NoError(t, err)
}

func TestSerializeDeserialize(t *testing.T) {

	nm := NewNameSpaceManager(time.Second)
	defer nm.Shutdown()

	// Populate the namespaceManager with some data (directories, files, etc.)
	err := nm.MkDir(common.Path("/home"))
	assert.NoError(t, err)
	err = nm.MkDir(common.Path("/home/usr"))
	assert.NoError(t, err)
	err = nm.Create("/home/usr/crab.txt")
	assert.NoError(t, err)
	err = nm.Delete("/home/usr/crab.txt")
	assert.NoError(t, err)

	serializedData := nm.Serialize()
	deserializedTree := nm.Deserialize(serializedData)

	assert.True(t, treesAreEqual(nm.root, deserializedTree))
}
