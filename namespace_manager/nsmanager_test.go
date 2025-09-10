package namespacemanager

import (
	"context"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// treesAreEqual compares two NsTree nodes for equality, checking their paths, directory status,
// chunk counts, and child nodes recursively.
func treesAreEqual(tree1, tree2 *NsTree) bool {
	if tree1 == nil && tree2 == nil {
		return true
	}
	if tree1 == nil || tree2 == nil {
		return false
	}

	if tree1.Path != tree2.Path || tree1.IsDir != tree2.IsDir || tree1.Chunks != tree2.Chunks {
		return false
	}

	if tree1.IsDir {
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

func TestNamespaceManager(t *testing.T) {
	ctx := context.Background()
	cleanupInterval := 3 * time.Second

	newManager := func() *NamespaceManager {
		nm := NewNameSpaceManager(ctx, cleanupInterval)
		return nm
	}

	t.Run("MkDir", func(t *testing.T) {
		nm := newManager()

		tests := []struct {
			name        string
			path        common.Path
			shouldError bool
		}{
			{
				name:        "Create valid directory",
				path:        "/home",
				shouldError: false,
			},
			{
				name:        "Create another valid directory",
				path:        "/everest",
				shouldError: false,
			},
			{
				name:        "Create nested directory without parent",
				path:        "/home/usr/lib",
				shouldError: true,
			},
			{
				name:        "Create directory in existing parent",
				path:        "/home/bin",
				shouldError: false,
			},
		}

		// Create /home directory first for dependent test cases
		require.NoError(t, nm.MkDir(common.Path("/home")), "Failed to create /home directory")

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := nm.MkDir(tt.path)
				if tt.shouldError {
					assert.Error(t, err, "Expected error for path %s", tt.path)
				} else {
					assert.NoError(t, err, "Unexpected error for path %s", tt.path)
				}
			})
		}
	})

	t.Run("MkDirAll", func(t *testing.T) {
		nm := newManager()

		tests := []struct {
			name        string
			path        common.Path
			shouldError bool
		}{
			{
				name:        "Create nested directory",
				path:        "/home/usr",
				shouldError: false,
			},
			{
				name:        "Create deeper nested directory",
				path:        "/home/usr/local",
				shouldError: false,
			},
			{
				name:        "Recreate existing directory",
				path:        "/home/usr",
				shouldError: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := nm.MkDirAll(tt.path)
				if tt.shouldError {
					assert.Error(t, err, "Expected error for path %s", tt.path)
				} else {
					assert.NoError(t, err, "Unexpected error for path %s", tt.path)
					// Verify directory exists
					node, err := nm.Get(tt.path)
					assert.NoError(t, err, "Failed to get directory %s", tt.path)
					assert.True(t, node.IsDir, "Path %s should be a directory", tt.path)
				}
			})
		}
	})

	t.Run("CreateFile", func(t *testing.T) {
		nm := newManager()

		tests := []struct {
			name        string
			path        common.Path
			setup       func(t *testing.T, nm *NamespaceManager)
			shouldError bool
		}{
			{
				name: "Create file in existing directory",
				path: "/home/usr/crab.txt",
				setup: func(t *testing.T, nm *NamespaceManager) {
					require.NoError(t, nm.MkDirAll(common.Path("/home/usr")), "Failed to create /home/usr")
				},
				shouldError: false,
			},
			{
				name:        "Create file in non-existing directory",
				path:        "/nonexistent/crab.txt",
				setup:       func(t *testing.T, nm *NamespaceManager) {},
				shouldError: true,
			},
			{
				name: "Create existing file",
				path: "/home/usr/crab.txt",
				setup: func(t *testing.T, nm *NamespaceManager) {
					require.NoError(t, nm.MkDirAll(common.Path("/home/usr")), "Failed to create /home/usr")
				},
				shouldError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setup(t, nm)
				err := nm.Create(tt.path)
				if tt.shouldError {
					assert.Error(t, err, "Expected error for path %s", tt.path)
				} else {
					assert.NoError(t, err, "Unexpected error for path %s", tt.path)
					node, err := nm.Get(tt.path)
					assert.NoError(t, err, "Failed to get file %s", tt.path)
					assert.False(t, node.IsDir, "Path %s should be a file", tt.path)
				}
			})
		}
	})

	t.Run("DeleteFile", func(t *testing.T) {
		nm := newManager()

		tests := []struct {
			name        string
			path        common.Path
			setup       func(t *testing.T, nm *NamespaceManager)
			shouldError bool
		}{
			{
				name: "Delete existing file",
				path: "/home/usr/crab.txt",
				setup: func(t *testing.T, nm *NamespaceManager) {
					require.NoError(t, nm.MkDirAll(common.Path("/home/usr")), "Failed to create /home/usr")
					require.NoError(t, nm.Create(common.Path("/home/usr/crab.txt")), "Failed to create crab.txt")
				},
				shouldError: false,
			},
			{
				name:        "Delete non-existing file",
				path:        "/home/usr/nonexistent.txt",
				setup:       func(t *testing.T, nm *NamespaceManager) {},
				shouldError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setup(t, nm)
				err := nm.Delete(tt.path)
				if tt.shouldError {
					assert.Error(t, err, "Expected error for path %s", tt.path)
				} else {
					assert.NoError(t, err, "Unexpected error for path %s", tt.path)
					time.Sleep(cleanupInterval + 1*time.Second)
					_, err := nm.Get(tt.path)
					assert.Error(t, err, "Expected file %s to be deleted", tt.path)
				}
			})
		}
	})

	t.Run("Rename", func(t *testing.T) {
		nm := newManager()

		tests := []struct {
			name        string
			source      common.Path
			target      common.Path
			setup       func(t *testing.T, nm *NamespaceManager)
			shouldError bool
		}{
			{
				name:   "Rename directory",
				source: "/home/usr",
				target: "/home/local",
				setup: func(t *testing.T, nm *NamespaceManager) {
					require.NoError(t, nm.MkDirAll(common.Path("/home/usr")), "Failed to create /home/usr")
				},
				shouldError: false,
			},
			{
				name:   "Rename file",
				source: "/home/local/crab.txt",
				target: "/home/local/renamed.txt",
				setup: func(t *testing.T, nm *NamespaceManager) {
					require.NoError(t, nm.MkDirAll(common.Path("/home/local")), "Failed to create /home/local")
					require.NoError(t, nm.Create(common.Path("/home/local/crab.txt")), "Failed to create crab.txt")
				},
				shouldError: false,
			},
			{
				name:        "Rename non-existing path",
				source:      "/home/nonexistent",
				target:      "/home/renamed",
				setup:       func(t *testing.T, nm *NamespaceManager) {},
				shouldError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setup(t, nm)
				err := nm.Rename(tt.source, tt.target)
				if tt.shouldError {
					assert.Error(t, err, "Expected error for renaming %s to %s", tt.source, tt.target)
				} else {
					assert.NoError(t, err, "Unexpected error for renaming %s to %s", tt.source, tt.target)
					node, err := nm.Get(tt.target)
					assert.NoError(t, err, "Failed to get target %s", tt.target)
					assert.Equal(t, tt.target, node.Path, "Target path mismatch")
					_, err = nm.Get(tt.source)
					assert.Error(t, err, "Expected source %s to be deleted", tt.source)
				}
			})
		}
	})

	t.Run("SerializeDeserialize", func(t *testing.T) {
		nm := newManager()

		require.NoError(t, nm.MkDir(common.Path("/home")), "Failed to create /home")
		require.NoError(t, nm.MkDir(common.Path("/home/usr")), "Failed to create /home/usr")
		require.NoError(t, nm.Create(common.Path("/home/usr/crab.txt")), "Failed to create crab.txt")
		require.NoError(t, nm.Delete(common.Path("/home/usr/crab.txt")), "Failed to delete crab.txt")

		serializedData := nm.Serialize()
		deserializedTree := nm.Deserialize(serializedData)

		assert.True(t, treesAreEqual(nm.root, deserializedTree), "Deserialized tree does not match original")
	})

	t.Run("List", func(t *testing.T) {
		nm := newManager()

		require.NoError(t, nm.MkDirAll(common.Path("/home/usr/local")), "Failed to create /home/usr/local")
		require.NoError(t, nm.Create(common.Path("/home/usr/local/foo.go")), "Failed to create foo.go")
		require.NoError(t, nm.Create(common.Path("/home/usr/local/crab.txt")), "Failed to create crab.txt")

		tests := []struct {
			name        string
			path        common.Path
			expected    []string
			shouldError bool
		}{
			{
				name:     "List root directory",
				path:     "/",
				expected: []string{"/home", "/home/usr", "/home/usr/local", "/home/usr/local/foo.go", "/home/usr/local/crab.txt"},
			},
			{
				name:     "List nested directory",
				path:     "/home/usr/local",
				expected: []string{"/home/usr/local/foo.go", "/home/usr/local/crab.txt"},
			},
			{
				name:        "List non-existing directory",
				path:        "/nonexistent",
				shouldError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				info, err := nm.List(tt.path)
				if tt.shouldError {
					assert.Error(t, err, "Expected error for listing %s", tt.path)
					return
				}
				assert.NoError(t, err, "Unexpected error for listing %s", tt.path)

				// Extract paths from PathInfo
				paths := make([]string, len(info))
				for i, pi := range info {
					paths[i] = pi.Path
				}
				assert.ElementsMatch(t, tt.expected, paths, "Listed paths do not match expected")
			})
		}
	})
}

func TestNewNameSpaceManagerWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	nm := NewNameSpaceManager(ctx, 3*time.Second)

	cancel()
	time.Sleep(100 * time.Millisecond)

	err := nm.MkDir(common.Path("/home"))
	assert.NoError(t, err, "MkDir should succeed despite context cancellation")
}
