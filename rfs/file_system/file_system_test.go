// Package filesystem provides tests for the FileSystem type using the testify package.
package filesystem

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestFS creates a new FileSystem instance with a temporary root directory
// and returns a cleanup function to remove the directory after the test.
// It uses require to fail the test immediately if setup fails.
func setupTestFS(t *testing.T) (*FileSystem, func()) {
	t.Helper()
	root, err := os.MkdirTemp("", "fs-test-*")
	require.NoError(t, err, "failed to create temporary directory")
	fs := NewFileSystem(root)
	return fs, func() {
		require.NoError(t, os.RemoveAll(root), "failed to clean up temporary directory")
	}
}

func TestCreateDir(t *testing.T) {
	type testCase struct {
		name       string
		paths      []string
		shouldFail bool
	}

	tests := []testCase{
		{
			name:       "Valid Paths",
			paths:      []string{".", "", "test1", "test1/app_1", "test1/app_2", "test1/app_1/test2"},
			shouldFail: false,
		},
		{
			name:       "Invalid Paths (Path Traversal)",
			paths:      []string{"../.", "test3/.op/../../../k"},
			shouldFail: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs, cleanup := setupTestFS(t)
			defer cleanup()

			for _, p := range tc.paths {
				err := fs.MkDir(p)
				if tc.shouldFail {
					assert.Error(t, err, "expected error for path %s", p)
					return
				}
				assert.NoError(t, err, "failed to create directory %s", p)
			}
		})
	}
}

func TestCreateFile(t *testing.T) {
	fs, cleanup := setupTestFS(t)
	defer cleanup()

	paths := []string{"test1.txt", "dir1/test2.txt", "dir1/subdir/test3.txt"}
	require.NoError(t, fs.MkDir("dir1/subdir"), "failed to create parent directories")

	for _, p := range paths {
		err := fs.CreateFile(p)
		assert.NoError(t, err, "failed to create file %s", p)
		info, err := fs.GetStat(p)
		assert.NoError(t, err, "failed to stat file %s", p)
		assert.True(t, info.Mode().IsRegular(), "path %s is not a regular file", p)
	}

	err := fs.CreateFile(paths[0])
	log.Printf("%s", err)
	assert.Error(t, err, "expected error when creating existing file %s", paths[0])
}

func TestDeleteFile(t *testing.T) {
	fs, cleanup := setupTestFS(t)
	defer cleanup()

	paths := []string{"test1.txt", "dir1/test2.txt", "dir1/subdir/test3.txt"}
	require.NoError(t, fs.MkDir("dir1/subdir"), "failed to create parent directories")
	for _, p := range paths {
		require.NoError(t, fs.CreateFile(p), "failed to create file %s", p)
	}

	for _, p := range paths {
		err := fs.RemoveFile(p)
		assert.NoError(t, err, "failed to remove file %s", p)
		_, err = fs.GetStat(p)
		assert.Error(t, err, "expected error when stating deleted file %s", p)
	}
	err := fs.RemoveFile("nonexistent.txt")
	assert.Error(t, err, "expected error when deleting nonexistent file")
}

func TestRemoveDir(t *testing.T) {
	fs, cleanup := setupTestFS(t)
	defer cleanup()

	require.NoError(t, fs.MkDir("testdir/subdir"), "failed to create directories")
	require.NoError(t, fs.CreateFile("testdir/file.txt"), "failed to create file")

	err := fs.RemoveDir("testdir")
	assert.NoError(t, err, "failed to remove directory testdir")

	_, err = fs.GetStat("testdir")
	assert.Error(t, err, "expected error when stating deleted directory")
	assert.False(t, os.IsExist(err), "expected not exist error for deleted directory")

	require.NoError(t, fs.CreateFile("file.txt"), "failed to create file")
	err = fs.RemoveDir("file.txt")
	assert.Error(t, err, "expected error when removing non-directory path")
}

func TestRename(t *testing.T) {
	fs, cleanup := setupTestFS(t)
	defer cleanup()

	require.NoError(t, fs.CreateFile("old.txt"), "failed to create file")
	err := fs.Rename("old.txt", "new.txt")
	assert.NoError(t, err, "failed to rename file")

	_, err = fs.GetStat("old.txt")
	assert.Error(t, err, "expected error when stating renamed file")
	assert.False(t, os.IsExist(err), "expected not exist error for old file")

	info, err := fs.GetStat("new.txt")
	assert.NoError(t, err, "failed to stat renamed file")
	assert.True(t, info.Mode().IsRegular(), "renamed path is not a regular file")

	require.NoError(t, fs.MkDir("olddir"), "failed to create directory")
	err = fs.Rename("olddir", "newdir")
	assert.NoError(t, err, "failed to rename directory")

	_, err = fs.GetStat("olddir")
	assert.Error(t, err, "expected error when stating renamed directory")
	assert.False(t, os.IsExist(err), "expected not exist error for old directory")

	info, err = fs.GetStat("newdir")
	assert.NoError(t, err, "failed to stat renamed directory")
	assert.True(t, info.IsDir(), "renamed path is not a directory")

	require.NoError(t, fs.CreateFile("existing.txt"), "failed to create existing file")
	err = fs.Rename("new.txt", "existing.txt")
	assert.NoError(t, err, "expected error when renaming to existing path")

	err = fs.Rename("nonexistent.txt", "newname.txt")
	assert.Error(t, err, "expected error when renaming nonexistent path")
}
