// Package filesystem provides a thread-safe interface for managing files and
// directories relative to a specified root path. It supports operations such as
// creating, renaming, removing, and querying files and directories.
package filesystem

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// filePerm defines the default file and directory permissions (owner: rwx, group: rx, others: rx).
const filePerm = 0777

// FileSystem provides thread-safe operations for managing files and directories
// relative to a root path.
type FileSystem struct {
	mu   sync.Mutex // synchronizes file system operations
	root string     // base path for all file operations
}

// NewFileSystem creates a new FileSystem instance with the specified root path.
// The root path defines the base directory for all file operations.
//
// Parameters:
//   - root: The base directory path for file operations.
//
// Returns:
//   - A pointer to a new FileSystem instance.
func NewFileSystem(root string) *FileSystem {
	return &FileSystem{
		root: root,
	}
}

// getFileInfo retrieves file information for the specified path.
//
// Parameters:
//   - p: The path to the file or directory.
//
// Returns:
//   - fs.FileInfo: Information about the file or directory.
//   - error: An error if the path does not exist or cannot be accessed.
func getFileInfo(p string) (fs.FileInfo, error) {
	info, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (fs *FileSystem) restrictToRoot(path string) (string, error) {
	path = filepath.Join(fs.root, filepath.Clean(path))
	if !strings.HasPrefix(path, fs.root) {
		return "", fmt.Errorf("provided path %s is restricted to the filesystem root", path)
	}
	return path, nil
}

// MkDir creates a directory at the specified path relative to the root.
// If the directory already exists, it does nothing. Parent directories are
// created as needed.
//
// Parameters:
//   - path: The relative path of the directory to create.
//
// Returns:
//   - error: An error if the directory cannot be created.
func (fs *FileSystem) MkDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.restrictToRoot(path)
	if err != nil {
		return err
	}

	return os.MkdirAll(path, filePerm)
}

// RemoveDir removes a directory and its contents at the specified path relative
// to the root.
//
// Parameters:
//   - path: The relative path of the directory to remove.
//
// Returns:
//   - error: An error if the path does not exist, is not a directory, or cannot
//     be removed.
func (fs *FileSystem) RemoveDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.restrictToRoot(path)
	if err != nil {
		return err
	}
	info, err := getFileInfo(path)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("provided path %s is not a directory", path)
	}

	return os.RemoveAll(path)
}

// GetStat retrieves file or directory information for the specified path
// relative to the root.
//
// Parameters:
//   - path: The relative path to the file or directory.
//
// Returns:
//   - fs.FileInfo: Information about the file or directory.
//   - error: An error if the path does not exist or cannot be accessed.
func (fs *FileSystem) GetStat(path string) (fs.FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.restrictToRoot(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, err
	}

	return info, nil
}

// Rename renames a file or directory from oldPath to newPath, both relative to
// the root. It fails if the old path does not exist or the new path already
// exists.
//
// Parameters:
//   - oldPath: The relative path of the file or directory to rename.
//   - newPath: The relative path to rename the file or directory to.
//
// Returns:
//   - error: An error if the old path does not exist, the new path already
//     exists, or the rename operation fails.
func (fs *FileSystem) Rename(oldPath, newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	oldPath, err := fs.restrictToRoot(oldPath)
	if err != nil {
		return err
	}

	newPath, err = fs.restrictToRoot(newPath)
	if err != nil {
		return err
	}

	if _, err := getFileInfo(oldPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("old path %s does not exist", oldPath)
		}
		return err
	}

	if _, err := getFileInfo(newPath); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("new path %s already exist", oldPath)
		}
	}

	return os.Rename(oldPath, newPath)
}

// CreateFile creates a new file at the specified path relative to the root. If
// the file already exists, it returns an error.
//
// Parameters:
//   - path: The relative path of the file to create.
//
// Returns:
//   - error: An error if the file already exists or cannot be created.
func (fs *FileSystem) CreateFile(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.restrictToRoot(path)
	if err != nil {
		return err
	}

	info, err := getFileInfo(path)
	if info != nil && info.Mode().IsRegular() {
		return fmt.Errorf("path %s already exists", path)
	} else if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("path %s already exists", path)
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	return file.Close()
}

// GetFile opens a file at the specified path relative to the root with the given
// flag and mode.
//
// Parameters:
//   - path: The relative path of the file to open.
//   - flag: The file open flag (e.g., os.O_RDONLY, os.O_WRONLY).
//   - mode: The file mode (permissions) to use if the file is created.
//
// Returns:
//   - *os.File: A file handle to the opened file.
//   - error: An error if the path does not exist, is not a file, or cannot be
//     opened.
func (fs *FileSystem) GetFile(path string, flag int, mode os.FileMode) (*os.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.restrictToRoot(path)
	if err != nil {
		return nil, err
	}

	info, err := getFileInfo(path)
	if err != nil {
		return nil, err
	}

	if info.Mode().IsDir() {
		return nil, fmt.Errorf("%s is not a file", path)
	}

	file, err := os.OpenFile(path, flag, mode)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", path, err)
	}

	return file, nil
}

// RemoveFile removes a file at the specified path relative to the root.
//
// Parameters:
//   - path: The relative path of the file to remove.
//
// Returns:
//   - error: An error if the path does not exist, is not a file, or cannot be
//     removed.
func (fs *FileSystem) RemoveFile(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.restrictToRoot(path)
	if err != nil {
		return err
	}
	info, err := getFileInfo(path)
	if err != nil {
		return err
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("path %s is not a regular file", path)
	}
	return os.Remove(path)
}
