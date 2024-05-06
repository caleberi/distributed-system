package filesystem

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const FilePerm = 0755

type FileSystem struct {
	// synchronize file mutation
	mu sync.Mutex
	//  defines the base path url of the file system
	root string
}

func NewFileSystem(root string) *FileSystem {
	return &FileSystem{
		root: root,
	}
}

func getFileInfo(p string) (fs.FileInfo, error) {
	info, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func removeDir(path string) error {
	dirpath := path
	fileSystem := os.DirFS(path)
	return fs.WalkDir(fileSystem, ".",
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type().IsRegular() {
				path = filepath.Join(dirpath, path)
				return os.Remove(path) // delete the file
			}
			return nil
		})
}

func (fs *FileSystem) RenameFile(oldPath string, newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	_, err := os.Stat(oldPath)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New("oldPath does not exist")
		}
		return err
	}

	_, err = os.Stat(newPath)
	if err == nil {
		return errors.New("newPath already exists")
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	return nil
}

func (fs *FileSystem) MkDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = filepath.Clean(filepath.Join(fs.root, path))

	err := os.MkdirAll(path, 0777)
	if err != nil {
		return err
	}

	return nil
}

func (fs *FileSystem) RemoveDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	defer func() {
		if err := recover(); err != nil {
			e := err.(error)
			log.Fatalf("error :: %v", errors.Unwrap(e))
		}
	}()
	path = filepath.Join(fs.root, path)
	info, err := getFileInfo(path)
	if err != nil {
		return err
	}
	if !info.IsDir() { // directory exist before (No need for creation)
		return errors.New("provided path is not a directory")
	}
	// recursively clean the directory
	err = removeDir(path)
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func (fs *FileSystem) GetStat(path string) (fs.FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	path = filepath.Join(fs.root, path)
	info, err := getFileInfo(path)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("path [%s] already exist", err.Error())
		}
		return nil, err
	}
	return info, nil
}

func (fs *FileSystem) Rename(oldPath, newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldPath = filepath.Clean(oldPath)
	newPath = filepath.Clean(newPath)
	oldPath = filepath.Join(fs.root, oldPath)
	newPath = filepath.Join(fs.root, newPath)
	_, err := getFileInfo(oldPath)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("path [%s] already exist", err.Error())
		}
	}
	err = os.Rename(oldPath, newPath)
	if err != nil {
		return err
	}
	return nil
}

func (fs *FileSystem) CreateFile(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	path = filepath.Clean(path)
	path = filepath.Join(fs.root, path)
	_, err := getFileInfo(path)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("path [%s] already exist", err.Error())
		}
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("error occurrd closing file after create")
		}
	}(file)
	return nil
}

func (fs *FileSystem) GetFile(path string, flag int, mode os.FileMode) (*os.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	path = filepath.Join(fs.root, path)
	info, err := getFileInfo(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a file", path)
	}
	file, err := os.OpenFile(path, flag, mode)
	if err != nil {
		log.Printf(" OpenFile err: %s", err)
		return nil, err
	}
	return file, nil
}

func (fs *FileSystem) RemoveFile(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	path = filepath.Join(fs.root, path)
	info, err := getFileInfo(path)
	if err != nil {
		return err
	}
	if info.Mode().IsRegular() {
		return os.Remove(path)
	}
	return nil
}
