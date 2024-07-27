package common

import (
	"compress/gzip"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	filesystem "github.com/caleberi/distributed-system/rfs/fs"
)

const ZIP_EXT = ".gz"

type ResultInfo struct {
	Path Path
	Err  error
}
type CompressPipeline struct {
	Task   chan Path
	Result chan ResultInfo
}

type DecompressPipeline struct {
	Task   chan Path
	Result chan ResultInfo
}

// Archiver helps to compress regular file that have not been
// access over a given time span until
type Archiver struct {
	// for signaling file decompression
	DecompressPipeline DecompressPipeline
	// for signaling file compression
	CompressPipeline CompressPipeline
	ShutdownChan     chan struct{}
	fileSystem       *filesystem.FileSystem
	mu               sync.Mutex
}

func NewArchiver(fileSystem *filesystem.FileSystem) *Archiver {
	archiver := &Archiver{
		CompressPipeline: CompressPipeline{
			Task:   make(chan Path, 10),
			Result: make(chan ResultInfo, 10),
		},
		DecompressPipeline: DecompressPipeline{
			Task:   make(chan Path, 10),
			Result: make(chan ResultInfo, 10),
		},
		fileSystem:   fileSystem,
		ShutdownChan: make(chan struct{}, 1),
	}

	go func(archiver *Archiver) {
		for {
			select {
			case <-archiver.ShutdownChan:
				archiver.Close()
				return
			case p := <-archiver.CompressPipeline.Task:
				np, err := archiver.compress(p)
				archiver.CompressPipeline.Result <- ResultInfo{
					Path: Path(np),
					Err:  err,
				}

			case p := <-archiver.DecompressPipeline.Task:
				np, err := archiver.decompress(p)
				archiver.DecompressPipeline.Result <- ResultInfo{
					Path: Path(np),
					Err:  err,
				}
			}
		}
	}(archiver)

	return archiver
}

func (ac *Archiver) decompress(path Path) (string, error) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	compressedFile, err := ac.fileSystem.GetFile(string(path), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return "", err
	}
	defer compressedFile.Close()

	decompressor, err := gzip.NewReader(compressedFile)
	if err != nil {
		return "", err
	}
	defer decompressor.Close()

	newPath := strings.Replace(string(path), ZIP_EXT, "", 1)
	err = ac.fileSystem.CreateFile(newPath)
	if err != nil {
		return "", err
	}

	decompressedFile, err := ac.fileSystem.GetFile(newPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return "", err
	}
	defer decompressedFile.Close()

	_, err = io.Copy(decompressedFile, decompressor)

	if err != nil {
		return "", err
	}

	err = ac.fileSystem.RemoveFile(string(path))

	if err != nil {
		return "", err
	}

	return newPath, err
}

func (ac *Archiver) compress(path Path) (string, error) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	file, err := ac.fileSystem.GetFile(string(path), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		log.Fatalf(" Could not get file err: %s", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return "", err
	}
	if stat.Size() == 0 {
		return "", errors.New("file is empty")
	}

	newPath := string(path) + ZIP_EXT
	err = ac.fileSystem.CreateFile(newPath)
	if err != nil {
		return "", err
	}

	gzippedFile, err := ac.fileSystem.GetFile(newPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}

	defer gzippedFile.Close()

	compressor := gzip.NewWriter(gzippedFile)

	_, err = io.Copy(compressor, file)
	if err != nil {
		return "", err
	}

	err = compressor.Close()
	if err != nil {
		return "", err
	}

	_, err = gzippedFile.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}
	stat, err = gzippedFile.Stat()
	if err != nil {
		return "", err
	}
	if stat.Size() == 0 {
		return "", errors.New("compressed file is empty")
	}

	err = ac.fileSystem.RemoveFile(string(path))

	if err != nil {
		return "", err
	}
	return newPath, err
}

func (ac *Archiver) Close() {
	close(ac.DecompressPipeline.Result)
	close(ac.CompressPipeline.Result)
	close(ac.CompressPipeline.Task)
	close(ac.DecompressPipeline.Task)
}
