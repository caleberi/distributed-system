package archivemanager

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/common"
	filesystem "github.com/caleberi/distributed-system/file_system"
)

const ZIP_EXT = ".gz"

type ResultInfo struct {
	Path common.Path
	Err  error
}

type CompressPipeline struct {
	Task   chan common.Path
	Result chan ResultInfo
}

type DecompressPipeline struct {
	Task   chan common.Path
	Result chan ResultInfo
}

type ArchiverManager struct {
	CompressPipeline   CompressPipeline
	DecompressPipeline DecompressPipeline
	fileSystem         *filesystem.FileSystem
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
}

func NewArchiver(fileSystem *filesystem.FileSystem, numWorkers int) *ArchiverManager {
	ctx, cancel := context.WithCancel(context.Background())
	archiver := &ArchiverManager{
		CompressPipeline: CompressPipeline{
			Task:   make(chan common.Path, 10),
			Result: make(chan ResultInfo, 10),
		},
		DecompressPipeline: DecompressPipeline{
			Task:   make(chan common.Path, 10),
			Result: make(chan ResultInfo, 10),
		},
		fileSystem: fileSystem,
		ctx:        ctx,
		cancel:     cancel,
	}
	archiver.startWorkers(numWorkers)
	return archiver
}

func (ac *ArchiverManager) startWorkers(numWorkers int) {
	compressWorkers := numWorkers / 2
	if compressWorkers == 0 {
		compressWorkers = 1
	}
	decompressWorkers := numWorkers - compressWorkers
	if decompressWorkers == 0 {
		decompressWorkers = 1
	}

	ac.wg.Add(compressWorkers)
	for i := 0; i < compressWorkers; i++ {
		go ac.compressWorker()
	}

	ac.wg.Add(decompressWorkers)
	for i := 0; i < decompressWorkers; i++ {
		go ac.decompressWorker()
	}
}

func (ac *ArchiverManager) compressWorker() {
	defer ac.wg.Done()
	for {
		select {
		case <-ac.ctx.Done():
			return
		case p, ok := <-ac.CompressPipeline.Task:
			if !ok {
				return
			}
			np, err := ac.compress(p)
			select {
			case ac.CompressPipeline.Result <- ResultInfo{Path: common.Path(np), Err: err}:
			case <-ac.ctx.Done():
				return
			}
		}
	}
}

func (ac *ArchiverManager) decompressWorker() {
	defer ac.wg.Done()
	for {
		select {
		case <-ac.ctx.Done():
			return
		case p, ok := <-ac.DecompressPipeline.Task:
			if !ok {
				return
			}
			np, err := ac.decompress(p)
			select {
			case ac.DecompressPipeline.Result <- ResultInfo{Path: common.Path(np), Err: err}:
			case <-ac.ctx.Done():
				return
			}
		}
	}
}

func (ac *ArchiverManager) decompress(path common.Path) (string, error) {

	if !strings.HasSuffix(string(path), ZIP_EXT) {
		return "", errors.New("file does not have .gz extension")
	}

	sourceFile, err := ac.fileSystem.GetFile(string(path), os.O_RDWR, 0644)
	if err != nil {
		return "", err
	}
	defer sourceFile.Close()

	decompressor, err := gzip.NewReader(sourceFile)
	if err != nil {
		return "", err
	}
	defer decompressor.Close()

	uncompressedFilePath, err := removeGzipExtension(string(path))
	if err != nil {
		return "", err
	}

	err = ac.fileSystem.CreateFile(uncompressedFilePath)
	if err != nil {
		return "", err
	}

	decompressedFile, err := ac.fileSystem.GetFile(
		uncompressedFilePath, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer decompressedFile.Close()

	_, err = io.Copy(decompressedFile, decompressor)
	if err != nil {
		ac.fileSystem.RemoveFile(string(path))
		return "", err
	}

	stat, err := decompressedFile.Stat()
	if err != nil || stat.Size() == 0 {
		ac.fileSystem.RemoveFile(string(path))
		return "", errors.New("decompressed file is invalid or empty")
	}

	return uncompressedFilePath, ac.fileSystem.RemoveFile(string(path))
}

func (ac *ArchiverManager) compress(path common.Path) (string, error) {
	sourceFile, err := ac.fileSystem.GetFile(string(path), os.O_RDWR, 0644)
	if err != nil {
		return "", err
	}
	defer sourceFile.Close()

	destinationPath := string(path) + ZIP_EXT
	_, err = ac.fileSystem.GetStat(destinationPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := ac.fileSystem.CreateFile(destinationPath)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	compressedFile, err := ac.fileSystem.GetFile(
		destinationPath, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer compressedFile.Close()

	compressor := gzip.NewWriter(compressedFile)
	_, err = io.Copy(compressor, sourceFile)
	if err != nil {
		ac.fileSystem.RemoveFile(destinationPath)
		return "", err
	}

	if err := compressor.Close(); err != nil {
		ac.fileSystem.RemoveFile(destinationPath)
		return "", err
	}

	return destinationPath, ac.fileSystem.RemoveFile(string(path))
}

func (ac *ArchiverManager) Close() {
	defer func() {
		ac.cancel()

		close(ac.CompressPipeline.Task)
		close(ac.DecompressPipeline.Task)

		ac.wg.Wait()

		close(ac.CompressPipeline.Result)
		close(ac.DecompressPipeline.Result)
	}()

	timeout := time.After(10 * time.Second)
	for {
		compressLen := len(ac.CompressPipeline.Task)
		decompressLen := len(ac.DecompressPipeline.Task)
		if compressLen == 0 && decompressLen == 0 {
			break
		}
		select {
		case <-timeout:
			return
		case <-time.After(100 * time.Millisecond):
			continue
		}
	}

}
func (ac *ArchiverManager) SubmitCompress(path common.Path) error {
	select {
	case ac.CompressPipeline.Task <- path:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timeout submitting compression task")
	case <-ac.ctx.Done():
		return errors.New("archiver is shutting down")
	}
}

func (ac *ArchiverManager) SubmitDecompress(path common.Path) error {
	select {
	case ac.DecompressPipeline.Task <- path:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timeout submitting decompression task")
	case <-ac.ctx.Done():
		return errors.New("archiver is shutting down")
	}
}

func removeGzipExtension(path string) (string, error) {
	if !strings.HasSuffix(path, ZIP_EXT) {
		return "", errors.New("file does not have .gz extension")
	}
	return strings.TrimSuffix(path, ZIP_EXT), nil
}
