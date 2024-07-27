package common

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	filesystem "github.com/caleberi/distributed-system/rfs/fs"
)

var (
	rootDir   string = "./usr"
	fsys      *filesystem.FileSystem
	archiver  *Archiver
	data      string
	filepaths = []string{
		"test_data2/test_data1/test_1.txt",
		"test_data2/test_1.txt",
		"test_data2/test_data1/test_data3/test_2.txt",
		"test_data1/test_2.txt",
	}
	dirpaths = []string{
		"test_data1",
		"test_data1/test_data",
		"test_data2/test_data1/test_data3",
	}
)

func init() {
	content, err := os.ReadFile("./data/random.txt")
	if err != nil {
		log.Fatal(err)
	}
	data = string(content)
}

func testSetup(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("error occurred  : %s", err)
	}

	fsys = filesystem.NewFileSystem(filepath.Join(cwd, rootDir))

	archiver = NewArchiver(fsys)

	for _, path := range dirpaths {
		if err := fsys.MkDir(path); err != nil {
			t.Fatalf("error occurred  : %s", err)
		}
	}

	for _, path := range filepaths {
		if err := fsys.CreateFile(path); err != nil {
			t.Fatalf("error occurred  : %s", err)
		}
	}

	// write  content into the files
	handler := func(w http.ResponseWriter, _ *http.Request) {
		rd := strings.NewReader(data)
		_, err := io.Copy(w, rd)
		if err != nil {
			return
		}
	}
	req := httptest.NewRequest("GET", "http://example.com", strings.NewReader(data))
	w := httptest.NewRecorder()
	handler(w, req)
	if err != nil {
		t.Fatalf("error occurred  : %s", err)
	}
	resp := w.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("error occurred  : %s", err)
	}

	for _, path := range filepaths {
		f, err := fsys.GetFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			t.Fatalf("error occurred  : %s", err)
		}
		if _, err := f.Write(body); err != nil {
			err := f.Close()
			if err != nil {
				t.Fatalf("error occurred  : %s", err)
			}
			t.Fatalf("error occurred  : %s", err)
		}
		err = f.Close()
		if err != nil {
			t.Fatalf("error occurred  : %s", err)
		}
	}
}

func TestArchiveCompressWithFileSystem(t *testing.T) {
	defer t.Cleanup(func() {
		if err := fsys.RemoveDir("."); err != nil {
			log.Fatalf("error occurred : %s", err)
		}
	})

	errs := make(chan error, len(filepaths))
	testSetup(t)

	pathStats := map[Path]struct {
		size int64
		path string
	}{}

	go func(mp map[Path]struct {
		size int64
		path string
	}) {
		for _, path := range filepaths {
			p := Path(path)
			info, err := fsys.GetStat(path)
			if err != nil {
				errs <- err
			}
			mp[p] = struct {
				size int64
				path string
			}{size: info.Size(), path: path}
			archiver.CompressPipeline.Task <- p
		}
	}(pathStats)

	go func(mp map[Path]struct {
		size int64
		path string
	}) {
		for i := 0; i < len(filepaths); i++ {
			result := <-archiver.CompressPipeline.Result
			if result.Err != nil {
				errs <- result.Err
			}
			newPath := strings.Replace(string(result.Path), ZIP_EXT, "", 1)
			pStat := mp[Path(newPath)]
			oldSize := pStat.size
			info, err := fsys.GetStat(string(result.Path))
			if err != nil {
				errs <- err
			}
			compressedSize := info.Size()
			if compressedSize > oldSize &&
				strings.HasSuffix(string(result.Path), ZIP_EXT) {
				errs <- fmt.Errorf(
					"expected path %s to have a lower size from old size %d  but got %d",
					result.Path, oldSize, compressedSize,
				)
			}
		}
		close(errs)
		archiver.ShutdownChan <- struct{}{}
	}(pathStats)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestArchiveDecompressionWithFileSystem(t *testing.T) {
	defer t.Cleanup(func() {
		if err := fsys.RemoveDir("."); err != nil {
			log.Fatalf("error occurred : %s", err)
		}
	})

	cmperrs := make(chan error, len(filepaths))
	compressedPaths := make([]string, 0)

	var compressionWG sync.WaitGroup
	testSetup(t)

	pathStats := map[Path]struct {
		size int64
		path string
	}{}

	compressionWG.Add(1)
	go func(mp map[Path]struct {
		size int64
		path string
	}) {
		defer compressionWG.Done()
		for _, path := range filepaths {
			p := Path(path)
			info, err := fsys.GetStat(path)
			if err != nil {
				cmperrs <- err
			}
			mp[p] = struct {
				size int64
				path string
			}{size: info.Size(), path: path}
			archiver.CompressPipeline.Task <- p
		}
	}(pathStats)

	compressionWG.Add(1)
	go func(mp map[Path]struct {
		size int64
		path string
	}) {
		defer compressionWG.Done()
		for i := 0; i < len(filepaths); i++ {
			result := <-archiver.CompressPipeline.Result
			if result.Err != nil {
				cmperrs <- result.Err
			}
			compressedPaths = append(compressedPaths, string(result.Path))
			newPath := strings.Replace(string(result.Path), ZIP_EXT, "", 1)
			pStat := mp[Path(newPath)]
			oldSize := pStat.size
			info, err := fsys.GetStat(string(result.Path))
			if err != nil {
				cmperrs <- err
			}
			compressedSize := info.Size()
			if compressedSize >= oldSize {
				cmperrs <- fmt.Errorf(
					"expected path %s to have a lower size from old size %d  but got %d",
					result.Path, oldSize, compressedSize,
				)
			}
		}
		close(cmperrs)
	}(pathStats)

	compressionWG.Wait()

	for err := range cmperrs {
		if err != nil {
			t.Fatal(err)
		}
	}

	dcmperrs := make(chan error, len(compressedPaths))
	var decompressionWG sync.WaitGroup
	decompressionWG.Add(1)
	go func(mp map[Path]struct {
		size int64
		path string
	}) {
		defer decompressionWG.Done()
		for _, path := range compressedPaths {
			p := Path(path)
			info, err := fsys.GetStat(path)
			if err != nil {
				dcmperrs <- err
			}
			mp[p] = struct {
				size int64
				path string
			}{size: info.Size(), path: path}
			archiver.DecompressPipeline.Task <- p
		}
	}(pathStats)

	decompressionWG.Add(1)
	go func(mp map[Path]struct {
		size int64
		path string
	}) {
		defer decompressionWG.Done()
		for i := 0; i < len(compressedPaths); i++ {
			result := <-archiver.DecompressPipeline.Result
			if result.Err != nil {
				dcmperrs <- result.Err
			}
			newPath := result.Path
			pStat := mp[newPath]
			oldSize := pStat.size
			info, err := fsys.GetStat(string(result.Path))
			if err != nil {
				dcmperrs <- err
			}
			decompressedSize := info.Size()
			if decompressedSize > oldSize || strings.HasSuffix(string(result.Path), ZIP_EXT) {
				dcmperrs <- fmt.Errorf(
					"expected path %s to have a higher size from old size %d  but got %d",
					result.Path, oldSize, decompressedSize,
				)
			}
		}
		close(dcmperrs)
		archiver.ShutdownChan <- struct{}{}
	}(pathStats)

	decompressionWG.Wait()
	for err := range dcmperrs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNormalCompression(t *testing.T) {

}
