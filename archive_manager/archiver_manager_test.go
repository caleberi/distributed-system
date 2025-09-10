package archivemanager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/common"
	filesystem "github.com/caleberi/distributed-system/file_system"
	"github.com/stretchr/testify/assert"
)

const (
	dataFile = "./source.txt"
	zipExt   = ".gz"
	timeout  = 5 * time.Second
)

var (
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

func testSetup(t *testing.T) (*filesystem.FileSystem, *ArchiverManager, []byte, string) {
	t.Helper()

	data, err := os.ReadFile(dataFile)
	assert.NoError(t, err)

	tempDir := t.TempDir()
	fsys := filesystem.NewFileSystem(tempDir)

	for _, dir := range dirpaths {
		assert.NoErrorf(t, fsys.MkDir(dir), "failed to create directory %s", dir)
	}

	for _, path := range filepaths {
		assert.NoErrorf(t, fsys.CreateFile(path), "failed to create file %s: %v", path, err)
	}

	for _, path := range filepaths {
		f, err := fsys.GetFile(path, os.O_RDWR|os.O_TRUNC, 0644)
		assert.NoError(t, err, "failed to retrieve file %s: %v", path, err)
		n, err := f.Write(data)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, n, 0)
		assert.NoError(t, f.Close(), "failed to close file %s: %v", path, err)
	}

	archiver := NewArchiver(fsys, 2)
	return fsys, archiver, data, tempDir
}

func TestMain(m *testing.M) {
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(dataFile), 0755)
		os.WriteFile(dataFile, []byte("test data for compression"), 0644)
	}
	os.Exit(m.Run())
}

type pathStat struct {
	size int64
	path string
}

func TestArchiveCompressWithFileSystem(t *testing.T) {
	fsys, archiver, _, _ := testSetup(t)
	defer archiver.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex

	pathStats := make(map[common.Path]pathStat)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, path := range filepaths {
			p := common.Path(path)
			info, err := fsys.GetStat(path)
			assert.NoError(t, err)
			if info != nil {
				mu.Lock()
				pathStats[p] = pathStat{size: info.Size(), path: path}
				mu.Unlock()
			}
			assert.NoError(t, archiver.SubmitCompress(p))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range filepaths {
			select {
			case result := <-archiver.CompressPipeline.Result:
				assert.NoError(t, result.Err)
				assert.NotEmpty(t, result.Path)
				info, err := fsys.GetStat(string(result.Path))
				assert.NoError(t, err)
				assert.True(t, info.Size() >= 0)
				assert.True(t, strings.HasSuffix(string(result.Path), zipExt))
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
}

func TestArchiveDecompressionWithFileSystem(t *testing.T) {
	fsys, archiver, _, _ := testSetup(t)
	defer archiver.Close()

	for _, path := range filepaths {
		assert.NoError(t, archiver.SubmitCompress(common.Path(path)))
	}

	go func() {
		for result := range archiver.CompressPipeline.Result {
			assert.NoError(t, result.Err)
			info, err := fsys.GetStat(string(result.Path))
			assert.NoError(t, err)
			assert.True(t, info.Mode().IsRegular())
			assert.NoError(t, archiver.SubmitDecompress(result.Path))
		}
	}()

	go func() {
		for result := range archiver.DecompressPipeline.Result {
			assert.NoError(t, result.Err)
			assert.True(t, !strings.HasSuffix(string(result.Path), zipExt))
			info, err := fsys.GetStat(string(result.Path))
			assert.NoError(t, err)
			assert.True(t, info.Mode().IsRegular())
			f, err := fsys.GetFile(string(result.Path), os.O_RDONLY, 0644)
			assert.NoError(t, err)
			assert.NoError(t, f.Close())
		}
	}()
}

func TestNormalCompression(t *testing.T) {
	fsys, archiver, originalData, _ := testSetup(t)

	tests := []struct {
		name      string
		path      string
		content   []byte
		expectErr bool
	}{
		{
			name:      "valid file",
			path:      "test.txt",
			content:   originalData,
			expectErr: false,
		},
		{
			name:      "empty file",
			path:      "empty.txt",
			content:   []byte{},
			expectErr: false,
		},
		{
			name:      "non-existent file",
			path:      "nonexistent.txt",
			content:   nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.content != nil {
				err := fsys.CreateFile(tt.path)
				assert.NoError(t, err)
				f, err := fsys.GetFile(tt.path, os.O_RDWR|os.O_TRUNC, 0644)
				assert.NoError(t, err)
				_, err = f.Write(tt.content)
				assert.NoError(t, err)
				assert.NoError(t, f.Close())
			}

			newPath, err := archiver.compress(common.Path(tt.path))
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.True(t, strings.HasSuffix(newPath, zipExt))
			info, err := fsys.GetStat(newPath)
			assert.NoError(t, err)
			assert.False(t, info.Size() == 0)
		})
	}
}
