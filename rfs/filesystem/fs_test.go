package filesystem

import (
	"log"
	"testing"
)

func createTestFS() *FileSystem {
	root := "./fs-test"
	pfs := NewFileSystem(root)
	return pfs
}

var pfs *FileSystem

func init() {
	pfs = createTestFS()
}

func TestCreateDir(t *testing.T) {
	type testDefinition struct {
		name       string
		paths      []string
		buildStubs func(t *testing.T, paths []string)
	}
	testCases := []testDefinition{
		{
			name: "[1].Testing MkDir Valid Paths",
			paths: []string{
				".",
				"",
				"/test1",
				"/test1/app_1",
				"/test1/app_2",
				"/test1/app_1/test2",
				"/test1/app_1/../../7",
				"/test3/.op/../../k",
			},
			buildStubs: func(t *testing.T, paths []string) {
				for _, p := range paths {
					if err := pfs.MkDir(p); err != nil {
						t.Fatalf("%v", err)
					}
				}
			},
		},
		{
			name: "[2]. Test MkDir Error Paths",
			paths: []string{
				"../.",
				"/test3/.op/../../../k",
			},
			buildStubs: func(t *testing.T, paths []string) {
				e0 := 0
				for _, p := range paths {
					if err := pfs.MkDir(p); err != nil {
						log.Printf("%v", err)
						e0++
					}
				}
				if e0 != 0 {
					t.Fatalf("expected count of error to be %d but got %d", len(paths), e0)
				}
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			test.buildStubs(t, test.paths)
		})
	}
}

func TestCreateFile(t *testing.T) {
	paths := []string{
		"/test1.txt",
		"/test1/app_1.txt",
		"/test1/app_1/app_2.txt",
	}
	for _, p := range paths {
		if err := pfs.CreateFile(p); err != nil {
			t.Fatalf("cannot create file : %s got error [%s]", p, err)
		}
	}

}

func TestDeleteFile(t *testing.T) {
	paths := []string{
		"/test1.txt",
		"/test1/app_1.txt",
		"/test1/app_1/app_2.txt",
	}

	for _, p := range paths {
		if err := pfs.RemoveFile(p); err != nil {
			t.Fatalf("cannot remove file : %s got error [%s]", p, err)
		}
	}
}

func TestRemoveDirInFileSystem(t *testing.T) {
	// wait for all other test to run before
	// deleting all files and folder
	if err := pfs.RemoveDir("."); err != nil {
		t.Fatalf("expected no error from pfs.RemoveDir but got %v", err)
	}
}
