package chunkserver

import (
	"syscall"
	"testing"
	"time"
)

func TestNewChunkServer(t *testing.T) {
	chunkServer := NewChunkServer("127.0.0.1:8080", "www.google.com", "./chunks")
	time.Sleep(time.Second * 20)
	chunkServer.shutdownChan <- syscall.SIGINT
}
