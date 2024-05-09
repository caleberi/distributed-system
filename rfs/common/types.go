package common

type MutationType int
type Offset int64
type ChunkVersion int64
type ChunkHandle int64
type ChunkIndex int64
type ServerAddr string
type Checksum string
type Event string
type Path string
type ErrorCode int

const (
	Success = iota
	UnknownError
	Timeout
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
	DownloadBufferMiss
)

type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

type PathInfo struct {
	Path   string // path to file/dirname
	Name   string // filename or dirname
	IsDir  bool   // register as a directory if the path is not the basefile
	Length int64
	Chunk  int64
}

type BufferId struct {
	Handle    ChunkHandle
	Timestamp int
}

type MachineInfo struct {
	RoundTripProximityTime float64
	Hostname               string
}

type PersistedChunkInfo struct {
	Handle   ChunkHandle
	Version  ChunkVersion
	Length   Offset
	Checksum Checksum
}
type Memory struct {
	Alloc      uint64
	TotalAlloc uint64
	Sys        uint64
	NumGC      uint64
}

type FileInfo struct {
	IsDir  bool
	Length int64
	Chunks int64
}
