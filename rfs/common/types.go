package common

import "time"

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
	Timestamp int64
}

type MachineInfo struct {
	RoundTripProximityTime float64
	Hostname               string
}

type Mutation struct {
	MutationType MutationType // action type for this mutation
	Data         []byte       // the data to be append / written/ deleted from chunk
	Offset       Offset       // takes note of the starting point of a particular mutation
}

type PersistedChunkInfo struct {
	Handle               ChunkHandle
	Version              ChunkVersion
	Length               Offset
	Checksum             Checksum
	Mutations            map[ChunkVersion]Mutation
	Completed, Abandoned bool      // this handle is completed <that it is filled>
	ChunkSize            int64     // Size of the chunk
	CreationTime         time.Time // Creation time of the chunk
	LastModified         time.Time // Last modified time of the chunk
	AccessTime           time.Time // Last access time of the chunk
	Replication          int       // Replication level of the chunk (we need to know the number of replication  that we have )
	ServerIP             string    // IP address of the chunk server
	ServerStatus         int       // Status of the chunk server (last know server )
	MetadataVersion      int       // Version of metadata associated with the chunk
	StatusFlags          []string  // Flags indicating the status of the chunk
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

type Lease struct {
	Handle      ChunkHandle // each lease acquired should be associated to a particular chunkhandle
	Expire      time.Time
	InUse       bool
	Primary     ServerAddr   // the current chunk server (as primary)
	Secondaries []ServerAddr // to notice all secondary  chunk server
}

func (ls *Lease) IsExpired(u time.Time) bool {
	return ls.Expire.Before(u)
}
