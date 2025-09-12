package common

import "time"

type BranchInfo struct {
	Err   error
	Event string
}

const (
	HeartBeat         Event = "HeartBeat"
	GarbageCollection Event = "GarbageCollection"
	PersistMetaData   Event = "PersistMetaData"
	PersistOpsLog     Event = "PersistOpsLog"
	MasterHeartBeat   Event = "MasterHeartBeat"
	Archival          Event = "Archival"
)

const (
	DeletedNamespaceFilePrefix string = "___deleted__"

	// archiving mechanism
	ArchivalDaySpan                    = 5
	ArchiveChunkInterval time.Duration = ArchivalDaySpan * 24 * time.Hour

	// failure dectection
	FailureDetectorKeyExipiryTime time.Duration = 5 * time.Minute

	// chunk server
	HeartBeatInterval         time.Duration = 5 * time.Second
	GarbageCollectionInterval time.Duration = 5 * time.Minute
	PersistMetaDataInterval   time.Duration = 10 * time.Minute

	// chunk file information
	ChunkMetaDataFileName = "chunk.server.meta"
	ChunkFileNameFormat   = "chunk-%v.chk"
	ChunkMaxSizeInMb      = 64
	ChunkMaxSizeInByte    = 64 << 20 // 1024 * 1024 * 64
	AppendMaxSizeInByte   = ChunkMaxSizeInByte / 4

	// downloadbuffer
	DownloadBufferItemExpire = 60 * time.Second
	DownloadBufferTick       = 15 * time.Second

	// master server
	ServerHealthCheckInterval time.Duration = 10 * time.Second
	MasterPersistMetaInterval time.Duration = 15 * time.Hour
	ServerHealthCheckTimeout  time.Duration = 60 * time.Second
	MasterMetaDataFileName                  = "master.server.meta"

	// replicationFactor
	MinimumReplicationFactor = 3

	LeaseTimeout = 60 * time.Second

	// file constant
	FileMode = 0755

	// Mutation flags
	MutationAppend = (iota + 1) << 1
	MutationWrite
	MutationPad
)
