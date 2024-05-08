package common

import "time"

type BranchInfo struct {
	Err   error
	Event string
}

const (
	DeletedNamespaceFilePrefix string = "___deleted__"
	HeartBeat                  Event  = "HeartBeat"
	GarbageCollection          Event  = "GarbageCollection"
	PersistMetaData            Event  = "PersistMetaData"
	PersistOpsLog              Event  = "PersistOpsLog"
	MasterHeartBeat            Event  = "MasterHeartBeat"

	// chunk server
	HeartBeatInterval         time.Duration = 30 * time.Second
	GarbageCollectionInterval time.Duration = 30 * time.Minute
	PersistMetaDataInterval   time.Duration = 10 * time.Hour
	ChunkMetaDataFileName                   = "chunk.server.meta"
	ChunkMaxSizeInMb                        = 64
	ChunkMaxSizeInByte                      = 64 * 1024 * 1024

	// downloadbuffer
	DownloadBufferItemExpire = 1 * time.Minute
	DownloadBufferTick       = 15 * time.Second

	// master server
	ServerHealthCheckInterval time.Duration = 60 * time.Second
	MasterPersistMetaInterval time.Duration = 1 * time.Hour
	ServerHealthCheckTimeout  time.Duration = 60 * time.Second
	MasterMetaDataFileName                  = "master.server.meta"

	// replicationFactor
	MinimumReplicationFactor = 3

	// file constant
	FileMode = 0755

	MutationAppend = (iota + 1) << 1
	MutationWrite
	MutationPad
)
