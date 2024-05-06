package common

import "time"

const (
	DeletedNamespaceFilePrefix string = "___deleted__"
	HeartBeat                  Event  = "HeartBeat"
	GarbageCollection          Event  = "GarbageCollection"
	PersistMetaData            Event  = "PersistMetaData"
	PersistOpsLog              Event  = "PersistOpsLog"

	// chunk server
	HeartBeatInterval         time.Duration = 30 * time.Second
	GarbageCollectionInterval time.Duration = 1 * time.Minute
	PersistMetaDataInterval   time.Duration = 2 * time.Minute

	// downloadbuffer
	DownloadBufferItemExpire = 1 * time.Minute
	DownloadBufferTick       = 15 * time.Second

	// master server
	ServerHealthCheckInterval time.Duration = 400 * time.Millisecond
	MasterPersistMetaInterval time.Duration = 1 * time.Hour
	ServerHealthCheckTimeout  time.Duration = 30 * time.Second

	// replicationFactor
	MinimumReplicationFactor = 3

	// file constant
	FileMode = 0755

	MutationAppend = (iota + 1) << 1
	MutationWrite
	MutationPad

	ChunkMetaDataFileName = "chunk.server.meta"
	ChunkMaxSizeInMb      = 64
	ChunkMaxSizeInByte    = 64 * 1024 * 1024

	MasterMetaDataFileName          = "master.server.meta"
	MasterNamespaceMetaDataFileName = "master-namespace.server.meta"
)

type BranchInfo struct {
	Err   error
	Event string
}
