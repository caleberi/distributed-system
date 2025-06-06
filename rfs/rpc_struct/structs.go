package rpc_struct

import (
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/shared"
)

type HeartBeatArg struct {
	Address       common.ServerAddr
	PendingLeases []*common.Lease
	MachineInfo   common.MachineInfo
	ExtendLease   bool
}
type HeartBeatReply struct {
	LastHeartBeat   time.Time
	LeaseExtensions []*common.Lease
	Garbage         []common.ChunkHandle
	NetworkData     shared.NetworkData
}

type SysReportInfoArg struct{}
type SysReportInfoReply struct {
	SysMem common.Memory
	Chunks []common.PersistedChunkInfo
}

type CheckChunkVersionArg struct {
	Handle  common.ChunkHandle
	Version common.ChunkVersion
}
type CheckChunkVersionReply struct {
	Stale bool
}

type ReadChunkArgs struct {
	Offset common.Offset
	Data   []byte
	Length int64
	Handle common.ChunkHandle
	Lease  *common.Lease
}

type ReadChunkReply struct {
	Data      []byte
	Length    int64
	ErrorCode common.ErrorCode
}

type CreateChunkArgs struct {
	Handle common.ChunkHandle
}

type CreateChunkReply struct {
	ErrorCode common.ErrorCode
}

type ForwardDataArgs struct {
	DownloadBufferId common.BufferId
	Data             []byte
	Replicas         []common.ServerAddr
}

type ForwardDataReply struct {
	ErrorCode common.ErrorCode
}

type WriteChunkArgs struct {
	DownloadBufferId common.BufferId
	Offset           common.Offset
	Replicas         []common.ServerAddr
}

type WriteChunkReply struct {
	Length    int
	ErrorCode common.ErrorCode
}

type ApplyMutationArgs struct {
	MutationType     common.MutationType
	DownloadBufferId common.BufferId
	Offset           common.Offset
}

type ApplyMutationReply struct {
	ErrorCode common.ErrorCode
}

type AppendChunkArgs struct {
	DownloadBufferId common.BufferId
	Replicas         []common.ServerAddr
}

type AppendChunkReply struct {
	Offset    common.Offset
	ErrorCode common.ErrorCode
}

type GetSnapshotArgs struct {
	Handle   common.ChunkHandle
	Replicas common.ServerAddr
}

type GetSnapshotReply struct {
	ErrorCode common.ErrorCode
}

type ApplyCopyArgs struct {
	Handle  common.ChunkHandle
	Data    []byte
	Version common.ChunkVersion
}
type ApplyCopyReply struct {
	ErrorCode common.ErrorCode
}

type PrimaryAndSecondaryServersInfoReply struct {
	Primary          common.ServerAddr
	SecondaryServers []common.ServerAddr
	Expire           time.Time
}

type PrimaryAndSecondaryServersInfoArg struct {
	Handle common.ChunkHandle
}

type GetChunkHandleReply struct {
	Handle common.ChunkHandle
}

type GetChunkHandleArgs struct {
	Path  common.Path
	Index common.ChunkIndex
}

type GetPathInfoArgs struct {
	Path   common.Path
	Handle common.ChunkHandle
}

type GetPathInfoReply struct {
	Entries []common.PathInfo
}

type MakeDirectoryArgs struct {
	Path common.Path
}

type MakeDirectoryReply struct{}

type RenameFileArgs struct {
	Source common.Path
	Target common.Path
}

type RenameFileReply struct{}

type CreateFileArgs struct {
	Path common.Path
}
type CreateFileReply struct{}

type DeleteFileArgs struct {
	Path common.Path
}

type DeleteFileReply struct{}

type GetFileInfoArgs struct {
	Path common.Path
}

type GetFileInfoReply struct {
	IsDir  bool
	Length int64
	Chunks int64
}

type RetrieveReplicasArgs struct {
	Handle common.ChunkHandle
}
type RetrieveReplicasReply struct {
	Locations []common.ServerAddr
}

type GetPathInfoArg struct {
	Handle common.ChunkHandle
}

type GrantLeaseInfoArgs struct {
	Handle      common.ChunkHandle
	Expire      time.Time
	InUse       bool
	Primary     common.ServerAddr
	Secondaries []common.ServerAddr
}

type GrantLeaseInfoReply struct {
}
