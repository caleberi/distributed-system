package rpc_struct

const (
	MRPCGetChunkHandleHandler                    = "MasterServer.RPCGetChunkHandleHandler"
	MRPCGetPrimaryAndSecondaryServersInfoHandler = "MasterServer.RPCGetPrimaryAndSecondaryServersInfoHandler"
	MRPCListHandler                              = "MasterServer.RPCListHandler"
	MRPCMkdirHandler                             = "MasterServer.RPCMkdirHandler"
	MRPCCreateFileHandler                        = "MasterServer.RPCCreateFileHandler"
	MRPCDeleteFileHandler                        = "MasterServer.RPCDeleteFileHandler"
	MRPCRenameHandler                            = "MasterServer.RPCRenameHandler"
	MRPCGetFileInfoHandler                       = "MasterServer.RPCGetFileInfoHandler"
	MRPCGetReplicasHandler                       = "MasterServer.RPCGetReplicasHandler"
	MRPCHeartBeatHandler                         = "MasterServer.RPCHeartBeatHandler"

	CRPCReadChunkHandler   = "ChunkServer.RPCReadChunkHandler"
	CRPCForwardDataHandler = "ChunkServer.RPCForwardDataHandler"
	CRPCWriteChunkHandler  = "ChunkServer.RPCWriteChunkHandler"
	CRPCAppendChunkHandler = "ChunkServer.RPCAppendChunkHandler"
)
