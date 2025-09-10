package master_server

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/caleberi/distributed-system/common"
	filesystem "github.com/caleberi/distributed-system/file_system"
	namespacemanager "github.com/caleberi/distributed-system/namespace_manager"
	"github.com/caleberi/distributed-system/rpc_struct"
	"github.com/caleberi/distributed-system/shared"
	"github.com/caleberi/distributed-system/utils"
	"github.com/rs/zerolog/log"
)

type chunkServerInfo struct {
	sync.RWMutex
	lastHeatBeat time.Time
	chunks       map[common.ChunkHandle]bool
	garbages     []common.ChunkHandle
	serverInfo   common.MachineInfo
}

type chunkInfo struct {
	sync.RWMutex
	locations []common.ServerAddr
	primary   common.ServerAddr
	expire    time.Time // ??
	version   common.ChunkVersion
	checksum  common.Checksum
	path      common.Path
}

func (chkInfo *chunkInfo) isExpired(u time.Time) bool {
	return chkInfo.expire.Before(u)
}

type fileInfo struct {
	sync.RWMutex
	handles []common.ChunkHandle
}

type serialChunkInfo struct {
	Path common.Path
	Info []common.PersistedChunkInfo
}

type PesistentMeta struct {
	Namespace []namespacemanager.SerializedNsTreeNode
	ChunkInfo []serialChunkInfo
}

type MasterServer struct {
	sync.RWMutex
	ServerAddr         common.ServerAddr
	rootDir            *filesystem.FileSystem
	listener           net.Listener
	namespaceManager   *namespacemanager.NamespaceManager
	chunkServerManager *CSManager
	isDead             bool
	shutdownChan       chan os.Signal
}

func NewMasterServer(ctx context.Context, serverAddress common.ServerAddr, root string) *MasterServer {
	ma := &MasterServer{
		ServerAddr:         serverAddress,
		rootDir:            filesystem.NewFileSystem(root),
		namespaceManager:   namespacemanager.NewNameSpaceManager(ctx, 10*time.Hour),
		chunkServerManager: NewCSManager(),
		shutdownChan:       make(chan os.Signal, 1),
	}

	// register rpc server
	server := rpc.NewServer()
	err := server.Register(ma)

	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return nil
	}
	l, err := net.Listen("tcp", string(ma.ServerAddr))
	if err != nil {
		log.Err(err).Stack().Msg(fmt.Sprintf("cannot start a listener on %s", ma.ServerAddr))
		return nil
	}

	ma.listener = l
	err = ma.rootDir.MkDir(".")
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot create root directory (%s)\n", root))
		return nil
	}
	// load metadata that will be replicated to another backup server <to avoid SOF>
	err = ma.loadMetadata()
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot load metadata due to error (%s)\n", err))
		return nil
	}

	// create server listener
	go func(listener net.Listener) {
		defer func(listener net.Listener) {
			err := listener.Close()
			if err != nil {
				log.Err(err).Stack().Send()
			}
		}(listener)
		for {
			select {
			case <-ma.shutdownChan:
				return
			default:
			}
			conn, err := listener.Accept()
			if err != nil {
				if ma.isDead {
					log.Err(err).Stack().Msg(fmt.Sprintf("Server [%s] died\n", ma.ServerAddr))
				}
				continue
			}

			// server each connection concurrently
			go func() {
				server.ServeConn(conn)
				err := conn.Close()
				if err != nil {
					return
				}
			}()
		}
	}(ma.listener)

	signal.Notify(ma.shutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// background task
	// for doing heartbeat checks, server disconnection,
	// garbage collection, stale replica removal & detection
	go func() {
		persistMetadataCheck := time.NewTicker(common.MasterPersistMetaInterval)
		serverHealthCheck := time.NewTicker(common.ServerHealthCheckInterval)
		for {
			var branchInfo common.BranchInfo
			select {
			case <-ma.shutdownChan:
				return
			case <-serverHealthCheck.C:
				branchInfo.Event = string(common.MasterHeartBeat)
				branchInfo.Err = ma.serverHeartBeat()
			case <-persistMetadataCheck.C:
				branchInfo.Event = string(common.PersistMetaData)
				branchInfo.Err = ma.persistMetaData()
			default:
			}

			if err != nil {
				log.Err(err).Stack().Msg(fmt.Sprintf("Error (%s) - from background event (%s)", branchInfo.Err, branchInfo.Event))
			}
		}
	}()

	log.Info().Msg(fmt.Sprintf("Master is running now. Address = [%s] ", string(ma.ServerAddr)))
	return ma
}

func (ma *MasterServer) serverHeartBeat() error {
	deadServers := ma.chunkServerManager.detectDeadServer()
	for _, addr := range deadServers {
		log.Info().Msg(fmt.Sprintf(">> Removing Server %v from Master's servers list", addr))
		handles, err := ma.chunkServerManager.removeServer(addr)
		if err != nil {
			return err
		}
		err = ma.chunkServerManager.removeChunks(handles, addr)
		if err != nil {
			return err
		}
	}

	//  deadserver have nothing to do with the replication logic
	handles := ma.chunkServerManager.getReplicationMigrationList()
	log.Info().Msg(fmt.Sprintf("MasterServer : Replication needed for handles - %v", handles))
	for i := 0; i < len(handles); i++ {
		ck, ok := ma.chunkServerManager.getChunk(handles[i])
		if !ok {
			continue
		}
		if ck.expire.Before(time.Now()) {
			ck.Lock() // don't grant lease during copy
			log.Info().Msg(fmt.Sprintf("Replication in progress >>> for handle [%v] chunk [%v]", handles[i], ck))
			err := ma.performReplication(handles[i])
			if err != nil {
				log.Err(err).Stack().Msg(err.Error())
				ck.Unlock()
				continue
			}
			ck.Unlock()
		}
	}

	return nil
}

func (ma *MasterServer) performReplication(handle common.ChunkHandle) error {
	from, to, err := ma.chunkServerManager.chooseReplicationServer(handle)
	log.Info().Msgf(">>> Moving handle[%v] from %s to %s", handle, from, to)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	log.Warn().Msg(fmt.Sprintf("allocate new chunk %v from %v to %v", handle, from, to))

	var cr rpc_struct.CreateChunkReply
	err = shared.UnicastToRPCServer(string(to), "ChunkServer.RPCCreateChunkHandler", rpc_struct.CreateChunkArgs{Handle: handle}, &cr)
	if err != nil {
		return err
	}

	// CONTINUE FROM HERE  LATER
	var sr rpc_struct.GetSnapshotReply
	err = shared.UnicastToRPCServer(string(from), "ChunkServer.RPCGetSnapshotHandler", rpc_struct.GetSnapshotArgs{Handle: handle, Replicas: to}, &sr)
	if err != nil {
		return err
	}

	err = ma.chunkServerManager.registerReplicas(handle, to, true)
	if err != nil {
		return err
	}
	ma.chunkServerManager.addChunk([]common.ServerAddr{to}, handle)
	return nil
}

func (ma *MasterServer) loadMetadata() error {
	file, err := ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDONLY, common.FileMode)
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			err = ma.rootDir.CreateFile(common.MasterMetaDataFileName)
			if err != nil {
				return err
			}
		}
		file, err = ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDONLY, common.FileMode)
		if err != nil {
			return err
		}
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Err(err).Stack().Send()
		}
	}(file)

	var meta PesistentMeta
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&meta)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("error occurred while loading metadata (%v)", err)
			return err
		}
	}

	if len(meta.Namespace) != 0 {
		ma.namespaceManager.Deserialize(meta.Namespace)
	}
	if len(meta.ChunkInfo) != 0 {
		ma.chunkServerManager.DeserializeChunks(meta.ChunkInfo)
	}
	return nil
}

func (ma *MasterServer) Shutdown() {
	if ma.isDead {
		log.Printf("Server [%s] is dead\n", ma.ServerAddr)
		return
	}

	if err := ma.listener.Close(); err != nil {
		log.Err(err).Stack().Send()
	}

	if err := ma.persistMetaData(); err != nil {
		log.Err(err).Stack().Send()
	}

	close(ma.shutdownChan)
}

func (ma *MasterServer) persistMetaData() error {

	file, err := ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDWR, common.FileMode)
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			err = ma.rootDir.CreateFile(common.MasterMetaDataFileName)
			if err != nil {
				return err
			}
		}
		file, err = ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDWR, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Err(err).Stack().Send()
		}
	}(file)

	var meta PesistentMeta
	meta.Namespace = ma.namespaceManager.Serialize()
	meta.ChunkInfo = ma.chunkServerManager.SerializeChunks()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(&meta)
}

// ///////////////////////////////////
//
//	RPC METHODS
//
// /////////////////////////////////
func (ma *MasterServer) RPCHeartBeatHandler(args rpc_struct.HeartBeatArg, reply *rpc_struct.HeartBeatReply) error {
	firstHeartBeat := ma.chunkServerManager.HeartBeat(args.Address, args.MachineInfo, reply)
	time.Sleep(time.Duration(rand.Intn(10)) * time.Second) // to mimic downtime
	reply.NetworkData.ForwardTrip.ReceivedAt = time.Now()
	defer func() { reply.NetworkData.BackwardTrip.SentAt = time.Now() }()

	if args.ExtendLease {
		var newLeases []*common.Lease
		for _, lease := range reply.LeaseExtensions {
			chk, err := ma.chunkServerManager.extendLease(lease.Handle, lease.Primary)
			if err != nil {
				log.Err(err).Stack().Msg(err.Error())
				continue
			}

			newLeases = append(newLeases, &common.Lease{
				Expire:      chk.expire,
				Handle:      lease.Handle,
				InUse:       false,
				Primary:     lease.Primary,
				Secondaries: lease.Secondaries,
			})
		}

		reply.LeaseExtensions = newLeases
	}

	if firstHeartBeat {
		systemReportArg := rpc_struct.SysReportInfoArg{}
		systemReportReply := rpc_struct.SysReportInfoReply{}
		err := shared.UnicastToRPCServer(string(args.Address), "ChunkServer.RPCSysReportHandler", systemReportArg, &systemReportReply)
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return err
		}
		log.Info().Msg(fmt.Sprintf("Got %#v from ChunkServer = %s", systemReportReply, args.Address))

		if len(systemReportReply.Chunks) == 0 {
			return nil
		}

		for _, chunkInfo := range systemReportReply.Chunks {
			chk, ok := ma.chunkServerManager.getChunk(chunkInfo.Handle)
			if !ok {
				log.Info().Msg(fmt.Sprintf("=> handle : %v not found on master server ", chunkInfo.Handle))
				log.Info().Msg(fmt.Sprintf("=> requesting chunkserver %v to  record as garbage", args.Address))
				reply.Garbage = append(reply.Garbage, chunkInfo.Handle)
				continue
			} else {
				if chk.version != chunkInfo.Version {
					log.Info().Msg(fmt.Sprintf("* handle : %v version on master server is different ", chunkInfo.Handle))
					log.Info().Msg(fmt.Sprintf("* verifying possible stale chunk %v on chunkserver %v", chunkInfo.Handle, args.Address))

					var (
						chunkVersionArg   rpc_struct.CheckChunkVersionArg
						chunkVersionReply rpc_struct.CheckChunkVersionReply
					)
					chunkVersionArg.Handle = chunkInfo.Handle
					chunkVersionArg.Version = chunkInfo.Version

					if chk.primary != "" {
						err := shared.UnicastToRPCServer(
							string(chk.primary),
							"ChunkServer.RPCCheckChunkVersionHandler",
							chunkVersionArg, &chunkVersionReply)
						if err != nil {
							log.Err(err).Stack().Msg(err.Error())
							return err
						}
						if chunkVersionReply.Stale {
							log.Info().Msg(fmt.Sprintf("=> requesting chunkserver %v to record as garbage since chunk is stale", args.Address))
							reply.Garbage = append(reply.Garbage, chunkInfo.Handle)
							continue
						}
					} else {
						log.Info().Msg("Missing chunk primary server so version verification failed")
						if len(chk.locations) != 0 {
							reply.Garbage = append(reply.Garbage, chunkInfo.Handle)
							continue
						}
					}
				}
				if err := ma.chunkServerManager.registerReplicas(chunkInfo.Handle, args.Address, false); err != nil {
					log.Err(err).Stack().Msg(err.Error())
				}
				ma.chunkServerManager.addChunk([]common.ServerAddr{args.Address}, chunkInfo.Handle)
				log.Info().Msgf("replication after first ping %v", ma.chunkServerManager.getReplicationMigrationList())
			}
		}
	} else {
		log.Info().Msgf("Got <HEART_BEAT> from ChunkServer = %s", args.Address)
		log.Info().Msgf("Garbage chunks [%v]", ma.chunkServerManager.getGarbages())
	}

	return nil
}

func (ma *MasterServer) RPCGetPrimaryAndSecondaryServersInfoHandler(
	args rpc_struct.PrimaryAndSecondaryServersInfoArg,
	reply *rpc_struct.PrimaryAndSecondaryServersInfoReply) error {
	lease, staleServers, err := ma.chunkServerManager.getLeaseHolder(args.Handle)
	if err != nil {
		log.Debug().Msg("Tried get a lease here")
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	utils.ForEach(staleServers, func(v common.ServerAddr) { ma.chunkServerManager.addGarbage(v, args.Handle) })
	reply.Expire = lease.Expire
	reply.SecondaryServers = lease.Secondaries
	reply.Primary = lease.Primary
	return nil
}

func (ma *MasterServer) RPCGetChunkHandleHandler(
	args rpc_struct.GetChunkHandleArgs,
	reply *rpc_struct.GetChunkHandleReply) error {
	dirpath, filename := ma.namespaceManager.RetrievePartitionFromPath(args.Path)
	_, err := utils.ValidateFilenameStr(filename, args.Path)
	if err != nil {
		return err
	}
	err = ma.namespaceManager.MkDirAll(common.Path(dirpath))
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	var file *namespacemanager.NsTree

	// Try to create the path if it created before it err
	// We then try to get it instead
	err = ma.namespaceManager.Create(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		file, err = ma.namespaceManager.Get(args.Path)
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return err
		}
	} else {
		file, err = ma.namespaceManager.Get(args.Path)
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return err
		}
	}

	file.Lock()
	defer file.Unlock()
	if args.Index == common.ChunkIndex(file.Chunks) {
		file.Chunks++
		// Note: since one of the servers on creating chunk should be the
		//  primary, the other 3 should be a replica.
		addrs, err := ma.chunkServerManager.chooseServers(common.MinimumReplicationFactor + 1) // sample out of the servers we have
		if err != nil {
			return err
		}
		addrs = utils.Filter(addrs, func(addr common.ServerAddr) bool { return addr != common.ServerAddr("") })
		reply.Handle, addrs, err = ma.chunkServerManager.createChunk(args.Path, addrs)
		if err != nil {
			return err
		}
		ma.chunkServerManager.addChunk(addrs, reply.Handle)
	} else {
		reply.Handle, err = ma.chunkServerManager.getChunkHandle(args.Path, args.Index)
		if err != nil {
			return err
		}
	}

	return err
}

func (ma *MasterServer) RPCListHandler(
	args rpc_struct.GetPathInfoArgs,
	reply *rpc_struct.GetPathInfoReply) error {
	entries, err := ma.namespaceManager.List(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	reply.Entries = entries
	return nil
}

func (ma *MasterServer) RPCMkdirHandler(args rpc_struct.MakeDirectoryArgs,
	reply *rpc_struct.MakeDirectoryReply) error {
	err := ma.namespaceManager.MkDirAll(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return nil
}

func (ma *MasterServer) RPCRenameHandler(args rpc_struct.RenameFileArgs,
	reply *rpc_struct.RenameFileReply) error {
	err := ma.namespaceManager.Rename(args.Source, args.Target)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return ma.chunkServerManager.UpdateFilePath(args.Source, args.Target)
}

func (ma *MasterServer) RPCCreateFileHandler(args rpc_struct.CreateFileArgs,
	reply *rpc_struct.CreateFileReply) error {
	err := ma.namespaceManager.Create(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return nil
}

func (ma *MasterServer) RPCDeleteFileHandler(
	args rpc_struct.DeleteFileArgs,
	reply *rpc_struct.DeleteFileReply) error {

	err := ma.namespaceManager.Delete(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	handles, err := ma.chunkServerManager.GetChunkHandles(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	utils.ForEach(handles, func(handle common.ChunkHandle) {
		ma.chunkServerManager.deleteChunk(handle)
	})

	return nil
}

func (ma *MasterServer) RPCGetFileInfoHandler(
	args rpc_struct.GetFileInfoArgs, reply *rpc_struct.GetFileInfoReply) error {
	file, err := ma.namespaceManager.Get(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	file.Lock()
	defer file.Unlock()
	reply.Chunks = file.Chunks
	reply.IsDir = file.IsDir
	reply.Length = file.Length
	return nil
}

func (ma *MasterServer) RPCGetReplicasHandler(
	args rpc_struct.RetrieveReplicasArgs,
	reply *rpc_struct.RetrieveReplicasReply) error {
	servers, err := ma.chunkServerManager.getReplicas(args.Handle)
	if err != nil {
		return err
	}
	utils.ForEach(servers, func(v common.ServerAddr) {
		reply.Locations = append(reply.Locations, v)
	})
	return nil
}
