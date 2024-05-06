package chunkserver

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/filesystem"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/utils"
)

type Mutation struct {
	mutationType common.MutationType // action type for this mutation
	data         []byte              // the data to be append / written/ deleted from chunk
	offset       common.Offset       // takes note of the starting point of a particular mutation
}

type chunkInfo struct {
	sync.RWMutex                                  // handling lock during mutation ops
	length       common.Offset                    //last know offed of this chunk
	mutations    map[common.ChunkVersion]Mutation // all necessary mutation to be committed for this chuck to FS
	checksum     common.Checksum                  // tracking data corruption
	version      common.ChunkVersion              // version for data reconciliation (current version) probably
	completed    bool                             // check if mutation was ever marked done
	abandoned    bool                             //  check if mutation was ever marked abandoned
}

type ChunkServer struct {
	ServerAddr     common.ServerAddr
	MasterAddr     common.ServerAddr
	MachineInfo    common.MachineInfo
	listener       net.Listener
	rootDir        *filesystem.FileSystem
	mu             sync.RWMutex
	downloadBuffer *dbuffer
	// deal with lease here
	// I figure there is a need for chunkservers to ask for more
	// lease if need be while still using the current lease
	// this leases are queued up as a unique queue set
	pendingLeases *common.LeaseHolder
	chunks        map[common.ChunkHandle]*chunkInfo
	garbages      utils.Deque[common.ChunkHandle] // for chunks that needs to be deleted (as informed by master)
	shutdownChan  chan os.Signal                  // to gracefully shutdown server
	isDead        bool                            // server status
}

// PersistedMetaData represents metadata associated with a GFS chunk.
type PersistedMetaData struct {
	Handle               common.ChunkHandle     // Unique identifier for the chunk
	Version              common.ChunkVersion    // Version number of the chunk
	Length               common.Offset          //  offset in the chunk
	Completed, Abandoned bool                   // this handle is completed <that is it is filled>
	ChunkSize            int64                  // Size of the chunk
	CreationTime         time.Time              // Creation time of the chunk
	LastModified         time.Time              // Last modified time of the chunk
	AccessTime           time.Time              // Last access time of the chunk
	Checksum             common.Checksum        // Checksum or hash of the chunk data
	Replication          int                    // Replication level of the chunk (we need to know the number of replication  that we have )
	ServerIP             string                 // IP address of the chunk server
	ServerStatus         int                    // Status of the chunk server (last know server )
	MetadataVersion      int                    // Version of metadata associated with the chunk
	Permissions          []string               // Permissions associated with the chunk
	AccessControl        map[string]bool        // Access control list for the chunk
	StatusFlags          []string               // Flags indicating the status of the chunk
	StoragePolicy        string                 // Storage policy for the chunk
	ChunkAttributes      map[string]interface{} // Additional attributes of the chunk
}

func NewChunkServer(serverAddr common.ServerAddr, masterAddr common.ServerAddr, root string) *ChunkServer {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("cannot retrieve chunk server hostname")
	}

	// // retrieve the coordinate location of server to calulate the primary and secondaries
	var machineInfo common.MachineInfo
	machineInfo.Hostname = hostname
	machineInfo.RoundTripProximityTime = calculateRoundTripProximity(15, string(masterAddr))

	cs := &ChunkServer{
		ServerAddr:    serverAddr,
		MasterAddr:    masterAddr,
		MachineInfo:   machineInfo,
		rootDir:       filesystem.NewFileSystem(root),
		pendingLeases: common.NewLeaseHolder(10),
		chunks:        make(map[common.ChunkHandle]*chunkInfo),
		shutdownChan:  make(chan os.Signal),
		garbages:      utils.Deque[common.ChunkHandle]{},
		isDead:        false,
		// 	Each chunkserver will store
		// the data in an internal LRU buffer cache until the
		// data is used or aged out.
		downloadBuffer: NewDBuffer(common.DownloadBufferTick, common.DownloadBufferItemExpire),
	}

	rpc := rpc.NewServer()
	err = rpc.Register(cs)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return nil
	}
	l, err := net.Listen("tcp", string(cs.ServerAddr))
	if err != nil {
		log.Err(err).Stack().Msg(fmt.Sprintf("cannot trigger chunk server %s to listen\n", cs.ServerAddr))
		os.Exit(1)
	}
	cs.listener = l
	err = cs.rootDir.MkDir(".") // create a root directory
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot create root directory (%s)\n", root))
	}
	err = cs.loadMetadata()
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot load metadata due to error (%s)\n", err))
	}

	signal.Notify(cs.shutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func(listener net.Listener) {
		for {
			select {
			case <-cs.shutdownChan:
				return
			default:
			}
			conn, err := listener.Accept()
			if err != nil {
				if cs.isDead {
					log.Fatal().Msg("server died \n")
				}
				continue
			}
			go func() {
				rpc.ServeConn(conn) // handle individual connection
				conn.Close()
			}()
		}
	}(cs.listener)

	// run background worker for garbage collection,
	// heart beat synchronization with master
	go func() {
		heartBeatTicker := time.NewTicker(common.HeartBeatInterval)
		garbageCollectionTicker := time.NewTicker(common.GarbageCollectionInterval)
		persistMetaDataTicker := time.NewTicker(common.PersistMetaDataInterval)
		quickStart := make(chan struct{}, 1)
		quickStart <- struct{}{}

		// the paper mentioned the chunk server sending heartbeat at every
		// scheduled interval and obvisously it seems the only to send
		// an immediated heartbeat on start up after some time
		// I noticed there are two ways :
		// 1. the chunk-server should inform the master of their status [X]
		// 2. the master polls for the status of the chunk server []
		//
		var branchInfo common.BranchInfo
		log.Info().Msg("running background task...")
		for {
			select {
			case <-quickStart:
				branchInfo.Event = string(common.HeartBeat)
				branchInfo.Err = cs.heartBeat()
			case <-cs.shutdownChan:
				log.Info().Msg("shutting down background workers") // close background workers
				log.Info().Msg(fmt.Sprintf("Gracefully shutting down server (%s)...\n", serverAddr))
				time.Sleep(time.Second * 1)
				cs.downloadBuffer.Done()
				return
			case <-heartBeatTicker.C: // TODO: handle heartbeat communitcation
				branchInfo.Event = string(common.HeartBeat)
				branchInfo.Err = cs.heartBeat()
			case <-persistMetaDataTicker.C: // TODO handle data persistence to file using decoding
				branchInfo.Event = string(common.PersistMetaData)
				branchInfo.Err = cs.persistMetadata()
			case <-garbageCollectionTicker.C:
				branchInfo.Event = string(common.GarbageCollection)
				branchInfo.Err = cs.garbageCollection()
			default:
			}

			if branchInfo.Err != nil {
				log.Info().Msg(fmt.Sprintf("Server %s  background-(%s) event triggered an error (%s)\n", cs.ServerAddr, branchInfo.Event, branchInfo.Err))
				continue
			}
			// log.Info().Msg(fmt.Sprintf("Server %s background-(%s) event trigged\n", cs.ServerAddr, branchInfo.Event))
		}
	}()

	log.Printf("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", serverAddr, root, masterAddr)
	return cs
}

func (cs *ChunkServer) IsAlive() bool {
	return !cs.isDead
}

// Loads meta data information into server memory
func (cs *ChunkServer) loadMetadata() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	file, err := cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDONLY, common.FileMode)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			err = cs.rootDir.CreateFile(common.ChunkMetaDataFileName)
			if err != nil {
				return err
			}
		}
		file, err = cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDONLY, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer file.Close()

	metas := []PersistedMetaData{}
	// decode stored gob object
	decorder := gob.NewDecoder(file)
	err = decorder.Decode(&metas)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("error occurred while loading metadata (%v)", err)
			return err
		}
	}

	log.Info().Msg(fmt.Sprintf("Server %s found metas with length %d", cs.ServerAddr, len(metas)))
	// load meta into server memory
	for _, m := range metas {
		log.Info().Msg(fmt.Sprintf("Server %s restoring chunk-%d with version: %d length: %d", cs.ServerAddr, m.Handle, m.Version, m.Length))
		cs.chunks[m.Handle] = &chunkInfo{
			length:    m.Length,
			version:   m.Version,
			completed: m.Completed,
			checksum:  m.Checksum,
		}
	}

	return nil
}

func (cs *ChunkServer) heartBeat() error {
	// for heartbeat , few things need to be a handled
	// 1. block till all pending lease are removed
	pendingLeases := cs.pendingLeases.ReleaseAll()
	arg := rpc_struct.HeartBeatArg{
		Address:       cs.ServerAddr,
		PendingLeases: pendingLeases,
		MachineInfo:   cs.MachineInfo,
	}

	var reply rpc_struct.HeartBeatReply
	// 2. send rpc calls over to the master
	if err := utils.CallRPCServer(string(cs.MasterAddr), "MasterServer.RPCHeartBeatHandler", arg, &reply); err != nil {
		log.Err(err).Stack().Msg("cannot call MasterServer.RPCHeartBeatHandler")
		return err
	}
	for _, lease := range reply.LeaseExtensions {
		cs.pendingLeases.Enqueue(lease)
	}

	for _, gb := range reply.Garbage {
		// TODO:  try to understand why it the master needs to control what
		//  the chunk server deletes : https://chat.openai.com/c/6abff53f-3bc2-4a27-8b72-6c74cec2d358
		cs.garbages.PushBack(gb)
	}
	return nil
}

func (cs *ChunkServer) persistMetadata() error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	log.Info().Msg("<<< persisting metadata to file >>> ")
	file, err := cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDWR, common.FileMode)
	if err != nil {
		return err
	}
	defer file.Close()

	metadatas := []PersistedMetaData{}

	for handle, ch := range cs.chunks {
		metadatas = append(metadatas, PersistedMetaData{
			ChunkSize:    int64(reflect.TypeOf(ch.mutations).Size()) / 1024, // in KB
			Version:      ch.version,
			CreationTime: time.Now(),
			Length:       ch.length,
			Handle:       handle,
			Checksum:     ch.checksum,
			Completed:    ch.completed,
		})
	}
	log.Printf("Server %v : store metadata len: %v", cs.ServerAddr, len(metadatas))
	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadatas)
}

func (cs *ChunkServer) garbageCollection() error {
	// https://chat.openai.com/c/670528fb-ee6a-4684-8b14-0c9e68366be0
	log.Info().Msg("::: Doing some garabage collection >>> ")
	for cs.garbages.Length() > 0 {
		handle := cs.garbages.PopFront()
		err := cs.deleteChunk(handle)
		if err != nil {
			log.Err(err).Stack()
		}
	}
	return nil
}

func (cs *ChunkServer) deleteChunk(handle common.ChunkHandle) error {
	cs.mu.Lock()
	delete(cs.chunks, handle)
	cs.mu.Unlock()
	return cs.rootDir.RemoveFile(fmt.Sprintf("chunk%v.cnk", handle))
}

func (cs *ChunkServer) Shutdown() {
	if cs.isDead {
		log.Printf("Server %v is dead\n", cs.ServerAddr)
		return
	}
	cs.isDead = true
	log.Printf("%s clearing buffer before shutdown >>>...\n", cs.ServerAddr)
	cs.downloadBuffer.Done()
	if err := cs.listener.Close(); err != nil {
		log.Err(err).Stack()
	}
	log.Printf("Saving metadata >>>...\n")
	if err := cs.persistMetadata(); err != nil {
		log.Err(err).Stack()
	}
	cs.shutdownChan <- syscall.SIGINT
	close(cs.shutdownChan)
}

// ///////////////////////////////////
//
//	RPC METHODS
//
// /////////////////////////////////
func (cs *ChunkServer) RPCSysReportHandler(args rpc_struct.SysReportInfoArg, reply *rpc_struct.SysReportInfoReply) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	bToMb := func(b uint64) uint64 {
		return b / 1024 / 1024
	}
	log.Info().Msg(fmt.Sprintf("<<< Gathering sys start  for %v >>> ", cs.ServerAddr))
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	alloc := bToMb(m.Alloc)
	totalAlloc := bToMb(m.TotalAlloc)
	tSys := bToMb(m.Sys)
	numGC := bToMb(uint64(m.NumGC))

	log.Info().Msg(fmt.Sprintf("Alloc = %v MiB", alloc))
	log.Info().Msg(fmt.Sprintf("\tTotalAlloc = %v MiB", totalAlloc))
	log.Info().Msg(fmt.Sprintf("\tSys = %v MiB", tSys))
	log.Info().Msg(fmt.Sprintf("\tNumGC = %v\n", numGC))

	mem := common.Memory{
		TotalAlloc: totalAlloc,
		Sys:        tSys,
		NumGC:      numGC,
		Alloc:      alloc,
	}

	chunkInfos := make([]common.PersistedChunkInfo, 0)
	for h, ch := range cs.chunks {
		chunkInfos = append(chunkInfos, common.PersistedChunkInfo{
			Handle:   h,
			Checksum: ch.checksum,
			Length:   ch.length,
			Version:  ch.version,
		})
	}

	reply.Chunks = chunkInfos
	reply.SysMem = mem

	log.Info().Msg("<<< Done with sys stat retrival >>> ")
	return nil
}

// The master will detect that this chunkserver has a stale replica
// when the chunkserver restarts and reports its set of chunks
// and their associated version numbers. If the master sees a
// version number greater than the one in its records, the master
// assumes that it failed when granting the lease and so
// takes the higher version to be up-to-date.
func (cs *ChunkServer) RPCCheckChunkVersionHandler(args rpc_struct.CheckChunkVersionArg, reply *rpc_struct.CheckChunkVersionReply) error {
	cs.mu.RLock()
	chinfo, ok := cs.chunks[args.Handle]
	cs.mu.RUnlock()

	if !ok {
		reply.Stale = true
		chinfo.abandoned = true
		return nil
	}
	// compare the version provided byt the calling server with the server's own
	//  determine whether or not the version here is stale

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// check if the cs is behind by 1
	// synchronized cus we don't want the chinfo mutated while we
	// yet to verify it staleness
	if chinfo.version+common.ChunkVersion(1) == args.Version {
		reply.Stale = false
		chinfo.completed = true
		chinfo.version++ // advance the version by 1
		return nil
	}

	log.Printf("%v :  stale chunk %v", cs.ServerAddr, chinfo)
	chinfo.abandoned = true
	reply.Stale = false
	return nil
}

func (cs *ChunkServer) RPCReadChunk(args rpc_struct.ReadChunkArgs, reply *rpc_struct.ReadChunkReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[args.Handle]
	cs.mu.RUnlock()

	if !ok || chInfo.abandoned {
		return fmt.Errorf("cannot find Chunk %v in available chunks or is abandoned", handle)
	}

	var err error
	reply.Data = make([]byte, args.Length)
	chInfo.RLock()
	n, err := cs.readChunk(handle, args.Offset, reply.Data)
	reply.Length = int64(n)
	chInfo.RUnlock()

	if err != io.EOF {
		reply.ErrorCode = common.ReadEOF
		return nil
	}
	return err
}

func (cs *ChunkServer) RPCCreateChunkHandler(args rpc_struct.CreateChunkArgs, reply *rpc_struct.CreateChunkReply) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	log.Info().Msg(fmt.Sprintf("Server (%v) - create chunk %v", cs.ServerAddr, args.Handle))

	if _, ok := cs.chunks[args.Handle]; ok {
		log.Info().Msg("[ignoring] chunk exist already >>>")
		return nil
	}

	cs.chunks[args.Handle] = &chunkInfo{length: 0}
	filename := fmt.Sprintf("chunk%v.chk", args.Handle)
	err := cs.rootDir.CreateFile(filename)
	if err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) RPCForwardDataHandler(args rpc_struct.ForwardDataArgs, reply *rpc_struct.ForwardDataReply) error {
	// check if we have the item previously
	_, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if ok {
		log.Printf("seems like %v exist in buffer already", args.DownloadBufferId)
		return nil
	}

	log.Printf("storing %v on %v's buffer cache", args.DownloadBufferId, cs.ServerAddr)
	cs.downloadBuffer.Set(args.DownloadBufferId, args.Data)

	if len(args.Replicas) == 0 {
		return nil
	}

	replicaAddr := args.Replicas[0]
	client, err := rpc.Dial("tcp", string(replicaAddr))
	if err != nil {
		return err
	}
	args.Replicas = args.Replicas[1:]
	log.Printf("forwarding data to replica (%v)", replicaAddr)
	err = client.Call("ChunkServer.RPCForwardDataHandler", args, &reply)
	if err != nil {
		return err
	}

	return nil
}

func (cs *ChunkServer) RPCWriteChunkHandler(args rpc_struct.WriteChunkArgs, reply *rpc_struct.WriteChunkReply) error {
	// lock the server for a write

	bToMb := func(b uint64) uint64 {
		return b / 1024 / 1024
	}

	data, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if !ok {
		reply.ErrorCode = common.DownloadBufferMiss
		return fmt.Errorf(
			"could not locate %v in buffer (might have expired ...)",
			args.DownloadBufferId)
	}

	// calculate the next offset from the prevous cursor position
	// assumption is that the data in the buffer is greated than 64 << 20
	dataSize := bToMb(uint64(args.Offset) + uint64(len(data)))
	if dataSize > common.ChunkMaxSizeInMb {
		return fmt.Errorf("provided data size for write action [%v] is larger than the max allowed data size of %v mb", args.DownloadBufferId, common.ChunkMaxSizeInMb)
	}
	trial := 3
time_extension:
	//  we might need to acquire a lease to continue from the server
	lease, _ := cs.pendingLeases.Acquire()
	ticker := time.NewTicker(time.Duration(time.Now().Add(time.Duration(lease.Expire.Second())).Second()))
	defer ticker.Stop()
	done := make(chan bool, 1)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup, done chan<- bool, errCh chan<- error) {
		defer wg.Wait()
		for {
			select {
			case <-ticker.C:
				cs.pendingLeases.Release() // release a
				done <- false              // request for more time for next trial
				return
			default:
				handle := args.DownloadBufferId.Handle
				cs.mu.RLock()
				chInfo, ok := cs.chunks[handle]
				cs.mu.RUnlock()
				if !ok || chInfo.abandoned {
					errCh <- fmt.Errorf("%v is either abandoned or lives in another dimension", handle)
					return
				}

				mutation := &Mutation{mutationType: common.MutationWrite, data: data, offset: args.Offset}
				wait := make(chan error, 1)
				// update the file on this server (async)
				go func() {
					wait <- cs.mutate(handle, mutation)
				}()

				var e error
				var applyMutationArgs rpc_struct.ApplyMutationArgs
				var applyMutationReply rpc_struct.ApplyMutationReply

				applyMutationArgs.DownloadBufferId = args.DownloadBufferId
				applyMutationArgs.MutationType = common.MutationWrite
				applyMutationArgs.Offset = args.Offset

				// forward a write call to secondary server
				for i := 0; i < len(args.Replicas); i++ {
					replicaAddr := args.Replicas[i]
					log.Printf("forwarding data to replica (%v)", replicaAddr)
					err := utils.CallRPCServer(string(replicaAddr), "ChunkServer.RPCApplyMutationHandler", applyMutationArgs, &applyMutationReply)
					if err != nil {
						e = err
						break
					}
				}
				if e != nil {
					errCh <- e
					return
				}

				if err := <-wait; err != nil {
					done <- false
				}
				done <- true
			}
		}
	}(&wg, done, errCh)

	wg.Wait()

	select {
	case d := <-done:
		if !d {
			if trial > 0 {
				lease.Expire = lease.Expire.Add(args.LeaseExtension)
				trial--
				cs.pendingLeases.Enqueue(lease)
				goto time_extension
			}
			reply.ErrorCode = common.Timeout
			return nil
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func (cs *ChunkServer) ApplyMutationHandler(args rpc_struct.ApplyMutationArgs, reply *rpc_struct.ApplyMutationReply) error {

	data, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if !ok {
		reply.ErrorCode = common.DownloadBufferMiss
		return fmt.Errorf(
			"could not locate %v in buffer (might have expired ...)",
			args.DownloadBufferId)
	}

	// calculate the next offset from the prevous cursor position
	// assumption is that the data in the buffer is greated than 64 << 20
	dataSize := bToMb(uint64(args.Offset) + uint64(len(data)))
	if dataSize > common.ChunkMaxSizeInMb {
		return fmt.Errorf("provided data size for append action [%v] is larger than the max allowed data size of %v mb", args.DownloadBufferId, common.ChunkMaxSizeInMb)
	}

	handle := args.DownloadBufferId.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("%v is either abandoned or lives in another dimension", handle)
	}

	mutation := &Mutation{mutationType: common.MutationWrite, data: data, offset: args.Offset}

	chInfo.Lock()
	err := cs.mutate(handle, mutation)
	if err != nil {
		chInfo.abandoned = true
		return err
	}
	return nil
}

func (cs *ChunkServer) ApplyAppendChunkHandler(args rpc_struct.AppendChunkArgs, reply *rpc_struct.AppendChunkReply) error {
	data, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if !ok {
		reply.ErrorCode = common.DownloadBufferMiss
		return fmt.Errorf(
			"could not locate %v in buffer (might have expired ...)",
			args.DownloadBufferId)
	}

	handle := args.DownloadBufferId.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("%v is either abandoned or lives in another dimension", handle)
	}

	var mutationType common.MutationType
	offset := chInfo.length
	newLength := chInfo.length + common.Offset(len(data))
	dataSize := bToMb(uint64(newLength))

	if dataSize > common.ChunkMaxSizeInMb {
		mutationType = common.MutationPad
		chInfo.length = common.ChunkMaxSizeInByte
		reply.ErrorCode = common.AppendExceedChunkSize
	} else {
		mutationType = common.MutationAppend
		chInfo.length = newLength
	}

	reply.Offset = offset
	mutation := &Mutation{mutationType: mutationType, data: data, offset: offset}
	wait := make(chan error, 1)
	// update the file on this server (async)
	go func() {
		wait <- cs.mutate(handle, mutation)
	}()

	var e error
	var applyMutationArgs rpc_struct.ApplyMutationArgs
	var applyMutationReply rpc_struct.ApplyMutationReply

	applyMutationArgs.DownloadBufferId = args.DownloadBufferId
	applyMutationArgs.MutationType = mutationType
	applyMutationArgs.Offset = offset

	// forward a write call to secondary server
	for i := 0; i < len(args.Replicas); i++ {
		replicaAddr := args.Replicas[i]
		client, err := rpc.Dial("tcp", string(replicaAddr))
		if err != nil {
			e = err
			break
		}
		log.Printf("forwarding data to replica (%v)", replicaAddr)
		err = client.Call("ChunkServer.RPCApplyMutationHandler", applyMutationArgs, &applyMutationReply)
		if err != nil {
			e = err
			break
		}
	}
	if err := <-wait; err != nil {
		return err
	}
	return e
}

func (cs *ChunkServer) RPCGetSnapshotHandler(args rpc_struct.GetSnapshotArgs, reply *rpc_struct.GetSnapshotReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	chInfo.RLock()
	defer chInfo.RUnlock()

	log.Printf("Server %v : Send copy of %v to %v", cs.ServerAddr, handle, args.Replicas)
	data := make([]byte, chInfo.length)
	_, err := cs.readChunk(handle, 0, data)
	if err != nil {
		return err
	}

	var r rpc_struct.ApplyCopyReply
	for i := 0; i < len(args.Replicas); i++ {
		replicaAddr := args.Replicas[i]
		client, err := rpc.Dial("tcp", string(replicaAddr))
		if err != nil {
			return err
		}
		applyCopyArgs := rpc_struct.ApplyCopyArgs{Handle: handle, Data: data, Version: chInfo.version}
		log.Printf("forwarding data to replica (%v)", replicaAddr)
		err = client.Call("ChunkServer.RPCApplyCopyHandler", applyCopyArgs, &r)
		if err != nil {
			return err
		}
	}

	return nil

}

func (cs *ChunkServer) RPCApplyCopyHandler(args rpc_struct.ApplyCopyArgs, reply *rpc_struct.AppendChunkReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	chInfo.Lock()
	defer chInfo.Unlock()

	log.Printf("Server %v : Apply copy of %v", cs.ServerAddr, handle)

	chInfo.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0, true)
	if err != nil {
		return err
	}
	log.Printf("Server %v : Apply done", cs.ServerAddr)
	return nil
}

//////////////////////////////////////////////
//        HELPER FUNCTIONS
////////////////////////////////////////////

func (cs *ChunkServer) mutate(handle common.ChunkHandle, mutation *Mutation) error {
	var shouldLock bool

	if mutation.mutationType == common.MutationAppend {
		shouldLock = true
	} else {
		shouldLock = false
	}

	var err error

	if mutation.mutationType == common.MutationPad {
		mutation.data = []byte{0}
		err = cs.writeChunk(handle, mutation.data, common.ChunkMaxSizeInByte-8, shouldLock)
	} else {
		err = cs.writeChunk(handle, mutation.data, mutation.offset, shouldLock)
	}

	if err != nil {
		cs.mu.RLock()
		chInfo, ok := cs.chunks[handle]
		cs.mu.RUnlock()
		if ok {
			chInfo.completed = false
			chInfo.abandoned = true
		}
	}

	return err
}

func (cs *ChunkServer) writeChunk(handle common.ChunkHandle, data []byte, offset common.Offset, lock bool) error {

	cs.mu.RLock()
	chInfo := cs.chunks[handle]
	cs.mu.RUnlock()

	newLen := offset + common.Offset(len(data))
	if newLen > chInfo.length {
		chInfo.length = newLen
	}

	if newLen > common.ChunkMaxSizeInByte {
		return fmt.Errorf("data size is larger than maximum allowed per chunk")
	}

	if lock {
		cs.mu.Lock()
		defer cs.mu.Unlock()
	}
	fs, err := cs.rootDir.GetFile(fmt.Sprintf("chunk%v.chk", handle), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fs.Close()
	n, err := fs.WriteAt(data, int64(offset))
	if err != nil {
		log.Printf("error occurred while writing from %v from offset %v", chInfo, offset)
		return err
	}
	log.Printf("Wrote %d from %v from offset %v", n, chInfo, offset)
	return nil
}

func (cs *ChunkServer) readChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
	f, err := cs.rootDir.GetFile(fmt.Sprintf("chunk%v.chk", handle), os.O_RDONLY, common.FileMode)
	if err != nil {
		return -1, err
	}
	defer f.Close()
	log.Printf("Server %v reading data from offset %v for chunk handle [%v]", cs.ServerAddr, offset, handle)
	return f.ReadAt(data, int64(offset))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func calculateRoundTripProximity(duration int, masterAddr string) float64 {
	var (
		buffer bytes.Buffer
		wg     sync.WaitGroup
	)
	// forked process - not a great solution so far
	cmd := exec.Command("ping", "-c", fmt.Sprintf("%d", duration), masterAddr)
	wg.Add(1)
	go func(wg *sync.WaitGroup, cmd *exec.Cmd, buf *bytes.Buffer) {
		defer wg.Done()
		cmd.Stdout = buf
		if cmd.Run() != nil {
			buf.Reset()
			buf.WriteString("time=0.00 ms") // use this as a base case
			log.Printf("failed to  ping master (%s) \n", masterAddr)
		}
	}(&wg, cmd, &buffer)

	wg.Wait()
	output := buffer.String()
	rrt, err := extractAverageRTT(output)
	if err != nil {
		log.Printf("error extracting average round-trip time: %v", err)
	}
	return rrt
}

func extractAverageRTT(input string) (float64, error) {
	re := regexp.MustCompile(`time=([\d.]+)\s*ms`)
	matches := re.FindAllStringSubmatch(input, -1)
	total := 0.0
	cnt := 0
	for _, match := range matches {
		v, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			return 0.0, err
		}
		cnt += 1
		total += v
	}
	return total / float64(cnt), nil
}
