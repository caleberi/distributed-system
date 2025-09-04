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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/caleberi/distributed-system/rfs/common"
	filesystem "github.com/caleberi/distributed-system/rfs/fs"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/shared"
	"github.com/caleberi/distributed-system/rfs/utils"
)

type chunkInfo struct {
	sync.RWMutex                                         // handling lock during mutation ops
	length       common.Offset                           // last know offed of this chunk
	mutations    map[common.ChunkVersion]common.Mutation // all necessary mutation to be committed for this chuck to FS
	checksum     common.Checksum                         // tracking data corruption
	version      common.ChunkVersion                     // latest version for data reconciliation (current version) probably
	isCompressed bool                                    // compression check flag
	completed    bool                                    // check if mutation was ever marked done
	abandoned    bool                                    // check if mutation was ever marked abandoned
	creationTime time.Time                               // creation time of the chunk
	lastModified time.Time                               // last modified time of the chunk
	accessTime   time.Time                               // last access time of the chunk
	replication  int
	serverStatus int
}

type ChunkServer struct {
	ServerAddr  common.ServerAddr
	MasterAddr  common.ServerAddr
	MachineInfo common.MachineInfo

	listener        net.Listener
	failureDetector *shared.FailureDetector
	rootDir         *filesystem.FileSystem
	mu              sync.RWMutex
	downloadBuffer  *dbuffer
	archiver        *common.Archiver
	leases          utils.Deque[*common.Lease]
	// leasesUnderMutation map[*common.Lease]bool
	failureDetectionCh chan string
	chunks             map[common.ChunkHandle]*chunkInfo
	garbage            utils.Deque[common.ChunkHandle]
	shutdownChan       chan os.Signal
	isDead             bool
}

// PersistedMetaData represents metadata associated with a GFS chunk.
type PersistedMetaData struct {
	Handle               common.ChunkHandle  // Unique identifier for the chunk
	Version              common.ChunkVersion // Latest Persisted Version number of the chunk
	Length               common.Offset       //  offset in the chunk
	Mutations            map[common.ChunkVersion]common.Mutation
	Completed, Abandoned bool            // this handle is completed <that it is filled>
	ChunkSize            int64           // Size of the chunk
	CreationTime         time.Time       // Creation time of the chunk
	LastModified         time.Time       // Last modified time of the chunk
	AccessTime           time.Time       // Last access time of the chunk
	Checksum             common.Checksum // Checksum or hash of the chunk data
	Replication          int             // Replication level of the chunk (we need to know the number of replication  that we have )
	ServerIP             string          // IP address of the chunk server
	ServerStatus         int             // Status of the chunk server (last know server )
	MetadataVersion      int             // Version of metadata associated with the chunk
	StatusFlags          []string        // Flags indicating the status of the chunk
}

func NewChunkServer(serverAddr common.ServerAddr, masterAddr common.ServerAddr, root string) *ChunkServer {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("cannot retrieve chunk server hostname")
	}

	log.Info().Msg(fmt.Sprintf("Starting ChunkServer = %s to communicate with @%v", serverAddr, masterAddr))

	// // retrieve the coordinate location of server to calculate the primary and secondaries
	var machineInfo common.MachineInfo
	machineInfo.Hostname = hostname
	machineInfo.RoundTripProximityTime = calculateRoundTripProximity(15, string(masterAddr))

	failureDetector, err := shared.NewFailureDetector(
		string(masterAddr), 2,
		&redis.Options{Addr: "localhost:6379"},
		5*time.Minute,
		shared.SuspicionLevel{
			AccruementThreshold: 70,
			UpperBoundThreshold: 30,
		})
	if err != nil {
		log.Fatal().Msg("failure detector could not be started\n")
	}

	fs := filesystem.NewFileSystem(root)
	cs := &ChunkServer{
		ServerAddr:         serverAddr,
		MasterAddr:         masterAddr,
		MachineInfo:        machineInfo,
		rootDir:            fs,
		leases:             utils.Deque[*common.Lease]{},
		chunks:             make(map[common.ChunkHandle]*chunkInfo),
		shutdownChan:       make(chan os.Signal),
		failureDetectionCh: make(chan string),
		garbage:            utils.Deque[common.ChunkHandle]{},
		isDead:             false,
		archiver:           common.NewArchiver(fs),
		failureDetector:    failureDetector,
		downloadBuffer: NewDBuffer(
			common.DownloadBufferTick,
			common.DownloadBufferItemExpire,
		),
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
		return nil
	}
	err = cs.loadMetadata()
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot load metadata due to error (%s)\n", err))
		return nil
	}

	signal.Notify(cs.shutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func(listener net.Listener) {
		for {
			select {
			case <-cs.shutdownChan:
				cs.failureDetector.ShutdownCh <- true
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
				err := conn.Close()
				if err != nil {
					return
				}
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
		// scheduled interval and obviously it seems the only to send
		// an immediate heartbeat on start up after some time
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
				log.Err(branchInfo.Err).Stack().Send()
			}
			//else {
			// log.Info().Msg(fmt.Sprintf("Server %s background-(%s) event trigged\n", cs.ServerAddr, branchInfo.Event))
			//}
		}
	}()

	// store cold chunks in cold storage
	go func() {
		archiveChunkTicker := time.NewTicker(common.ArchiveChunkInterval)
		var branchInfo common.BranchInfo
		log.Info().Msg("Archival background worker .....")
		for {
			select {
			case <-cs.shutdownChan:
				log.Info().Msg("shutting down archiving workers") // close background workers
				cs.archiver.Close()
				return
			case <-archiveChunkTicker.C:
				branchInfo.Event = string(common.Archival)
				branchInfo.Err = cs.archiveChunks()
			default:
			}
			if branchInfo.Err != nil {
				log.Info().Msg(
					fmt.Sprintf("Server %s  background-(%s) event triggered an error (%s)\n",
						cs.ServerAddr, branchInfo.Event, branchInfo.Err))
				continue
			}
		}
	}()

	log.Printf("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", serverAddr, root, masterAddr)
	return cs
}

//func (cs *ChunkServer) IsAlive() bool {
//	return !cs.isDead
//}

func (cs *ChunkServer) archiveChunks() error {
	chunksToArchive := map[common.ChunkHandle]*chunkInfo{}
	cs.mu.Lock()
	utils.ExtractFromMap(
		cs.chunks,
		chunksToArchive,
		func(value *chunkInfo) bool {
			return time.Until(value.accessTime).Hours()/24 > common.ArchivalDaySpan
		},
	)
	cs.mu.Unlock()

	numberOfCompressionOps := len(chunksToArchive)
	pathCompressionMap := make(map[common.Path]*chunkInfo)
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)

	wg.Add(2)

	go func() {
		defer wg.Done()
		for handle, chkInfo := range chunksToArchive {
			filename := fmt.Sprintf(common.ChunkFileNameFormat, handle)
			fpath := common.Path(filename)
			cs.archiver.CompressPipeline.Task <- fpath
			mu.Lock()
			pathCompressionMap[fpath] = chkInfo
			mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < numberOfCompressionOps; i++ {
			result := <-cs.archiver.CompressPipeline.Result
			if result.Err != nil {
				log.Err(result.Err).Stack().Msg(string(result.Path) + " : " + result.Err.Error())
				continue
			}
			mu.Lock()
			pathCompressionMap[result.Path].isCompressed = true
			mu.Unlock()
			log.Info().Msg(fmt.Sprintf("Compression Action Successfully [%v]\n", result.Path))
		}
	}()
	wg.Wait()

	return nil
}

func (cs *ChunkServer) decompressCompressedChunk(handle common.ChunkHandle) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	chunkInfo, ok := cs.chunks[handle]
	if !ok {
		return fmt.Errorf("cannot attempt to decompress handle [%v]", handle)
	}

	if chunkInfo.isCompressed {
		filename := fmt.Sprintf(common.ChunkFileNameFormat+common.ZIP_EXT, handle)
		cs.archiver.DecompressPipeline.Task <- common.Path(filename)
		result := <-cs.archiver.DecompressPipeline.Result
		if result.Err != nil {
			log.Err(result.Err).Stack().Msg(string(result.Path) + " : " + result.Err.Error())
			return result.Err
		}
		log.Info().Msg(fmt.Sprintf("Decompression Action [%v]\n", result.Path))
		chunkInfo.isCompressed = false
	}

	return nil
}

// Loads meta data information into server memory
func (cs *ChunkServer) loadMetadata() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	file, err := cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDONLY, common.FileMode)
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
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
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Err(err).Stack().Send()
			return
		}
	}(file)

	var metas []PersistedMetaData
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&metas)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("error occurred while loading metadata (%v)", err)
			return err
		}
	}

	log.Info().Msg(fmt.Sprintf("Server %s found metas with length %d", cs.ServerAddr, len(metas)))
	utils.ForEach(metas, func(m PersistedMetaData) {
		log.Info().Msg(fmt.Sprintf(
			"Server %s restoring chunk-%d with version: %d length: %d",
			cs.ServerAddr, m.Handle, m.Version, m.Length))

		cs.chunks[m.Handle] = &chunkInfo{
			length: m.Length, version: m.Version,
			completed: m.Completed, checksum: m.Checksum,
			abandoned: m.Abandoned, creationTime: m.CreationTime,
			accessTime: m.AccessTime, lastModified: m.LastModified,
			replication: m.Replication, mutations: m.Mutations,
			serverStatus: m.ServerStatus,
		}
	})

	return nil
}

func (cs *ChunkServer) heartBeat() error {
	arg := rpc_struct.HeartBeatArg{
		Address:     cs.ServerAddr,
		MachineInfo: cs.MachineInfo,
	}
	if cs.leases.Length() != 0 {
		arg.ExtendLease = true
	}
	var reply rpc_struct.HeartBeatReply
	reply.NetworkData = shared.NetworkData{
		RoundTrip: 0,
		ForwardTrip: shared.TripInfo{
			SentAt: time.Now(),
		},
	}
	if err := shared.UnicastToRPCServer(
		string(cs.MasterAddr),
		"MasterServer.RPCHeartBeatHandler",
		arg, &reply); err != nil {
		log.Err(err).Stack().Msg("cannot call MasterServer.RPCHeartBeatHandler")
		return err
	}
	reply.NetworkData.BackwardTrip.ReceivedAt = time.Now()
	err := cs.failureDetector.RecordSample(reply.NetworkData)
	if err != nil {
		log.Err(err).Stack().Msg("err storing network data for prediction")
		return err
	}
	go func() {
		utils.ForEach(reply.LeaseExtensions, func(lease *common.Lease) {
			cs.leases.PushBack(lease)
		})
	}()
	utils.ForEach(reply.Garbage, func(handle common.ChunkHandle) {
		cs.garbage.PushBack(handle)
	})

	prediction, err := cs.failureDetector.PredictFailure()
	if err != nil {
		log.Err(err).Stack().Msg("")
	}
	log.Info().Msgf("server=%s prediction=%.2f  message=%s", cs.ServerAddr, prediction.Phi, prediction.Message)
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
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Err(err).Stack()
		}
	}(file)

	var metadatas []PersistedMetaData

	for handle, ch := range cs.chunks {
		persistMetadata := PersistedMetaData{
			ChunkSize:       int64(reflect.TypeOf(ch.mutations).Size()) / 1024, // in KB
			Mutations:       ch.mutations,
			Version:         ch.version,
			CreationTime:    ch.creationTime,
			AccessTime:      ch.accessTime,
			Abandoned:       ch.abandoned,
			Replication:     ch.replication,
			ServerStatus:    ch.serverStatus,
			MetadataVersion: time.Now().Second(),
			Length:          ch.length,
			Handle:          handle,
			Checksum:        ch.checksum,
			ServerIP:        string(cs.ServerAddr),
			Completed:       ch.completed,
		}
		metadatas = append(metadatas, persistMetadata)
	}
	log.Printf("Server %v : store metadata len: %v", cs.ServerAddr, len(metadatas))
	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadatas)
}

func (cs *ChunkServer) garbageCollection() error {
	// https://chat.openai.com/c/670528fb-ee6a-4684-8b14-0c9e68366be0
	log.Info().Msg("::: Doing some garabage collection >>> ")
	for cs.garbage.Length() > 0 {
		handle := cs.garbage.PopFront()
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
	err := cs.rootDir.RemoveFile(fmt.Sprintf(common.ChunkFileNameFormat, handle))
	if err == nil {
		return nil
	}
	return cs.rootDir.RemoveFile(fmt.Sprintf(common.ChunkFileNameFormat, handle) + common.ZIP_EXT)
}

func (cs *ChunkServer) Shutdown() {
	if cs.isDead {
		log.Info().Msgf("Server %v is dead\n", cs.ServerAddr)
		return
	}
	cs.isDead = true
	log.Info().Msgf("%s clearing buffer before shutdown >>>...\n", cs.ServerAddr)
	cs.downloadBuffer.Done()
	log.Info().Msgf("Saving metadata >>>...\n")
	if err := cs.persistMetadata(); err != nil {
		log.Err(err).Stack()
	}

	if err := cs.listener.Close(); err != nil {
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

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	chunkInfos := make([]common.PersistedChunkInfo, 0)
	for h, ch := range cs.chunks {
		chunkInfos = append(chunkInfos, common.PersistedChunkInfo{
			Handle:       h,
			Checksum:     ch.checksum,
			Length:       ch.length,
			Version:      ch.version,
			Completed:    ch.completed,
			Abandoned:    ch.abandoned,
			CreationTime: ch.creationTime,
			AccessTime:   ch.accessTime,
			LastModified: ch.lastModified,
			Mutations:    ch.mutations,
			Replication:  ch.replication,
			ServerStatus: ch.serverStatus,
		})
	}

	reply.Chunks = chunkInfos
	reply.SysMem = mem

	log.Info().Msg("<<< Done with sys stat retrieval >>> ")
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
		return nil
	}
	// compare the version provided by the calling server with the server's own
	// determining whether the version here is stale

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// check if the cs is behind by 1
	// synchronized cus we don't want the chinfo mutated while we
	// yet to verify it staleness
	if chinfo.version+common.ChunkVersion(1) == args.Version {
		reply.Stale = false
		chinfo.lastModified = time.Now()
		chinfo.version++ // advance the version by 1
		return nil
	}

	log.Printf("%v :  stale chunk %v", cs.ServerAddr, chinfo)
	chinfo.abandoned = true
	chinfo.lastModified = time.Now()
	reply.Stale = false
	return nil
}

func (cs *ChunkServer) RPCReadChunkHandler(args rpc_struct.ReadChunkArgs, reply *rpc_struct.ReadChunkReply) error {
	cs.mu.RLock()
	chInfo, ok := cs.chunks[args.Handle]
	cs.mu.RUnlock()

	if !ok || chInfo.abandoned {
		return fmt.Errorf("cannot find Chunk %v in available chunks or is abandoned", args.Handle)
	}

	var err error
	reply.Data = make([]byte, args.Length)
	chInfo.RLock()
	n, err := cs.readChunk(args.Handle, args.Offset, reply.Data)
	if err != nil {
		log.Err(err).Stack().Send()
	}
	reply.Length = int64(n)
	chInfo.RUnlock()

	chInfo.Lock()
	chInfo.accessTime = time.Now()
	chInfo.Unlock()

	if err == io.EOF {
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

	cs.chunks[args.Handle] = &chunkInfo{
		length:       0,
		version:      0,
		mutations:    make(map[common.ChunkVersion]common.Mutation),
		isCompressed: false,
		abandoned:    false,
		completed:    false,
		creationTime: time.Now(),
		lastModified: time.Now(),
		replication:  0,
		serverStatus: 200,
	}

	filename := fmt.Sprintf(common.ChunkFileNameFormat, args.Handle)
	return cs.rootDir.CreateFile(filename)
}

func (cs *ChunkServer) RPCForwardDataHandler(args rpc_struct.ForwardDataArgs, reply *rpc_struct.ForwardDataReply) error {
	// check if we have the item previously
	_, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if ok {
		return nil
	}

	log.Printf("storing %v on %v's buffer cache", args.DownloadBufferId, cs.ServerAddr)
	cs.downloadBuffer.Set(args.DownloadBufferId, args.Data)
	if len(args.Replicas) == 0 {
		return nil
	}

	replicaAddr := args.Replicas[0]
	args.Replicas = args.Replicas[1:]
	err := shared.UnicastToRPCServer(string(replicaAddr), "ChunkServer.RPCForwardDataHandler", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) RPCGrantLeaseHandler(args rpc_struct.GrantLeaseInfoArgs, reply *rpc_struct.GrantLeaseInfoReply) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.leases.PushBack(&common.Lease{
		Expire:      args.Expire,
		Primary:     args.Primary,
		Secondaries: args.Secondaries,
		InUse:       true,
	})
	return nil
}

func (cs *ChunkServer) RPCWriteChunkHandler(args rpc_struct.WriteChunkArgs, reply *rpc_struct.WriteChunkReply) error {
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
		return fmt.Errorf("provided data size for write action [%v] is larger than the max allowed data size of %v mb",
			args.DownloadBufferId, common.ChunkMaxSizeInMb)
	}

	log.Info().Msgf("args.Replicas => %#v", args.Replicas)

	lease := cs.leases.PopFront()
	if lease == nil || lease.IsExpired(time.Now()) {
		return fmt.Errorf("could not acquire Write lease / lease has expired")
	}

	if err := doWriteOperation(args, cs, data); err != nil {
		return err
	}
	cs.leases.PopFront()
	return nil
}

func doWriteOperation(args rpc_struct.WriteChunkArgs, cs *ChunkServer, data []byte) error {
	log.Info().Msgf("[writing to chunk] %v", cs.ServerAddr)
	errCh := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup, errCh chan error) {
		defer wg.Done()
		handle := args.DownloadBufferId.Handle
		cs.mu.RLock()
		chInfo, ok := cs.chunks[handle]
		cs.mu.RUnlock()
		log.Info().Msgf("writing to chunk %v : %#v", handle, chInfo)
		if ok && chInfo.isCompressed {
			err := cs.decompressCompressedChunk(handle)
			if err != nil {
				errCh <- err
				return
			}
		} else {
			log.Info().Msgf("chunk [%v] not compressed ", handle)
		}

		if chInfo.completed {
			log.Err(fmt.Errorf("chunk-%v cannot be written to as it is currently complete", handle)).Send()
			errCh <- fmt.Errorf("chunk-%v cannot be written to as it is currently complete", handle)
			return
		}

		if !ok || chInfo.abandoned {
			log.Err(fmt.Errorf("%v is either abandoned or lives in another dimension", handle)).Send()
			errCh <- fmt.Errorf("%v is either abandoned or lives in another dimension", handle)
			return
		}

		mutation := &common.Mutation{
			MutationType: common.MutationWrite,
			Data:         data,
			Offset:       args.Offset,
		}

		go func() {
			log.Info().Msgf("<<< commencing mutation >>>>")
			err := cs.mutate(handle, mutation)
			if err != nil {
				errCh <- err
			}
			chInfo.Lock()
			chInfo.mutations[chInfo.version] = *mutation
			chInfo.Unlock()
			log.Info().Msgf("<<< done with mutation >>>>")
		}()

		var applyMutationReply rpc_struct.ApplyMutationReply
		utils.ForEach(args.Replicas, func(replicaAddr common.ServerAddr) {
			log.Info().Msgf("forwarding data to replica (%v)", replicaAddr)
			err := shared.UnicastToRPCServer(
				string(replicaAddr),
				"ChunkServer.RPCApplyMutationHandler",
				rpc_struct.ApplyMutationArgs{
					DownloadBufferId: args.DownloadBufferId,
					MutationType:     common.MutationWrite,
					Offset:           args.Offset,
				}, &applyMutationReply)
			if err != nil {
				errCh <- err
			}
		})
	}(&wg, errCh)

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errs []string
	for e := range errCh {
		errs = append(errs, e.Error())
	}
	if len(errs) != 0 {
		return errors.New(strings.Join(errs, ";"))
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.chunks[args.DownloadBufferId.Handle].length += common.Offset(len(data))
	return nil
}

func (cs *ChunkServer) RPCApplyMutationHandler(args rpc_struct.ApplyMutationArgs, reply *rpc_struct.ApplyMutationReply) error {

	data, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if !ok {
		reply.ErrorCode = common.DownloadBufferMiss
		return fmt.Errorf(
			"could not locate %v in buffer (might have expired ...)",
			args.DownloadBufferId)
	}

	// calculate the next offset from the previous cursor position
	// assumption is that the data in the buffer is greater than 64 << 20
	dataSize := bToMb(uint64(args.Offset) + uint64(len(data)))
	if dataSize > common.ChunkMaxSizeInMb {
		return fmt.Errorf("provided data size for append action [%v] is larger than the max allowed data size of %v mb", args.DownloadBufferId, common.ChunkMaxSizeInMb)
	}

	handle := args.DownloadBufferId.Handle
	err := cs.decompressCompressedChunk(handle)
	if err != nil {
		return err
	}
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("%v is either abandoned or lives in another dimension", handle)
	}

	mutation := &common.Mutation{
		MutationType: args.MutationType,
		Data:         data,
		Offset:       args.Offset,
	}

	chInfo.Lock()
	defer chInfo.Unlock()
	err = cs.mutate(handle, mutation)
	if err != nil {
		return err
	}

	return nil
}

func (cs *ChunkServer) RPCAppendChunkHandler(args rpc_struct.AppendChunkArgs, reply *rpc_struct.AppendChunkReply) error {
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

	if chInfo.isCompressed {
		err := cs.decompressCompressedChunk(handle)
		if err != nil {
			return err
		}
	}
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
	}

	reply.Offset = offset
	mutation := &common.Mutation{
		MutationType: mutationType,
		Data:         data,
		Offset:       offset,
	}
	errs := make(chan error)
	// update the file on this server (async)
	go func() {
		err := cs.mutate(handle, mutation)
		if err != nil {
			errs <- err
		}
	}()

	applyMutationArgs := rpc_struct.ApplyMutationArgs{
		DownloadBufferId: args.DownloadBufferId,
		MutationType:     mutationType,
		Offset:           offset,
	}

	// forward a written call to secondary server
	go func() {
		utils.ForEach(args.Replicas, func(addr common.ServerAddr) {
			var applyMutationReply rpc_struct.ApplyMutationReply
			err := shared.UnicastToRPCServer(string(addr), "ChunkServer.RPCApplyMutationHandler", applyMutationArgs, &applyMutationReply)
			if err != nil {
				errs <- err
			}
		})
		close(errs)
	}()

	var errArr []string

	for e := range errs {
		errArr = append(errArr, e.Error())
	}

	if len(errArr) != 0 {
		return errors.New(strings.Join(errArr, ";"))
	}
	chInfo.length = newLength
	return nil
}

func (cs *ChunkServer) RPCGetSnapshotHandler(args rpc_struct.GetSnapshotArgs, reply *rpc_struct.GetSnapshotReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	err := cs.decompressCompressedChunk(handle)
	if err != nil {
		return err
	}
	chInfo.RLock()
	defer chInfo.RUnlock()

	data := make([]byte, chInfo.length)
	_, err = cs.readChunk(handle, 0, data)
	if err != nil {
		return err
	}

	var r rpc_struct.ApplyCopyReply

	applyCopyArgs := rpc_struct.ApplyCopyArgs{
		Handle:  handle,
		Data:    data,
		Version: chInfo.version,
	}

	return shared.UnicastToRPCServer(string(args.Replicas), "ChunkServer.RPCApplyCopyHandler", applyCopyArgs, &r)
}

func (cs *ChunkServer) RPCApplyCopyHandler(args rpc_struct.ApplyCopyArgs, reply *rpc_struct.AppendChunkReply) error {
	handle := args.Handle
	err := cs.decompressCompressedChunk(handle)
	if err != nil {
		return err
	}
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	chInfo.Lock()
	defer chInfo.Unlock()

	chInfo.version = args.Version
	err = cs.writeChunk(handle, args.Data, common.MutationWrite, 0, true)
	if err != nil {
		return err
	}
	log.Printf("Server %v : Apply done", cs.ServerAddr)
	return nil
}

//////////////////////////////////////////////
//        HELPER FUNCTIONS
////////////////////////////////////////////

func (cs *ChunkServer) mutate(handle common.ChunkHandle, mutation *common.Mutation) error {
	var shouldLock bool

	if mutation.MutationType == common.MutationAppend || mutation.MutationType == common.MutationWrite {
		shouldLock = true
	} else {
		shouldLock = false
	}

	var err error

	if mutation.MutationType == common.MutationPad {
		mutation.Data = []byte{0}
		err = cs.writeChunk(handle, mutation.Data, mutation.MutationType, common.ChunkMaxSizeInByte-8, shouldLock)
	} else {
		err = cs.writeChunk(handle, mutation.Data, mutation.MutationType, mutation.Offset, shouldLock)
	}
	return err
}

func (cs *ChunkServer) writeChunk(handle common.ChunkHandle, data []byte, mutationType common.MutationType, offset common.Offset, lock bool) error {

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
	fileFlag := os.O_CREATE
	if mutationType == common.MutationAppend {
		fileFlag |= os.O_APPEND
	} else {
		fileFlag |= os.O_RDWR
	}
	fs, err := cs.rootDir.GetFile(fmt.Sprintf(common.ChunkFileNameFormat, handle), fileFlag, 0644)
	if err != nil {
		return err
	}
	defer func(fs *os.File) {
		err := fs.Close()
		if err != nil {
			log.Err(err).Stack()
		}
	}(fs)
	n, err := fs.WriteAt(data, int64(offset))
	if err != nil {
		log.Info().Msgf("error occurred while writing from %v from offset %v", chInfo, offset)
		return err
	}
	log.Printf("Wrote %d from %v from offset %v", n, chInfo, offset)
	chInfo.lastModified = time.Now()
	content, err := io.ReadAll(fs)
	if err != nil {
		log.Info().Msgf("error occurred while reading file for checksum calculation [offset : %v] [chinfo : %v ]", chInfo, offset)
		log.Err(err).Send()
		return err
	}
	if newLen >= common.ChunkMaxSizeInByte {
		chInfo.completed = true
	}
	if len(content) != 0 {
		chInfo.checksum = common.Checksum(utils.ComputeChecksum(string(content)))
	}
	return nil
}

func (cs *ChunkServer) readChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
	filename := fmt.Sprintf(common.ChunkFileNameFormat, handle)

	f, err := cs.rootDir.GetFile(filename, os.O_RDONLY, common.FileMode)
	if err != nil {
		log.Err(err).Stack().Send()
		return -1, err
	}
	defer func(fs *os.File) {
		err := fs.Close()
		if err != nil {
			log.Err(err).Stack()
		}
	}(f)
	log.Printf(
		"Server %v reading data from offset %v for chunk handle [%v] - %s",
		cs.ServerAddr, offset, handle, filename)
	n, err := f.ReadAt(data, int64(offset))
	if err != nil {
		log.Err(err).Stack().Send()
	}
	return n, err
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func calculateRoundTripProximity(duration int, masterAddr string) float64 {
	var buffer bytes.Buffer
	// forked process - not a great solution so far
	cmd := exec.Command("ping", "-c", fmt.Sprintf("%d", duration), masterAddr)
	cmd.Stdout = &buffer
	if cmd.Run() != nil {
		buffer.Reset()
		buffer.WriteString("time=0.00 ms") // use this as a base case
		log.Printf("failed to  ping master (%s) \n", masterAddr)
	}
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
