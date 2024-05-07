package master_server

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/filesystem"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

const (
	// lease expiration time
	leaseTimeout = 1 * time.Second
)

type chunkServerInfo struct {
	// last contact time
	lastHeatBeat time.Time
	// chunks on this server
	chunks     map[common.ChunkHandle]bool
	garbages   []common.ChunkHandle
	serverInfo common.MachineInfo
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

type fileInfo struct {
	sync.RWMutex
	handles []common.ChunkHandle
}

type serialChunkInfo struct {
	Path common.Path
	Info []common.PersistedChunkInfo
}

type PesistentMeta struct {
	Namespace []serializedNsTreeNode
	ChunkInfo []serialChunkInfo
}

type MasterServer struct {
	sync.RWMutex
	ServerAddr       common.ServerAddr
	rootDir          *filesystem.FileSystem
	listener         net.Listener
	namespaceManager *namespaceManager
	isDead           bool
	shutdownChan     chan os.Signal

	// store server heatbeat chunk and garbage for cross
	// master to chunkserver house keep & synchronization
	servers map[common.ServerAddr]*chunkServerInfo
	chunks  map[common.ChunkHandle]*chunkInfo
	files   map[common.Path]*fileInfo

	// It seems if a server fails then we need to create
	//  list  to hold a chunks to move to a locations
	replicaMigrationList       []common.ChunkHandle
	numberOfCreatedChunkHandle common.ChunkHandle
}

func NewMasterServer(serverAddress common.ServerAddr, root string) *MasterServer {
	ma := &MasterServer{
		ServerAddr:                 serverAddress,
		rootDir:                    filesystem.NewFileSystem(root),
		namespaceManager:           NewNameSpaceManager(10 * time.Hour),
		servers:                    make(map[common.ServerAddr]*chunkServerInfo),
		chunks:                     make(map[common.ChunkHandle]*chunkInfo),
		files:                      make(map[common.Path]*fileInfo),
		replicaMigrationList:       make([]common.ChunkHandle, 0),
		shutdownChan:               make(chan os.Signal, 1),
		numberOfCreatedChunkHandle: 0,
	}

	// register rpc server
	rpc := rpc.NewServer()
	err := rpc.Register(ma)

	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return nil
	}
	l, err := net.Listen("tcp", string(ma.ServerAddr))
	if err != nil {
		log.Err(err).Stack().Msg(fmt.Sprintf("cannot start a listener on %s", ma.ServerAddr))
	}

	ma.listener = l
	err = ma.rootDir.MkDir(".")
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot create root directory (%s)\n", root))
	}
	// load metadata that will replicated to another backup server  <to avoid SOF>
	err = ma.loadMetadata()
	if err != nil {
		log.Err(err).Stack()
		log.Fatal().Msg(fmt.Sprintf("cannot load metadata due to error (%s)\n", err))
	}

	// create server listener
	go func(listener net.Listener) {
		defer listener.Close()
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

			// server each connection conncurrently
			go func() {
				rpc.ServeConn(conn)
				conn.Close()
			}()
		}
	}(ma.listener)

	signal.Notify(ma.shutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// background task
	// for doing heartbeat checks, server disconnection,
	// garbage collection, stale replica removal & detection
	go func() {
		persistMetadataCheck := time.NewTicker(common.MasterPersistMetaInterval)

		for {
			var branchInfo common.BranchInfo
			select {
			case <-ma.shutdownChan:
				return
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

func (Ma *MasterServer) loadMetadata() error {
	nmspaceFile, err := Ma.rootDir.GetFile(common.MasterNamespaceMetaDataFileName, os.O_RDONLY, common.FileMode)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			err = Ma.rootDir.CreateFile(common.MasterNamespaceMetaDataFileName)
			if err != nil {
				return err
			}
		}
		nmspaceFile, err = Ma.rootDir.GetFile(common.MasterNamespaceMetaDataFileName, os.O_RDONLY, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer nmspaceFile.Close()

	var serializedNodes []serializedNsTreeNode
	decoder := gob.NewDecoder(nmspaceFile)
	err = decoder.Decode(&serializedNodes)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("error occurred while loading metadata (%v)", err)
			return err
		}
	}

	log.Info().Msg(fmt.Sprintf("Server %s found serializedNodes with length %d", Ma.ServerAddr, len(serializedNodes)))

	if len(serializedNodes) != 0 {
		root := Ma.namespaceManager.Deserialize(serializedNodes)

		if root != nil {
			Ma.namespaceManager.root = root
		}
	}

	file, err := Ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDONLY, common.FileMode)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			err = Ma.rootDir.CreateFile(common.MasterMetaDataFileName)
			if err != nil {
				return err
			}
		}
		file, err = Ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDONLY, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer file.Close()

	var chunkInfos []serialChunkInfo
	decoder = gob.NewDecoder(file)
	err = decoder.Decode(&chunkInfos)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("error occurred while loading metadata (%v)", err)
			return err
		}
	}

	Ma.Lock()
	defer Ma.Unlock()

	for _, chunk := range chunkInfos {
		fileInfo := &fileInfo{
			handles: make([]common.ChunkHandle, 0),
		}
		for _, info := range chunk.Info {
			fileInfo.handles = append(fileInfo.handles, info.Handle)
			Ma.chunks[info.Handle] = &chunkInfo{
				version:  info.Version,
				checksum: info.Checksum,
				path:     chunk.Path,
				expire:   time.Now(),
			}
		}
		Ma.files[chunk.Path] = fileInfo
	}

	return nil
}
func (Ma *MasterServer) Shutdown() {
	if Ma.isDead {
		log.Printf("Server [%s] is dead\n", Ma.ServerAddr)
		return
	}

	if err := Ma.listener.Close(); err != nil {
		log.Err(err).Stack().Send()
	}

	if err := Ma.persistMetaData(); err != nil {
		log.Err(err).Stack().Send()
	}

	close(Ma.shutdownChan)
}

func (Ma *MasterServer) persistMetaData() error {

	nmspaceFile, err := Ma.rootDir.GetFile(common.MasterNamespaceMetaDataFileName, os.O_RDWR, common.FileMode)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			err = Ma.rootDir.CreateFile(common.MasterNamespaceMetaDataFileName)
			if err != nil {
				return err
			}
		}
		nmspaceFile, err = Ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDWR, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer nmspaceFile.Close()

	serizalizedData := Ma.namespaceManager.Serialize()
	encoder := gob.NewEncoder(nmspaceFile)
	err = encoder.Encode(&serizalizedData)
	if err != nil {
		log.Err(err).Stack().Send()
		return err
	}
	file, err := Ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDWR, common.FileMode)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			err = Ma.rootDir.CreateFile(common.MasterMetaDataFileName)
			if err != nil {
				return err
			}
		}
		file, err = Ma.rootDir.GetFile(common.MasterMetaDataFileName, os.O_RDWR, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer file.Close()
	serizalizedChunks := Ma.serializeChunks()
	encoder = gob.NewEncoder(file)
	return encoder.Encode(&serizalizedChunks)
}

// when the client request the location chunk server to
//
//	figure out where to write to. we need to retireve the location of the  server  from the chunk
//
// likewise also need to register it to before that too
func (Ma *MasterServer) GetReplica(handle common.ChunkHandle) ([]common.ServerAddr, error) {
	Ma.RLock()
	chunkInfo, ok := Ma.chunks[handle]
	Ma.RUnlock()

	if !ok {
		return nil, fmt.Errorf("could not retrieve replica for chunk(%v)", handle)
	}

	return chunkInfo.locations, nil
}

func (Ma *MasterServer) RegisterReplicas(handle common.ChunkHandle, addr common.ServerAddr, readLock bool) error {
	var (
		chunkInfo *chunkInfo
		ok        bool
	)

	if readLock {
		Ma.RLock()
		chunkInfo, ok = Ma.chunks[handle]
		Ma.RUnlock()
	} else {
		chunkInfo, ok = Ma.chunks[handle]
	}

	if ok {
		return fmt.Errorf("cannot register the same server address again since it already registered")
	}

	Ma.Lock()
	chunkInfo.locations = append(chunkInfo.locations, addr)
	Ma.Unlock()
	return nil
}

func (Ma *MasterServer) GetChunkHandle(filePath common.Path, idx common.ChunkIndex) (common.ChunkHandle, error) {
	Ma.RLock()
	defer Ma.RUnlock()

	fileInfo, ok := Ma.files[filePath]
	if !ok {
		return -1, fmt.Errorf("cannot get handle for path =>%v[%v]", filePath, idx)
	}

	if idx < 0 || int(idx) >= len(fileInfo.handles) {
		return -1, fmt.Errorf("invalid chunk index %v", idx)
	}
	return fileInfo.handles[common.ChunkHandle(idx)], nil
}

func (Ma *MasterServer) createChunk(path common.Path, addrs []common.ServerAddr) (common.ChunkHandle, []common.ServerAddr, error) {
	Ma.Lock()
	defer Ma.Unlock()

	currentHandle := Ma.numberOfCreatedChunkHandle
	Ma.numberOfCreatedChunkHandle++

	file, ok := Ma.files[path]

	if !ok {
		Ma.files[path] = &fileInfo{
			handles: make([]common.ChunkHandle, 0),
		}
		file = Ma.files[path]
	}
	file.handles = append(file.handles, currentHandle) // record the new chunkhandle for this path

	// create a chunk and update the record on master
	chk := &chunkInfo{path: path}
	Ma.chunks[currentHandle] = chk // record the chunk on the master for later persistence

	errs := []string{}
	success := []string{}

	args := rpc_struct.CreateChunkArgs{Handle: currentHandle}

	utils.ForEach(addrs, func(addr common.ServerAddr) {
		var reply rpc_struct.CreateChunkReply

		err := utils.CallRPCServer(string(addr), "ChunkServer.RPCCreateChunkHandler", args, &reply)

		if err != nil {
			errs = append(errs, err.Error())
			return
		}

		// update this particular chunk information before handing it o
		chk.locations = append(chk.locations, addr)
		success = append(success, string(addr))
	})

	servers := utils.Map(success, func(v string) common.ServerAddr { return common.ServerAddr(v) })
	errStr := strings.Join(errs, ";")

	if len(errs) != 0 {
		Ma.replicaMigrationList = append(Ma.replicaMigrationList, currentHandle)
		return currentHandle, servers, fmt.Errorf(errStr)
	}
	return currentHandle, servers, nil
}

func (Ma *MasterServer) RemoveChunks(handles []common.ChunkHandle, server common.ServerAddr) error {

	errs := []string{}

	for _, handle := range handles {
		// we need to lock read the chunk map
		Ma.RLock()
		chk, ok := Ma.chunks[handle]
		Ma.RUnlock()

		if !ok {
			errs = append(errs, fmt.Sprintf("chunk handle (%v) does not exist", handle))
			continue
		}

		chk.Lock()
		chk.locations = utils.Filter(chk.locations, func(v common.ServerAddr) bool { return v != server })
		chk.expire = time.Now()
		num := len(chk.locations) //  calulate the number of chunk replica if it is less that the
		// the replication factor which is ususally 3 then we need more server to fulfill this
		chk.Unlock()

		if num < common.MinimumReplicationFactor {
			Ma.replicaMigrationList = append(Ma.replicaMigrationList, handle)
			if num == 0 {
				msg := fmt.Sprintf("Lost all replicas of chk (%v)", handle)
				log.Info().Msg(msg)
				errs = append(errs, msg)
			}
		}
	}

	errStr := strings.Join(errs, ";")

	if len(errs) != 0 {
		return fmt.Errorf(errStr)
	}
	return nil
}

func (Ma *MasterServer) GetReplicationNeedList() []common.ChunkHandle {
	Ma.Lock()
	defer Ma.Unlock()

	var newReplicationNeedList []int

	for _, v := range Ma.replicaMigrationList {
		if len(Ma.chunks[v].locations) < common.MinimumReplicationFactor {
			newReplicationNeedList = append(newReplicationNeedList, int(v))
		}
	}

	sort.Ints(newReplicationNeedList)
	Ma.replicaMigrationList = make([]common.ChunkHandle, 0)
	for i, v := range newReplicationNeedList {
		if i == 0 || v != newReplicationNeedList[i-1] {
			Ma.replicaMigrationList = append(Ma.replicaMigrationList, common.ChunkHandle(v))
		}
	}

	if len(Ma.replicaMigrationList) == 0 {
		return nil
	}

	return Ma.replicaMigrationList
}

func (Ma *MasterServer) ExtendLease(handle common.ChunkHandle, primary common.ServerAddr) (*chunkInfo, error) {
	Ma.RLock()
	chk, ok := Ma.chunks[handle]
	Ma.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cannot extend lease for %v", primary)
	}

	if chk.primary != primary && chk.expire.After(time.Now()) {
		return nil, fmt.Errorf("%v does not hold lease for %v ", primary, handle)
	}

	chk.expire = chk.expire.Add(leaseTimeout)
	return chk, nil

}

func (Ma *MasterServer) GetLeaseHolder(handle common.ChunkHandle) (*common.Lease, []common.ServerAddr, error) {
	Ma.RLock()
	chk, ok := Ma.chunks[handle]
	Ma.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("cannot find chunk handle %v - Invalid most likely", handle)
	}

	chk.Lock()
	defer chk.Unlock()

	var staleServers []common.ServerAddr
	lease := &common.Lease{}

	if chk.expire.Before(time.Now()) { // lease has expired so extend it
		chk.version++

		arg := rpc_struct.CheckChunkVersionArg{
			Version: chk.version,
			Handle:  handle,
		}

		var newLocationList []string
		var lock sync.Mutex

		var wg sync.WaitGroup
		wg.Add(len(chk.locations))

		for _, v := range chk.locations {
			go func(addr common.ServerAddr) {
				defer wg.Done()

				var reply rpc_struct.CheckChunkVersionReply

				err := utils.CallRPCServer(string(addr), "ChunkServer.RPCCheckChunkVersionHandler", arg, &reply)
				if err != nil || reply.Stale {
					log.Warn().Msg(fmt.Sprintf("stale chunk %v detected in %v ", handle, addr))
					lock.Lock()
					staleServers = append(staleServers, addr)
					lock.Unlock()
				} else {
					lock.Lock()
					newLocationList = append(newLocationList, string(addr))
					lock.Unlock()
				}
			}(v)
		}

		wg.Wait()

		chk.locations = make([]common.ServerAddr, len(newLocationList))
		for i := range newLocationList {
			chk.locations[i] = common.ServerAddr(newLocationList[i])
		}

		if len(chk.locations) < common.MinimumReplicationFactor {
			Ma.Lock()
			Ma.replicaMigrationList = append(Ma.replicaMigrationList, handle)
			Ma.Unlock()

			if len(chk.locations) == 0 {
				chk.version--
				return nil, nil, fmt.Errorf("no replica for %v", handle)
			}
		}

		chk.primary = chk.locations[0]
		chk.expire = chk.expire.Add(leaseTimeout)
	}

	lease.Primary = chk.primary
	lease.Expire = chk.expire
	lease.Secondaries = utils.Filter(chk.locations, func(v common.ServerAddr) bool { return v != chk.primary })
	return lease, staleServers, nil
}

func (Ma *MasterServer) serializeChunks() []serialChunkInfo {
	Ma.RLock()
	defer Ma.RUnlock()

	var ret []serialChunkInfo
	for k, v := range Ma.files {
		var chunks []common.PersistedChunkInfo
		for _, handle := range v.handles {
			chunks = append(chunks, common.PersistedChunkInfo{
				Handle:   handle,
				Version:  Ma.chunks[handle].version,
				Checksum: "",
				Length:   0,
			})
		}
		ret = append(ret, serialChunkInfo{Path: k, Info: chunks})
	}
	return ret
}

func (Ma *MasterServer) deserializeChunks(files []serialChunkInfo) []common.PersistedChunkInfo {
	Ma.RLock()
	defer Ma.RUnlock()

	now := time.Now()
	for _, v := range files {
		log.Info().Msg(fmt.Sprint("Master restore files ", v.Path))
		f := new(fileInfo)
		for _, ck := range v.Info {
			f.handles = append(f.handles, ck.Handle)
			log.Info().Msg(fmt.Sprint("Master restore files ", ck.Handle))
			Ma.chunks[ck.Handle] = &chunkInfo{
				expire:   now,
				version:  ck.Version,
				checksum: ck.Checksum,
			}
		}
		Ma.numberOfCreatedChunkHandle += common.ChunkHandle(len(v.Info))
		Ma.files[v.Path] = f
	}

	return nil
}

func (Ma *MasterServer) DetectDeadServer() []common.ServerAddr {
	Ma.Lock()
	defer Ma.Unlock()

	var ret []common.ServerAddr
	for serverAddr, chk := range Ma.servers {
		if chk.lastHeatBeat.Add(common.ServerHealthCheckTimeout).Before(time.Now()) {
			// no heartbeat happend since 30sec after our last hearbeat
			ret = append(ret, serverAddr)
		}
	}

	return ret
}

func (Ma *MasterServer) RemoveServer(addr common.ServerAddr) ([]common.ChunkHandle, error) {
	Ma.Lock()
	defer Ma.Unlock()

	chk, ok := Ma.servers[addr]
	if !ok {
		return nil, fmt.Errorf("server %v not found", addr)
	}

	var handles []common.ChunkHandle
	for handle, ok := range chk.chunks {
		if ok {
			handles = append(handles, handle)
		}
	}

	delete(Ma.servers, addr)

	return handles, nil
}

// In a HeartBeat message regularly exchanged with the master,
// each chunkserver reports a subset of the chunks it has, and
// the master replies with the identity of all chunks that are no
// longer present in the master’s metadata. The chunkserver
// is free to delete its replicas of such chunks.
func (Ma *MasterServer) HeartBeat(addr common.ServerAddr, info common.MachineInfo, reply *rpc_struct.HeartBeatReply) bool {
	Ma.RLock()
	srv, ok := Ma.servers[addr]
	Ma.RUnlock()
	if !ok {
		log.Info().Msg(fmt.Sprintf("adding new server %v to master", addr))
		Ma.Lock()
		Ma.servers[addr] = &chunkServerInfo{
			lastHeatBeat: reply.LastHeartBeat,
			garbages:     make([]common.ChunkHandle, 0),
			chunks:       make(map[common.ChunkHandle]bool),
			serverInfo:   info,
		}
		Ma.Unlock()
		return true
	} else {
		reply.Garbage = srv.garbages
		srv.garbages = make([]common.ChunkHandle, 0)
		srv.lastHeatBeat = time.Now()
	}

	return false
}

func (Ma *MasterServer) addChunk(addrs []common.ServerAddr, handle common.ChunkHandle) {
	for _, v := range addrs {
		Ma.RLock()
		sv, ok := Ma.servers[v]
		Ma.RUnlock()
		if ok {
			Ma.Lock()
			sv.chunks[handle] = true
			Ma.Unlock()
		} else {
			log.Warn().Msg(fmt.Sprintf("add chunk in removed server %v", sv))
		}
	}
}

func (Ma *MasterServer) addGarbage(addr common.ServerAddr, handle common.ChunkHandle) {
	Ma.Lock()
	defer Ma.Unlock()

	sv, ok := Ma.servers[addr]
	if ok {
		sv.garbages = append(sv.garbages, handle)
	}
}

func (Ma *MasterServer) chooseServers(num int) ([]common.ServerAddr, error) {
	type addrToRRTL struct {
		addr common.ServerAddr
		rrtl float64
	}

	if num > len(Ma.servers) {
		return nil, fmt.Errorf("no enough servers for %v replicas", num)
	}
	var (
		intermediateArr []addrToRRTL
		ret             []common.ServerAddr
	)

	Ma.RLock()
	for a, server := range Ma.servers {
		intermediateArr = append(intermediateArr, addrToRRTL{
			addr: a,
			rrtl: server.serverInfo.RoundTripProximityTime,
		})
	}
	Ma.RUnlock()

	slices.SortFunc(intermediateArr, func(a, b addrToRRTL) int {
		if a.rrtl < b.rrtl {
			return -1
		} else if a.rrtl > b.rrtl {
			return 1
		}
		return 0
	})

	all := utils.Map(intermediateArr, func(d addrToRRTL) common.ServerAddr { return d.addr })
	choose, err := utils.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

func (csm *MasterServer) chooseReplicationServer(handle common.ChunkHandle) (from, to common.ServerAddr, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil
	for a, v := range csm.servers {
		if v.chunks[handle] {
			from = a
		} else {
			to = a
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("no enough server for replica %v", handle)
	return
}

// ///////////////////////////////////
//
//	RPC METHODS
//
// /////////////////////////////////
func (csm *MasterServer) RPCHeartBeatHandler(args rpc_struct.HeartBeatArg, reply *rpc_struct.HeartBeatReply) error {
	firstHeartBeat := csm.HeartBeat(args.Address, args.MachineInfo, reply)
	newLeases := []*common.Lease{}
	/*

		When writing a chunk and a lease expires, the chunkserver takes the following actions:

		The chunkserver sends a heartbeat message to the master to inform it about the expired lease. ​
		The master checks if the lease has expired and if the chunkserver has acknowledged the expiration. ​
		If the lease has indeed expired and the chunkserver has acknowledged it, the master revokes the lease from the chunkserver. ​
		The master then selects a new primary replica for the chunk and assigns a new lease to the selected replica. ​
		The master sends a message to the new primary replica, informing it about the new lease and instructing it to take over as the primary replica for the chunk. ​
		The new primary replica takes over the responsibility of handling client requests for the chunk and continues the write operation. ​
		If there were any other replicas for the chunk, they are also notified about the new primary replica and update their metadata accordingly. ​
		The chunkserver that had the expired lease becomes a secondary replica for the chunk and continues to store and serve the chunk data. ​

		When a lease expires, the chunk server does not stop working immediately.
		Instead, it continues to function and serve the chunk data as a secondary replica. ​
		The chunk server loses its primary status and the responsibility for handling client requests,
		but it remains operational and participates in replication and data consistency operations for the chunk. ​
		The expired lease triggers a process where the master selects a new primary replica to take over the primary responsibilities. ​

	*/
	for _, lease := range reply.LeaseExtensions {
		chk, err := csm.ExtendLease(lease.Handle, lease.Primary)
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			// ?? who is th primary here if the lease has  expired
			// ??  who becomes the secondary
			currentPrimary := lease.Primary
			newPrimary := lease.Secondaries[0]
			lease.Secondaries = lease.Secondaries[1:]
			lease.Secondaries = append(lease.Secondaries, currentPrimary)
			newLeases = append(newLeases, &common.Lease{
				Expire:      lease.Expire.Add(leaseTimeout),
				Handle:      lease.Handle,
				InUse:       false,
				Primary:     newPrimary,
				Secondaries: lease.Secondaries,
			})
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

	if firstHeartBeat {
		systemReportArg := rpc_struct.SysReportInfoArg{}
		systemReportReply := rpc_struct.SysReportInfoReply{}
		err := utils.CallRPCServer(string(args.Address), "ChunkServer.RPCSysReportHandler", systemReportArg, &systemReportReply)
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return err
		}
		log.Info().Msg(fmt.Sprintf("Got %#v from ChunkServer = %s", systemReportReply, args.Address))

		if len(systemReportReply.Chunks) != 0 {
			// TODO: Use a SOC to break the master server into component
			for _, chunkInfo := range systemReportReply.Chunks {
				csm.RLock()
				chk, ok := csm.chunks[chunkInfo.Handle]
				csm.RUnlock()
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

						err := utils.CallRPCServer(string(chk.primary), "ChunkServer.RPCCheckChunkVersion", chunkVersionArg, &chunkVersionReply)
						if err != nil {
							log.Err(err).Stack().Msg(err.Error())
							return err
						}
						if chunkVersionReply.Stale {
							log.Info().Msg(fmt.Sprintf("=> requesting chunkserver %v to record as garbage since chunk is stale", args.Address))
							reply.Garbage = append(reply.Garbage, chunkInfo.Handle)
						}
					} else {
						if err := csm.RegisterReplicas(chunkInfo.Handle, args.Address, true); err != nil {
							log.Err(err).Stack().Msg(err.Error())
						}
						csm.addChunk([]common.ServerAddr{args.Address}, chunkInfo.Handle)
					}
				}

			}
		}
	} else {
		log.Info().Msg(fmt.Sprintf("Got <HEART_BEAT> from ChunkServer = %s", args.Address))
	}

	return nil
}

func (csm *MasterServer) RPCGetPrimaryAndSecondaryServersInfoHandler(
	args rpc_struct.PrimaryAndSecondaryServersInfoArg, reply *rpc_struct.PrimaryAndSecondaryServersInfoReply) error {
	lease, staleServers, err := csm.GetLeaseHolder(args.Handle)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	utils.ForEach(staleServers, func(v common.ServerAddr) { csm.addGarbage(v, args.Handle) })
	reply.Expire = lease.Expire
	reply.SecondaryServers = lease.Secondaries
	reply.Primary = lease.Primary
	return nil
}

func (csm *MasterServer) RPCGetChunkHandleHandler(args rpc_struct.GetChunkHandleArgs, reply *rpc_struct.GetChunkHandleReply) error {
	err := csm.namespaceManager.MkDirAll(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	pds := strings.Split(string(args.Path), "/")
	fp := args.Path + "/" + common.Path(pds[len(pds)-1])
	err = csm.namespaceManager.Create(fp)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	file, err := csm.namespaceManager.Get(args.Path)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	file.Lock()
	defer file.Unlock()
	if args.Index == common.ChunkIndex(file.chunks) {
		file.chunks++
		addrs, err := csm.chooseServers(common.MinimumReplicationFactor) // sample out of the servers we have
		if err != nil {
			return err
		}
		reply.Handle, addrs, err = csm.createChunk(fp, addrs)
		if err != nil {
			return err
		}
		csm.addChunk(addrs, reply.Handle)
	} else {
		reply.Handle, err = csm.GetChunkHandle(args.Path, args.Index)
		if err != nil {
			return err
		}
	}

	return err
}
