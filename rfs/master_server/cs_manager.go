package master_server

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/shared"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type CSManager struct {
	sync.RWMutex                         // global lock
	serverMutex, chunkMutex sync.RWMutex // individual locks for server and chunks
	servers                 map[common.ServerAddr]*chunkServerInfo
	chunks                  map[common.ChunkHandle]*chunkInfo
	replicaMigration        []common.ChunkHandle
	// store server heatbeat chunk and garbage for cross
	// master to chunkserver house keep & synchronization
	// servers             map[common.ServerAddr]*chunkServerInfo
	files                      map[common.Path]*fileInfo
	handleToPathMapping        map[common.ChunkHandle]common.Path
	numberOfCreatedChunkHandle common.ChunkHandle
}

func NewCSManager() *CSManager {
	return &CSManager{
		servers:                    make(map[common.ServerAddr]*chunkServerInfo),
		chunkMutex:                 sync.RWMutex{},
		serverMutex:                sync.RWMutex{},
		chunks:                     make(map[common.ChunkHandle]*chunkInfo),
		replicaMigration:           make([]common.ChunkHandle, 0),
		files:                      make(map[common.Path]*fileInfo),
		handleToPathMapping:        make(map[common.ChunkHandle]common.Path),
		numberOfCreatedChunkHandle: 0,
	}
}

func (csm *CSManager) getReplicas(handle common.ChunkHandle) ([]common.ServerAddr, error) {
	csm.chunkMutex.RLock()
	chunkInfo, ok := csm.chunks[handle]
	csm.chunkMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("could not retrieve replica for chunk(%v)", handle)
	}

	return chunkInfo.locations, nil
}

func (csm *CSManager) getGarbages() []common.ChunkHandle {
	csm.RLock()
	defer csm.RUnlock()
	garbageChunks := make([]common.ChunkHandle, 0)
	for _, srv := range csm.servers {
		garbageChunks = append(garbageChunks, srv.garbages...)
	}
	return garbageChunks
}
func (csm *CSManager) registerReplicas(handle common.ChunkHandle, addr common.ServerAddr, readLock bool) error {
	var (
		chunkInfo *chunkInfo
		ok        bool
	)

	if readLock {
		csm.chunkMutex.RLock()
		chunkInfo, ok = csm.chunks[handle]
		csm.chunkMutex.RUnlock()

		//  not necessary since the chunk has been locked outside
		// csm.Lock()
		// defer csm.Unlock()
	} else {
		csm.Lock()
		defer csm.Unlock()
		chunkInfo, ok = csm.chunks[handle]
	}

	if !ok {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	chunkInfo.locations = append(chunkInfo.locations, addr)
	log.Info().Msgf("Registering replicas for handle=%v with location=%v data=%v", handle, chunkInfo.locations, chunkInfo)
	return nil
}

func (csm *CSManager) detectDeadServer() []common.ServerAddr {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	var ret []common.ServerAddr
	for serverAddr, chk := range csm.servers {
		if time.Now().After(chk.lastHeatBeat.Add(common.ServerHealthCheckTimeout)) {
			ret = append(ret, serverAddr)
		}
	}

	log.Print("===== DEAD SERVERS =====\n")
	utils.ForEach(ret, func(v common.ServerAddr) { log.Printf(">. %v", v) })
	log.Print("===== DEAD SERVERS =====\n")
	return ret
}

func (csm *CSManager) removeChunks(handles []common.ChunkHandle, server common.ServerAddr) error {

	errs := []string{}

	for _, handle := range handles {
		// we need to lock read the chunk map
		csm.chunkMutex.Lock()
		chk, ok := csm.chunks[handle]
		csm.chunkMutex.Unlock()

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
			csm.replicaMigration = append(csm.replicaMigration, handle)
			if num == 0 {
				msg := fmt.Sprintf("Lost all replicas of chk (%v)", handle)
				log.Info().Msg(msg)
				errs = append(errs, msg)
			}
		}
	}

	errStr := strings.Join(errs, ";")

	if len(errs) != 0 {
		return errors.New(errStr)
	}
	return nil
}

func (csm *CSManager) removeServer(addr common.ServerAddr) ([]common.ChunkHandle, error) {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	chk, ok := csm.servers[addr]
	if !ok {
		return nil, fmt.Errorf("server %v not found", addr)
	}

	var handles []common.ChunkHandle
	utils.LoopOverMap(chk.chunks, func(handle common.ChunkHandle, _ bool) { handles = append(handles, handle) })

	delete(csm.servers, addr)
	return handles, nil
}

func (csm *CSManager) addChunk(addrs []common.ServerAddr, handle common.ChunkHandle) {
	for _, v := range addrs {
		csm.serverMutex.RLock()
		sv, ok := csm.servers[v]
		csm.serverMutex.RUnlock()
		if ok {
			sv.Lock()
			sv.chunks[handle] = true
			sv.Unlock()
		} else {
			log.Warn().Msg(fmt.Sprintf("add chunk in removed server %v", sv))
		}
	}
}

func (csm *CSManager) addGarbage(addr common.ServerAddr, handle common.ChunkHandle) {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	sv, ok := csm.servers[addr]
	if ok {
		sv.Lock()
		sv.garbages = append(sv.garbages, handle)
		sv.Unlock()
	}
}

func (csm *CSManager) getReplicationMigrationList() []common.ChunkHandle {
	csm.Lock()
	defer csm.Unlock()

	var newReplicationList []int

	utils.ForEach(csm.replicaMigration, func(v common.ChunkHandle) {
		if len(csm.chunks[v].locations) < common.MinimumReplicationFactor {
			newReplicationList = append(newReplicationList, int(v))
		}
	})

	sort.Ints(newReplicationList)
	csm.replicaMigration = make([]common.ChunkHandle, 0)
	for i, v := range csm.replicaMigration {
		if i == 0 || v != common.ChunkHandle(newReplicationList[i-1]) { // avoid duplicate
			csm.replicaMigration = append(csm.replicaMigration, common.ChunkHandle(v))
		}
	}

	return csm.replicaMigration
}

func (csm *CSManager) extendLease(handle common.ChunkHandle, primary common.ServerAddr) (*chunkInfo, error) {
	csm.chunkMutex.RLock()
	chk, ok := csm.chunks[handle]
	csm.chunkMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cannot extend lease for %v", primary)
	}

	if chk.primary != primary && chk.expire.After(time.Now()) {
		return nil, fmt.Errorf("%v does not hold lease for %v ", primary, handle)
	}

	chk.expire = chk.expire.Add(common.LeaseTimeout)
	return chk, nil

}

func (csm *CSManager) getLeaseHolder(handle common.ChunkHandle) (*common.Lease, []common.ServerAddr, error) {
	csm.chunkMutex.RLock()
	chk, ok := csm.chunks[handle]
	csm.chunkMutex.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("cannot find chunk handle %v - Invalid most likely", handle)
	}

	chk.Lock()
	defer chk.Unlock()

	var staleServers []common.ServerAddr
	lease := &common.Lease{}

	// log.Info().Msgf("reading chunk [%#v] in order to produce new lease with locations -> %v", chk, chk.locations)
	if chk.isExpired(time.Now()) { // chunk has expired so move it
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

				err := shared.UnicastToRPCServer(string(addr), "ChunkServer.RPCCheckChunkVersionHandler", arg, &reply)
				lock.Lock()
				defer lock.Unlock()
				if err != nil || reply.Stale {
					log.Warn().Msg(fmt.Sprintf("stale chunk %v detected in %v ", handle, addr))
					staleServers = append(staleServers, addr)
					return
				}
				newLocationList = append(newLocationList, string(addr))
			}(v)
		}

		wg.Wait()

		chk.locations = make([]common.ServerAddr, 0)
		utils.ForEach(newLocationList, func(v string) { chk.locations = append(chk.locations, common.ServerAddr(v)) })

		if len(chk.locations) < common.MinimumReplicationFactor {
			csm.Lock()
			csm.replicaMigration = append(csm.replicaMigration, handle) // migrate the chunk
			csm.Unlock()

			if len(chk.locations) == 0 {
				chk.version--
				return nil, nil, fmt.Errorf("no replica for %v", handle)
			}
		} else {
			chk.primary = chk.locations[0]
			chk.expire = chk.expire.Add(common.LeaseTimeout)
		}
	}

	lease.Primary = chk.primary
	lease.Secondaries = utils.Filter(chk.locations, func(v common.ServerAddr) bool { return v != chk.primary })

	if lease.Primary == "" && len(lease.Secondaries) > 0 {
		lease.Primary = lease.Secondaries[0]
		lease.Secondaries = lease.Secondaries[1:]
	}
	lease.Expire = time.Now().Add(common.LeaseTimeout)
	go func() {
		var (
			args  rpc_struct.GrantLeaseInfoArgs
			reply rpc_struct.GrantLeaseInfoReply
		)
		args.Expire = lease.Expire
		args.Primary = lease.Primary
		args.Secondaries = lease.Secondaries
		args.Handle = lease.Handle
		err := shared.UnicastToRPCServer(string(lease.Primary), "ChunkServer.RPCGrantLeaseHandler", args, &reply)
		if err != nil {
			log.Err(err).Stack().Send()
			log.Warn().Msg(fmt.Sprintf("could not grant lease to primary = %v", chk.primary))
		}
	}()

	return lease, staleServers, nil
}

func (csm *CSManager) chooseServers(num int) ([]common.ServerAddr, error) {
	type addrToRRTL struct {
		addr common.ServerAddr
		rrtl float64
	}

	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	if num > len(csm.servers) {
		return nil, fmt.Errorf("no enough servers for %v replicas", num)
	}

	var (
		intermediateArr []addrToRRTL
		ret             []common.ServerAddr
	)

	for a, server := range csm.servers {
		intermediateArr = append(intermediateArr, addrToRRTL{
			addr: a,
			rrtl: server.serverInfo.RoundTripProximityTime,
		})
	}

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

	log.Info().Msg(fmt.Sprintf("Chose servers %v for replication", ret))
	return ret, nil
}

func (csm *CSManager) createChunk(path common.Path, addrs []common.ServerAddr) (common.ChunkHandle, []common.ServerAddr, error) {
	csm.Lock()
	defer csm.Unlock()

	currentHandle := csm.numberOfCreatedChunkHandle
	csm.numberOfCreatedChunkHandle++

	file, ok := csm.files[path]

	if !ok {
		csm.files[path] = &fileInfo{
			handles: make([]common.ChunkHandle, 0),
		}
		file = csm.files[path]
	}
	file.handles = append(file.handles, currentHandle) // record the new chunkhandle for this path
	csm.handleToPathMapping[currentHandle] = path

	// create a chunk and update the record on master
	chk := &chunkInfo{
		path:   path,
		expire: time.Now().Add(common.LeaseTimeout),
	}
	csm.chunks[currentHandle] = chk // record the chunk on the master for later persistence

	errs := []string{}
	success := []string{}

	args := rpc_struct.CreateChunkArgs{Handle: currentHandle}

	utils.ForEach(addrs, func(addr common.ServerAddr) {
		var reply rpc_struct.CreateChunkReply
		err := shared.UnicastToRPCServer(string(addr), "ChunkServer.RPCCreateChunkHandler", args, &reply)
		if err != nil {
			errs = append(errs, err.Error())
		} else {
			// update this particular chunk information before handing it o
			chk.Lock()
			defer chk.Unlock()
			chk.locations = append(chk.locations, addr)
			success = append(success, string(addr))
			log.Info().Msg(fmt.Sprintf("chk location updated after creation locations => %v", chk.locations))
		}
	})

	servers := utils.Map(success, func(v string) common.ServerAddr { return common.ServerAddr(v) })
	errStr := strings.Join(errs, ";")

	// if err occurred during the creation of chunk, then
	// we register chunk for re-migration
	if len(errs) != 0 {
		log.Err(errors.New(errStr)).Stack()
		csm.replicaMigration = append(csm.replicaMigration, currentHandle)
		return currentHandle, servers, fmt.Errorf(errStr)
	}
	return currentHandle, servers, nil
}

func (csm *CSManager) chooseReplicationServer(handle common.ChunkHandle) (from, to common.ServerAddr, err error) {
	csm.serverMutex.RLock()
	defer csm.serverMutex.RUnlock()

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

func (csm *CSManager) SerializeChunks() []serialChunkInfo {
	csm.RLock()
	defer csm.RUnlock()

	var ret []serialChunkInfo
	for k, v := range csm.files {
		var chunks []common.PersistedChunkInfo
		for _, handle := range v.handles {
			chunks = append(chunks, common.PersistedChunkInfo{
				Handle:   handle,
				Version:  csm.chunks[handle].version,
				Checksum: "",
				Length:   0,
			})
		}
		ret = append(ret, serialChunkInfo{Path: k, Info: chunks})
	}
	return ret
}

func (csm *CSManager) HeartBeat(addr common.ServerAddr, info common.MachineInfo, reply *rpc_struct.HeartBeatReply) bool {
	csm.serverMutex.RLock()
	srv, ok := csm.servers[addr]
	csm.serverMutex.RUnlock()
	if !ok {
		log.Info().Msg(fmt.Sprintf("adding new server %v to master", addr))
		csm.serverMutex.Lock()
		csm.servers[addr] = &chunkServerInfo{
			lastHeatBeat: reply.LastHeartBeat,
			garbages:     make([]common.ChunkHandle, 0),
			chunks:       make(map[common.ChunkHandle]bool),
			serverInfo:   info,
		}
		csm.serverMutex.Unlock()
		return true
	} else {
		srv.Lock()
		reply.Garbage = srv.garbages
		srv.garbages = make([]common.ChunkHandle, 0)
		srv.lastHeatBeat = time.Now()
		srv.Unlock()
	}

	return false
}

func (csm *CSManager) DeserializeChunks(chunkInfos []serialChunkInfo) {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()

	for _, chunk := range chunkInfos {
		log.Info().Msg("ChunkServerManager restore files " + string(chunk.Path))
		fileInfo := &fileInfo{
			handles: make([]common.ChunkHandle, 0),
		}
		for _, info := range chunk.Info {
			log.Info().Msg(fmt.Sprintf("ChunkServerManager restore handle %v", info.Handle))
			fileInfo.handles = append(fileInfo.handles, info.Handle)
			csm.chunks[info.Handle] = &chunkInfo{
				version:  info.Version,
				checksum: info.Checksum,
				path:     chunk.Path,
				expire:   time.Now(),
			}
			csm.handleToPathMapping[info.Handle] = chunk.Path
		}
		csm.files[chunk.Path] = fileInfo
		csm.numberOfCreatedChunkHandle += common.ChunkHandle(len(fileInfo.handles))
	}
}

func (csm *CSManager) getChunkHandle(filePath common.Path, idx common.ChunkIndex) (common.ChunkHandle, error) {
	csm.RLock()
	defer csm.RUnlock()

	fileInfo, ok := csm.files[filePath]
	if !ok {
		return -1, fmt.Errorf("cannot get handle for path => %v-%v", filePath, idx)
	}

	if idx < 0 || int(idx) >= len(fileInfo.handles) {
		return -1, fmt.Errorf("invalid chunk index %v", idx)
	}
	return fileInfo.handles[common.ChunkHandle(idx)], nil
}

func (csm *CSManager) getChunk(handle common.ChunkHandle) (*chunkInfo, bool) {
	csm.chunkMutex.RLock()
	defer csm.chunkMutex.RUnlock()
	chk, ok := csm.chunks[handle]
	return chk, ok
}

func (csm *CSManager) deleteChunk(handle common.ChunkHandle) {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()
	delete(csm.chunks, handle)
}

func (csm *CSManager) GetChunkHandles(filePath common.Path) ([]common.ChunkHandle, error) {
	csm.RLock()
	defer csm.RUnlock()

	fileInfo, ok := csm.files[filePath]
	if !ok {
		return nil, fmt.Errorf("cannot get handles for path => %v", filePath)
	}

	var ret []common.ChunkHandle
	copy(ret, fileInfo.handles)
	return ret, nil
}

func (csm *CSManager) UpdateFilePath(source common.Path, target common.Path) error {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()

	if _, ok := csm.files[source]; !ok {
		return fmt.Errorf("could not locate file with path = %s", source)
	}

	val := csm.files[source]
	delete(csm.files, source)
	csm.files[target] = val
	return nil
}
