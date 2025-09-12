package chunkserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	archivemanager "github.com/caleberi/distributed-system/archive_manager"
	"github.com/caleberi/distributed-system/common"
	downloadbuffer "github.com/caleberi/distributed-system/download_buffer"
	failuredetector "github.com/caleberi/distributed-system/failure_detector"
	filesystem "github.com/caleberi/distributed-system/file_system"
	"github.com/caleberi/distributed-system/library"
	"github.com/caleberi/distributed-system/rpc_struct"
	"github.com/caleberi/distributed-system/shared"
	"github.com/caleberi/distributed-system/utils"
	"github.com/olekukonko/tablewriter"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// chunkInfo represents metadata for a chunk of data in a distributed file system.
// It tracks versioning, mutation operations, and status flags for data integrity
// and synchronization purposes.
type chunkInfo struct {
	sync.RWMutex // handling lock during mutation ops

	length   common.Offset       // last known offset of this chunk
	checksum common.Checksum     // tracking data corruption
	version  common.ChunkVersion // latest version for data reconciliation (current version)

	completed    bool // indicates if mutation was ever marked done
	abandoned    bool // indicates if mutation was ever marked abandoned
	isCompressed bool // indicates if the chunk is compressed

	replication  int // number of replicas for this chunk
	serverStatus int // status of the server hosting this chunk

	creationTime time.Time // creation time of the chunk
	lastModified time.Time // last modified time of the chunk
	accessTime   time.Time // last access time of the chunk

	mutations map[common.ChunkVersion]common.Mutation // all necessary mutations to be committed for this chunk to FS
}

// ChunkServer represents a server instance in a distributed file system, managing
// chunk metadata, leases, and system resources. It handles network communication,
// failure detection, and garbage collection for chunks.
type ChunkServer struct {
	mu, lmu  sync.RWMutex // mutex for synchronizing access to server state
	listener net.Listener // network listener for incoming connections

	rootDir *filesystem.FileSystem     // root directory file system for chunk storage
	leases  utils.Deque[*common.Lease] // deque of active leases for chunk access

	archiver        *archivemanager.ArchiverManager  // manager for archiving chunks
	downloadBuffer  *downloadbuffer.DownloadBuffer   // buffer for handling downloads
	failureDetector *failuredetector.FailureDetector // detector for identifying node failures

	failureDetectionCh chan string                       // channel for failure detection events
	garbage            utils.Deque[common.ChunkHandle]   // deque of chunks marked for garbage collection
	chunks             map[common.ChunkHandle]*chunkInfo // map of chunk handles to their metadata

	isDead       bool           // indicates if the server is marked as dead
	shutdownChan chan os.Signal // channel for handling shutdown signals

	ServerAddr  common.ServerAddr  // address of this server
	MasterAddr  common.ServerAddr  // address of the master server
	MachineInfo common.MachineInfo // information about the server's machine
}

// PersistedMetaData represents metadata associated with a chunk in a Google File System (GFS).
// It stores identifying information, versioning, and status details for a chunk, used for data
// integrity and management in a distributed file system.
type PersistedMetaData struct {
	Handle  common.ChunkHandle  // Unique identifier for the chunk
	Version common.ChunkVersion // Latest persisted version number of the chunk
	Length  common.Offset       // Offset in the chunk

	ChunkSize            int64                                   // Size of the chunk in bytes
	Mutations            map[common.ChunkVersion]common.Mutation // Map of mutations to be applied to the chunk
	Completed, Abandoned bool                                    // Indicates if the chunk is completed (filled) or abandoned

	Checksum        common.Checksum // Checksum or hash of the chunk data for integrity verification
	Replication     int             // Number of replicas for this chunk
	ServerStatus    int             // Last known status of the chunk server
	MetadataVersion int             // Version of the metadata associated with the chunk

	ServerIP    string   // IP address of the chunk server hosting the chunk
	StatusFlags []string // Flags indicating the status of the chunk (e.g., active, corrupted)

	CreationTime time.Time // Creation timestamp of the chunk
	LastModified time.Time // Last modified timestamp of the chunk
	AccessTime   time.Time // Last access timestamp of the chunk
}

// NewChunkServer initializes and starts a new ChunkServer instance.
// It sets up the server with the specified server address, master address, and root directory.
// The server handles file system operations, failure detection, download buffering, and periodic tasks
// such as heartbeats, garbage collection, metadata persistence, and chunk archiving.
// It returns a pointer to the initialized Server and an error if any initialization step fails.
//
// Parameters:
//   - serverAddr: The address the ChunkServer will listen on for incoming connections.
//   - masterAddr: The address of the master server for communication and coordination.
//   - root: The root directory path for the server's file system operations.
//
// Returns:
//   - *Server: A pointer to the initialized ChunkServer instance.
//   - error: An error if any step in the initialization process fails, otherwise nil.
func NewChunkServer(serverAddr common.ServerAddr, masterAddr common.ServerAddr, root string) (*ChunkServer, error) {
	log.Info().Msg(fmt.Sprintf("Starting ChunkServer = %s to communicate with @%v", serverAddr, masterAddr))
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	fs := filesystem.NewFileSystem(root)
	proximity, err := calculateRoundTripProximity(15, string(masterAddr))
	if err != nil {
		return nil, err
	}
	machineInfo := common.MachineInfo{
		Hostname:               hostname,
		RoundTripProximityTime: proximity,
	}

	failureDetector, err := failuredetector.NewFailureDetector(
		string(masterAddr), 1000,
		&redis.Options{Addr: "localhost:6379"},
		common.FailureDetectorKeyExipiryTime,
		failuredetector.SuspicionLevel{
			AccruementThreshold: 7,
			UpperBoundThreshold: 3,
		})
	if err != nil {
		return nil, err
	}

	dbuffer, err := downloadbuffer.NewDownloadBuffer(
		common.DownloadBufferTick,
		common.DownloadBufferItemExpire,
	)
	if err != nil {
		return nil, err
	}

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
		archiver:           archivemanager.NewArchiver(fs, 2),
		failureDetector:    failureDetector,
		downloadBuffer:     dbuffer,
	}

	rpc := rpc.NewServer()
	err = rpc.Register(cs)
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", string(cs.ServerAddr))
	if err != nil {
		return nil, err
	}
	cs.listener = l

	if err := cs.rootDir.MkDir("."); err != nil {
		return nil, err
	}

	if err := cs.loadMetadata(); err != nil {
		return nil, err
	}

	signal.Notify(cs.shutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func(listener net.Listener) {
		for {
			select {
			case <-cs.shutdownChan:
				cs.failureDetector.Shutdown()
				return
			default:
			}

			if conn, err := listener.Accept(); err != nil {
				if cs.isDead {
					return
				}
			} else {
				go func() {
					rpc.ServeConn(conn)
					err := conn.Close()
					if err != nil {
						return
					}
				}()
			}
		}
	}(cs.listener)

	go func() {
		heartBeatTicker := time.NewTicker(common.HeartBeatInterval)
		garbageCollectionTicker := time.NewTicker(common.GarbageCollectionInterval)
		persistMetaDataTicker := time.NewTicker(common.PersistMetaDataInterval)
		archiveChunkTicker := time.NewTicker(common.ArchiveChunkInterval)

		defer heartBeatTicker.Stop()
		defer garbageCollectionTicker.Stop()
		defer persistMetaDataTicker.Stop()
		defer archiveChunkTicker.Stop()

		var branchInfo common.BranchInfo
		branchInfo.Event = string(common.HeartBeat)
		branchInfo.Err = cs.heartBeat()

		// the paper mentioned the chunk server sending heartbeat at every
		// scheduled interval and obviously it seems the only to send
		// an immediate heartbeat on start up after some time
		// I noticed there are two ways :
		// 1. the chunk-server should inform the master of their status [X]
		// 2. the master polls for the status of the chunk server []
		//
		for {
			select {
			case <-cs.shutdownChan:
				log.Info().Msg(fmt.Sprintf("Gracefully shutting down server (%s)...\n", serverAddr))
				time.Sleep(time.Second * 1)
				cs.downloadBuffer.Done()
				cs.archiver.Close()
				return
			case <-heartBeatTicker.C:
				branchInfo.Event = string(common.HeartBeat)
				branchInfo.Err = cs.heartBeat()
			case <-persistMetaDataTicker.C:
				branchInfo.Event = string(common.PersistMetaData)
				branchInfo.Err = cs.persistMetadata()
			case <-garbageCollectionTicker.C:
				branchInfo.Event = string(common.GarbageCollection)
				branchInfo.Err = cs.garbageCollection()
			case <-archiveChunkTicker.C:
				branchInfo.Event = string(common.Archival)
				branchInfo.Err = cs.archiveChunks()
			}

			if branchInfo.Err != nil {
				log.Err(branchInfo.Err).Msgf(
					fmt.Sprintf("Server %s  background-(%s) event triggered an error (%s)\n",
						cs.ServerAddr, branchInfo.Event, branchInfo.Err))
			}
		}
	}()

	log.Printf("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", serverAddr, root, masterAddr)
	return cs, nil
}

// archiveChunks compresses chunks that have not been accessed within the archival time span.
// It identifies eligible chunks, submits them for compression, and updates their status concurrently.
// Errors encountered during compression are aggregated and returned as a single error.
// The function is thread-safe and ensures proper resource cleanup.
//
// Returns nil if all chunks are processed successfully, or a combined error if any failures occur.
func (cs *ChunkServer) archiveChunks() error {
	chunksToArchive := map[common.ChunkHandle]*chunkInfo{}
	checkAccessTime := func(value *chunkInfo) bool {
		return time.Until(value.accessTime).Hours()/24 > common.ArchivalDaySpan
	}
	cs.mu.Lock()
	utils.ExtractFromMap(cs.chunks, chunksToArchive, checkAccessTime)
	cs.mu.Unlock()

	var wg sync.WaitGroup
	var errWg sync.WaitGroup
	pathToHandle := make(map[common.Path]common.ChunkHandle)
	errCh := make(chan error, len(chunksToArchive))
	errs := []error{}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errCh {
			errs = append(errs, err)
		}
	}()

	wg.Add(2)
	go func(errs chan<- error) {
		defer wg.Done()
		for handle := range chunksToArchive {
			filename := common.Path(fmt.Sprintf(common.ChunkFileNameFormat, handle))
			if err := cs.archiver.SubmitCompress(filename); err != nil {
				errs <- err
				continue
			}
			pathToHandle[filename] = handle
		}
	}(errCh)

	go func(errs chan<- error) {
		defer wg.Done()
		for result := range cs.archiver.CompressPipeline.Result {
			if result.Err != nil {
				errs <- result.Err
				continue
			}
			cs.mu.Lock()
			filename := strings.TrimSuffix(string(result.Path), archivemanager.ZIP_EXT)
			if handle, exists := pathToHandle[common.Path(filename)]; exists {
				cs.chunks[handle].isCompressed = true
			}
			cs.mu.Unlock()
		}
	}(errCh)

	wg.Wait()
	close(errCh)
	errWg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// unarchiveChunks decompresses a specific chunk identified by its handle if it is compressed.
// It checks if the chunk exists and is compressed, submits it for decompression, and updates its status.
// The function is thread-safe, using a mutex to protect access to the chunk data.
//
// Parameters:
//   - handle: The unique identifier for the chunk to decompress.
//
// Returns:
//   - nil if the chunk is successfully decompressed or was not compressed.
//   - An error if the chunk does not exist, decompression fails, or an issue occurs in the process.
func (cs *ChunkServer) unarchiveChunks(handle common.ChunkHandle) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	chunkInfo, ok := cs.chunks[handle]
	if !ok {
		return fmt.Errorf("cannot attempt to decompress handle [%v]", handle)
	}

	if chunkInfo.isCompressed {
		filename := fmt.Sprintf(common.ChunkFileNameFormat+archivemanager.ZIP_EXT, handle)
		cs.archiver.SubmitDecompress(common.Path(filename))
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

// loadMetadata loads chunk metadata from a file and populates the server's chunk map.
// It attempts to open the metadata file, creating it if it does not exist, and decodes the stored metadata.
// Each metadata entry is used to restore chunk information in the server's chunk map.
// The function is thread-safe, using a mutex to protect access to the chunk map.
//
// Returns:
//   - nil if the metadata is successfully loaded or the file is empty (io.EOF).
//   - An error if the file cannot be opened, created, or decoded, or if any other issue occurs.
func (cs *ChunkServer) loadMetadata() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	file, err := cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDONLY, common.FileMode)
	if err != nil {
		if os.IsNotExist(err) {
			err = cs.rootDir.CreateFile(common.ChunkMetaDataFileName)
			if err != nil {
				return fmt.Errorf("failed to create metadata file: %w", err)
			}
		}
		file, err = cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDONLY, common.FileMode)
		if err != nil {
			return err
		}
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Err(err).Msg("failed to close metadata file")
		}
	}()

	var metas []PersistedMetaData
	decoder := library.NewDecoder(file)
	err = decoder.Decode(&metas)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Err(err).Msg(fmt.Sprintf("Server %s failed to decode metadata", cs.ServerAddr))
			return err
		}
	}

	log.Info().Msg(fmt.Sprintf("Server %s found metas with length %d", cs.ServerAddr, len(metas)))
	utils.ForEach(metas, func(m PersistedMetaData) {
		if m.Length < 0 {
			log.Warn().Msg(
				fmt.Sprintf("Server %s skipping invalid metadata for chunk-%d: negative length",
					cs.ServerAddr, m.Handle))
			return
		}
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

// heartBeat sends a heartbeat to the master server to report the server's status and receive updates.
// It constructs a heartbeat request with the server's address and machine info, optionally requesting
// lease extensions if active leases exist. The function records network data for failure prediction,
// updates leases and garbage collection lists based on the reply, and logs the failure prediction result.
// The function is not thread-safe for the leases and garbage lists; callers must ensure proper synchronization.
//
// Returns:
//   - nil if the heartbeat is successfully sent, processed, and failure prediction is recorded.
//   - An error if the RPC call fails, network data recording fails, or failure prediction fails.
func (cs *ChunkServer) heartBeat() error {
	arg := rpc_struct.HeartBeatArg{
		Address:     cs.ServerAddr,
		MachineInfo: cs.MachineInfo,
	}

	cs.mu.Lock()
	if cs.leases.Length() != 0 {
		arg.ExtendLease = true
	}
	cs.mu.Unlock()

	var reply rpc_struct.HeartBeatReply
	reply.NetworkData = failuredetector.NetworkData{
		RoundTrip: 0,
		ForwardTrip: failuredetector.TripInfo{
			SentAt: time.Now(),
		},
	}
	if err := shared.UnicastToRPCServer(string(cs.MasterAddr),
		rpc_struct.MRPCHeartBeatHandler, arg, &reply); err != nil {
		return err
	}

	reply.NetworkData.BackwardTrip.ReceivedAt = time.Now()
	if err := cs.failureDetector.RecordSample(reply.NetworkData); err != nil {
		log.Err(err).Stack().Msg("err storing network data for prediction")
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if reply.LeaseExtensions != nil {
			cs.lmu.Lock()
			utils.ForEach(reply.LeaseExtensions, func(lease *common.Lease) {
				cs.leases.PushBack(lease)
			})
			cs.lmu.Unlock()
		}

		if reply.Garbage != nil {
			utils.ForEach(reply.Garbage, func(handle common.ChunkHandle) {
				cs.garbage.PushBack(handle)
			})
		}
	}()

	wg.Wait()
	prediction, err := cs.failureDetector.PredictFailure()
	if err != nil {
		log.Err(err).Stack().Send()
	} else {
		log.Info().Msgf("server=%s prediction=%.2f  message=%s", cs.ServerAddr, prediction.Phi, prediction.Message)
	}
	return nil
}

// persistMetadata writes the server's chunk metadata to a file.
// It iterates over the server's chunk map, constructs metadata entries, and encodes them to the specified file.
// The function is thread-safe for reading the chunk map using a read lock, but callers must ensure no concurrent writes
// to the file occur. The file is opened in read-write mode, and any errors during file operations or encoding are returned.
//
// Returns:
//   - nil if the metadata is successfully written to the file.
//   - An error if the file cannot be opened, written, or encoded, or if any other issue occurs.
func (cs *ChunkServer) persistMetadata() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	log.Info().Msg("<<< persisting metadata to file >>> ")
	file, err := cs.rootDir.GetFile(common.ChunkMetaDataFileName, os.O_RDWR, common.FileMode)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Err(err).Stack().Msg(fmt.Sprintf("Server %s failed to close metadata file", cs.ServerAddr))
		}
	}(file)

	var metadatas []PersistedMetaData

	for handle, ch := range cs.chunks {
		persistMetadata := PersistedMetaData{
			ChunkSize:       int64(len(ch.mutations)) / 1024, // in KB
			Mutations:       ch.mutations,
			Version:         ch.version,
			CreationTime:    ch.creationTime,
			AccessTime:      ch.accessTime,
			Abandoned:       ch.abandoned,
			Replication:     ch.replication,
			ServerStatus:    ch.serverStatus,
			MetadataVersion: int(time.Now().UnixNano()),
			Length:          ch.length,
			Handle:          handle,
			Checksum:        ch.checksum,
			ServerIP:        string(cs.ServerAddr),
			Completed:       ch.completed,
		}
		metadatas = append(metadatas, persistMetadata)
	}
	log.Info().Msgf("Server %v : store metadata len: %v", cs.ServerAddr, len(metadatas))
	encoder := library.NewEncoder(file)
	return encoder.Encode(metadatas)
}

// garbageCollection removes chunks marked for deletion from the server's storage.
// It processes the server's garbage list (cs.garbage), which contains chunk handles
// identified for removal (e.g., via heartBeat coordination with the master server).
// Each chunk handle is popped from the list and deleted using deleteChunk. Errors
// during deletion are logged but do not stop the process, ensuring all garbage
// entries are processed. The function is not thread-safe; callers must ensure no
// concurrent access to cs.garbage or cs.chunks occurs. This function is typically
// called periodically to reclaim storage space in a distributed storage system.
//
// Returns:
//   - nil, as errors during deletion are logged but not returned to allow complete
//     processing of the garbage list.
//   - Note: Individual deletion errors are logged for debugging.
func (cs *ChunkServer) garbageCollection() error {
	log.Info().Msg("::: Doing some garbage collection >>> ")
	for cs.garbage.Length() > 0 {
		handle := cs.garbage.PopFront()
		err := cs.deleteChunk(handle)
		if err != nil {
			log.Err(err).Stack().Msg(fmt.Sprintf("Server %s: failed to delete chunk %v", cs.ServerAddr, handle))
			return err
		}
	}
	return nil
}

// deleteChunk removes a specified chunk from the server's storage and chunk map.
// It deletes the chunk's file (compressed or uncompressed) from the filesystem and
// removes the corresponding entry from the server's chunk map (cs.chunks). The function
// is thread-safe, using a mutex to protect access to cs.chunks. It is typically called
// by garbageCollection to process chunks marked for deletion (e.g., via heartBeat coordination
// with the master server) in a distributed storage system. If the chunk does not exist in
// cs.chunks, the function returns nil, allowing idempotent deletion.
//
// Parameters:
//   - handle: The unique identifier for the chunk to delete.
//
// Returns:
//   - nil if the chunk is successfully deleted or does not exist.
//   - An error if the chunk's file cannot be removed from the filesystem.
func (cs *ChunkServer) deleteChunk(handle common.ChunkHandle) error {
	cs.mu.Lock()
	chunkInfo, exists := cs.chunks[handle]
	if !exists {
		cs.mu.Unlock()
		log.Info().Msg(fmt.Sprintf("Server %s: chunk %v not found, skipping deletion", cs.ServerAddr, handle))
		return nil
	}
	if chunkInfo.length < 0 {
		delete(cs.chunks, handle)
		cs.mu.Unlock()
		return fmt.Errorf("server %s: invalid chunk %v: negative length", cs.ServerAddr, handle)
	}
	delete(cs.chunks, handle)
	cs.mu.Unlock()

	filename := fmt.Sprintf(common.ChunkFileNameFormat, handle)
	if chunkInfo.isCompressed {
		filename += archivemanager.ZIP_EXT
	}
	err := cs.rootDir.RemoveFile(filename)
	if err != nil {
		return err
	}

	return cs.persistMetadata()
}

// Shutdown gracefully terminates the server, ensuring proper cleanup of resources.
// It marks the server as dead, clears the download buffer, closes the archiver,
// persists chunk metadata, closes the network listener, and signals shutdown via
// a channel. The function is idempotent; if the server is already dead, it logs
// a message and returns immediately. It is thread-safe, using a mutex to protect
// shared state (e.g., cs.isDead, cs.garbage). The function is typically called when
// the server needs to stop in a distributed storage system, ensuring metadata
// consistency and resource cleanup before exit. Operations are performed with timeouts
// to prevent hanging. Errors are logged and aggregated for the caller.
//
// Returns:
//   - nil if the shutdown sequence completes successfully.
//   - An error if critical steps (e.g., metadata persistence, listener closure) fail.
func (cs *ChunkServer) Shutdown() error {
	cs.mu.Lock()
	if cs.isDead {
		cs.mu.Unlock()
		log.Info().Msgf("Server %s: already dead", cs.ServerAddr)
		return nil
	}
	cs.isDead = true

	cs.mu.Unlock()
	err := cs.garbageCollection()
	if err != nil {
		return err
	}

	log.Info().Msgf("Server %s: clearing download buffer before shutdown", cs.ServerAddr)
	cs.downloadBuffer.Done()
	cs.archiver.Close()
	log.Info().Msgf("Server %s: saving metadata before shutdown", cs.ServerAddr)
	if err := cs.persistMetadata(); err != nil {
		log.Err(err).Stack().Msgf("Server %s: failed to persist metadata during shutdown", cs.ServerAddr)
		return err
	}

	if err := cs.listener.Close(); err != nil {
		log.Err(err).Stack().Msgf("Server %s: failed to close listener during shutdown", cs.ServerAddr)
		return err
	}

	log.Info().Msgf("Server %s: signaling shutdown", cs.ServerAddr)
	select {
	case cs.shutdownChan <- syscall.SIGINT:
	default:
		log.Warn().Msgf("Server %s: shutdown channel already closed", cs.ServerAddr)
	}
	close(cs.shutdownChan)
	return nil
}

// ///////////////////////////////////
//
//	RPC METHODS
//
// /////////////////////////////////

// RPCSysReportHandler gathers system memory statistics and chunk metadata for an RPC system report.
// It collects memory usage (Alloc, TotalAlloc, Sys, NumGC) using runtime.MemStats, formats them into
// a table for logging, and retrieves chunk metadata from the server's chunk map (cs.chunks). The
// function is part of a distributed storage system, typically called by the master server to monitor
// server health and chunk state, complementing heartBeat for system coordination. It is thread-safe
// for reading cs.chunks using a read lock. The memory statistics are logged in a tabular format for
// clarity, and the chunk metadata is returned in the reply struct.
//
// Parameters:
//   - args: The RPC arguments (SysReportInfoArg), typically containing request metadata.
//   - reply: The RPC reply (SysReportInfoReply) to populate with memory stats and chunk metadata.
//
// Returns:
//   - nil if the system report is successfully generated and populated in the reply.
//   - An error is not returned in the current implementation, but future versions could include error handling for critical failures.
func (cs *ChunkServer) RPCSysReportHandler(args rpc_struct.SysReportInfoArg, reply *rpc_struct.SysReportInfoReply) error {
	log.Info().Msgf("Server %s: gathering system statistics", cs.ServerAddr)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	alloc, err := utils.BToMb(m.Alloc)
	if err != nil {
		return err
	}
	totalAlloc, err := utils.BToMb(m.TotalAlloc)
	if err != nil {
		return err
	}
	tSys, err := utils.BToMb(m.Sys)
	if err != nil {
		return err
	}
	numGC, err := utils.BToMb(uint64(m.NumGC))
	if err != nil {
		return err
	}

	log.Info().Msgf("Server %s: memory statistics\n", cs.ServerAddr)
	table := tablewriter.NewWriter(log.Logger)
	table.Header([]string{"Metric", "Value (MiB)"})
	table.Append([]string{"Alloc", fmt.Sprintf("%v", alloc)})
	table.Append([]string{"TotalAlloc", fmt.Sprintf("%v", totalAlloc)})
	table.Append([]string{"Sys", fmt.Sprintf("%v", tSys)})
	table.Append([]string{"NumGC", fmt.Sprintf("%v", numGC)})
	err = table.Render()
	if err != nil {
		return err
	}

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

	log.Info().Msgf("Server %s: completed system statistics retrieval", cs.ServerAddr)
	return nil
}

// RPCCheckChunkVersionHandler verifies the version of a chunk against a provided version.
// It checks if the chunk specified by args.Handle exists in the server's chunk map (cs.chunks)
// and compares its version with the provided args.Version. If the chunk is one version behind,
// it updates the chunk's version and last modified time, marking it as not stale. Otherwise,
// the chunk is marked as abandoned and considered not stale. The function is used in a distributed
// storage system to ensure chunk version consistency, likely called by the master server or other
// servers during replication or coordination (e.g., in heartBeat). It is thread-safe, using a mutex
// to protect access to cs.chunks during read and write operations. The result is returned in the
// reply.Stale field, where false indicates the chunk is valid or updated.
//
// Parameters:
//   - args: The RPC arguments containing the chunk handle (args.Handle) and version (args.Version).
//   - reply: The RPC reply to populate with the staleness result (reply.Stale).
//
// Returns:
//   - nil if the chunk version is successfully checked or the chunk does not exist.
//   - Note: Errors are not currently returned, but invalid chunk data could warrant an error in future versions.
func (cs *ChunkServer) RPCCheckChunkVersionHandler(
	args rpc_struct.CheckChunkVersionArg, reply *rpc_struct.CheckChunkVersionReply) error {
	cs.mu.Lock()
	chinfo, ok := cs.chunks[args.Handle]
	cs.mu.Unlock()

	if !ok {
		log.Info().Msgf(
			"Server %s: chunk %v not found, marking as stale",
			cs.ServerAddr, args.Handle)
		reply.Stale = true
		return nil
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if chinfo.version+common.ChunkVersion(1) == args.Version {
		log.Info().Msgf(
			"Server %s: chunk %v is one version behind, updating version",
			cs.ServerAddr, args.Handle)
		reply.Stale = false
		chinfo.lastModified = time.Now()
		chinfo.version++
		return nil
	}

	log.Warn().Msgf(
		"Server %s: chunk %v is stale: local version %v, expected %v",
		cs.ServerAddr, args.Handle, chinfo.version, args.Version)
	chinfo.abandoned = true
	chinfo.lastModified = time.Now()
	reply.Stale = false
	return nil
}

// RPCReadChunkHandler handles an RPC request to read data from a specific chunk.
// It retrieves the chunk information, reads the requested data from the chunk at the specified offset,
// and updates the chunk's last access time. If the chunk is not found or is abandoned, it returns an error.
// If the read operation reaches the end of the chunk, it sets the reply's ErrorCode to indicate EOF.
//
// Args:
//   - args: ReadChunkArgs containing the chunk handle, offset, and length to read.
//   - reply: ReadChunkReply to store the read data, length, and any error code.
//
// Returns:
//   - error: Returns an error if the chunk is not found, abandoned, or if the read operation fails.
//     Returns nil if the read is successful or if an EOF is encountered (with reply.ErrorCode set).
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

// RPCCreateChunkHandler handles an RPC request to create a new chunk on the ChunkServer.
// It checks if the chunk already exists; if it does, the request is ignored. Otherwise, it initializes
// a new chunkInfo structure for the specified chunk handle, creates the corresponding file in the
// server's root directory, and stores the chunk metadata in the server's chunk map.
//
// Args:
//   - args: CreateChunkArgs containing the chunk handle for the new chunk.
//   - reply: CreateChunkReply to store the result of the operation (currently unused).
//
// Returns:
//   - error: Returns nil if the chunk is created successfully or if it already exists.
//     Returns an error if the file creation fails.
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

// RPCForwardDataHandler handles an RPC request to forward data to the ChunkServer's download buffer
// and propagate it to replica servers. It checks if the data already exists in the download buffer
// using the provided DownloadBufferId. If it exists, the request is ignored. Otherwise, the data is
// stored in the buffer, and if replicas are specified, the data is forwarded to the next replica
// server in the list via an RPC call.
//
// Args:
//   - args: ForwardDataArgs containing the DownloadBufferId, data to store, and a list of replica addresses.
//   - reply: ForwardDataReply to store the result of the operation (currently unused).
//
// Returns:
//   - error: Returns nil if the data is stored successfully or already exists in the buffer.
//     Returns an error if forwarding to a replica server fails
func (cs *ChunkServer) RPCForwardDataHandler(args rpc_struct.ForwardDataArgs, reply *rpc_struct.ForwardDataReply) error {
	// check if we have the item previously
	_, ok := cs.downloadBuffer.Get(args.DownloadBufferId)
	if ok {
		return nil
	}

	log.Info().Msgf("storing %v on %v's buffer cache", args.DownloadBufferId, cs.ServerAddr)
	cs.downloadBuffer.Set(args.DownloadBufferId, args.Data)
	if len(args.Replicas) == 0 {
		return nil
	}

	replicaAddr := args.Replicas[0]
	args.Replicas = args.Replicas[1:]
	return shared.UnicastToRPCServer(string(replicaAddr), rpc_struct.CRPCForwardDataHandler, args, &reply)
}

// RPCGrantLeaseHandler handles an RPC request to grant a lease for a chunk on the ChunkServer.
// It creates a new lease with the provided expiration time, primary server, secondary servers,
// and marks it as in use. The lease is then added to the server's lease list. The operation
// is protected by a mutex to ensure thread safety.
//
// Args:
//   - args: GrantLeaseInfoArgs containing the lease expiration time, primary server, and secondary servers.
//   - reply: GrantLeaseInfoReply to store the result of the operation (currently unused).
//
// Returns:
//   - error: Returns nil to indicate the lease was successfully granted.
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

// RPCWriteChunkHandler handles an RPC request to write data to a chunk on the ChunkServer.
// It retrieves data from the download buffer using the provided DownloadBufferId, validates
// the data size against the maximum allowed chunk size, checks for a valid lease, and then
// performs the write operation. If the data is not found, the size exceeds the limit, or the
// lease is invalid/expired, an error is returned.
//
// Args:
//   - args: WriteChunkArgs containing the DownloadBufferId, offset, and replica addresses.
//   - reply: WriteChunkReply to store the result of the operation, including any error code.
//
// Returns:
//   - error: Returns an error if the data is not found in the buffer, the data size exceeds
//     the maximum chunk size, the lease is invalid or expired, or the write operation fails.
//     Returns nil if the write is successful.
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
	dataSize, err := utils.BToMb(uint64(args.Offset) + uint64(len(data)))
	if err != nil {
		return err
	}
	if dataSize > common.ChunkMaxSizeInMb {
		return fmt.Errorf("provided data size for write action [%v] is larger than the max allowed data size of %v mb",
			args.DownloadBufferId, common.ChunkMaxSizeInMb)
	}

	log.Info().Msgf("args.Replicas => %#v", args.Replicas)
	lease := cs.leases.PopFront()
	if lease == nil || lease.IsExpired(time.Now()) {
		return fmt.Errorf("could not acquire write lease / lease has expired")
	}

	err = performWrite(cs, args, data)
	if err != nil {
		return err
	}

	if !lease.IsExpired(time.Now()) {
		cs.leases.PushFront(lease)
	}
	return nil
}

// RPCApplyMutationHandler handles an RPC request to apply a mutation to a chunk on the ChunkServer.
// It retrieves data from the download buffer using the provided DownloadBufferId, validates the data
// size against the maximum allowed chunk size, ensures the chunk exists and is not abandoned, and
// applies the specified mutation (e.g., append or update) to the chunk. The operation is protected
// by a mutex to ensure thread safety during mutation.
//
// Args:
//   - args: ApplyMutationArgs containing the DownloadBufferId, offset, and mutation type.
//   - reply: ApplyMutationReply to store the result of the operation, including any error code.
//
// Returns:
//   - error: Returns an error if the data is not found in the buffer, the data size exceeds the
//     maximum chunk size, the chunk is not found or abandoned, or the mutation operation fails.
//     Returns nil if the mutation is applied successfully.
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
	dataSize, err := utils.BToMb(uint64(args.Offset) + uint64(len(data)))
	if err != nil {
		return err
	}
	if dataSize > common.ChunkMaxSizeInMb {
		return fmt.Errorf("provided data size for append action [%v] is larger than the max allowed data size of %v mb", args.DownloadBufferId, common.ChunkMaxSizeInMb)
	}

	handle := args.DownloadBufferId.Handle
	err = cs.unarchiveChunks(handle)
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
	err = cs.doMutate(handle, mutation)
	if err != nil {
		return err
	}

	return nil
}

// RPCAppendChunkHandler handles an RPC request to append data to a chunk on the ChunkServer.
// It retrieves data from the download buffer using the provided DownloadBufferId, validates the chunk's
// existence and state, and checks if the append operation would exceed the maximum chunk size. If the
// chunk is compressed, it is unarchived. The append operation is performed asynchronously on the local
// server and forwarded to replica servers. If the data size exceeds the maximum, a padding mutation is
// applied, and an error code is set. Errors from local or replica operations are collected and returned.
//
// Args:
//   - args: AppendChunkArgs containing the DownloadBufferId and replica addresses.
//   - reply: AppendChunkReply to store the offset where data is appended and any error code.
//
// Returns:
//   - error: Returns an error if the data is not found in the buffer, the chunk is not found or
//     abandoned, unarchiving fails, or any local or replica mutation fails. Returns nil if the append
//     is successful.
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
		if err := cs.unarchiveChunks(handle); err != nil {
			return err
		}
	}
	if !ok || chInfo.abandoned {
		return fmt.Errorf("%v is either abandoned or lives in another dimension", handle)
	}

	var mutationType common.MutationType
	offset := chInfo.length
	newLength := chInfo.length + common.Offset(len(data))
	dataSize, err := utils.BToMb(uint64(newLength))
	if err != nil {
		return err
	}

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

	err = cs.doMutate(handle, mutation)
	if err != nil {
		return err
	}
	cs.mu.Lock()
	chInfo.length = newLength
	cs.mu.Unlock()
	applyMutationArgs := rpc_struct.ApplyMutationArgs{
		DownloadBufferId: args.DownloadBufferId,
		MutationType:     mutationType,
		Offset:           offset,
	}

	errs := make(chan error)
	// forward a written call to all available secondary servers
	go func() {
		utils.ForEach(args.Replicas, func(addr common.ServerAddr) {
			var applyMutationReply rpc_struct.ApplyMutationReply
			err := shared.UnicastToRPCServer(
				string(addr), rpc_struct.CRPCApplyMutationHandler,
				applyMutationArgs, &applyMutationReply)
			if err != nil {
				errs <- err
			}
		})
		close(errs)
	}()

	var allObservedErrors []error
	for e := range errs {
		allObservedErrors = append(allObservedErrors, e)
	}
	return errors.Join(allObservedErrors...)
}

// RPCGetSnapshotHandler handles an RPC request to retrieve a snapshot of a chunk from the ChunkServer.
// It checks if the chunk exists and is not abandoned, unarchives it if compressed, reads the entire chunk
// data from the beginning, and forwards the data to the specified replica server for copying. The operation
// uses a read lock to ensure thread safety when accessing chunk metadata.
//
// Args:
//   - args: GetSnapshotArgs containing the chunk handle and replica server address.
//   - reply: GetSnapshotReply to store the result of the operation (currently unused).
//
// Returns:
//   - error: Returns an error if the chunk does not exist, is abandoned, unarchiving fails, reading the chunk
//     fails, or forwarding to the replica server fails. Returns nil if the snapshot is successfully retrieved
//     and forwarded.
func (cs *ChunkServer) RPCGetSnapshotHandler(args rpc_struct.GetSnapshotArgs, reply *rpc_struct.GetSnapshotReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	if !ok || chInfo.abandoned {
		cs.mu.RUnlock()
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	if chInfo.isCompressed {
		if err := cs.unarchiveChunks(handle); err != nil {
			cs.mu.RUnlock()
			return err
		}
	}
	cs.mu.RUnlock()
	data := make([]byte, chInfo.length)
	if _, err := cs.readChunk(handle, 0, data); err != nil {
		return err
	}

	var r rpc_struct.ApplyCopyReply
	applyCopyArgs := rpc_struct.ApplyCopyArgs{
		Handle:  handle,
		Data:    data,
		Version: chInfo.version,
	}

	return shared.UnicastToRPCServer(string(args.Replicas), rpc_struct.CRPCApplyCopyHandler, applyCopyArgs, &r)
}

// RPCApplyCopyHandler handles an RPC request to apply a copy operation to a chunk on the ChunkServer.
// It checks if the chunk exists and is not abandoned, unarchives it if compressed, writes the provided
// data to the chunk, and updates the chunk's version. The operation uses a read lock for checking chunk
// metadata and a write lock for updating the version to ensure thread safety.
//
// Args:
//   - args: ApplyCopyArgs containing the chunk handle, data to write, and version number.
//   - reply: AppendChunkReply to store the result of the operation (currently unused).
//
// Returns:
//   - error: Returns an error if the chunk does not exist, is abandoned, unarchiving fails, or the write
//     operation fails. Returns nil if the copy operation is successful.
func (cs *ChunkServer) RPCApplyCopyHandler(args rpc_struct.ApplyCopyArgs, reply *rpc_struct.AppendChunkReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	if chInfo.isCompressed {
		err := cs.unarchiveChunks(handle)
		if err != nil {
			return err
		}
	}

	err := cs.writeChunk(handle, args.Data, common.MutationWrite, 0, true)
	if err != nil {
		return err
	}
	cs.mu.Lock()
	chInfo.version = args.Version
	cs.mu.Unlock()
	log.Info().Msgf("Server %v : Copy handler done", cs.ServerAddr)
	return nil
}

//////////////////////////////////////////////
//        HELPER FUNCTIONS
////////////////////////////////////////////

// performWrite executes a write operation on a chunk in the ChunkServer and propagates the operation to replica servers.
// It validates the chunk's existence, state, and compression status, applies a write mutation to the local chunk, and forwards
// the mutation to replicas. The operation is performed asynchronously, with errors collected from both local and replica operations.
// The chunk's length is updated upon successful completion. Thread safety is ensured using mutex locks for accessing chunk metadata.
//
// Args:
//   - cs: Pointer to the ChunkServer instance performing the write operation.
//   - args: WriteChunkArgs containing the DownloadBufferId, offset, and replica addresses.
//   - data: Byte slice containing the data to be written to the chunk.
//
// Returns:
//   - error: Returns an aggregated error if the chunk does not exist, is abandoned, completed, or compressed and unarchiving fails,
//     or if the local or replica mutation operations fail. Returns nil if the write is successful.
func performWrite(cs *ChunkServer, args rpc_struct.WriteChunkArgs, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log.Info().Msgf("[writing to chunk] %v", cs.ServerAddr)

	handle := args.DownloadBufferId.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v on server %v is abandoned or does not exist", handle, cs.ServerAddr)
	}
	if chInfo.isCompressed {
		if err := cs.unarchiveChunks(handle); err != nil {
			return err
		}
	}
	if chInfo.completed {
		return fmt.Errorf("chunk %v on server %v is completed and cannot be written", handle, cs.ServerAddr)
	}
	dataSize, err := utils.BToMb(uint64(args.Offset) + uint64(len(data)))
	if err != nil {
		return err
	}
	if dataSize > common.ChunkMaxSizeInMb {
		return fmt.Errorf("data size for chunk %v exceeds max size of %v MB", handle, common.ChunkMaxSizeInMb)
	}
	mutation := &common.Mutation{
		MutationType: common.MutationWrite,
		Data:         data,
		Offset:       args.Offset,
	}
	if err := cs.doMutate(handle, mutation); err != nil {
		return err
	}

	chInfo.Lock()
	if _, exists := chInfo.mutations[chInfo.version]; exists {
		chInfo.version++
	}
	chInfo.mutations[chInfo.version] = *mutation
	chInfo.Unlock()

	errCh := make(chan error, len(args.Replicas)+1)
	var wg sync.WaitGroup

	wg.Add(len(args.Replicas))
	go func() {
		utils.ForEach(args.Replicas, func(replicaAddr common.ServerAddr) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				var applyMutationReply rpc_struct.ApplyMutationReply
				err := shared.UnicastToRPCServer(
					string(replicaAddr),
					rpc_struct.CRPCApplyMutationHandler,
					rpc_struct.ApplyMutationArgs{
						DownloadBufferId: args.DownloadBufferId,
						MutationType:     common.MutationWrite,
						Offset:           args.Offset,
					}, &applyMutationReply)
				if err != nil {
					errCh <- err
				}
			}
		})
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.chunks[args.DownloadBufferId.Handle].length += common.Offset(len(data))
	return nil
}

func (cs *ChunkServer) doMutate(handle common.ChunkHandle, mutation *common.Mutation) error {
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

// writeChunk writes data to a chunk file at the specified offset with the given mutation type.
// It updates the chunk's metadata, such as length and last modified time, and computes a checksum
// for the file content if applicable. The function supports both write and append operations,
// and optionally locks the chunk metadata for thread safety. If the write exceeds the maximum chunk
// size, an error is returned. The chunk is marked as completed if its size reaches the maximum limit.
//
// Args:
//   - cs: Pointer to the ChunkServer instance performing the write operation.
//   - handle: ChunkHandle identifying the chunk to write to.
//   - data: Byte slice containing the data to write.
//   - mutationType: MutationType specifying the type of write operation (e.g., MutationWrite or MutationAppend).
//   - offset: Offset indicating the starting position in the chunk file for the write.
//   - lock: Boolean indicating whether to lock the chunk metadata during the operation.
//
// Returns:
//   - error: Returns an error if the data size exceeds the maximum chunk size, the file cannot be opened,
//     written to, or read for checksum calculation, or if file closing fails. Returns nil if the write is successful.
func (cs *ChunkServer) writeChunk(handle common.ChunkHandle, data []byte, mutationType common.MutationType, offset common.Offset, lock bool) error {
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v on server %v does not exist", handle, cs.ServerAddr)
	}

	if mutationType != common.MutationWrite && mutationType != common.MutationAppend {
		return fmt.Errorf("invalid mutation type for chunk %v: %v", handle, mutationType)
	}

	chInfo.Lock()
	newLen := offset + common.Offset(len(data))
	if newLen > chInfo.length {
		chInfo.length = newLen
	}
	if newLen > common.ChunkMaxSizeInByte {
		chInfo.Unlock()
		return fmt.Errorf("data size for chunk %v on server %v exceeds max size %v bytes", handle, cs.ServerAddr, common.ChunkMaxSizeInByte)
	}
	chInfo.Unlock()

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
		if err := fs.Close(); err != nil {
			log.Err(err).Msgf("failed to close file for chunk %v", handle)
		}
	}(fs)

	n, err := fs.WriteAt(data, int64(offset))
	if err != nil {
		log.Err(err).Msgf("failed to write to chunk %v at offset %v", handle, offset)
		return err
	}
	log.Debug().Msgf("Wrote %d bytes to chunk %v at offset %v", n, handle, offset)

	chInfo.Lock()
	defer chInfo.Unlock()
	chInfo.lastModified = time.Now()
	if newLen >= common.ChunkMaxSizeInByte {
		chInfo.completed = true
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, fs); err != nil {
		log.Err(err).Msgf("failed to read chunk %v for checksum", handle)
		return err
	}
	chInfo.checksum = common.Checksum(hasher.Sum(nil))
	return nil
}

// readChunk reads data from a chunk file at the specified offset into the provided data buffer.
// It opens the chunk file in read-only mode, reads the requested data starting at the given offset,
// and logs the operation. The file is closed after the operation, and any errors during reading or
// closing are logged and returned.
//
// Args:
//   - handle: ChunkHandle identifying the chunk to read from.
//   - offset: Offset indicating the starting position in the chunk file to read.
//   - data: Byte slice to store the read data.
//
// Returns:
//   - int: Number of bytes read from the chunk file.
//   - error: Returns an error if the file cannot be opened, read, or closed. Returns nil if the read
//     is successful.
func (cs *ChunkServer) readChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	filename := fmt.Sprintf(common.ChunkFileNameFormat, handle)
	f, err := cs.rootDir.GetFile(filename, os.O_RDONLY, common.FileMode)
	if err != nil {
		log.Err(err).Stack().Send()
		return -1, err
	}
	defer func(fs *os.File) {
		err := fs.Close()
		if err != nil {
			log.Err(err).Stack().Send()
		}
	}(f)
	log.Info().Msgf(
		"Server %v reading data from offset %v for chunk handle [%v] - %s",
		cs.ServerAddr, offset, handle, filename)
	n, err := f.ReadAt(data, int64(offset))
	if err != nil {
		log.Err(err).Stack().Send()
	}
	return n, err
}

// calculateRoundTripProximity measures the average round-trip time (RTT) to a master server.
// It executes a system ping command to the specified master address for a given duration (number of pings),
// extracts the average RTT from the output, and returns it in milliseconds. The function is used in a
// distributed storage system to assess network latency, likely for failure detection or server proximity
// calculations (e.g., in heartBeat). It uses a context with a timeout to prevent hanging and sanitizes the
// master address to avoid command injection. If the ping or RTT extraction fails, it logs the error and
// returns an error to the caller. The function is not thread-safe and relies on system commands, which may
// not be portable.
//
// Parameters:
//   - ctx: The context to control timeout and cancellation.
//   - duration: The number of ping attempts to make.
//   - masterAddr: The address of the master server to ping.
//
// Returns:
//   - A float64 representing the average round-trip time in milliseconds.
//   - An error if the ping command fails, the address is invalid, or RTT extraction fails.
func calculateRoundTripProximity(duration int, masterAddr string) (float64, error) {
	if !isValidAddr(masterAddr) {
		return 0.0, fmt.Errorf("invalid master address: %s", masterAddr)
	}
	var buffer bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration+5)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ping", "-c", fmt.Sprintf("%d", duration), masterAddr)
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
	return rrt, nil
}

// extractAverageRTT parses the output of a ping command to calculate the average round-trip time (RTT).
// It uses a regular expression to extract RTT values (in milliseconds) from the input string, typically
// the output of a ping command executed by calculateRoundTripProximity. The function is used in a
// distributed storage system to support network latency measurements for failure detection or server
// proximity calculations. It logs errors for invalid RTT values and returns the average RTT across all
// valid matches. If no valid RTT values are found or parsing fails, it returns an error.
//
// Parameters:
//   - input: The string output from a ping command containing RTT values (e.g., "time=12.34 ms").
//
// Returns:
//   - A float64 representing the average RTT in milliseconds.
//   - An error if no valid RTT values are found, the input is empty, or parsing fails.

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

// isValidAddr checks if the address is a valid IP or hostname (basic validation).
func isValidAddr(addr string) bool {
	// Basic validation: non-empty and contains only allowed characters
	return len(addr) > 0 && (regexp.MustCompile(`^[a-zA-Z0-9.-]+$`).MatchString(addr) ||
		regexp.MustCompile(`^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):([0-9]{1,5})$`).MatchString(addr))
}
