package chunkserver

import (
	"bytes"
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

// Server represents a server instance in a distributed file system, managing
// chunk metadata, leases, and system resources. It handles network communication,
// failure detection, and garbage collection for chunks.
type Server struct {
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
func NewChunkServer(serverAddr common.ServerAddr, masterAddr common.ServerAddr, root string) (*Server, error) {
	log.Info().Msg(fmt.Sprintf("Starting ChunkServer = %s to communicate with @%v", serverAddr, masterAddr))
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	fs := filesystem.NewFileSystem(root)
	machineInfo := common.MachineInfo{
		Hostname:               hostname,
		RoundTripProximityTime: calculateRoundTripProximity(15, string(masterAddr)),
	}

	failureDetector, err := failuredetector.NewFailureDetector(
		string(masterAddr), 2,
		&redis.Options{Addr: "localhost:6379"},
		5*time.Minute,
		failuredetector.SuspicionLevel{
			AccruementThreshold: 70,
			UpperBoundThreshold: 30,
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

	cs := &Server{
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
		for {
			select {
			case <-quickStart:
				branchInfo.Event = string(common.HeartBeat)
				branchInfo.Err = cs.heartBeat()
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
				return
			case <-archiveChunkTicker.C:
				branchInfo.Event = string(common.Archival)
				branchInfo.Err = cs.archiveChunks()
			default:
			}

			if branchInfo.Err != nil {
				log.Err(branchInfo.Err).Msgf(
					fmt.Sprintf("Server %s  background-(%s) event triggered an error (%s)\n",
						cs.ServerAddr, branchInfo.Event, branchInfo.Err))
				return
			}

			log.Info().Msg(fmt.Sprintf(
				"Server %s background-(%s) event trigged\n",
				cs.ServerAddr, branchInfo.Event))
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
func (cs *Server) archiveChunks() error {
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
func (cs *Server) unarchiveChunks(handle common.ChunkHandle) error {
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
func (cs *Server) loadMetadata() error {
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
func (cs *Server) heartBeat() error {
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

	prediction, err := cs.failureDetector.PredictFailure()
	if err != nil {
		return err
	}
	log.Info().Msgf("server=%s prediction=%.2f  message=%s", cs.ServerAddr, prediction.Phi, prediction.Message)
	wg.Wait()
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
func (cs *Server) persistMetadata() error {
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
func (cs *Server) garbageCollection() error {
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
func (cs *Server) deleteChunk(handle common.ChunkHandle) error {
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
func (cs *Server) Shutdown() error {
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
func (cs *Server) RPCSysReportHandler(args rpc_struct.SysReportInfoArg, reply *rpc_struct.SysReportInfoReply) error {
	log.Info().Msgf("Server %s: gathering system statistics", cs.ServerAddr)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	alloc := bToMb(m.Alloc)
	totalAlloc := bToMb(m.TotalAlloc)
	tSys := bToMb(m.Sys)
	numGC := bToMb(uint64(m.NumGC))

	var buf bytes.Buffer
	table := tablewriter.NewWriter(&buf)
	table.Header([]string{"Metric", "Value (MiB)"})
	table.Append([]string{"Alloc", fmt.Sprintf("%v", alloc)})
	table.Append([]string{"TotalAlloc", fmt.Sprintf("%v", totalAlloc)})
	table.Append([]string{"Sys", fmt.Sprintf("%v", tSys)})
	table.Append([]string{"NumGC", fmt.Sprintf("%v", numGC)})
	table.Render()
	log.Info().Msgf("Server %s: memory statistics\n%s", cs.ServerAddr, buf.String())

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

// The master will detect that this chunkserver has a stale replica
// when the chunkserver restarts and reports its set of chunks
// and their associated version numbers. If the master sees a
// version number greater than the one in its records, the master
// assumes that it failed when granting the lease and so
// takes the higher version to be up-to-date.
func (cs *Server) RPCCheckChunkVersionHandler(args rpc_struct.CheckChunkVersionArg, reply *rpc_struct.CheckChunkVersionReply) error {
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

func (cs *Server) RPCReadChunkHandler(args rpc_struct.ReadChunkArgs, reply *rpc_struct.ReadChunkReply) error {
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

func (cs *Server) RPCCreateChunkHandler(args rpc_struct.CreateChunkArgs, reply *rpc_struct.CreateChunkReply) error {
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

func (cs *Server) RPCForwardDataHandler(args rpc_struct.ForwardDataArgs, reply *rpc_struct.ForwardDataReply) error {
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

func (cs *Server) RPCGrantLeaseHandler(args rpc_struct.GrantLeaseInfoArgs, reply *rpc_struct.GrantLeaseInfoReply) error {
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

func (cs *Server) RPCWriteChunkHandler(args rpc_struct.WriteChunkArgs, reply *rpc_struct.WriteChunkReply) error {
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

func doWriteOperation(args rpc_struct.WriteChunkArgs, cs *Server, data []byte) error {
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
			err := cs.unarchiveChunks(handle)
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

func (cs *Server) RPCApplyMutationHandler(args rpc_struct.ApplyMutationArgs, reply *rpc_struct.ApplyMutationReply) error {

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
	err := cs.unarchiveChunks(handle)
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

func (cs *Server) RPCAppendChunkHandler(args rpc_struct.AppendChunkArgs, reply *rpc_struct.AppendChunkReply) error {
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
		err := cs.unarchiveChunks(handle)
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

func (cs *Server) RPCGetSnapshotHandler(args rpc_struct.GetSnapshotArgs, reply *rpc_struct.GetSnapshotReply) error {
	handle := args.Handle
	cs.mu.RLock()
	chInfo, ok := cs.chunks[handle]
	cs.mu.RUnlock()
	if !ok || chInfo.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	err := cs.unarchiveChunks(handle)
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

func (cs *Server) RPCApplyCopyHandler(args rpc_struct.ApplyCopyArgs, reply *rpc_struct.AppendChunkReply) error {
	handle := args.Handle
	err := cs.unarchiveChunks(handle)
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

func (cs *Server) mutate(handle common.ChunkHandle, mutation *common.Mutation) error {
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

func (cs *Server) writeChunk(handle common.ChunkHandle, data []byte, mutationType common.MutationType, offset common.Offset, lock bool) error {

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

func (cs *Server) readChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
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
