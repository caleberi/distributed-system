package client

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	chunkserver "github.com/caleberi/distributed-system/rfs/chunkserver"
	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type Client struct {
	mu           sync.RWMutex
	done         chan bool
	masterServer common.ServerAddr
	leaseCache   map[common.ChunkHandle]*common.Lease
}

func NewClient(
	addr common.ServerAddr,
	cacheTickerDuration time.Duration) *Client {
	cl := &Client{
		masterServer: addr,
		mu:           sync.RWMutex{},
		done:         make(chan bool, 1),
		leaseCache:   make(map[common.ChunkHandle]*common.Lease),
	}

	go func() {
		tick := time.NewTicker(cacheTickerDuration)
		action := func(
			handle common.ChunkHandle, lease *common.Lease) {
			if lease.IsExpired(time.Now().Add(30 * time.Second)) {
				delete(cl.leaseCache, lease.Handle)
			}
		}
		for {
			select {
			case <-tick.C:
				cl.mu.Lock()
				utils.IterateOverMap(cl.leaseCache, action)
				cl.mu.Unlock()
			case <-cl.done:
				return
			}
		}
	}()

	return cl
}

func (c *Client) Close() {
	c.done <- true
	close(c.done)
}

func (c *Client) getLease(
	handle common.ChunkHandle,
	offset common.Offset) (*common.Lease, common.Offset, error) {
	c.mu.RLock()
	lease, ok := c.leaseCache[handle]
	c.mu.RUnlock()
	if ok {
		return lease, 0, nil
	}
	ls, err := c.GetChunkServers(handle)
	if err != nil {
		log.Err(err).Stack()
		return nil, offset, common.Error{
			Code: common.UnknownError,
			Err:  "could not retrieve lease",
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ls.IsExpired(time.Now()) {
		log.Info().Msgf("getLease = %v has expired before use", ls)
		return nil, 0, fmt.Errorf("getLease = %v has expired before use", ls)
	}
	c.leaseCache[handle] = ls
	return ls, 0, nil
}

func (c *Client) GetChunkHandle(path common.Path, offset common.ChunkIndex) (common.ChunkHandle, error) {
	var reply rpc_struct.GetChunkHandleReply
	err := utils.CallRPCServer(
		string(c.masterServer),
		"MasterServer.RPCGetChunkHandleHandler",
		rpc_struct.GetChunkHandleArgs{
			Path:  path,
			Index: offset,
		}, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return -1, err
	}
	return reply.Handle, nil
}

func (c *Client) GetChunkServers(handle common.ChunkHandle) (*common.Lease, error) {
	c.mu.RLock()
	ls, ok := c.leaseCache[handle]
	c.mu.RUnlock()

	if !ok {
		var info rpc_struct.PrimaryAndSecondaryServersInfoReply
		err := utils.CallRPCServer(
			string(c.masterServer),
			"MasterServer.RPCGetPrimaryAndSecondaryServersInfoHandler",
			rpc_struct.PrimaryAndSecondaryServersInfoArg{Handle: handle},
			&info,
		)
		if err != nil {
			return nil, err
		}
		nls := &common.Lease{
			Handle:      handle,
			Expire:      info.Expire,
			Primary:     info.Primary,
			Secondaries: info.SecondaryServers,
		}

		if nls.IsExpired(time.Now()) {
			log.Info().Msgf("GetChunkServers = %v has expired before use", nls)
		}
		c.mu.Lock()
		c.leaseCache[handle] = nls
		c.mu.Unlock()

		return nls, nil
	}
	return ls, nil
}

func (c *Client) List(path common.Path) ([]common.PathInfo, error) {
	var (
		result []common.PathInfo
		args   rpc_struct.GetPathInfoArgs
		reply  rpc_struct.GetPathInfoReply
	)
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCListHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return nil, err
	}

	result = reply.Entries
	return result, nil
}

func (c *Client) MkDir(path common.Path) error {
	var (
		args  rpc_struct.MakeDirectoryArgs
		reply rpc_struct.MakeDirectoryReply
	)
	args.Path = path
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCMkdirHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return nil
}

func (c *Client) CreateFile(path common.Path) error {
	var (
		args  rpc_struct.CreateFileArgs
		reply rpc_struct.CreateFileReply
	)
	args.Path = path
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCCreateFileHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return nil
}

func (c *Client) DeleteFile(path common.Path) error {
	var (
		args  rpc_struct.DeleteFileArgs
		reply rpc_struct.DeleteFileReply
	)
	args.Path = path
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCDeleteFileHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return nil
}

func (c *Client) RenameFile(source, target common.Path) error {
	var (
		args  rpc_struct.RenameFileArgs
		reply rpc_struct.RenameFileReply
	)
	args.Source = source
	args.Target = target
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCRenameHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}
	return nil
}

func (c *Client) GetFile(path common.Path) (*common.FileInfo, error) {
	var (
		args  rpc_struct.GetFileInfoArgs
		reply rpc_struct.GetFileInfoReply
	)
	args.Path = path
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCGetFileInfoHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return nil, err
	}

	var fileInfo common.FileInfo
	fileInfo.Chunks = reply.Chunks
	fileInfo.IsDir = reply.IsDir
	fileInfo.Length = reply.Length
	return &fileInfo, err
}

func (c *Client) Read(path common.Path, offset common.Offset, data []byte) (n int, err error) {
	var (
		args  rpc_struct.GetFileInfoArgs
		reply rpc_struct.GetFileInfoReply
	)
	args.Path = path
	err = utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCGetFileInfoHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return
	}

	if offset/common.ChunkMaxSizeInByte > common.Offset(reply.Chunks) {
		err = fmt.Errorf("offset [%v] cannot be greater than the file size", offset)
		return
	}

	if reply.IsDir {
		err = fmt.Errorf("cannot read %s since it is a directory", path)
		return
	}

	pos := 0
	for pos < len(data) {
		index := common.Offset(offset / common.ChunkMaxSizeInByte)
		chunkOffset := offset % common.ChunkMaxSizeInByte

		if index > common.Offset(reply.Chunks) {
			err = common.Error{Code: common.ReadEOF, Err: "EOF over chunk"}
			return
		}

		var handle common.ChunkHandle
		handle, err = c.GetChunkHandle(args.Path, common.ChunkIndex(index))
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return
		}
		var n int
		for {
			n, err = c.ReadChunk(handle, chunkOffset, data[pos:])
			if err == nil || err.(common.Error).Code == common.ReadEOF {
				break
			}
			log.Err(err).Stack().Msg(err.Error())
		}

		offset += common.Offset(n)
		pos += n
		if err != nil {
			break
		}
	}

	if err != nil && err.(common.Error).Code == common.ReadEOF {
		return pos, io.EOF
	}

	return pos, err
}

func (c *Client) ReadChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
	var readLength int

	if common.ChunkMaxSizeInByte-offset > common.Offset(len(data)) {
		readLength = len(data)
	} else {
		readLength = int(common.ChunkMaxSizeInByte - offset)
	}

	var (
		replicasArgs  rpc_struct.RetrieveReplicasArgs
		replicasReply rpc_struct.RetrieveReplicasReply
	)
	replicasArgs.Handle = handle
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCGetReplicasHandler", replicasArgs, &replicasReply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return 0, common.Error{Code: common.UnknownError, Err: err.Error()}
	}
	locations := replicasReply.Locations

	if len(locations) == 0 {
		return 0, common.Error{Code: common.UnknownError, Err: "no available replica"}
	}

	chosenReadServer := locations[rand.Intn(len(replicasReply.Locations))]

	var (
		readChunkArg   rpc_struct.ReadChunkArgs
		readChunkReply rpc_struct.ReadChunkReply
	)
	readChunkArg.Handle = handle
	readChunkArg.Data = data
	readChunkArg.Length = int64(readLength)
	readChunkArg.Offset = offset
	err = utils.CallRPCServer(string(chosenReadServer), "ChunkServer.RPCReadChunkHandler", readChunkArg, &readChunkReply)

	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return 0, common.Error{Code: common.UnknownError, Err: err.Error()}
	}

	if readChunkReply.ErrorCode == common.ReadEOF {
		return int(readChunkReply.Length), common.Error{Code: common.ReadEOF, Err: "EOF error during read"}
	}

	copy(data, readChunkReply.Data)
	return int(readChunkReply.Length), nil

}

func (c *Client) Write(path common.Path, offset common.Offset, data []byte) error {
	var (
		args  rpc_struct.GetFileInfoArgs
		reply rpc_struct.GetFileInfoReply
	)
	args.Path = path
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCGetFileInfoHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return err
	}

	if offset/common.ChunkMaxSizeInByte > common.Offset(reply.Chunks) {
		return fmt.Errorf("write offset [%v] cannot be greater than the file size", offset)
	}

	if reply.IsDir {
		return fmt.Errorf("cannot read %s since it is a directory", path)
	}

	pos := 0
	for pos < len(data) {
		index := common.Offset(offset / common.ChunkMaxSizeInByte)
		chunkOffset := offset % common.ChunkMaxSizeInByte
		handle, err := c.GetChunkHandle(args.Path, common.ChunkIndex(index))
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return err
		}

		writeMax := int(common.ChunkMaxSizeInByte - chunkOffset)
		var writeLength int
		if pos+writeMax > len(data) {
			writeLength = len(data) - pos
		} else {
			writeLength = writeMax
		}

		err = c.WriteChunk(handle, chunkOffset, data[pos:pos+writeLength])
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			break
		}

		offset += common.Offset(writeLength)
		pos += writeLength
		if pos == len(data) {
			break
		}
	}

	return nil
}

func (c *Client) WriteChunk(handle common.ChunkHandle, offset common.Offset, data []byte) error {
	totalDataLengthToWrite := len(data) + int(offset)

	if totalDataLengthToWrite > common.ChunkMaxSizeInByte {
		return fmt.Errorf("totalDataLengthToWrite = %v is greater than the max chunk size %v", totalDataLengthToWrite, common.ChunkMaxSizeInByte)
	}

	writeLease, offset, err := c.getLease(handle, offset)
	if err != nil {
		return err
	}
	servers := append(writeLease.Secondaries, writeLease.Primary)
	copy(utils.Filter(servers, func(v common.ServerAddr) bool { return string(v) != "" }), servers)
	if len(servers) == 0 {
		return common.Error{Code: common.UnknownError, Err: "no replica"}
	}

	if writeLease.Primary == "" {
		writeLease.Primary = servers[0]
		servers = servers[1:]
	}

	log.Info().Msgf("Servers from the lease = %v", servers)
	dataID := chunkserver.NewDBufferId(handle)

	var errs []string
	log.Info().Msgf("RPCForwardDataHandler = %v", servers)
	utils.ForEach(servers, func(addr common.ServerAddr) {
		var d rpc_struct.ForwardDataReply
		if addr != "" {
			replicas := utils.Filter(servers, func(v common.ServerAddr) bool { return v != addr })
			err = utils.CallRPCServer(string(addr),
				"ChunkServer.RPCForwardDataHandler",
				rpc_struct.ForwardDataArgs{
					DownloadBufferId: dataID,
					Data:             data,
					Replicas:         replicas,
				}, &d)
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
	})
	if len(errs) != 0 {
		errStr := strings.Join(errs, ";")
		log.Err(errors.New(errStr)).Stack()
	}

	writeArgs := rpc_struct.WriteChunkArgs{
		DownloadBufferId: dataID,
		Offset:           offset,
		Replicas:         servers,
	}

	return utils.CallRPCServer(
		string(writeLease.Primary),
		"ChunkServer.RPCWriteChunkHandler",
		writeArgs,
		&rpc_struct.WriteChunkReply{},
	)
}

func (c *Client) Append(path common.Path, data []byte) (offset common.Offset, err error) {
	if len(data) > common.AppendMaxSizeInByte {
		return 0, fmt.Errorf("len of data [%v] > max append size [%v]", len(data), common.AppendMaxSizeInByte)
	}

	var (
		args  rpc_struct.GetFileInfoArgs
		reply rpc_struct.GetFileInfoReply
	)
	args.Path = path
	err = utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCGetFileInfoHandler", args, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return
	}

	// use the last chunk we created on the master server since
	// we are doing an append mutation
	start := common.ChunkIndex(math.Max(float64(reply.Chunks-1), 0.0))
	var (
		handle      common.ChunkHandle
		chunkOffset common.Offset
	)

	totalWritten := 0
	for totalWritten < len(data) {
		handle, err = c.GetChunkHandle(args.Path, common.ChunkIndex(start))
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			return
		}

		chunkOffset, err = c.AppendChunk(handle, data)
		if err != nil {
			log.Err(err).Stack().Msg(err.Error())
			if err.(common.Error).Code == common.AppendExceedChunkSize {
				continue
			}
			break
		}

		totalWritten += int(chunkOffset)
		start++
		log.Info().Msg("padding more on next chunk")
	}
	offset = common.Offset(start)*common.ChunkMaxSizeInByte + chunkOffset
	return
}

func (c *Client) AppendChunk(handle common.ChunkHandle, data []byte) (common.Offset, error) {
	var offset common.Offset

	if len(data) > common.AppendMaxSizeInByte {
		return offset, common.Error{
			Code: common.UnknownError,
			Err:  fmt.Sprintf("len(data)[%v]  > max append size (%v)", len(data), common.AppendExceedChunkSize),
		}
	}

	appendLease, offset, err := c.getLease(handle, 0)
	if err != nil {
		return offset, err
	}

	servers := append(appendLease.Secondaries, appendLease.Primary)
	copy(utils.Filter(servers, func(v common.ServerAddr) bool { return string(v) != "" }), servers)
	if len(servers) == 0 {
		return offset, common.Error{Code: common.UnknownError, Err: "no replica"}
	}

	if appendLease.Primary == "" {
		appendLease.Primary = servers[0]
		appendLease.Secondaries = servers[1:]
	}
	if len(servers) == 0 {
		return offset, common.Error{Code: common.UnknownError, Err: "no replica"}
	}

	dataID := chunkserver.NewDBufferId(handle)
	var errs []string
	log.Info().Msgf("RPCForwardDataHandler = %v", servers)
	utils.ForEach(servers, func(addr common.ServerAddr) {
		var d rpc_struct.ForwardDataReply
		if addr != "" {
			replicas := utils.Filter(servers, func(v common.ServerAddr) bool { return v != addr })
			err = utils.CallRPCServer(string(addr),
				"ChunkServer.RPCForwardDataHandler",
				rpc_struct.ForwardDataArgs{
					DownloadBufferId: dataID,
					Data:             data,
					Replicas:         replicas,
				}, &d)
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
	})
	if len(errs) != 0 {
		errStr := strings.Join(errs, ";")
		log.Err(errors.New(errStr)).Stack()
	}

	var (
		appendArgs  rpc_struct.AppendChunkArgs
		appendReply rpc_struct.AppendChunkReply
	)
	appendArgs.DownloadBufferId = dataID
	appendArgs.Replicas = appendLease.Secondaries
	err = utils.CallRPCServer(
		string(appendLease.Primary),
		"ChunkServer.RPCAppendChunkHandler",
		appendArgs, &appendReply)
	if err != nil {
		return -1, common.Error{Code: common.UnknownError, Err: err.Error()}
	}
	if appendReply.ErrorCode == common.AppendExceedChunkSize {
		return appendReply.Offset, common.Error{
			Code: common.UnknownError,
			Err:  "exceed append chunk size",
		}
	}
	return appendReply.Offset, nil
}
