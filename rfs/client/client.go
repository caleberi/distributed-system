package client

import (
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type Client struct {
	masterServer common.ServerAddr
	mu           sync.RWMutex
	leaseCache   map[common.ChunkHandle]*common.Lease
	done         chan bool
}

func NewClient(addr common.ServerAddr, cacheTickerDuration time.Duration) *Client {
	cl := &Client{
		masterServer: addr,
		mu:           sync.RWMutex{},
		leaseCache:   make(map[common.ChunkHandle]*common.Lease),
		done:         make(chan bool, 1),
	}

	go func() {
		tick := time.NewTicker(cacheTickerDuration)
		for {
			select {
			case <-tick.C:
				cl.mu.Lock()
				for _, item := range cl.leaseCache {
					if item.Expire.Before(time.Now()) {
						delete(cl.leaseCache, item.Handle)
					}
				}
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
