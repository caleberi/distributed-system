package client

import (
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/rpc_struct"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type Credentials struct {
	Username string
	Password string
}

type Client struct {
	masterServer common.ServerAddr
	credentials  Credentials

	mu                 sync.RWMutex
	chunkToServerCache map[common.ChunkHandle]*common.Lease
	done               chan bool
}

func NewClient(addr common.ServerAddr, cacheTickerDuration time.Duration, credentials Credentials) *Client {
	cl := &Client{
		masterServer:       addr,
		credentials:        credentials,
		mu:                 sync.RWMutex{},
		chunkToServerCache: make(map[common.ChunkHandle]*common.Lease),
		done:               make(chan bool, 1),
	}

	go func() {
		tick := time.NewTicker(cacheTickerDuration)
		for {
			select {
			case <-tick.C:
				cl.mu.Lock()
				for _, item := range cl.chunkToServerCache {
					if item.Expire.Before(time.Now()) {
						delete(cl.chunkToServerCache, item.Handle)
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
	<-c.done
	close(c.done)
}

// GetChunkHandle allows the client to pass a path and index and retrieve a handle
// if the handle does not exist then create one in the path & namespace
func (c *Client) GetChunkHandle(path common.Path, idx common.ChunkIndex) (common.ChunkHandle, error) {
	var reply rpc_struct.GetChunkHandleReply
	err := utils.CallRPCServer(string(c.masterServer), "MasterServer.RPCGetChunkHandleHandler", rpc_struct.GetChunkHandleArgs{Path: path, Index: idx}, &reply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return -1, err
	}
	return reply.Handle, nil
}

func (c *Client) GetChunkServers(handle common.ChunkHandle) (*common.Lease, error) {
	c.mu.RLock()
	ls, ok := c.chunkToServerCache[handle]
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
		c.chunkToServerCache[handle] = nls
		c.mu.Unlock()

		return nls, nil
	}
	return ls, nil
}
