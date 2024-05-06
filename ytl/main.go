package main

import (
	"fmt"
	"time"

	"github.com/caleberi/distributed-system/rfs/client"
	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/rs/zerolog/log"
)

func main() {
	addr := "127.0.0.1:9090"
	client := client.NewClient(common.ServerAddr(addr), 30*time.Millisecond, client.Credentials{})
	handle, err := client.GetChunkHandle("/tmp/test8", common.ChunkIndex(0))
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return
	}
	log.Info().Msg(fmt.Sprintf("Got a new handle : %v", handle))
	lease, err := client.GetChunkServers(handle)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return
	}
	log.Debug().Msg(fmt.Sprintf("lease gotten %#v", lease))
	client.Close()
}