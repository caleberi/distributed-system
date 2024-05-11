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
	client := client.NewClient(common.ServerAddr(addr), 30*time.Millisecond)
	// handle, err := client.GetChunkHandle("/movies/avatar-sky.mp4", common.ChunkIndex(0))
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// log.Info().Msg(fmt.Sprintf("Got a new handle : %v", handle))
	// lease, err := client.GetChunkServers(handle)
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// log.Debug().Msg(fmt.Sprintf("lease gotten %#v", lease))

	// pathInfos, err := client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })

	// err = client.MkDir("/test-files/js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// err = client.CreateFile("/test-files/js/index.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// pathInfos, err = client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })

	// err = client.DeleteFile("/test-files/js/index.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// pathInfos, err = client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })

	// err = client.CreateFile("/test-files/js/app.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// err = client.RenameFile("/test-files/js/app.js", "/test-files/js/ok.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// pathInfos, err := client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })
	fileInfo, err := client.GetFile("/movies")
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return
	}
	log.Info().Msg(fmt.Sprintf("FileInfo :=> %v", *fileInfo))

	client.Close()
}
