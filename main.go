package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	chunkserver "github.com/caleberi/distributed-system/rfs/chunkserver"
	"github.com/caleberi/distributed-system/rfs/common"
	masterserver "github.com/caleberi/distributed-system/rfs/master"
	"github.com/rs/zerolog"
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func main() {
	master := flag.Bool("isMaster", false, "should run program as master server")
	addr := flag.String("serverAddress", "127.0.0.1:8085", "port to listen on")
	maddr := flag.String("masterAddr", "127.0.0.1:9090", "master server addr")
	rootFs := flag.String("rootDir", "./croot", "file system dir to use")

	flag.Parse()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	if *master {
		masterserver := masterserver.NewMasterServer(common.ServerAddr(*maddr), "./mroot")
		<-quit
		masterserver.Shutdown()
	} else {
		chunkserver := chunkserver.NewChunkServer(common.ServerAddr(*addr), common.ServerAddr(*maddr), *rootFs)
		<-quit
		chunkserver.Shutdown()
	}

}
