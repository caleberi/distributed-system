// main.go is the entry point for the Hercules distributed system, launching either a MasterServer
// or a ChunkServer based on command-line flags. It sets up signal handling for graceful shutdown
// and initializes the server with configurable parameters for address, root directory, and logging.
//
// Usage:
//
//	go run main.go [-isMaster] [-serverAddress <address>] [-masterAddr <address>] [-rootDir <directory>] [-logLevel <level>]
//
// Example:
//
//	# Run as MasterServer
//	go run main.go -isMaster -serverAddress 127.0.0.1:9090 -rootDir mroot
//	# Run as ChunkServer
//	go run main.go -serverAddress 127.0.0.1:8085 -masterAddr 127.0.0.1:9090 -rootDir croot
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	chunkserver "github.com/caleberi/distributed-system/chunkserver"
	"github.com/caleberi/distributed-system/common"
	masterserver "github.com/caleberi/distributed-system/master_server"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Config struct {
	IsMaster      bool
	ServerAddress common.ServerAddr
	MasterAddress common.ServerAddr
	RootDir       string
	LogLevel      string
}

func parseConfig() (Config, error) {
	isMaster := flag.Bool("isMaster", false, "run as master server (default: chunk server)")
	serverAddress := flag.String("serverAddress", "127.0.0.1:8085", "server address to listen on (host:port)")
	masterAddress := flag.String("masterAddr", "127.0.0.1:9090", "master server address (host:port)")
	rootDir := flag.String("rootDir", "mroot", "root directory for file system storage")
	logLevel := flag.String("logLevel", "debug", "logging level (debug, info, warn, error)")

	flag.Parse()

	absRootDir, err := filepath.Abs(*rootDir)
	if err != nil {
		return Config{}, fmt.Errorf("failed to resolve root directory %s: %w", *rootDir, err)
	}

	switch *logLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, fmt.Errorf("invalid log level: %s; must be debug, info, warn, or error", *logLevel)
	}

	return Config{
		IsMaster:      *isMaster,
		ServerAddress: common.ServerAddr(*serverAddress),
		MasterAddress: common.ServerAddr(*masterAddress),
		RootDir:       absRootDir,
		LogLevel:      *logLevel,
	}, nil
}

func setupLogger(level string) error {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		return fmt.Errorf("unsupported log level: %s", level)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	return nil
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse configuration: %v\n", err)
		os.Exit(1)
	}

	if err := setupLogger(cfg.LogLevel); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logger: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	if cfg.IsMaster {
		log.Info().Msgf("Starting MasterServer on %s with root directory %s", cfg.ServerAddress, cfg.RootDir)
		server := masterserver.NewMasterServer(ctx, cfg.MasterAddress, cfg.RootDir)
		go func() {
			<-quit
			log.Info().Msg("Received shutdown signal, stopping MasterServer...")
			server.Shutdown()
			cancel()
		}()
		<-ctx.Done()
	} else {
		log.Info().Msgf("Starting ChunkServer on %s, connecting to master at %s, using root directory %s",
			cfg.ServerAddress, cfg.MasterAddress, cfg.RootDir)
		server, err := chunkserver.NewChunkServer(cfg.ServerAddress, cfg.MasterAddress, cfg.RootDir)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create ChunkServer")
			os.Exit(1)
		}
		go func() {
			<-quit
			log.Info().Msg("Received shutdown signal, stopping ChunkServer...")
			server.Shutdown()
			cancel()
		}()
		<-ctx.Done()
	}

	log.Info().Msg("Server shutdown complete")
}
