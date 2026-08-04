package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"
)

type ServerConfig struct {
	dir        string
	dbfilename string
}

func main() {
	slog.Info("Logs from your program will appear here!")

	dirFlag := flag.String("dir", "", "Directory where RDB files are stored")
	dbFlag := flag.String("dbfilename", "", "Name of the RDB file name")
	flag.Parse()

	config := ServerConfig{
		dir:        *dirFlag,
		dbfilename: *dbFlag,
	}
	slog.Debug("Starting server", "dir", config.dir, "dbfilename", config.dbfilename)

	// 1. Check if both flags are provided
	var initialData map[string]any
	var initialExpiry map[string]time.Time

	if config.dir != "" && config.dbfilename != "" {
		rdbPath := filepath.Join(config.dir, config.dbfilename)
		slog.Info("Loading RDB file", "path", rdbPath)

		// Load both maps
		initialData, initialExpiry = LoadRDB(rdbPath)
		slog.Info("RDB load summary", "keys_loaded", len(initialData), "expiry_entries", len(initialExpiry))
	} else {
		initialData = make(map[string]any)
		initialExpiry = make(map[string]time.Time)
	}

	// Update NewStore call
	store := NewStore(initialData, initialExpiry)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		slog.Error("failed to bind to port 6379", "err", err)
		os.Exit(1)
	}
	defer l.Close()

	slog.Info("listening connection on port : 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("error accepting connection", "err", err)
			continue
		}

		slog.Debug("accepted connection", "remote", conn.RemoteAddr())
		// Pass the shared store to the handler
		go handleConnection(conn, store, config)
	}
}
