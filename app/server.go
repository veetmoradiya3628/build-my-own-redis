package main

import (
	"flag"
	"fmt"
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
	fmt.Println("Logs from your program will appear here!")

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
	} else {
		initialData = make(map[string]any)
		initialExpiry = make(map[string]time.Time)
	}

	// Update NewStore call
	store := NewStore(initialData, initialExpiry)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	slog.Info("listening connection on port : 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		// Pass the shared store to the handler
		go handleConnection(conn, store, config)
	}
}
