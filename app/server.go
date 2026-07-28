package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
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
	if config.dir != "" && config.dbfilename != "" {
		// Construct the full path securely
		rdbPath := filepath.Join(config.dir, config.dbfilename)
		slog.Info("Loading RDB file", "path", rdbPath)

		// Use the LoadRDB function we built earlier
		initialData = LoadRDB(rdbPath)
	} else {
		// No flags provided, start with an empty map
		initialData = make(map[string]any)
	}

	// We need to update your NewStore function to accept this initial map
	store := NewStore(initialData)

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
