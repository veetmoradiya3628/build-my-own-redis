package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
)

type ServerConfig struct {
	dir        string
	dbfilename string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	dirFlag := flag.String("dir", "", "Directory where RDB files are stored")
	dbFlag := flag.String("dbfilename", "", "Name of the RDB file name")
	flag.Parse()
	config := ServerConfig{
		dir:        *dirFlag,
		dbfilename: *dbFlag,
	}
	slog.Debug("Starting server with dir: %s, dbfilename: %s", dirFlag, dbFlag)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	for {
		slog.Info("listening connection on port : ", "port", 6379)
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		store := NewStore() // Initialize the in-memory store
		go handleConnection(conn, store, config)
	}
}
