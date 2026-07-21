package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// format: *<number of elements>\r\n<element1><element2>...<elementN>
func handleRESPArray(reader *bufio.Reader) (interface{}, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// Parse the number of elements (trimming \r\n)
	count, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return nil, err
	}

	// Handle Null Arrays (e.g., *-1\r\n)
	if count == -1 {
		return nil, nil
	}

	// Recursively parse each element in the array
	var array []interface{}
	for i := 0; i < count; i++ {
		val, err := ParseRESP(reader)
		if err != nil {
			return nil, err
		}
		array = append(array, val)
	}
	return array, nil
}

// handleRESPBulkString parses a RESP Bulk String from the reader.
// It reads the length header, then reads that many bytes and consumes
// the trailing CRLF. Returns a Go string or nil for a Null Bulk String.
// format: $<number of bytes>\r\n<bytes>\r\n
func handleRESPBulkString(reader *bufio.Reader) (interface{}, error) {
	// Read length line (e.g., "3\r\n")
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	items, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return nil, err
	}
	if items == -1 {
		return nil, nil
	}
	buf := make([]byte, items)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}
	// consume trailing CRLF
	if _, err := reader.Discard(2); err != nil {
		return nil, err
	}
	return string(buf), nil
}

// handleRESPSimpleString reads a RESP Simple String ("+...") and returns
// it as a Go string (without CRLF).
// format: +<string>\r\n
func handleRESPSimpleString(reader *bufio.Reader) (interface{}, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// handleRESPError reads a RESP Error ("-...") and returns it as an error.
// format: -<error message>\r\n
func handleRESPError(reader *bufio.Reader) (interface{}, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%s", strings.TrimRight(line, "\r\n"))
}

// handleRESPInteger reads a RESP Integer (":...\r\n") and returns it
// as an int.
// format: :<number>\r\n
func handleRESPInteger(reader *bufio.Reader) (interface{}, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	number, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return nil, err
	}
	return number, nil
}

// ParseRESP parses a RESP value from the reader.
func ParseRESP(reader *bufio.Reader) (interface{}, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '*': // RESP Array
		return handleRESPArray(reader)
	case '$': // RESP Bulk String
		return handleRESPBulkString(reader)
	case '+': // RESP Simple String
		return handleRESPSimpleString(reader)
	case '-': // RESP Error
		return handleRESPError(reader)
	case ':': // RESP Integer
		return handleRESPInteger(reader)
	default:
		return nil, fmt.Errorf("unknown prefix: %c", prefix)
	}
}

func writeBulkString(conn net.Conn, s string) error {
	_, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(s), s)
	return err
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		val, err := ParseRESP(reader)
		if err != nil {
			fmt.Println("parse error:", err)
			break
		}
		// val.([]interface{}) is the parsed RESP array
		// Expecting an array: [command, arg...]
		arr, ok := val.([]interface{})
		if !ok || len(arr) == 0 {
			// ignore non-array or empty
			continue
		}
		// command should be a string (bulk or simple)
		cmdRaw, ok := arr[0].(string)
		if !ok {
			continue
		}
		cmd := strings.ToUpper(cmdRaw)
		switch cmd {
		case "PING":
			if len(arr) == 1 {
				conn.Write([]byte("+PONG\r\n"))
			}
		case "ECHO":
			if len(arr) < 2 {
				// return nil bulk string
				conn.Write([]byte("$-1\r\n"))
				continue
			}
			arg, _ := arr[1].(string)
			writeBulkString(conn, arg)
		default:
			// Unknown command: respond with error
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
