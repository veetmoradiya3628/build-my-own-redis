package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

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
