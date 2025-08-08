package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleRequest(conn)
	}
}

// message_size
// Header
// Body

// request_api_key 	INT16 	The API key for the request
// request_api_version 	INT16 	The version of the API for the request
// correlation_id 	INT32 	A unique identifier for the request
// client_id 	NULLABLE_STRING 	The client ID for the request
// TAG_BUFFER 	COMPACT_ARRAY 	Optional tagged fields

func handleRequest(conn net.Conn) {
	buffer := make([]byte, 1024)

	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
	}

	// Prepare payload
	messageSize := uint32(0)
	requestApiVersion := binary.BigEndian.Uint16(buffer[6:8])
	correlationID := binary.BigEndian.Uint32(buffer[8:12])

	// fmt.Println(buffer)
	fmt.Println(requestApiVersion)
	fmt.Println(correlationID)

	errorCode := uint16(0)
	if requestApiVersion != 4 {
		errorCode = 35
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, messageSize)
	binary.Write(&buf, binary.BigEndian, correlationID)
	binary.Write(&buf, binary.BigEndian, errorCode)

	fmt.Println(buf.Bytes())

	_, err = conn.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error sending payload: ", err.Error())
		os.Exit(1)
	}
}
