package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/request"
	"github.com/codecrafters-io/kafka-starter-go/internal/response"
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
	defer conn.Close()

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}

	fmt.Printf("Read %d bytes\n", n)

	request, err := request.UnmarshallRequest(buffer[:n])
	if err != nil {
		fmt.Printf("Error parsing request: %s", err.Error())
		return
	}
	rh := request.Header

	reqJson, _ := json.MarshalIndent(*request, "", " ")
	fmt.Println("Request json: ", string(reqJson))

	// create response
	response := response.Response{
		Header: &response.ResponseHeaderV0{
			CorrelationId: rh.CorrelationId,
		},
		Body: &response.APIVersionsResponseV4{
			ErrorCode: 0,
			ApiVersions: []response.APIVersion{
				{
					ApiKey:     18,
					MinVersion: 0,
					MaxVersion: 4,
				},
			},
			ThrottleTime: 0,
		},
	}

	resJson, _ := json.MarshalIndent(response, "", " ")
	fmt.Println("Response json: ", string(resJson))

	respBytes := response.MarshallResponse()
	n, err = conn.Write(respBytes)
	if err != nil {
		fmt.Println("Error sending response payload: ", err.Error())
		return
	}

	fmt.Printf("Sent %d bytes\n", n)
}
