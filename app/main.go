package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	constant "github.com/codecrafters-io/kafka-starter-go/internal/constants"
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

func handleRequest(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}

		fmt.Printf("Read %d bytes\n", n)

		req, err := request.UnmarshallRequest(buffer[:n])
		if err != nil {
			fmt.Printf("Error parsing request: %s", err.Error())
			return
		}
		rh, ok := req.Header.(*request.RequestHeaderV2)
		if !ok {
			fmt.Printf("Invalid request header type")
			return
		}

		reqJson, _ := json.MarshalIndent(*req, "", " ")
		fmt.Println("Request json: ", string(reqJson))

		// create response
		var res response.Response
		switch rh.RequestApiKey {
		case constant.ApiVersions:
			errorCode := constant.UNSUPPORTED_VERSION
			if rh.RequestApiVersion >= 0 && rh.RequestApiVersion <= 4 {
				errorCode = 0
			}
			res = response.Response{
				Header: &response.ResponseHeaderV0{
					CorrelationId: rh.CorrelationId,
				},
				Body: &response.APIVersionsResponseV4{
					ErrorCode: errorCode,
					ApiVersions: []response.APIVersion{
						{
							ApiKey:     constant.ApiVersions,
							MinVersion: 0,
							MaxVersion: 4,
						},
						{
							ApiKey:     constant.DescribeTopicPartitions,
							MinVersion: 0,
							MaxVersion: 0,
						},
					},
				},
			}
		case constant.DescribeTopicPartitions:
			rb, ok := req.Body.(*request.DescribeTopicPartitionsV0)
			if !ok {
				fmt.Printf("Invalid request body type")
				return
			}
			topics := make([]response.Topic, len(rb.Topics))
			for i, topic := range rb.Topics {
				topics[i].ErrorCode = constant.UNKNOWN_TOPIC_OR_PARTITION
				topics[i].TopicName = topic.Name
				topics[i].TopicId = [16]byte{}
			}

			res = response.Response{
				Header: &response.ResponseHeaderV1{
					CorrelationId: rh.CorrelationId,
				},
				Body: &response.DescribeTopicPartitionsV0{
					Topics: topics,
					NextCursor: response.Cursor{
						TopicName: "first topic",
					},
				},
			}
		}

		resJson, _ := json.MarshalIndent(res, "", " ")
		fmt.Println("Response json: ", string(resJson))

		respBytes := res.MarshallResponse()
		n, err = conn.Write(respBytes)
		if err != nil {
			fmt.Println("Error sending response payload: ", err.Error())
			return
		}

		fmt.Printf("Sent %d bytes\n", n)
	}
}
