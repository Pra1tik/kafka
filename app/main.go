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

	client, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	messageSize := uint32(0)
	correlationID := uint32(7)

	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, messageSize)
	binary.Write(&buf, binary.BigEndian, correlationID)

	fmt.Println(buf.Bytes())

	_, err = client.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error sending payload: ", err.Error())
		os.Exit(1)
	}
}
