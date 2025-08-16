package request

import (
	"bytes"
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/internal/types"
)

type RequestHeader struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          types.NullableString
	TagBuffer         types.TaggedFields
}

type Request struct {
	MessageSize int32
	Header      RequestHeader
	Body        []byte
}

func ReadRequestHeader(r *bytes.Reader) (*RequestHeader, error) {
	rh := &RequestHeader{}
	binary.Read(r, binary.BigEndian, &rh.RequestApiKey)
	binary.Read(r, binary.BigEndian, &rh.RequestApiVersion)
	binary.Read(r, binary.BigEndian, &rh.CorrelationId)
	ci, err := types.ReadNullableString(r)
	if err != nil {
		return nil, err
	}
	rh.ClientId = *ci
	tb, err := types.ReadTaggedFields(r)
	if err != nil {
		return nil, err
	}
	rh.TagBuffer = *tb
	return rh, nil
}

func (rh *RequestHeader) WriteRequestHeader() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rh.RequestApiKey)
	binary.Write(&buf, binary.BigEndian, rh.RequestApiVersion)
	binary.Write(&buf, binary.BigEndian, rh.CorrelationId)
	rh.ClientId.WriteNullableString(&buf)
	rh.TagBuffer.WriteTaggedFields(&buf)
	return buf.Bytes()
}

func (r Request) MarshallRequest() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	binary.Write(&buf, binary.BigEndian, r.Header.WriteRequestHeader()) // TBC
	binary.Write(&buf, binary.BigEndian, r.Body)
	return buf.Bytes()
}

func UnmarshallRequest(b []byte) (*Request, error) {
	buf := bytes.NewReader(b)
	req := &Request{}
	binary.Read(buf, binary.BigEndian, &req.MessageSize)
	header, err := ReadRequestHeader(buf) // TBC
	if err != nil {
		return nil, err
	}
	req.Header = *header //TBC
	buf.Read(req.Body)   //TBC
	return req, nil
}

// unsigned varint -> read, write
// compact -> length encoded with unsigned varint
// nullable -> value can be null

// tagged fields
// tag -> unsigned varint
// data -> compact bytes
