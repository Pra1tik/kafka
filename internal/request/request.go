package request

import (
	"bytes"
	"encoding/binary"
	"io"

	constant "github.com/codecrafters-io/kafka-starter-go/internal/constants"
	"github.com/codecrafters-io/kafka-starter-go/internal/types"
)

type RequestHeader interface {
	WriteRequestHeader() []byte
	GetAPIKey() int16
}

type RequestBody interface {
	WriteRequestBody(w io.Writer) error
}

type RequestHeaderV2 struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          types.NullableString
	TagBuffer         types.TaggedFields
}

type Request struct {
	MessageSize int32
	Header      RequestHeader
	Body        RequestBody
}

func ReadRequestHeaderV4(r *bytes.Reader) (*RequestHeaderV2, error) {
	rh := &RequestHeaderV2{}
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

func (rh *RequestHeaderV2) WriteRequestHeader() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rh.RequestApiKey)
	binary.Write(&buf, binary.BigEndian, rh.RequestApiVersion)
	binary.Write(&buf, binary.BigEndian, rh.CorrelationId)
	rh.ClientId.WriteNullableString(&buf)
	rh.TagBuffer.WriteTaggedFields(&buf)
	return buf.Bytes()
}

func (rh *RequestHeaderV2) GetAPIKey() int16 {
	return rh.RequestApiKey
}

func (r Request) MarshallRequest() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	// binary.Write(&buf, binary.BigEndian, r.Header.WriteRequestHeader()) // TBC
	// binary.Write(&buf, binary.BigEndian, r.Body.WriteRequestBody())
	buf.Write(r.Header.WriteRequestHeader())
	r.Body.WriteRequestBody(&buf)
	return buf.Bytes()
}

func UnmarshallRequest(b []byte) (*Request, error) {
	buf := bytes.NewReader(b)
	req := &Request{}
	binary.Read(buf, binary.BigEndian, &req.MessageSize)
	header, err := ReadRequestHeaderV4(buf) // TBC
	if err != nil {
		return nil, err
	}
	req.Header = header
	req.Body, err = ParseRequestBody(header, buf)
	return req, err
}

func ParseRequestBody(h RequestHeader, r *bytes.Reader) (RequestBody, error) {
	switch h.GetAPIKey() {
	case constant.DescribeTopicPartitions:
		return ReadDescribeTopicPartitions(r)
	default:
		return nil, nil
	}
}

// request_api_key 	INT16 	The API key for the request
// request_api_version 	INT16 	The version of the API for the request
// correlation_id 	INT32 	A unique identifier for the request
// client_id 	NULLABLE_STRING 	The client ID for the request
// TAG_BUFFER 	COMPACT_ARRAY 	Optional tagged fields

// unsigned varint -> read, write
// compact -> length encoded with unsigned varint
// nullable -> value can be null

// tagged fields
// tag -> unsigned varint
// data -> compact bytes
