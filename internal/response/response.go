package response

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/internal/types"
)

type ResponseHeader interface {
	Write(io.Writer) error
}

type ResponseBody interface {
	Write(io.Writer) error
}

type ResponseHeaderV0 struct {
	CorrelationId int32
}

func (rh *ResponseHeaderV0) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, rh.CorrelationId)
}

type APIVersionsResponseV4 struct {
	ErrorCode    int16
	ApiVersions  []APIVersion
	ThrottleTime int32
	TagBuffer    types.TaggedFields
}

type APIVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  types.TaggedFields
}

func (rb *APIVersionsResponseV4) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, rb.ErrorCode); err != nil {
		return err
	}
	// apiVersions is a compact array
	types.WriteUvarint(w, uint64(len(rb.ApiVersions))) // TBC (varint or 1byte length)
	for _, apiVersion := range rb.ApiVersions {
		binary.Write(w, binary.BigEndian, apiVersion.ApiKey)
		binary.Write(w, binary.BigEndian, apiVersion.MinVersion)
		binary.Write(w, binary.BigEndian, apiVersion.MaxVersion)
		apiVersion.TagBuffer.WriteTaggedFields(w)
	}
	binary.Write(w, binary.BigEndian, rb.ThrottleTime)
	rb.TagBuffer.WriteTaggedFields(w) // TBC (required or not)
	return nil
}

type Response struct {
	MessageSize int32
	Header      ResponseHeader
	Body        ResponseBody
}

func (r Response) MarshallResponse() []byte {
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	r.Header.Write(&buf)
	r.Body.Write(&buf)
	bufBytes := buf.Bytes()
	binary.BigEndian.PutUint32(bufBytes[0:4], uint32(len(bufBytes)-4))
	return bufBytes
}
