package request

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/internal/types"
)

type DescribeTopicPartitionsV0 struct {
	Topics                 []Topic
	ResponsePartitionLimit int32
	Cursor                 int8 // nullable used for pagination
	TagBuffer              types.TaggedFields
}

type Topic struct {
	Name      types.CompactString
	TagBuffer types.TaggedFields
}

func ReadTopic(r *bytes.Reader) (*Topic, error) {
	name, err := types.ReadCompactString(r)
	if err != nil {
		return nil, err
	}
	tagBuffer, err := types.ReadTaggedFields(r)
	if err != nil {
		return nil, err
	}
	return &Topic{
		Name:      *name,
		TagBuffer: *tagBuffer,
	}, nil
}

func (t *Topic) WriteTopic(w io.Writer) error {
	if err := t.Name.WriteCompactString(w); err != nil {
		return err
	}
	return t.TagBuffer.WriteTaggedFields(w)
}

func ReadDescribeTopicPartitions(r *bytes.Reader) (*DescribeTopicPartitionsV0, error) {
	numTopics, err := types.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	numTopics -= 1
	describeTopicPartitions := &DescribeTopicPartitionsV0{
		Topics: make([]Topic, numTopics),
	}
	for i := range numTopics {
		topic, err := ReadTopic(r)
		if err != nil {
			return nil, err
		}
		describeTopicPartitions.Topics[i] = *topic
	}
	binary.Read(r, binary.BigEndian, &describeTopicPartitions.ResponsePartitionLimit)
	binary.Read(r, binary.BigEndian, &describeTopicPartitions.Cursor)
	tagBuffer, err := types.ReadTaggedFields(r)
	if err != nil {
		return nil, err
	}
	describeTopicPartitions.TagBuffer = *tagBuffer
	return describeTopicPartitions, nil
}

func (r *DescribeTopicPartitionsV0) WriteRequestBody(w io.Writer) error {
	if err := types.WriteUvarint(w, uint64(len(r.Topics))+1); err != nil {
		return err
	}
	for _, topic := range r.Topics {
		if err := topic.WriteTopic(w); err != nil {
			return err
		}
	}
	binary.Write(w, binary.BigEndian, r.ResponsePartitionLimit)
	binary.Write(w, binary.BigEndian, r.Cursor)
	return r.TagBuffer.WriteTaggedFields(w)
}
