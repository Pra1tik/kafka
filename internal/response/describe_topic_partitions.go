package response

import (
	"encoding/binary"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/internal/types"
)

type DescribeTopicPartitionsV0 struct {
	ThrottleTime int32
	Topics       []Topic
	NextCursor   Cursor
	TagBuffer    types.TaggedFields
}

func (r *DescribeTopicPartitionsV0) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ThrottleTime); err != nil {
		return err
	}
	if err := types.WriteUvarint(w, uint64(len(r.Topics)+1)); err != nil {
		return err
	}
	for _, topic := range r.Topics {
		if err := topic.Write(w); err != nil {
			return err
		}
	}
	if err := r.NextCursor.Write(w); err != nil {
		return nil
	}
	return r.TagBuffer.WriteTaggedFields(w)
}

type Cursor struct {
	TopicName      types.CompactString
	PartitionIndex int32
	TagBuffer      types.TaggedFields
}

func (c *Cursor) Write(w io.Writer) error {
	if err := c.TopicName.WriteCompactString(w); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, c.PartitionIndex); err != nil {
		return err
	}
	return c.TagBuffer.WriteTaggedFields(w)
}

type Topic struct {
	ErrorCode                int16
	TopicName                types.CompactString
	TopicId                  [16]byte
	IsInternal               byte
	Partitions               Partitions
	TopicAuthorizeOperations int32
	TagBuffer                types.TaggedFields
}

func (t *Topic) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, t.ErrorCode); err != nil {
		return err
	}
	if err := t.TopicName.WriteCompactString(w); err != nil {
		return err
	}
	if _, err := w.Write(t.TopicId[:]); err != nil {
		return err
	}
	if _, err := w.Write([]byte{t.IsInternal}); err != nil {
		return nil
	}
	if err := t.Partitions.Write(w); err != nil {
		return nil
	}
	if err := binary.Write(w, binary.BigEndian, t.TopicAuthorizeOperations); err != nil {
		return err
	}
	return t.TagBuffer.WriteTaggedFields(w)
}

type Partitions struct {
	Partitions []Partition
	TagBuffer  types.TaggedFields
}

func (p *Partitions) Write(w io.Writer) error {
	if err := types.WriteUvarint(w, uint64(len(p.Partitions)+1)); err != nil {
		return err
	}
	for _, partition := range p.Partitions {
		if err := partition.Write(w); err != nil {
			return err
		}
	}
	return p.TagBuffer.WriteTaggedFields(w)
}

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           Nodes
	ISRNodes               Nodes
	EligibleLeaderReplicas Nodes
	LastKnownELRs          Nodes
	OfflineReplicas        Nodes
}

func (p *Partition) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, p.ErrorCode); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, p.PartitionIndex); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, p.LeaderId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, p.LeaderEpoch); err != nil {
		return err
	}
	if err := p.ReplicaNodes.Write(w); err != nil {
		return err
	}
	if err := p.ISRNodes.Write(w); err != nil {
		return err
	}
	if err := p.EligibleLeaderReplicas.Write(w); err != nil {
		return err
	}
	if err := p.LastKnownELRs.Write(w); err != nil {
		return err
	}
	return p.OfflineReplicas.Write(w)
}

type Node struct {
	NodeId int32
}
type Nodes []Node

func (n *Nodes) Write(w io.Writer) error {
	if err := types.WriteUvarint(w, uint64(len(*n)+1)); err != nil {
		return err
	}
	for _, node := range *n {
		if err := binary.Write(w, binary.BigEndian, node.NodeId); err != nil {
			return err
		}
	}
	return nil
}
