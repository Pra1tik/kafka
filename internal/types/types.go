package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type NullableString struct {
	Length int16
	Data   string
}

type TaggedFields struct {
	Length uint64
	Fields map[uint64][]byte // map used for easy lookup by tag
}

func ReadUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func WriteUvarint(w io.Writer, val uint64) error {
	var buf [binary.MaxVarintLen64]byte
	bytes := binary.PutUvarint(buf[:], val) // [10]byte is an array, we need slice
	_, err := w.Write(buf[:bytes])
	return err
}

func ReadNullableString(r *bytes.Reader) (*NullableString, error) {
	ns := &NullableString{}

	binary.Read(r, binary.BigEndian, &ns.Length) // TBC

	if ns.Length != -1 {
		str := make([]byte, ns.Length)
		n, err := r.Read(str)
		if err != nil {
			return nil, fmt.Errorf("error reading nullable string: %s", err)
		}

		if n != int(ns.Length) {
			return nil, fmt.Errorf("error reading nullable string: Expected: %d, Got: %d", ns.Length, n)
		}

		ns.Data = string(str)
	}

	return ns, nil
}

func (ns *NullableString) WriteNullableString(w io.Writer) error {
	nsLengthBuf := make([]byte, 2) // nsLenght is int16
	binary.BigEndian.PutUint16(nsLengthBuf, uint16(ns.Length))
	_, err := w.Write(nsLengthBuf)
	if err != nil {
		return fmt.Errorf("error writing nullable string length: %s", err)
	}

	_, err = w.Write([]byte(ns.Data))
	return err
}

func ReadTaggedFields(r *bytes.Reader) (*TaggedFields, error) {
	taggedFieldCount, err := ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("error reading tagged field count: %s", err)
	}

	taggedFields := &TaggedFields{
		Length: taggedFieldCount,
		Fields: make(map[uint64][]byte),
	}
	for i := uint64(0); i < taggedFieldCount; i++ {
		tag, err := ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("error reading field %d tag: %s", i+1, err)
		}

		length, err := ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("error reading field %d length: %s", i+1, err)
		}

		data := make([]byte, length)
		// io.ReadFull(r, data)
		if _, err = r.Read(data); err != nil { // TBC
			return nil, fmt.Errorf("error reading field %d data: %s", i+1, err)
		}

		taggedFields.Fields[tag] = data
	}

	return taggedFields, nil
}

func (t *TaggedFields) WriteTaggedFields(w io.Writer) error {
	buf := bytes.Buffer{}

	err := WriteUvarint(&buf, t.Length)
	if err != nil {
		return fmt.Errorf("error writing tagged field count: %s", err)
	}

	i := 0
	for tag, data := range t.Fields {
		err := WriteUvarint(&buf, tag)
		if err != nil {
			return fmt.Errorf("error writing field %d tag: %s", i+1, err)
		}

		err = WriteUvarint(&buf, uint64(len(data)))
		if err != nil {
			return fmt.Errorf("error writing field %d length: %s", i+1, err)
		}

		if _, err = buf.Write(data); err != nil { // TBC (does buf content get modified or not)
			return fmt.Errorf("error writing field %d data: %s", i+1, err)
		}

		i++
	}

	_, err = w.Write(buf.Bytes())
	return err
}
