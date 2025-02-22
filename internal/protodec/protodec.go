// Package protodec provides utilities for manually decoding protobuf messages from a buffer slice.
package protodec

import (
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	errTruncatedMessage = errors.New("decoding: truncated message, cannot advance offset")
)

type Decoder struct {
	Buffers mem.BufferSlice // raw bytes of the message being processed
	// Decoding offsets
	Offset        uint64 // offset in the message relative to the data as a whole
	CurrentBuffer int    // index of the current buffer being processed
	CurrentOffset uint64 // offset in the current buffer
}

// HasMoreData returns true if there is more data to be read in the Buffers.
func (d *Decoder) HasMoreData() bool {
	return d.Offset < uint64(d.Buffers.Len())
}

// Peek ahead 10 bytes from the current offset in the Buffers. This will return a
// slice of the current buffer if the bytes are all in one buffer, but will copy
// the bytes into a new buffer if the distance is split across buffers. Use this
// to allow protowire methods to be used to parse tags & fixed values.
// The max length of a varint tag is 10 bytes, see
// https://protobuf.dev/programming-guides/encoding/#varints . Other int types
// are shorter.
func (d *Decoder) Peek() []byte {
	b := d.Buffers[d.CurrentBuffer].ReadOnlyData()
	// Check if the tag will fit in the current buffer. If not, copy the next 10
	// bytes into a new buffer to ensure that we can read the tag correctly
	// without it being divided between buffers.
	tagBuf := b[d.CurrentOffset:]
	remainingInBuf := len(tagBuf)
	// If we have less than 10 bytes remaining and are not in the final buffer,
	// copy up to 10 bytes ahead from the next buffer.
	if remainingInBuf < binary.MaxVarintLen64 && d.CurrentBuffer != len(d.Buffers)-1 {
		tagBuf = d.CopyNextBytes(10)
	}
	return tagBuf
}

// Copies up to next n bytes into a new buffer, or fewer if fewer bytes remain in the
// buffers overall. Does not advance offsets.
func (d *Decoder) CopyNextBytes(n int) []byte {
	remaining := n
	if r := d.Buffers.Len() - int(d.Offset); r < remaining {
		remaining = r
	}
	currBuf := d.CurrentBuffer
	currOff := d.CurrentOffset
	var buf []byte
	for remaining > 0 {
		b := d.Buffers[currBuf].ReadOnlyData()
		remainingInCurr := len(b[currOff:])
		if remainingInCurr < remaining {
			buf = append(buf, b[currOff:]...)
			remaining -= remainingInCurr
			currBuf++
			currOff = 0
		} else {
			buf = append(buf, b[currOff:currOff+uint64(remaining)]...)
			remaining = 0
		}
	}
	return buf
}

// Advance current buffer & byte offset in the decoding by n bytes. Returns an error if we
// go past the end of the data.
func (d *Decoder) AdvanceOffset(n uint64) error {
	remaining := n
	for remaining > 0 {
		remainingInCurr := uint64(d.Buffers[d.CurrentBuffer].Len()) - d.CurrentOffset
		if remainingInCurr <= remaining {
			remaining -= remainingInCurr
			d.CurrentBuffer++
			d.CurrentOffset = 0
		} else {
			d.CurrentOffset += remaining
			remaining = 0
		}
	}
	// If we have advanced past the end of the buffers, something went wrong.
	if (d.CurrentBuffer == len(d.Buffers) && d.CurrentOffset > 0) || d.CurrentBuffer > len(d.Buffers) {
		return errTruncatedMessage
	}
	d.Offset += n
	return nil

}

// ConsumeTag reads the next available tag in the input data and returns the field number and type.
// Advances the relevant offsets in the data.
func (d *Decoder) ConsumeTag() (protowire.Number, protowire.Type, error) {
	tagBuf := d.Peek()

	// Consume the next tag. This will tell us which field is next in the
	// buffer, its type, and how much space it takes up.
	fieldNum, fieldType, tagLength := protowire.ConsumeTag(tagBuf)
	if tagLength < 0 {
		return 0, 0, protowire.ParseError(tagLength)
	}
	// Update the offsets and current buffer depending on the tag length.
	if err := d.AdvanceOffset(uint64(tagLength)); err != nil {
		return 0, 0, fmt.Errorf("consuming tag: %w", err)
	}
	return fieldNum, fieldType, nil
}

// Consume a varint that represents the length of a bytes field. Return the length of
// the data, and advance the offsets by the length of the varint.
func (d *Decoder) ConsumeVarint() (uint64, error) {
	tagBuf := d.Peek()

	// Consume the next tag. This will tell us which field is next in the
	// buffer, its type, and how much space it takes up.
	dataLength, tagLength := protowire.ConsumeVarint(tagBuf)
	if tagLength < 0 {
		return 0, protowire.ParseError(tagLength)
	}

	// Update the offsets and current buffer depending on the tag length.
	d.AdvanceOffset(uint64(tagLength))
	return dataLength, nil
}

func (d *Decoder) ConsumeFixed32() (uint32, error) {
	valueBuf := d.Peek()

	// Consume the next tag. This will tell us which field is next in the
	// buffer, its type, and how much space it takes up.
	value, tagLength := protowire.ConsumeFixed32(valueBuf)
	if tagLength < 0 {
		return 0, protowire.ParseError(tagLength)
	}

	// Update the offsets and current buffer depending on the tag length.
	d.AdvanceOffset(uint64(tagLength))
	return value, nil
}

func (d *Decoder) ConsumeFixed64() (uint64, error) {
	valueBuf := d.Peek()

	// Consume the next tag. This will tell us which field is next in the
	// buffer, its type, and how much space it takes up.
	value, tagLength := protowire.ConsumeFixed64(valueBuf)
	if tagLength < 0 {
		return 0, protowire.ParseError(tagLength)
	}

	// Update the offsets and current buffer depending on the tag length.
	d.AdvanceOffset(uint64(tagLength))
	return value, nil
}

// Consume any field values up to the end offset provided and don't return anything.
// This is used to skip any values which are not going to be used.
// msgEndOff is indexed in terms of the overall data across all buffers.
func (d *Decoder) ConsumeFieldValue(fieldNum protowire.Number, fieldType protowire.Type) error {
	// reimplement protowire.ConsumeFieldValue without the extra case for groups (which
	// are are complicted and not a thing in proto3).
	var err error
	switch fieldType {
	case protowire.VarintType:
		_, err = d.ConsumeVarint()
	case protowire.Fixed32Type:
		_, err = d.ConsumeFixed32()
	case protowire.Fixed64Type:
		_, err = d.ConsumeFixed64()
	case protowire.BytesType:
		_, err = d.ConsumeBytes()
	default:
		return fmt.Errorf("unknown field type %v in field %v", fieldType, fieldNum)
	}
	if err != nil {
		return fmt.Errorf("consuming field %v of type %v: %w", fieldNum, fieldType, err)
	}

	return nil
}

type BufferSliceOffsets struct {
	startBuf, endBuf int    // indices of start and end buffers of object data in the msg
	startOff, endOff uint64 // offsets within these buffers where the data starts and ends.
	currBuf          int    // index of current buffer being read out to the user application.
	currOff          uint64 // offset of read in current buffer.
}

// Consume a bytes field from the input. Returns offsets for the data in the buffer slices
// and an error.
func (d *Decoder) ConsumeBytes() (BufferSliceOffsets, error) {
	// m is the length of the data past the tag.
	m, err := d.ConsumeVarint()
	if err != nil {
		return BufferSliceOffsets{}, fmt.Errorf("consuming bytes field: %w", err)
	}
	offsets := BufferSliceOffsets{
		startBuf: d.CurrentBuffer,
		startOff: d.CurrentOffset,
		currBuf:  d.CurrentBuffer,
		currOff:  d.CurrentOffset,
	}

	// Advance offsets to lengths of bytes field and capture where we end.
	err = d.AdvanceOffset(m)
	if err != nil {
		return BufferSliceOffsets{}, err
	}
	offsets.endBuf = d.CurrentBuffer
	offsets.endOff = d.CurrentOffset
	return offsets, nil
}

// Consume a bytes field from the input and copy into a new buffer if
// necessary (if the data is split across buffers in databuf).  This can be
// used to leverage proto.Unmarshal for small bytes fields (i.e. anything
// except object data).
func (d *Decoder) ConsumeBytesCopy() ([]byte, error) {
	// m is the length of the bytes data.
	m, err := d.ConsumeVarint()
	if err != nil {
		return nil, fmt.Errorf("consuming varint: %w", err)
	}
	// Copy the data into a buffer and advance the offset
	b := d.CopyNextBytes(int(m))
	if err := d.AdvanceOffset(m); err != nil {
		return nil, fmt.Errorf("advancing offset: %w", err)
	}
	return b, nil
}

// OffsetsLen returns the length of the data referenced by offsets.
func (d *Decoder) OffsetsLen(offsets BufferSliceOffsets) int {
	if offsets.startBuf == offsets.endBuf {
		return int(offsets.endOff - offsets.startOff)
	}
	n := d.Buffers[offsets.startBuf].Len() - int(offsets.startOff)
	for i := offsets.startBuf + 1; i < offsets.endBuf; i++ {
		n += d.Buffers[i].Len()
	}
	n += int(offsets.endOff)

	return n
}

// AppendOffsets appends the data from range referenced by offsets into dest.
func (d *Decoder) AppendOffsets(dest []byte, offsets BufferSliceOffsets) []byte {
	n := d.OffsetsLen(offsets)
	if n == 0 {
		return dest
	}
	remaining := cap(dest) - len(dest)
	if n > remaining {
		newDest := make([]byte, len(dest), len(dest)+n)
		copy(newDest, dest)
		dest = newDest
	}
	if offsets.startBuf == offsets.endBuf {
		return append(dest, d.Buffers[offsets.startBuf].ReadOnlyData()[offsets.startOff:offsets.endOff]...)
	}
	dest = append(dest, d.Buffers[offsets.startBuf].ReadOnlyData()[offsets.startOff:]...)
	for i := offsets.startBuf + 1; i < offsets.endBuf; i++ {
		dest = append(dest, d.Buffers[i].ReadOnlyData()...)
	}
	dest = append(dest, d.Buffers[offsets.endBuf].ReadOnlyData()[:offsets.endOff]...)
	return dest
}
