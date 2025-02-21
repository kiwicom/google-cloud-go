package bigtable

import (
	"fmt"

	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/internal/protodec"
	"google.golang.org/grpc/encoding"
	encproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

var protoCodec encoding.CodecV2

func init() {
	// referencing the name here ensures that encoding/proto is initialized first.
	protoCodec = encoding.GetCodecV2(encproto.Name)
}

// Custom codec to be used for unmarshaling ReadRowsResponse messages.
// This is used to avoid a copy of object data in proto.Unmarshal.
type bytesCodecV2 struct {
}

var _ encoding.CodecV2 = bytesCodecV2{}

// Marshal is used to encode messages to send for bytesCodecV2.
func (bytesCodecV2) Marshal(v any) (mem.BufferSlice, error) {
	return protoCodec.Marshal(v)
}

// Unmarshal is used for data received for ReadRowsResponse. We want to preserve
// the mem.BufferSlice in most cases rather than copying and calling proto.Unmarshal.
func (bytesCodecV2) Unmarshal(data mem.BufferSlice, v any) error {
	if v, ok := v.(*mem.BufferSlice); ok {
		*v = data
		// Pick up a reference to the data so that it is not freed while decoding.
		data.Ref()
		return nil
	}
	return protoCodec.Unmarshal(data, v)
}

func (bytesCodecV2) Name() string {
	return ""
}

// ReadRowsResponse field and subfield numbers.
const (
	// Top level fields.
	chunksField       = protowire.Number(1)
	lastScannedRowKey = protowire.Number(2)
	requestStatsField = protowire.Number(3)

	// Nested in Chunks
	rowKeyField          = protowire.Number(1)
	familyNameField      = protowire.Number(2)
	qualifierField       = protowire.Number(3)
	timestampMicrosField = protowire.Number(4)
	labelsField          = protowire.Number(5)
	valueField           = protowire.Number(6)
	valueSizeField       = protowire.Number(7)
	// Nested in Chunks.row_status
	rowStatusResetRowField  = protowire.Number(8)
	rowStatusCommitRowField = protowire.Number(9)
	// Nested in RequestStats
	requestStatsFullReadStatsViewField = protowire.Number(1)
)

// cache of all the parts of ReadRowsResponse.
type readRowsCache struct {
	readRowsResponse              bigtablepb.ReadRowsResponse
	requestStats                  bigtablepb.RequestStats
	requestStatsFullReadStatsView bigtablepb.RequestStats_FullReadStatsView
	fullReadStatsView             bigtablepb.FullReadStatsView
}

// reset all messages.
func (rc *readRowsCache) reset() {
	rc.readRowsResponse.Reset()
	rc.requestStats.Reset()
	rc.requestStatsFullReadStatsView = bigtablepb.RequestStats_FullReadStatsView{}
	rc.fullReadStatsView.Reset()
}

// readRowsResponseDecoder is a custom zero-allocation and zero-copy decoder for ReadRowsResponse messages.
type readRowsResponseDecoder struct {
	dec           protodec.Decoder
	rc            readRowsCache
	chunksOffsets protodec.BufferSliceOffsets
}

// decode the ReadRowsResponse message, except that Chunks is not populated.
func (d *readRowsResponseDecoder) decode() error {
	d.rc.reset()
	msg := &d.rc.readRowsResponse

	// Loop over the entire message, extracting fields as we go. This does not
	// handle field concatenation, in which the contents of a single field
	// are split across multiple protobuf tags.
	for d.dec.HasMoreData() {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming next tag: %w", err)
		}

		// Unmarshal the field according to its type. Only fields that are not
		// nil will be present.
		switch {
		case fieldNum == chunksField && fieldType == protowire.BytesType:
			d.chunksOffsets, err = d.dec.ConsumeBytes()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse.Chunks: %w", err)
			}
		case fieldNum == lastScannedRowKey && fieldType == protowire.BytesType:
			d.rc.readRowsResponse.LastScannedRowKey, err = d.dec.ConsumeBytesCopy()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse.LastScannedRowKey: %w", err)
			}
		case fieldNum == requestStatsField && fieldType == protowire.BytesType:
			err = d.decodeRequestStats()
			if err != nil {
				return err
			}
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in ReadRowsResponse: %w", err)
			}
		}
	}
	d.msg = msg
	return nil
}

func (d *readRowsResponseDecoder) decodeRequestStats() error {
	d.rc.readRowsResponse.RequestStats = &d.rc.requestStats

	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return fmt.Errorf("invalid length of ReadRowsResponse.RequestStats: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming tag in RequestStats: %v", err)
		}
		switch {
		case fieldNum == requestStatsFullReadStatsViewField && fieldType == protowire.BytesType:
			err = d.decodeFullReadStatsView()
			if err != nil {
				return err
			}
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in RequestStats: %w", err)
			}
		}
	}
}

func (d *readRowsResponseDecoder) decodeFullReadStatsView() error {
	d.rc.requestStats.StatsView = &d.rc.requestStatsFullReadStatsView

	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return fmt.Errorf("invalid length of RequestStats.FullReadStatsView: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming tag in FullReadStatsView: %v", err)
		}
		switch {
		case fieldNum == fullReadStatsViewReadRowsField && fieldType == protowire.BytesType:
			err = d.decodeReadRows()
			if err != nil {
				return err
			}
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in FullReadStatsView: %w", err)
			}
		}
	}
}
