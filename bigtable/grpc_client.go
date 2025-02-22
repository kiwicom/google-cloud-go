package bigtable

import (
	"fmt"

	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/internal/protodec"
	"google.golang.org/grpc/encoding"
	encproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/durationpb"
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
	// Nested in RequestStats.FullReadStatsView
	readIterationStatsField  = protowire.Number(1)
	requestLatencyStatsField = protowire.Number(2)
	// Nested in RequestStats.FullReadStatsView.ReadIterationStats
	rowsSeenCountField      = protowire.Number(1)
	rowsReturnedCountField  = protowire.Number(2)
	cellsSeenCountField     = protowire.Number(3)
	cellsReturnedCountField = protowire.Number(4)
	// Nested in RequestStats.FullReadStatsView.RequestLatencyStats
	frontendServerLatencyField = protowire.Number(1)
	// Nested in Duration
	secondsField = protowire.Number(1)
	nanosField   = protowire.Number(2)
)

// cache of all the parts of ReadRowsResponse.
type readRowsCache struct {
	readRowsResponse              bigtablepb.ReadRowsResponse
	requestStats                  bigtablepb.RequestStats
	requestStatsFullReadStatsView bigtablepb.RequestStats_FullReadStatsView
	fullReadStatsView             bigtablepb.FullReadStatsView
	readIterationStats            bigtablepb.ReadIterationStats
	requestLatencyStats           bigtablepb.RequestLatencyStats
	frontendServerLatency         durationpb.Duration
}

// reset all messages.
func (rc *readRowsCache) reset() {
	rc.readRowsResponse.Reset()
	rc.requestStats.Reset()
	rc.requestStatsFullReadStatsView = bigtablepb.RequestStats_FullReadStatsView{}
	rc.fullReadStatsView.ReadIterationStats = nil
	rc.fullReadStatsView.RequestLatencyStats = nil
	rc.readIterationStats.Reset()
	rc.requestLatencyStats.Reset()
	rc.frontendServerLatency.Reset()
}

// readRowsResponseDecoder is a custom zero-allocation and zero-copy decoder for ReadRowsResponse messages.
//
// It parses the message and populates rc.readRowsResponse with the decoded data, except:
// - Chunks field is not populated, there is a way to iterate chunks one by one.
// - LastScannedRowKey is not populated, it can be copied out from lastScannedRowKey.
type readRowsResponseDecoder struct {
	dec protodec.Decoder
	rc  readRowsCache
	// decoded data
	lastScannedRowKey protodec.BufferSliceOffsets
	chunksOffsets     protodec.BufferSliceOffsets
}

// decode the ReadRowsResponse message, except that Chunks and LastScannedRowKey is not populated.
func (d *readRowsResponseDecoder) decode() error {
	d.rc.reset()
	d.chunksOffsets = protodec.BufferSliceOffsets{}

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
			d.lastScannedRowKey, err = d.dec.ConsumeBytes()
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
	return nil
}

func (d *readRowsResponseDecoder) decodeFullReadStatsView() error {
	d.rc.requestStats.StatsView = &d.rc.requestStatsFullReadStatsView
	d.rc.requestStatsFullReadStatsView.FullReadStatsView = &d.rc.fullReadStatsView

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
		case fieldNum == readIterationStatsField && fieldType == protowire.BytesType:
			err = d.decodeReadIterationStats()
			if err != nil {
				return err
			}
		case fieldNum == requestLatencyStatsField && fieldType == protowire.BytesType:
			err = d.decodeRequestLatencyStats()
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
	return nil
}

func (d *readRowsResponseDecoder) decodeReadIterationStats() error {
	d.rc.fullReadStatsView.ReadIterationStats = &d.rc.readIterationStats
	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return fmt.Errorf("invalid length of RequestStats.FullReadStatsView.ReadIterationStats: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming tag in ReadIterationStats: %v", err)
		}
		switch {
		case fieldNum == rowsSeenCountField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadIterationStats.RowsSeenCount: %w", err)
			}
			d.rc.readIterationStats.RowsSeenCount = int64(v)
		case fieldNum == rowsReturnedCountField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadIterationStats.RowsReturnedCount: %w", err)
			}
			d.rc.readIterationStats.RowsReturnedCount = int64(v)
		case fieldNum == cellsSeenCountField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadIterationStats.CellsSeenCount: %w", err)
			}
			d.rc.readIterationStats.CellsSeenCount = int64(v)
		case fieldNum == cellsReturnedCountField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadIterationStats.CellsReturnedCount: %w", err)
			}
			d.rc.readIterationStats.CellsReturnedCount = int64(v)
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in ReadIterationStats: %w", err)
			}
		}
	}
	return nil
}

func (d *readRowsResponseDecoder) decodeRequestLatencyStats() error {
	d.rc.fullReadStatsView.RequestLatencyStats = &d.rc.requestLatencyStats
	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return fmt.Errorf("invalid length of RequestStats.FullReadStatsView.RequestLatencyStats: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming tag in RequestLatencyStats: %v", err)
		}
		switch {
		case fieldNum == frontendServerLatencyField && fieldType == protowire.BytesType:
			d.rc.requestLatencyStats.FrontendServerLatency = &d.rc.frontendServerLatency
			err = d.decodeDuration(&d.rc.frontendServerLatency)
			if err != nil {
				return fmt.Errorf("invalid RequestLatencyStats.FrontendServerLatency: %w", err)
			}
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in ReadIterationStats: %w", err)
			}
		}
	}
	return nil
}

func (d *readRowsResponseDecoder) decodeDuration(duration *durationpb.Duration) error {
	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return fmt.Errorf("invalid length of Duration: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming tag in Duration: %v", err)
		}
		switch {
		case fieldNum == secondsField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid Duration.seconds: %w", err)
			}
			duration.Seconds = int64(v)
		case fieldNum == nanosField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid Duration.nanos: %w", err)
			}
			duration.Nanos = int32(v)
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in Duration: %w", err)
			}
		}
	}
	return nil
}

// copyLastScannedRowKey copies the last scanned row key to dest and returns the updated dest.
// If the last scanned row does not fit into dest, a new slice is allocated.
func (d *readRowsResponseDecoder) copyLastScannedRowKey(dest []byte) []byte {
	dest = dest[:0]
	return d.dec.AppendOffsets(dest, d.lastScannedRowKey)
}

func (d *readRowsResponseDecoder) decodeChunks(func (chunk *bigtablepb.ReadRowsResponse_CellChunk) error) error {
	d.dec.Offset = d.chunksOffsets
	for d.dec.HasMoreData() {
		chunk := bigtablepb.ReadRowsResponse_CellChunk{}
		err := d.decodeChunk(&chunk)
		if err != nil {
			return err
		}
		err = func(&chunk)
		if err != nil {
			return err
		}
	}
	return nil
}
