package bigtable

import (
	"fmt"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
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
	// Nested in wrapperspb.StringValue/BytesValue
	wrappedValueField = protowire.Number(1)
)

// cache of all the parts of ReadRowsResponse.
type readRowsCache struct {
	readRowsResponse              btpb.ReadRowsResponse
	requestStats                  btpb.RequestStats
	requestStatsFullReadStatsView btpb.RequestStats_FullReadStatsView
	fullReadStatsView             btpb.FullReadStatsView
	readIterationStats            btpb.ReadIterationStats
	requestLatencyStats           btpb.RequestLatencyStats
	frontendServerLatency         durationpb.Duration
	cellChunk                     btpb.ReadRowsResponse_CellChunk
}

// reset all messages.
func (rc *readRowsCache) reset() {
	rc.readRowsResponse.Reset()
	rc.requestStats.Reset()
	rc.requestStatsFullReadStatsView = btpb.RequestStats_FullReadStatsView{}
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
	chunkCount        int
	lastScannedRowKey protodec.BufferSliceOffsets
}

// decode the ReadRowsResponse message, except that Chunks and LastScannedRowKey is not populated.
func (d *readRowsResponseDecoder) decode() error {
	d.rc.reset()

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
			d.chunkCount++
			// Skip the chunks field in the first pass, chunks will be decoded with decodeChunks.
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in ReadRowsResponse: %w", err)
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

// Corresponds to bigtablepb.ReadRowsResponse_CellChunk.
type decodedChunk struct {
	// The row key for this chunk of data.  If the row key is empty,
	// this CellChunk is a continuation of the same row as the previous
	// CellChunk in the response stream, even if that CellChunk was in a
	// previous ReadRowsResponse message.
	rowKey []byte
	// The column family name for this chunk of data.  If this message
	// is not present this CellChunk is a continuation of the same column
	// family as the previous CellChunk.  The empty string can occur as a
	// column family name in a response so clients must check
	// explicitly for the presence of this message, not just for
	// `family_name.value` being non-empty.
	familyName        protodec.BufferSliceOffsets
	familyNamePresent bool

	// The column qualifier for this chunk of data.  If this message
	// is not present, this CellChunk is a continuation of the same column
	// as the previous CellChunk.  Column qualifiers may be empty so
	// clients must check for the presence of this message, not just
	// for `qualifier.value` being non-empty.
	qualifier        protodec.BufferSliceOffsets
	qualifierPresent bool

	// The cell's stored timestamp, which also uniquely identifies it
	// within its column.  Values are always expressed in
	// microseconds, but individual tables may set a coarser
	// granularity to further restrict the allowed values. For
	// example, a table which specifies millisecond granularity will
	// only allow values of `timestamp_micros` which are multiples of
	// 1000.  Timestamps are only set in the first CellChunk per cell
	// (for cells split into multiple chunks).
	timestampMicros int64

	// Labels applied to the cell by a
	// [RowFilter][google.bigtable.v2.RowFilter].  Labels are only set
	// on the first CellChunk per cell.
	labels []string

	// The value stored in the cell.  Cell values can be split across
	// multiple CellChunks.  In that case only the value field will be
	// set in CellChunks after the first: the timestamp and labels
	// will only be present in the first CellChunk, even if the first
	// CellChunk came in a previous ReadRowsResponse.
	value protodec.BufferSliceOffsets

	// If this CellChunk is part of a chunked cell value and this is
	// not the final chunk of that cell, value_size will be set to the
	// total length of the cell value.  The client can use this size
	// to pre-allocate memory to hold the full cell value.
	valueSize int32

	// Row status flags.
	resetRow  bool
	commitRow bool
}

func (d *readRowsResponseDecoder) decodeChunks(fn func(chunk decodedChunk) error) error {
	d.dec.Offset = 0
	d.dec.CurrentBuffer = 0
	d.dec.CurrentOffset = 0
	for d.dec.HasMoreData() {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming next tag: %w", err)
		}

		// Unmarshal the field according to its type. Only fields that are not
		// nil will be present.
		switch {
		case fieldNum == chunksField && fieldType == protowire.BytesType:
			var chunk decodedChunk
			err = d.decodeChunk(&chunk)
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse.Chunks: %w", err)
			}
			err = fn(chunk)
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

func (d *readRowsResponseDecoder) decodeChunk(chunk *decodedChunk) error {
	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return fmt.Errorf("invalid length of ReadRowsResponse_CellChunk: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return fmt.Errorf("consuming tag in ReadRowsResponse_CellChunk: %v", err)
		}
		switch {
		case fieldNum == rowKeyField && fieldType == protowire.BytesType:
			chunk.rowKey, err = d.dec.ConsumeBytesCopy()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.RowKey: %w", err)
			}
		case fieldNum == familyNameField && fieldType == protowire.BytesType:
			chunk.familyName, chunk.familyNamePresent, err = d.decodeWrappedString()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.FamilyName: %w", err)
			}
		case fieldNum == qualifierField && fieldType == protowire.BytesType:
			chunk.qualifier, chunk.qualifierPresent, err = d.decodeWrappedString()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.FamilyName: %w", err)
			}
		case fieldNum == timestampMicrosField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.TimestampMicros: %w", err)
			}
			chunk.timestampMicros = int64(v)
		case fieldNum == labelsField && fieldType == protowire.BytesType:
			label, err := d.dec.ConsumeStringCopy()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.Labels: %w", err)
			}
			chunk.labels = append(chunk.labels, label)
		case fieldNum == valueField && fieldType == protowire.BytesType:
			chunk.value, err = d.dec.ConsumeBytes()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.Value: %w", err)
			}
		case fieldNum == valueSizeField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.ValueSize: %w", err)
			}
			chunk.valueSize = int32(v)
		case fieldNum == rowStatusResetRowField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.ResetRow: %w", err)
			}
			chunk.resetRow = v != 0
			chunk.commitRow = false
		case fieldNum == rowStatusCommitRowField && fieldType == protowire.VarintType:
			v, err := d.dec.ConsumeVarint()
			if err != nil {
				return fmt.Errorf("invalid ReadRowsResponse_CellChunk.CommitRow: %w", err)
			}
			chunk.commitRow = v != 0
			chunk.resetRow = false
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return fmt.Errorf("invalid field in ReadRowsResponse_CellChunk: %w", err)
			}
		}
	}
	return nil
}

func (d *readRowsResponseDecoder) decodeWrappedString() (protodec.BufferSliceOffsets, bool, error) {
	bytesFieldLen, err := d.dec.ConsumeVarint()
	if err != nil {
		return protodec.BufferSliceOffsets{}, false, fmt.Errorf("invalid length of StringValue: %v", err)
	}
	contentEndOffset := d.dec.Offset + bytesFieldLen
	var (
		offsets  protodec.BufferSliceOffsets
		hasValue bool
	)
	for d.dec.Offset < contentEndOffset {
		fieldNum, fieldType, err := d.dec.ConsumeTag()
		if err != nil {
			return protodec.BufferSliceOffsets{}, false, fmt.Errorf("consuming tag in StringValue: %v", err)
		}
		switch {
		case fieldNum == wrappedValueField && fieldType == protowire.BytesType:
			offsets, err = d.dec.ConsumeBytes()
			if err != nil {
				return protodec.BufferSliceOffsets{}, false, fmt.Errorf("invalid ReadRowsResponse_CellChunk.RowKey: %w", err)
			}
			hasValue = true
		default:
			err := d.dec.ConsumeFieldValue(fieldNum, fieldType)
			if err != nil {
				return protodec.BufferSliceOffsets{}, false, fmt.Errorf("invalid field in ReadRowsResponse_CellChunk: %w", err)
			}
		}
	}
	return offsets, hasValue, nil
}
