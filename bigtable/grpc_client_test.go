package bigtable

import (
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/internal/protodec"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func fullReadRowsResponse() *btpb.ReadRowsResponse {
	return &btpb.ReadRowsResponse{
		Chunks: []*btpb.ReadRowsResponse_CellChunk{
			{
				RowKey:          []byte("row"),
				FamilyName:      &wrapperspb.StringValue{Value: "family"},
				Qualifier:       &wrapperspb.BytesValue{Value: []byte("qualifier")},
				TimestampMicros: 123456,
				Labels:          []string{"label1", "label2"},
				Value:           []byte("value"),
				ValueSize:       0,
				RowStatus:       &btpb.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true},
			},
		},
		LastScannedRowKey: []byte("lastkey"),
		RequestStats: &btpb.RequestStats{
			StatsView: &btpb.RequestStats_FullReadStatsView{
				FullReadStatsView: &btpb.FullReadStatsView{
					ReadIterationStats: &btpb.ReadIterationStats{
						RowsSeenCount:      100,
						RowsReturnedCount:  1,
						CellsSeenCount:     200,
						CellsReturnedCount: 2,
					},
					RequestLatencyStats: &btpb.RequestLatencyStats{
						FrontendServerLatency: &durationpb.Duration{
							Seconds: 1,
							Nanos:   5000,
						},
					},
				},
			},
		},
	}
}

func TestReadRowsDecoder(t *testing.T) {
	tests := map[string]struct {
		msg *btpb.ReadRowsResponse
	}{
		"empty": {
			msg: &btpb.ReadRowsResponse{},
		},
		"full": {
			msg: fullReadRowsResponse(),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			data, err := proto.Marshal(test.msg)
			if err != nil {
				t.Fatalf("proto.Marshal: %v", err)
			}
			buf := mem.NewBuffer(&data, mem.DefaultBufferPool())
			defer buf.Free()
			bufs := mem.BufferSlice{buf}
			dec := readRowsResponseDecoder{
				dec: protodec.Decoder{
					Buffers: bufs,
				},
			}
			err = dec.decode()
			if err != nil {
				t.Fatalf("dec.decode: %v", err)
			}
			// Compare the result with the original ReadObjectResponse, without the chunks
			if diff := cmp.Diff(&dec.rc.readRowsResponse, test.msg, protocmp.Transform(),
				protocmp.IgnoreMessages(&btpb.ReadRowsResponse_CellChunk{})); diff != "" {
				t.Errorf("cmp.Diff message: got(-),want(+):\n%s", diff)
			}
		})
	}
}

var BenchmarkReadRowsDecoderResult *btpb.ReadRowsResponse

func BenchmarkReadRowsDecoder(b *testing.B) {
	data, err := proto.Marshal(fullReadRowsResponse())
	if err != nil {
		b.Fatalf("proto.Marshal: %v", err)
	}
	buf := mem.NewBuffer(&data, nil)
	bufs := mem.BufferSlice{buf}
	b.ReportAllocs()
	b.ResetTimer()
	dec := readRowsResponseDecoder{}
	for i := 0; i < b.N; i++ {
		dec.dec = protodec.Decoder{
			Buffers: bufs,
		}
		err := dec.decode()
		if err != nil {
			b.Fatalf("dec.decode: %v", err)
		}
		BenchmarkReadRowsDecoderResult = &dec.rc.readRowsResponse
	}
}

func BenchmarkReadRowsProto(b *testing.B) {
	data, err := proto.Marshal(fullReadRowsResponse())
	if err != nil {
		b.Fatalf("proto.Marshal: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var msg btpb.ReadRowsResponse
		err := proto.Unmarshal(data, &msg)
		if err != nil {
			b.Fatalf("proto.Unmarshal: %v", err)
		}
	}
}
