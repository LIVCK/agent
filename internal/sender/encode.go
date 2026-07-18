package sender

import (
	"fmt"

	"github.com/LIVCK/agent/pkg/wire"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

// encoder marshals a MetricBatch to protobuf and zstd-compresses it, matching
// the pulse decode path (Content-Type application/x-protobuf, Content-Encoding
// zstd). One encoder is reused for the process: zstd.Encoder.EncodeAll is safe
// for concurrent use and keeps allocations low.
type encoder struct {
	z *zstd.Encoder
}

func newEncoder() (*encoder, error) {
	// Concurrency 1: the default is GOMAXPROCS, which pre-allocates a full set of
	// compression buffers per core -- on a many-core host that alone drives the
	// agent's RSS tens of MB over the systemd MemoryMax budget. Batches are small
	// and infrequent (one EncodeAll per report), so a single worker is ample.
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("zstd writer: %w", err)
	}
	return &encoder{z: z}, nil
}

func (e *encoder) encode(batch *wire.MetricBatch) ([]byte, error) {
	raw, err := proto.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("proto marshal: %w", err)
	}
	return e.z.EncodeAll(raw, nil), nil
}

func (e *encoder) close() {
	if e.z != nil {
		_ = e.z.Close()
	}
}
