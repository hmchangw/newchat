package natsutil

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go/jetstream"
)

// Compression is signalled by the Nats-Encoding header. Only zstd is
// supported; any other value falls through to the raw body.
const (
	HeaderNatsEncoding = "Nats-Encoding"
	EncodingZstd       = "zstd"

	// maxDecompressedBytes caps zstd output at decode time via
	// WithDecoderMaxMemory — a hostile/corrupt frame is aborted rather than
	// grown unbounded.
	maxDecompressedBytes = 16 * 1024 * 1024
)

// Process-wide codecs reused across every message; klauspost's EncodeAll and
// DecodeAll are safe for concurrent use, and a per-message codec pays an init
// cost on the hot path.
var (
	zstdEncoder = mustNewZstdEncoder()
	zstdDecoder = mustNewZstdDecoder()
)

func mustNewZstdEncoder() *zstd.Encoder {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic(fmt.Sprintf("init zstd encoder: %v", err))
	}
	return e
}

func mustNewZstdDecoder() *zstd.Decoder {
	d, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.IgnoreChecksum(true),
		zstd.WithDecoderMaxMemory(maxDecompressedBytes),
	)
	if err != nil {
		panic(fmt.Sprintf("init zstd decoder: %v", err))
	}
	return d
}

// EncodeZstd compresses data with the process-wide encoder. Callers set the
// Nats-Encoding: zstd header on the published message.
func EncodeZstd(data []byte) []byte {
	return zstdEncoder.EncodeAll(data, nil)
}

// DecodePayload decompresses the body when Nats-Encoding signals zstd,
// otherwise returns it unchanged.
func DecodePayload(msg jetstream.Msg) ([]byte, error) {
	if msg.Headers().Get(HeaderNatsEncoding) != EncodingZstd {
		return msg.Data(), nil
	}
	out, err := zstdDecoder.DecodeAll(msg.Data(), nil)
	if err != nil {
		return nil, fmt.Errorf("decompress zstd payload: %w", err)
	}
	return out, nil
}
