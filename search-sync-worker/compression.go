package main

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go/jetstream"
)

// Compression is signalled by NATS headers on the incoming message.
// Currently only zstd is supported; unknown values fall through to the
// raw body.
const (
	headerNatsEncoding = "Nats-Encoding"
	encodingZstd       = "zstd"

	// maxDecompressedBytes caps zstd output size at decode time via
	// WithDecoderMaxMemory. A hostile or corrupt frame that decodes to
	// more than this is aborted mid-decode rather than allowed to grow
	// the destination buffer to arbitrary size.
	maxDecompressedBytes = 16 * 1024 * 1024
)

// zstdDecoder is a process-wide decoder reused across every message.
// klauspost's decoder is safe for concurrent DecodeAll calls, and
// creating one per message pays a decoder-init cost per JetStream
// delivery — a real allocation on the consumer hot path.
var zstdDecoder = mustNewZstdDecoder()

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

// decodePayload decompresses the body when Nats-Encoding signals a
// supported codec, otherwise returns it unchanged.
func decodePayload(msg jetstream.Msg) ([]byte, error) {
	if msg.Headers().Get(headerNatsEncoding) != encodingZstd {
		return msg.Data(), nil
	}
	out, err := zstdDecoder.DecodeAll(msg.Data(), nil)
	if err != nil {
		return nil, fmt.Errorf("decompress zstd payload: %w", err)
	}
	return out, nil
}
