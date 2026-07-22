package main

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	headerNatsEncoding = "Nats-Encoding"
	encodingZstd       = "zstd"

	// maxDecompressedBytes caps zstd output at decode time — a hostile/corrupt
	// frame is aborted rather than allowed to grow the buffer unbounded.
	maxDecompressedBytes = 16 * 1024 * 1024
)

// zstdDecoder is a process-wide decoder reused across every message; klauspost's
// DecodeAll is concurrency-safe.
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

// decodePayload decompresses the body when Nats-Encoding signals zstd,
// otherwise returns it unchanged.
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
