package natsutil_test

import (
	"bytes"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// zstdMsg embeds jetstream.Msg so only Data/Headers need implementing; any other
// method would panic, but DecodePayload calls only these two.
type zstdMsg struct {
	jetstream.Msg
	data    []byte
	headers nats.Header
}

func (m *zstdMsg) Data() []byte         { return m.data }
func (m *zstdMsg) Headers() nats.Header { return m.headers }

func zstdHeader() nats.Header {
	h := nats.Header{}
	h.Set(natsutil.HeaderNatsEncoding, natsutil.EncodingZstd)
	return h
}

func TestDecodePayload_RoundTrip(t *testing.T) {
	orig := []byte(`{"hello":"world","n":42}`)
	msg := &zstdMsg{data: natsutil.EncodeZstd(orig), headers: zstdHeader()}
	out, err := natsutil.DecodePayload(msg)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(orig, out), "decode(encode(x)) must equal x")
}

func TestDecodePayload_FallThrough(t *testing.T) {
	plain := []byte("not compressed")

	// absent header → raw body unchanged
	out, err := natsutil.DecodePayload(&zstdMsg{data: plain, headers: nil})
	require.NoError(t, err)
	assert.Equal(t, plain, out)

	// present-but-not-zstd header → raw body unchanged
	h := nats.Header{}
	h.Set(natsutil.HeaderNatsEncoding, "gzip")
	out, err = natsutil.DecodePayload(&zstdMsg{data: plain, headers: h})
	require.NoError(t, err)
	assert.Equal(t, plain, out)
}

func TestDecodePayload_CorruptFrameErrors(t *testing.T) {
	// flagged zstd but the body is not a valid frame → error, not a panic
	msg := &zstdMsg{data: []byte("garbage-not-a-zstd-frame"), headers: zstdHeader()}
	_, err := natsutil.DecodePayload(msg)
	assert.Error(t, err)
}

func TestDecodePayload_OversizeFrameErrors(t *testing.T) {
	// a valid frame that decodes past the 16 MiB cap must be aborted by the
	// WithDecoderMaxMemory guard, not grown unbounded.
	huge := natsutil.EncodeZstd(make([]byte, 17*1024*1024))
	_, err := natsutil.DecodePayload(&zstdMsg{data: huge, headers: zstdHeader()})
	assert.Error(t, err)
}
