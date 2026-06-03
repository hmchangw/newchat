package natsutil

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGzipPayload_RoundTrip(t *testing.T) {
	in := []byte(strings.Repeat(`{"k":"v"}`, 100))
	encoded, err := GzipPayload(in)
	require.NoError(t, err)
	assert.NotEqual(t, in, encoded, "encoded payload differs from input")
	assert.Less(t, len(encoded), len(in), "highly repetitive JSON should shrink under gzip")

	r, err := gzip.NewReader(bytes.NewReader(encoded))
	require.NoError(t, err)
	defer r.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)
	assert.Equal(t, in, buf.Bytes())
}

func TestGzipPayload_EmptyInput(t *testing.T) {
	encoded, err := GzipPayload(nil)
	require.NoError(t, err)
	// gzip framing means even empty input produces a non-empty (header+trailer) output.
	assert.NotEmpty(t, encoded)
	r, err := gzip.NewReader(bytes.NewReader(encoded))
	require.NoError(t, err)
	defer r.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)
	assert.Empty(t, buf.Bytes())
}

func TestNewGzipMsg_SetsHeadersAndCompresses(t *testing.T) {
	in := []byte(`{"hello":"world"}`)
	msg, err := NewGzipMsg("foo.bar", in, "")
	require.NoError(t, err)

	assert.Equal(t, "foo.bar", msg.Subject)
	assert.Equal(t, ContentEncodingGzip, msg.Header.Get(HeaderContentEncoding))
	assert.Equal(t, "application/json", msg.Header.Get(HeaderContentType), "default content type")

	decoded, err := DecodePayload(msg)
	require.NoError(t, err)
	assert.Equal(t, in, decoded)
}

func TestNewGzipMsg_CustomContentType(t *testing.T) {
	msg, err := NewGzipMsg("foo.bar", []byte("hi"), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "text/plain", msg.Header.Get(HeaderContentType))
}

func TestDecodePayload_NoEncoding_PassesThrough(t *testing.T) {
	msg := &nats.Msg{Data: []byte("raw bytes"), Header: nats.Header{}}
	out, err := DecodePayload(msg)
	require.NoError(t, err)
	assert.Equal(t, []byte("raw bytes"), out)
}

func TestDecodePayload_IdentityEncoding_PassesThrough(t *testing.T) {
	msg := &nats.Msg{Data: []byte("raw"), Header: nats.Header{}}
	msg.Header.Set(HeaderContentEncoding, "identity")
	out, err := DecodePayload(msg)
	require.NoError(t, err)
	assert.Equal(t, []byte("raw"), out)
}

func TestDecodePayload_NilHeader(t *testing.T) {
	msg := &nats.Msg{Data: []byte("raw"), Header: nil}
	out, err := DecodePayload(msg)
	require.NoError(t, err)
	assert.Equal(t, []byte("raw"), out)
}

func TestDecodePayload_UnsupportedEncoding(t *testing.T) {
	msg := &nats.Msg{Data: []byte("x"), Header: nats.Header{}}
	msg.Header.Set(HeaderContentEncoding, "br")
	_, err := DecodePayload(msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported Content-Encoding")
}

func TestDecodePayload_GzipCorrupt(t *testing.T) {
	msg := &nats.Msg{Data: []byte("not gzip"), Header: nats.Header{}}
	msg.Header.Set(HeaderContentEncoding, ContentEncodingGzip)
	_, err := DecodePayload(msg)
	require.Error(t, err)
}

func TestDecodePayload_GzipExceedsMaxDecodedSize(t *testing.T) {
	// Build a payload that decompresses to MaxDecodedPayloadSize+1 bytes. gzip on a
	// constant byte runs at ~1000× compression so the wire bytes stay tiny — exactly
	// the gzip-bomb shape we want the cap to reject.
	oversized := bytes.Repeat([]byte{'a'}, MaxDecodedPayloadSize+1)
	encoded, err := GzipPayload(oversized)
	require.NoError(t, err)
	assert.Less(t, len(encoded), MaxDecodedPayloadSize/100,
		"sanity: highly repetitive input should compress small enough to fit in a single NATS msg")

	msg := &nats.Msg{Data: encoded, Header: nats.Header{}}
	msg.Header.Set(HeaderContentEncoding, ContentEncodingGzip)
	_, err = DecodePayload(msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestDecodePayload_GzipAtMaxDecodedSize(t *testing.T) {
	// Boundary check: a payload that exactly hits the cap must still succeed.
	atLimit := bytes.Repeat([]byte{'b'}, MaxDecodedPayloadSize)
	encoded, err := GzipPayload(atLimit)
	require.NoError(t, err)

	msg := &nats.Msg{Data: encoded, Header: nats.Header{}}
	msg.Header.Set(HeaderContentEncoding, ContentEncodingGzip)
	out, err := DecodePayload(msg)
	require.NoError(t, err)
	assert.Len(t, out, MaxDecodedPayloadSize)
}

func TestDecodePayload_GzipTruncated(t *testing.T) {
	full, err := GzipPayload([]byte("hello world"))
	require.NoError(t, err)
	msg := &nats.Msg{Data: full[:len(full)-3], Header: nats.Header{}}
	msg.Header.Set(HeaderContentEncoding, ContentEncodingGzip)
	_, err = DecodePayload(msg)
	require.Error(t, err)
}
