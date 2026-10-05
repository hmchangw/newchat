package auditarchive

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegment_RoundTrip(t *testing.T) {
	h := Header{Version: FormatVersion, Site: "site-a", Lane: "events", FirstSeq: 10, LastSeq: 12, Count: 3}
	frames := [][]byte{[]byte("one"), []byte(""), bytes.Repeat([]byte{0xff}, 1000)}
	var buf bytes.Buffer
	offsets, trailer, err := WriteSegment(&buf, h, frames)
	require.NoError(t, err)
	require.Len(t, offsets, 3)
	assert.NotEqual(t, [32]byte{}, trailer)

	gotH, gotFrames, err := ReadSegment(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, h, gotH)
	assert.Equal(t, frames, gotFrames)

	for i, off := range offsets {
		f, err := ReadFrameAt(bytes.NewReader(buf.Bytes()), off)
		require.NoError(t, err, "frame %d", i)
		assert.Equal(t, frames[i], f)
	}
}

func TestSegment_CountMismatchIsRejected(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", Count: 2}, [][]byte{[]byte("x")})
	assert.Error(t, err)
}

func TestSegment_CorruptTrailerIsRejected(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", FirstSeq: 1, LastSeq: 1, Count: 1}, [][]byte{[]byte("x")})
	require.NoError(t, err)
	b := buf.Bytes()
	b[len(b)-1] ^= 1
	_, _, err = ReadSegment(bytes.NewReader(b))
	assert.True(t, errors.Is(err, ErrBadSegment))
}

func TestSegment_CorruptFrameIsRejected(t *testing.T) {
	var buf bytes.Buffer
	offsets, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", FirstSeq: 1, LastSeq: 1, Count: 1}, [][]byte{[]byte("xyz")})
	require.NoError(t, err)
	b := buf.Bytes()
	b[offsets[0]+4] ^= 1 // first payload byte
	_, _, err = ReadSegment(bytes.NewReader(b))
	assert.True(t, errors.Is(err, ErrBadSegment))
}

func TestSegment_TruncatedIsRejected(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", FirstSeq: 1, LastSeq: 1, Count: 1}, [][]byte{[]byte("xyz")})
	require.NoError(t, err)
	_, _, err = ReadSegment(bytes.NewReader(buf.Bytes()[:buf.Len()-10]))
	assert.True(t, errors.Is(err, ErrBadSegment))
}

func TestKeys(t *testing.T) {
	at := time.Date(2026, 10, 5, 14, 3, 0, 0, time.UTC)
	assert.Equal(t, "site-a/2026/10/05/14/events-100-250.seg", SegmentKey("site-a", "events", at, 100, 250))
	assert.Equal(t, "site-a/blobs/f1", BlobKey("site-a", "f1"))
	t.Run("non-UTC time is normalised", func(t *testing.T) {
		loc := time.FixedZone("x", 3600)
		assert.Equal(t, "site-a/2026/10/05/14/events-1-1.seg", SegmentKey("site-a", "events", at.In(loc), 1, 1))
	})
}
