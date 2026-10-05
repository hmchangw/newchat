package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// failAfter fails every Write once limit bytes have been accepted.
type failAfter struct {
	limit int
	n     int
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n+len(p) > f.limit {
		return 0, errBoom
	}
	f.n += len(p)
	return len(p), nil
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errBoom }

// withTrailer appends the SHA-256 trailer so a hand-built body passes the
// trailer check and exercises the structural checks behind it.
func withTrailer(body []byte) []byte {
	sum := sha256.Sum256(body)
	return append(append([]byte(nil), body...), sum[:]...)
}

func segmentHeader(version uint16, count uint32) []byte {
	var b bytes.Buffer
	b.WriteString("AUDSEG")
	_ = binary.Write(&b, binary.BigEndian, version)
	_ = binary.Write(&b, binary.BigEndian, uint16(1))
	b.WriteString("s")
	_ = binary.Write(&b, binary.BigEndian, uint16(1))
	b.WriteString("l")
	_ = binary.Write(&b, binary.BigEndian, uint16(0)) // empty key id
	_ = binary.Write(&b, binary.BigEndian, uint64(1))
	_ = binary.Write(&b, binary.BigEndian, uint64(1))
	_ = binary.Write(&b, binary.BigEndian, count)
	return b.Bytes()
}

func TestReadSegment_StructuralErrors(t *testing.T) {
	oversize := append(segmentHeader(FormatVersion, 1), 0xff, 0xff, 0xff, 0xff)
	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte("AUD")},
		{"bad magic", withTrailer(append([]byte("XXXXXX"), segmentHeader(FormatVersion, 0)[6:]...))},
		{"unsupported version", withTrailer(segmentHeader(2, 0))},
		{"trailing bytes", withTrailer(append(segmentHeader(FormatVersion, 0), 0x01))},
		{"frame length oversize", withTrailer(oversize)},
		{"frame body truncated", withTrailer(append(segmentHeader(FormatVersion, 1), 0, 0, 0, 9, 'x'))},
		{"missing frame", withTrailer(segmentHeader(FormatVersion, 1))},
		{"header cut after version", withTrailer(segmentHeader(FormatVersion, 0)[:8])},
		{"header cut in site", withTrailer(segmentHeader(FormatVersion, 0)[:10])},
		{"header cut in lane", withTrailer(segmentHeader(FormatVersion, 0)[:13])},
		{"header cut in keyId", withTrailer(segmentHeader(FormatVersion, 0)[:15])},
		{"keyId longer than the header", withTrailer(append(segmentHeader(FormatVersion, 0)[:14], 0xff, 0xff))},
		{"header cut in firstSeq", withTrailer(segmentHeader(FormatVersion, 0)[:16])},
		{"header cut in lastSeq", withTrailer(segmentHeader(FormatVersion, 0)[:24])},
		{"header cut in count", withTrailer(segmentHeader(FormatVersion, 0)[:34])},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ReadSegment(bytes.NewReader(tc.data))
			assert.True(t, errors.Is(err, ErrBadSegment), "got %v", err)
		})
	}
	t.Run("reader error", func(t *testing.T) {
		_, _, err := ReadSegment(failReader{})
		assert.True(t, errors.Is(err, errBoom))
	})
}

func TestWriteSegment_WriterFailures(t *testing.T) {
	h := Header{Site: "s", Lane: "l", FirstSeq: 1, LastSeq: 2, Count: 2}
	frames := [][]byte{[]byte("a"), []byte("b")}
	var ok bytes.Buffer
	_, _, err := WriteSegment(&ok, h, frames)
	require.NoError(t, err)
	// Every prefix shorter than the full object must fail somewhere: header,
	// a frame length, a frame body, or the trailer.
	for limit := 0; limit < ok.Len(); limit++ {
		_, _, err := WriteSegment(&failAfter{limit: limit}, h, frames)
		assert.True(t, errors.Is(err, errBoom), "limit %d: %v", limit, err)
	}
}

func TestReadFrameAt_Errors(t *testing.T) {
	t.Run("offset past the end", func(t *testing.T) {
		_, err := ReadFrameAt(bytes.NewReader([]byte{0, 0}), 10)
		assert.Error(t, err)
	})
	t.Run("oversize length", func(t *testing.T) {
		_, err := ReadFrameAt(bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff}), 0)
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
	t.Run("body shorter than its length", func(t *testing.T) {
		_, err := ReadFrameAt(bytes.NewReader([]byte{0, 0, 0, 9, 'x'}), 0)
		assert.Error(t, err)
	})
	t.Run("empty frame at the very end", func(t *testing.T) {
		f, err := ReadFrameAt(bytes.NewReader([]byte{0, 0, 0, 0}), 0)
		require.NoError(t, err)
		assert.Empty(t, f)
	})
}

func TestCipher_SealRandFailure(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	c.rand = failReader{}
	_, err = c.Seal([]byte("x"), nil)
	assert.True(t, errors.Is(err, errBoom))
}

func TestEncryptBlob_Failures(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	plain := bytes.Repeat([]byte{7}, 100)
	var ok bytes.Buffer
	_, _, err = EncryptBlob(&ok, bytes.NewReader(plain), c, "f", "k", 64)
	require.NoError(t, err)
	for limit := 0; limit < ok.Len(); limit++ {
		_, _, err := EncryptBlob(&failAfter{limit: limit}, bytes.NewReader(plain), c, "f", "k", 64)
		assert.True(t, errors.Is(err, errBoom), "limit %d: %v", limit, err)
	}
	t.Run("source read error", func(t *testing.T) {
		_, _, err := EncryptBlob(&bytes.Buffer{}, failReader{}, c, "f", "k", 64)
		assert.True(t, errors.Is(err, errBoom))
	})
	t.Run("rand failure while sealing", func(t *testing.T) {
		bad, err := NewCipher(testDEK())
		require.NoError(t, err)
		bad.rand = failReader{}
		_, _, err = EncryptBlob(&bytes.Buffer{}, bytes.NewReader(plain), bad, "f", "k", 64)
		assert.True(t, errors.Is(err, errBoom))
		_, _, err = EncryptBlob(&bytes.Buffer{}, bytes.NewReader(nil), bad, "f", "k", 64)
		assert.True(t, errors.Is(err, errBoom), "terminator seal")
	})
}

func TestDecryptBlob_StructuralErrors(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	var enc bytes.Buffer
	_, _, err = EncryptBlob(&enc, bytes.NewReader([]byte("hello")), c, "f", "k", 64)
	require.NoError(t, err)
	good := enc.Bytes()

	t.Run("bad magic", func(t *testing.T) {
		bad := append([]byte("XXXXXX"), good[6:]...)
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(bad), c, "f")
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
	t.Run("unsupported version", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[7] = 9
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(bad), c, "f")
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
	t.Run("header cut before chunk size", func(t *testing.T) {
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(good[:8]), c, "f")
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
	t.Run("header cut in key id", func(t *testing.T) {
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(good[:14]), c, "f")
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
	t.Run("oversize chunk length", func(t *testing.T) {
		bad := append(append([]byte(nil), good[:blobHeaderLen("k")]...), 0xff, 0xff, 0xff, 0xff)
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(bad), c, "f")
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
	t.Run("destination write error", func(t *testing.T) {
		_, _, _, err := DecryptBlob(&failAfter{limit: 0}, bytes.NewReader(good), c, "f")
		assert.True(t, errors.Is(err, errBoom))
	})
	t.Run("header only is truncated", func(t *testing.T) {
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(good[:blobHeaderLen("k")]), c, "f")
		assert.True(t, errors.Is(err, ErrBlobTruncated))
	})
	t.Run("source ends mid header", func(t *testing.T) {
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, io.LimitReader(bytes.NewReader(good), 3), c, "f")
		assert.True(t, errors.Is(err, ErrBadSegment))
	})
}
