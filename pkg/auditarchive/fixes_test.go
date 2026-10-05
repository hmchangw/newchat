package auditarchive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eofAtEndReader mimics a ranged object reader: io.ReaderAt allows a read that
// reaches the end of the data to return (n == len(p), io.EOF).
type eofAtEndReader struct{ data []byte }

func (r eofAtEndReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if off+int64(n) >= int64(len(r.data)) {
		return n, io.EOF
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// errAfterReader serves data, then fails with err instead of io.EOF.
type errAfterReader struct {
	data []byte
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestReadFrameAt_ReaderAtReturningEOFWithFullRead(t *testing.T) {
	var buf bytes.Buffer
	frames := [][]byte{[]byte("first"), []byte("last frame is non-empty")}
	offsets, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "l", FirstSeq: 1, LastSeq: 2, Count: 2}, frames)
	require.NoError(t, err)
	// Drop the 32-byte trailer so the last frame ends exactly at end of data,
	// the case where a ReaderAt reports io.EOF alongside the full read.
	data := buf.Bytes()[:buf.Len()-32]
	for i, off := range offsets {
		got, err := ReadFrameAt(eofAtEndReader{data: data}, off)
		require.NoError(t, err, "frame %d", i)
		assert.Equal(t, frames[i], got)
	}

	t.Run("a four byte frame prefix that ends the data", func(t *testing.T) {
		got, err := ReadFrameAt(eofAtEndReader{data: []byte{0, 0, 0, 0}}, 0)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestReadSegment_CountLargerThanBytesIsRejectedWithoutAllocating(t *testing.T) {
	data := withTrailer(segmentHeader(FormatVersion, 0xffffffff))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err := ReadSegment(bytes.NewReader(data))
	runtime.ReadMemStats(&after)
	assert.True(t, errors.Is(err, ErrBadSegment), "got %v", err)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20), "must not preallocate from the header count")
}

func TestDecryptBlob_SourceErrorsAreNotStructuralErrors(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	var enc bytes.Buffer
	_, _, err = EncryptBlob(&enc, bytes.NewReader(bytes.Repeat([]byte{3}, 100)), c, "f", 64)
	require.NoError(t, err)
	good := enc.Bytes()

	// Cut points: inside the magic, the version, the chunk size, a chunk's
	// length prefix, and a chunk's body.
	for _, cut := range []int{3, 7, 10, 13, 20} {
		t.Run(fmt.Sprintf("cut at byte %d", cut), func(t *testing.T) {
			src := &errAfterReader{data: append([]byte(nil), good[:cut]...), err: errBoom}
			_, _, err := DecryptBlob(&bytes.Buffer{}, src, c, "f")
			require.Error(t, err)
			assert.True(t, errors.Is(err, errBoom), "must wrap the source error: %v", err)
			assert.False(t, errors.Is(err, ErrBlobTruncated), "I/O failure is not truncation: %v", err)
			assert.False(t, errors.Is(err, ErrBadSegment), "I/O failure is not a bad blob: %v", err)
		})
	}
}

func TestWriteSegment_RejectsValuesTheReaderRefuses(t *testing.T) {
	t.Run("site too long", func(t *testing.T) {
		_, _, err := WriteSegment(&bytes.Buffer{}, Header{Site: strings.Repeat("s", 65536), Lane: "l"}, nil)
		assert.Error(t, err)
	})
	t.Run("lane too long", func(t *testing.T) {
		_, _, err := WriteSegment(&bytes.Buffer{}, Header{Site: "s", Lane: strings.Repeat("l", 65536)}, nil)
		assert.Error(t, err)
	})
	t.Run("longest permitted site and lane round trip", func(t *testing.T) {
		h := Header{Version: FormatVersion, Site: strings.Repeat("s", 65535), Lane: strings.Repeat("l", 65535)}
		var buf bytes.Buffer
		_, _, err := WriteSegment(&buf, h, nil)
		require.NoError(t, err)
		got, _, err := ReadSegment(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, h, got)
	})
	t.Run("frame larger than the reader bound", func(t *testing.T) {
		big := make([]byte, maxFrameBytes+1)
		_, _, err := WriteSegment(&bytes.Buffer{}, Header{Site: "s", Lane: "l", Count: 1}, [][]byte{big})
		assert.Error(t, err)
	})
}

func TestEncryptBlob_ChunkSizeBound(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	// The sealed chunk is chunkBytes plus nonce and tag, and the reader refuses
	// frames above maxFrameBytes, so a chunk size that cannot fit is rejected
	// before any buffer is allocated.
	for _, size := range []int{maxFrameBytes + 1, maxFrameBytes, maxFrameBytes - 27} {
		_, _, err := EncryptBlob(&bytes.Buffer{}, bytes.NewReader([]byte("x")), c, "f", size)
		assert.Error(t, err, "chunk size %d", size)
	}
}

// blobFrames splits an encrypted blob into its 10-byte header and the
// length-prefixed frames that follow, so tests can drop, swap or copy frames.
func blobFrames(t *testing.T, enc []byte) (header []byte, frames [][]byte) {
	t.Helper()
	header = enc[:12]
	rest := enc[12:]
	for len(rest) > 0 {
		n := int(rest[0])<<24 | int(rest[1])<<16 | int(rest[2])<<8 | int(rest[3])
		frames = append(frames, rest[:4+n])
		rest = rest[4+n:]
	}
	return header, frames
}

func joinBlob(header []byte, frames ...[]byte) []byte {
	out := append([]byte(nil), header...)
	for _, f := range frames {
		out = append(out, f...)
	}
	return out
}

func TestBlob_ChunkStructureTampering(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	plain := make([]byte, 3*64)
	for i := range plain {
		plain[i] = byte(i)
	}
	var enc bytes.Buffer
	_, _, err = EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", 64)
	require.NoError(t, err)
	header, frames := blobFrames(t, enc.Bytes())
	require.Len(t, frames, 4, "three data chunks and the terminator")

	decrypt := func(b []byte) error {
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b), c, "f1")
		return err
	}

	t.Run("untouched blob decrypts", func(t *testing.T) {
		require.NoError(t, decrypt(joinBlob(header, frames...)))
	})
	t.Run("dropping exactly the terminator is truncation", func(t *testing.T) {
		assert.True(t, errors.Is(decrypt(joinBlob(header, frames[:3]...)), ErrBlobTruncated))
	})
	t.Run("truncating at a chunk boundary is truncation", func(t *testing.T) {
		assert.True(t, errors.Is(decrypt(joinBlob(header, frames[:2]...)), ErrBlobTruncated))
	})
	t.Run("swapping two data chunks fails authentication", func(t *testing.T) {
		assert.True(t, errors.Is(decrypt(joinBlob(header, frames[1], frames[0], frames[2], frames[3])), ErrAuthFailed))
	})
	t.Run("terminator copied to an earlier position fails authentication", func(t *testing.T) {
		assert.True(t, errors.Is(decrypt(joinBlob(header, frames[0], frames[3], frames[1], frames[2], frames[3])), ErrAuthFailed))
	})
}
