package testutil

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZeroReader_ReadsNZeroBytes(t *testing.T) {
	z := &ZeroReader{N: 5}
	// A dirty destination proves Read writes zeros rather than trusting the
	// caller's buffer to already be zeroed.
	p := []byte{1, 2, 3, 4, 5, 6}
	n, err := z.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte{0, 0, 0, 0, 0}, p[:n])
	_, err = z.Read(p)
	assert.Equal(t, io.EOF, err)
}

func TestZeroReader_SeekAndReadAt(t *testing.T) {
	z := &ZeroReader{N: 10}
	end, err := z.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(10), end)
	pos, err := z.Seek(4, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(4), pos)

	p := []byte{9, 9, 9, 9}
	n, err := z.ReadAt(p, 8)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []byte{0, 0}, p[:n])
	_, err = z.ReadAt(p, 10)
	assert.Equal(t, io.EOF, err)
}

func TestPeakHeapDuring_SamplesWhileFnRuns(t *testing.T) {
	ran := false
	peak := PeakHeapDuring(func() {
		ran = true
		time.Sleep(30 * time.Millisecond) // outlive at least one 10ms sample tick
	})
	assert.True(t, ran)
	assert.Positive(t, peak, "a live process always has nonzero HeapAlloc")
}
