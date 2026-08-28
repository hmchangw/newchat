package testutil

import (
	"errors"
	"io"
	"runtime"
	"time"
)

// ZeroReader is a file of N zero bytes with no backing array, so heap growth
// measured around a transfer belongs to the code under test, never the
// fixture. It implements io.Reader, io.ReaderAt, io.Seeker, and io.Closer,
// which also satisfies mime/multipart.File.
type ZeroReader struct {
	N   int64
	pos int64
}

func (z *ZeroReader) Read(p []byte) (int, error) {
	if z.pos >= z.N {
		return 0, io.EOF
	}
	if int64(len(p)) > z.N-z.pos {
		p = p[:z.N-z.pos]
	}
	for i := range p {
		p[i] = 0
	}
	z.pos += int64(len(p))
	return len(p), nil
}

func (z *ZeroReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("testutil.ZeroReader.ReadAt: negative offset")
	}
	if off >= z.N {
		return 0, io.EOF
	}
	n := len(p)
	var err error
	if int64(n) > z.N-off {
		n = int(z.N - off)
		// io.ReaderAt contract: n < len(p) must come with an explaining error,
		// like bytes.Reader — a size-driven caller loops forever otherwise.
		err = io.EOF
	}
	for i := range p[:n] {
		p[i] = 0
	}
	return n, err
}

func (z *ZeroReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		z.pos = offset
	case io.SeekCurrent:
		z.pos += offset
	case io.SeekEnd:
		z.pos = z.N + offset
	}
	return z.pos, nil
}

func (z *ZeroReader) Close() error { return nil }

// PeakHeapDuring reports the maximum HeapAlloc observed while fn runs. It
// samples on a ticker because runtime.ReadMemStats stops the world; a busy
// loop would starve the very work being measured.
func PeakHeapDuring(fn func()) uint64 {
	runtime.GC()
	peak := make(chan uint64, 1)
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		var max uint64
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				peak <- max
				return
			case <-tk.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > max {
					max = ms.HeapAlloc
				}
			}
		}
	}()
	fn()
	close(done)
	return <-peak
}
