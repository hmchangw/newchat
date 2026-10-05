package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"time"
)

// FormatVersion is written into every segment header; readers refuse others.
const FormatVersion uint16 = 1

// ErrBadSegment covers every structural failure: bad magic, version, count,
// truncation, or a trailer that does not match.
var ErrBadSegment = errors.New("auditarchive: malformed segment")

var segmentMagic = []byte("AUDSEG")

const maxFrameBytes = 64 << 20 // sanity bound on one frame's length prefix

// Header is the plaintext prefix of a segment. It exposes only site, lane,
// sequence range and count; which messages are inside stays in the frames.
type Header struct {
	Version  uint16
	Site     string
	Lane     string
	FirstSeq uint64
	LastSeq  uint64
	Count    uint32
}

type countingWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.h.Write(p[:n])
	c.n += int64(n)
	return n, err
}

// WriteSegment emits header, frames and trailer. h.Count must equal
// len(frames); h.Version is forced to FormatVersion.
func WriteSegment(w io.Writer, h Header, frames [][]byte) ([]int64, [32]byte, error) {
	var zero [32]byte
	if int(h.Count) != len(frames) {
		return nil, zero, fmt.Errorf("auditarchive: header count %d != %d frames", h.Count, len(frames))
	}
	if len(h.Site) > math.MaxUint16 || len(h.Lane) > math.MaxUint16 {
		return nil, zero, fmt.Errorf("auditarchive: site (%d bytes) or lane (%d bytes) exceeds %d", len(h.Site), len(h.Lane), math.MaxUint16)
	}
	for i, f := range frames {
		if len(f) > maxFrameBytes {
			return nil, zero, fmt.Errorf("auditarchive: frame %d is %d bytes, over the %d limit", i, len(f), maxFrameBytes)
		}
	}
	h.Version = FormatVersion
	cw := &countingWriter{w: w, h: sha256.New()}
	be := binary.BigEndian
	var hdr bytes.Buffer
	hdr.Write(segmentMagic)
	_ = binary.Write(&hdr, be, h.Version)
	_ = binary.Write(&hdr, be, uint16(len(h.Site)))
	hdr.WriteString(h.Site)
	_ = binary.Write(&hdr, be, uint16(len(h.Lane)))
	hdr.WriteString(h.Lane)
	_ = binary.Write(&hdr, be, h.FirstSeq)
	_ = binary.Write(&hdr, be, h.LastSeq)
	_ = binary.Write(&hdr, be, h.Count)
	if _, err := cw.Write(hdr.Bytes()); err != nil {
		return nil, zero, fmt.Errorf("auditarchive: write header: %w", err)
	}
	offsets := make([]int64, len(frames))
	var lenBuf [4]byte
	for i, f := range frames {
		offsets[i] = cw.n
		be.PutUint32(lenBuf[:], uint32(len(f)))
		if _, err := cw.Write(lenBuf[:]); err != nil {
			return nil, zero, fmt.Errorf("auditarchive: write frame %d length: %w", i, err)
		}
		if _, err := cw.Write(f); err != nil {
			return nil, zero, fmt.Errorf("auditarchive: write frame %d: %w", i, err)
		}
	}
	var trailer [32]byte
	copy(trailer[:], cw.h.Sum(nil))
	if _, err := w.Write(trailer[:]); err != nil {
		return nil, zero, fmt.Errorf("auditarchive: write trailer: %w", err)
	}
	return offsets, trailer, nil
}

// ReadSegment parses and verifies a whole segment.
func ReadSegment(r io.Reader) (Header, [][]byte, error) {
	all, err := io.ReadAll(r)
	if err != nil {
		return Header{}, nil, fmt.Errorf("auditarchive: read segment: %w", err)
	}
	if len(all) < len(segmentMagic)+32 {
		return Header{}, nil, fmt.Errorf("%w: too short", ErrBadSegment)
	}
	body, trailer := all[:len(all)-32], all[len(all)-32:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:], trailer) {
		return Header{}, nil, fmt.Errorf("%w: trailer mismatch", ErrBadSegment)
	}
	rd := bytes.NewReader(body)
	var h Header
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(rd, magic); err != nil || !bytes.Equal(magic, segmentMagic) {
		return Header{}, nil, fmt.Errorf("%w: bad magic", ErrBadSegment)
	}
	be := binary.BigEndian
	readStr := func() (string, error) {
		var n uint16
		if err := binary.Read(rd, be, &n); err != nil {
			return "", err
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(rd, b); err != nil {
			return "", err
		}
		return string(b), nil
	}
	if err := binary.Read(rd, be, &h.Version); err != nil || h.Version != FormatVersion {
		return Header{}, nil, fmt.Errorf("%w: unsupported version", ErrBadSegment)
	}
	if h.Site, err = readStr(); err != nil {
		return Header{}, nil, fmt.Errorf("%w: site: %w", ErrBadSegment, err)
	}
	if h.Lane, err = readStr(); err != nil {
		return Header{}, nil, fmt.Errorf("%w: lane: %w", ErrBadSegment, err)
	}
	if err := binary.Read(rd, be, &h.FirstSeq); err != nil {
		return Header{}, nil, fmt.Errorf("%w: firstSeq", ErrBadSegment)
	}
	if err := binary.Read(rd, be, &h.LastSeq); err != nil {
		return Header{}, nil, fmt.Errorf("%w: lastSeq", ErrBadSegment)
	}
	if err := binary.Read(rd, be, &h.Count); err != nil {
		return Header{}, nil, fmt.Errorf("%w: count", ErrBadSegment)
	}
	// Each frame needs at least its 4-byte length prefix, so a larger count
	// cannot be honest; checking it also bounds the preallocation.
	if uint64(h.Count) > uint64(rd.Len())/4 {
		return Header{}, nil, fmt.Errorf("%w: count %d exceeds what %d bytes can hold", ErrBadSegment, h.Count, rd.Len())
	}
	frames := make([][]byte, 0, h.Count)
	for i := uint32(0); i < h.Count; i++ {
		var n uint32
		if err := binary.Read(rd, be, &n); err != nil || n > maxFrameBytes {
			return Header{}, nil, fmt.Errorf("%w: frame %d length", ErrBadSegment, i)
		}
		f := make([]byte, n)
		if _, err := io.ReadFull(rd, f); err != nil {
			return Header{}, nil, fmt.Errorf("%w: frame %d truncated", ErrBadSegment, i)
		}
		frames = append(frames, f)
	}
	if rd.Len() != 0 {
		return Header{}, nil, fmt.Errorf("%w: %d trailing bytes", ErrBadSegment, rd.Len())
	}
	return h, frames, nil
}

// ReadFrameAt reads the frame whose length prefix starts at offset. It does
// not verify the trailer; callers verify the frame itself with Cipher.Open.
func ReadFrameAt(ra io.ReaderAt, offset int64) ([]byte, error) {
	var lenBuf [4]byte
	if err := readFullAt(ra, lenBuf[:], offset); err != nil {
		return nil, fmt.Errorf("auditarchive: read frame length at %d: %w", offset, err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxFrameBytes {
		return nil, fmt.Errorf("%w: frame length %d", ErrBadSegment, n)
	}
	f := make([]byte, n)
	if n == 0 {
		return f, nil
	}
	if err := readFullAt(ra, f, offset+4); err != nil {
		return nil, fmt.Errorf("auditarchive: read frame at %d: %w", offset, err)
	}
	return f, nil
}

// readFullAt fills p from offset. io.ReaderAt may return io.EOF together with
// a complete read when the data ends exactly at the end of p, so only a short
// count is a failure.
func readFullAt(ra io.ReaderAt, p []byte, offset int64) error {
	n, err := ra.ReadAt(p, offset)
	if n == len(p) {
		return nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// SegmentKey is the object key for a sealed batch, time-ordered by prefix.
func SegmentKey(site, lane string, at time.Time, firstSeq, lastSeq uint64) string {
	u := at.UTC()
	return fmt.Sprintf("%s/%04d/%02d/%02d/%02d/%s-%d-%d.seg", site, u.Year(), int(u.Month()), u.Day(), u.Hour(), lane, firstSeq, lastSeq)
}

// BlobKey is the object key for an archived attachment.
func BlobKey(site, fileID string) string {
	return site + "/blobs/" + fileID
}
