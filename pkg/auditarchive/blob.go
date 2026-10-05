package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// DefaultChunkBytes is the plaintext size of one blob chunk.
const DefaultChunkBytes = 4 << 20

// ErrBlobTruncated means the stream ended before the terminating chunk.
var ErrBlobTruncated = errors.New("auditarchive: blob ended before its final chunk")

var blobMagic = []byte("AUDBLB")

// EncryptBlob encrypts src chunk by chunk so attachments of any size stream
// through bounded memory. Each chunk's AAD carries its index; the empty
// terminating chunk carries final=true, so dropping, reordering or
// truncating chunks fails on decrypt.
func EncryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string, chunkBytes int) (string, int64, error) {
	if chunkBytes <= 0 {
		return "", 0, fmt.Errorf("auditarchive: chunk size must be positive, got %d", chunkBytes)
	}
	be := binary.BigEndian
	var hdr bytes.Buffer
	hdr.Write(blobMagic)
	_ = binary.Write(&hdr, be, FormatVersion)
	_ = binary.Write(&hdr, be, uint32(chunkBytes))
	if _, err := dst.Write(hdr.Bytes()); err != nil {
		return "", 0, fmt.Errorf("auditarchive: write blob header: %w", err)
	}
	sum := sha256.New()
	buf := make([]byte, chunkBytes)
	var total int64
	var idx uint32
	writeFrame := func(sealed []byte) error {
		var l [4]byte
		be.PutUint32(l[:], uint32(len(sealed)))
		if _, err := dst.Write(l[:]); err != nil {
			return err
		}
		_, err := dst.Write(sealed)
		return err
	}
	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			sum.Write(buf[:n])
			total += int64(n)
			sealed, serr := c.Seal(buf[:n], ChunkAAD(fileID, idx, false))
			if serr != nil {
				return "", 0, serr
			}
			if werr := writeFrame(sealed); werr != nil {
				return "", 0, fmt.Errorf("auditarchive: write chunk %d: %w", idx, werr)
			}
			idx++
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return "", 0, fmt.Errorf("auditarchive: read blob source: %w", err)
		}
	}
	final, err := c.Seal(nil, ChunkAAD(fileID, idx, true))
	if err != nil {
		return "", 0, err
	}
	if err := writeFrame(final); err != nil {
		return "", 0, fmt.Errorf("auditarchive: write final chunk: %w", err)
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), total, nil
}

// DecryptBlob streams the plaintext to dst, verifying every chunk.
func DecryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string) (string, int64, error) {
	be := binary.BigEndian
	magic := make([]byte, len(blobMagic))
	if _, err := io.ReadFull(src, magic); err != nil || !bytes.Equal(magic, blobMagic) {
		return "", 0, fmt.Errorf("%w: bad blob magic", ErrBadSegment)
	}
	var version uint16
	var chunkBytes uint32
	if err := binary.Read(src, be, &version); err != nil || version != FormatVersion {
		return "", 0, fmt.Errorf("%w: unsupported blob version", ErrBadSegment)
	}
	if err := binary.Read(src, be, &chunkBytes); err != nil {
		return "", 0, fmt.Errorf("%w: blob chunk size", ErrBadSegment)
	}
	sum := sha256.New()
	var total int64
	for idx := uint32(0); ; idx++ {
		var n uint32
		if err := binary.Read(src, be, &n); err != nil {
			return "", 0, fmt.Errorf("%w: chunk %d", ErrBlobTruncated, idx)
		}
		if n > maxFrameBytes {
			return "", 0, fmt.Errorf("%w: chunk %d length %d", ErrBadSegment, idx, n)
		}
		sealed := make([]byte, n)
		if _, err := io.ReadFull(src, sealed); err != nil {
			return "", 0, fmt.Errorf("%w: chunk %d", ErrBlobTruncated, idx)
		}
		pt, err := c.Open(sealed, ChunkAAD(fileID, idx, false))
		if err != nil {
			// Not a data chunk: it must be the terminator, or it is tampered.
			if _, ferr := c.Open(sealed, ChunkAAD(fileID, idx, true)); ferr == nil {
				return "sha256:" + hex.EncodeToString(sum.Sum(nil)), total, nil
			}
			return "", 0, err
		}
		sum.Write(pt)
		total += int64(len(pt))
		if _, err := dst.Write(pt); err != nil {
			return "", 0, fmt.Errorf("auditarchive: write plaintext chunk %d: %w", idx, err)
		}
	}
}
