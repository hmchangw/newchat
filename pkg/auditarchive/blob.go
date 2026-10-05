package auditarchive

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
)

// DefaultChunkBytes is the plaintext size of one blob chunk.
const DefaultChunkBytes = 4 << 20

// ErrBlobTruncated means the stream ended before the terminating chunk.
var ErrBlobTruncated = errors.New("auditarchive: blob ended before its final chunk")

var blobMagic = []byte("AUDBLB")

// sealOverhead is what Cipher.Seal adds to a plaintext: 12-byte nonce, 16-byte tag.
const sealOverhead = 12 + 16

// readErr classifies a failed read of a blob: end of data is the structural
// error, anything else is an I/O failure that keeps its cause so a retryable
// fault is not mistaken for tampering.
func readErr(err, structural error, what string) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %s", structural, what)
	}
	return fmt.Errorf("auditarchive: read blob %s: %w", what, err)
}

// BlobHeader is the plaintext prefix of an encrypted blob. KeyID names the
// wrapped DEK the chunks are sealed under, so a reader can pick the key
// before decrypting; it may be empty.
type BlobHeader struct {
	Version    uint16
	ChunkBytes uint32
	KeyID      string
}

// ReadBlobHeader reads the header of an encrypted blob and leaves src at the
// first chunk.
func ReadBlobHeader(src io.Reader) (BlobHeader, error) {
	be := binary.BigEndian
	magic := make([]byte, len(blobMagic))
	if _, err := io.ReadFull(src, magic); err != nil {
		return BlobHeader{}, readErr(err, ErrBadSegment, "magic")
	}
	if !bytes.Equal(magic, blobMagic) {
		return BlobHeader{}, fmt.Errorf("%w: bad blob magic", ErrBadSegment)
	}
	var h BlobHeader
	if err := binary.Read(src, be, &h.Version); err != nil {
		return BlobHeader{}, readErr(err, ErrBadSegment, "version")
	}
	if h.Version != FormatVersion {
		return BlobHeader{}, fmt.Errorf("%w: unsupported blob version", ErrBadSegment)
	}
	if err := binary.Read(src, be, &h.ChunkBytes); err != nil {
		return BlobHeader{}, readErr(err, ErrBadSegment, "chunk size")
	}
	var n uint16
	if err := binary.Read(src, be, &n); err != nil {
		return BlobHeader{}, readErr(err, ErrBadSegment, "key id length")
	}
	kid := make([]byte, n)
	if _, err := io.ReadFull(src, kid); err != nil {
		return BlobHeader{}, readErr(err, ErrBadSegment, "key id")
	}
	h.KeyID = string(kid)
	return h, nil
}

// EncryptBlob encrypts src chunk by chunk so attachments of any size stream
// through bounded memory. Each chunk's AAD carries its index; the empty
// terminating chunk carries final=true, so dropping, reordering or
// truncating chunks fails on decrypt. It returns the keyed digest of the
// plaintext (Cipher.NewDigest, "hmac-sha256:<hex>") and its length.
func EncryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID, keyID string, chunkBytes int) (string, int64, error) {
	if chunkBytes <= 0 {
		return "", 0, fmt.Errorf("auditarchive: chunk size must be positive, got %d", chunkBytes)
	}
	// The sealed chunk adds a nonce and tag, and DecryptBlob refuses frames
	// above maxFrameBytes, so a larger chunk would be written but unreadable.
	if chunkBytes > maxFrameBytes-sealOverhead {
		return "", 0, fmt.Errorf("auditarchive: chunk size %d exceeds the %d limit", chunkBytes, maxFrameBytes-sealOverhead)
	}
	if len(keyID) > math.MaxUint16 {
		return "", 0, fmt.Errorf("%w: key id (%d bytes) exceeds %d", ErrBadSegment, len(keyID), math.MaxUint16)
	}
	be := binary.BigEndian
	var hdr bytes.Buffer
	hdr.Write(blobMagic)
	_ = binary.Write(&hdr, be, FormatVersion)
	_ = binary.Write(&hdr, be, uint32(chunkBytes))
	writeHeaderString(&hdr, keyID)
	if _, err := dst.Write(hdr.Bytes()); err != nil {
		return "", 0, fmt.Errorf("auditarchive: write blob header: %w", err)
	}
	sum := c.NewDigest()
	buf := make([]byte, chunkBytes)
	var total int64
	var idx uint32
	writeFrame := func(sealed []byte) error {
		var l [4]byte
		// #nosec G115 -- a sealed chunk is at most chunkBytes+sealOverhead <= maxFrameBytes (checked above), far below MaxUint32
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
	return DigestPrefix + hex.EncodeToString(sum.Sum(nil)), total, nil
}

// DecryptBlob streams the plaintext to dst, verifying every chunk. It returns
// the keyed plaintext digest, the plaintext length and the header's key id.
func DecryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string) (digest string, size int64, keyID string, err error) {
	h, err := ReadBlobHeader(src)
	if err != nil {
		return "", 0, "", err
	}
	be := binary.BigEndian
	sum := c.NewDigest()
	var total int64
	for idx := uint32(0); ; idx++ {
		var n uint32
		if err := binary.Read(src, be, &n); err != nil {
			return "", 0, "", readErr(err, ErrBlobTruncated, fmt.Sprintf("chunk %d length", idx))
		}
		if n > maxFrameBytes {
			return "", 0, "", fmt.Errorf("%w: chunk %d length %d", ErrBadSegment, idx, n)
		}
		sealed := make([]byte, n)
		if _, err := io.ReadFull(src, sealed); err != nil {
			return "", 0, "", readErr(err, ErrBlobTruncated, fmt.Sprintf("chunk %d", idx))
		}
		pt, err := c.Open(sealed, ChunkAAD(fileID, idx, false))
		if err != nil {
			// Not a data chunk: it must be the terminator, or it is tampered.
			if _, ferr := c.Open(sealed, ChunkAAD(fileID, idx, true)); ferr == nil {
				return DigestPrefix + hex.EncodeToString(sum.Sum(nil)), total, h.KeyID, nil
			}
			return "", 0, "", err
		}
		sum.Write(pt)
		total += int64(len(pt))
		if _, err := dst.Write(pt); err != nil {
			return "", 0, "", fmt.Errorf("auditarchive: write plaintext chunk %d: %w", idx, err)
		}
	}
}
