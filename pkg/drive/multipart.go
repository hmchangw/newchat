package drive

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
)

// sniffLen is the number of leading bytes http.DetectContentType inspects.
const sniffLen = 512

// quoteEscaper mirrors mime/multipart's own escaping. CR and LF become %0D/%0A so
// a crafted upload filename cannot inject extra headers into the part.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"", "\r", "%0D", "\n", "%0A")

// formField is one non-file multipart field.
type formField struct{ name, value string }

// streamedBody is a multipart body whose file bytes are never copied into memory.
type streamedBody struct {
	reader      io.Reader
	contentType string
	length      int64
}

// buildStreamedBody assembles a multipart body as a reader chain: small envelope
// snapshots (boundaries, part headers, form fields) interleaved with the callers'
// own file readers. Peak memory is therefore independent of file size. The exact
// body length is summed up front so the request still carries a Content-Length
// rather than falling back to chunked encoding.
//
// multipart.Writer only ever writes the envelope here — the part content is
// streamed around it — which is safe because it does not track part lengths.
func buildStreamedBody(fields []formField, files []MultipartFile) (*streamedBody, error) {
	var env bytes.Buffer
	mw := multipart.NewWriter(&env)

	var chain []io.Reader
	var length int64
	// flush moves everything the writer has emitted since the last call into the
	// chain. The bytes are copied out because env reuses its array after Reset.
	flush := func() {
		if env.Len() == 0 {
			return
		}
		b := append([]byte(nil), env.Bytes()...)
		env.Reset()
		chain = append(chain, bytes.NewReader(b))
		length += int64(len(b))
	}

	for _, f := range fields {
		if err := mw.WriteField(f.name, f.value); err != nil {
			return nil, fmt.Errorf("write form field %s: %w", f.name, err)
		}
	}

	for i, f := range files {
		size, err := fileSize(f.File)
		if err != nil {
			return nil, fmt.Errorf("measure file %d: %w", i, err)
		}
		// Sniff the leading bytes for the part's media type, then push them back to
		// the head of the chain so the file still arrives whole.
		head := make([]byte, sniffLen)
		n, err := io.ReadFull(f.File, head)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("sniff file %d: %w", i, err)
		}
		head = head[:n]

		hdr := partHeader(fmt.Sprintf("files[%d].file", i), f.Filename, http.DetectContentType(head))
		if _, err := mw.CreatePart(hdr); err != nil {
			return nil, fmt.Errorf("create part for file %d: %w", i, err)
		}
		flush()
		chain = append(chain, bytes.NewReader(head), io.LimitReader(f.File, size-int64(n)))
		length += size
	}

	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}
	flush()

	return &streamedBody{reader: io.MultiReader(chain...), contentType: mw.FormDataContentType(), length: length}, nil
}

// fileSize measures an upload by seeking to its end and rewinding. multipart.File
// is always an io.Seeker, so no caller has to supply (or can get wrong) a length.
func fileSize(f multipart.File) (int64, error) {
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek to end: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("rewind: %w", err)
	}
	return size, nil
}

// partHeader builds a file part's MIME header in mime/multipart's own format.
// Concatenated rather than %q-formatted: the values are already escaped by
// quoteEscaper, and Go quoting would escape them a second time.
func partHeader(field, filename, contentType string) textproto.MIMEHeader {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="`+quoteEscaper.Replace(field)+
		`"; filename="`+quoteEscaper.Replace(filename)+`"`)
	h.Set("Content-Type", contentType)
	return h
}
