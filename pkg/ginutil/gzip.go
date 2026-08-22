package ginutil

import (
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
)

// BestSpeed over the default: measured 39% faster for 3 KB more on a 200-row page.
// These endpoints exist to survive bursts, where CPU is the scarce resource.
var gzipPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

// Gzip compresses responses of at least minSize bytes for clients that accept it;
// smaller bodies pass through, since compressing them costs CPU and often grows
// the payload. Body length is unknown at header time, so the writer sniffs until
// the threshold is crossed before committing.
func Gzip(minSize int) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !acceptsGzip(c.Request.Header.Get("Accept-Encoding")) {
			c.Next()
			return
		}
		// Set even when this response ends up uncompressed: a shared cache must
		// key on Accept-Encoding either way.
		c.Header("Vary", "Accept-Encoding")

		gw := &gzipResponseWriter{ResponseWriter: c.Writer, minSize: minSize}
		c.Writer = gw
		defer func() {
			gw.close()
			c.Writer = gw.ResponseWriter
		}()
		c.Next()
	}
}

// acceptsGzip reports whether the Accept-Encoding header lists gzip, ignoring
// q-values (a q=0 refusal is rare enough not to warrant the parse).
func acceptsGzip(header string) bool {
	for enc := range strings.SplitSeq(header, ",") {
		name, _, _ := strings.Cut(enc, ";")
		if strings.EqualFold(strings.TrimSpace(name), "gzip") {
			return true
		}
	}
	return false
}

// gzipResponseWriter buffers the first minSize bytes, then either switches to gzip
// or, at close, flushes the buffer uncompressed.
//
// Written() and Size() deliberately still delegate, reporting what reached the wire
// rather than what the handler produced — which is what gin.Recovery needs.
type gzipResponseWriter struct {
	gin.ResponseWriter
	minSize int
	buf     []byte
	gz      *gzip.Writer
	status  int
	// plain is set once the response is committed to passing through uncompressed.
	plain  bool
	closed bool
}

// WriteHeader records the status without flushing: the encoding headers are not
// decided until the threshold check.
func (w *gzipResponseWriter) WriteHeader(status int) { w.status = status }

// WriteHeaderNow is a no-op for the same reason; close flushes the real header.
func (w *gzipResponseWriter) WriteHeaderNow() {}

func (w *gzipResponseWriter) Status() int {
	if w.status != 0 {
		return w.status
	}
	return w.ResponseWriter.Status()
}

func (w *gzipResponseWriter) Write(p []byte) (int, error) { return w.write(p) }

func (w *gzipResponseWriter) WriteString(s string) (int, error) { return w.write([]byte(s)) }

// write is the shared sniff-then-commit path behind Write and WriteString. The
// threshold is tested before buffering, so a whole-body write (every JSON render)
// reaches the encoder without being copied through buf first.
func (w *gzipResponseWriter) write(p []byte) (int, error) {
	switch {
	case w.gz != nil:
		return w.gz.Write(p)
	case w.plain, w.closed:
		// After close the wrapper is inert: taking another pooled writer here
		// would be one no close() will ever return.
		return w.ResponseWriter.Write(p)
	}
	if len(w.buf)+len(p) < w.minSize {
		w.buf = append(w.buf, p...)
		return len(p), nil
	}
	if err := w.commit(p); err != nil {
		return 0, err
	}
	return w.write(p)
}

// commit decides the encoding once the threshold is reached and drains whatever
// sub-threshold prefix was buffered. pending is the write that triggered the
// commit — not written here, only sniffed, since a large first write never lands
// in buf. A handler that set its own Content-Encoding is left alone.
func (w *gzipResponseWriter) commit(pending []byte) error {
	h := w.Header()
	buf := w.buf
	w.buf = nil

	if h.Get("Content-Encoding") != "" {
		w.plain = true
		w.flushStatus()
		if len(buf) == 0 {
			return nil
		}
		_, err := w.ResponseWriter.Write(buf)
		return err
	}

	// net/http sniffs the type from the first bytes written, which are about to be
	// deflate output. Settle it from the plaintext while we still have it.
	if h.Get("Content-Type") == "" {
		if probe := sniffProbe(buf, pending); len(probe) > 0 {
			h.Set("Content-Type", http.DetectContentType(probe))
		}
	}
	h.Set("Content-Encoding", "gzip")
	// A Content-Length measured on the uncompressed body would truncate the response.
	h.Del("Content-Length")
	w.flushStatus()

	w.gz = gzipPool.Get().(*gzip.Writer)
	w.gz.Reset(w.ResponseWriter)
	if len(buf) == 0 {
		return nil
	}
	_, err := w.gz.Write(buf)
	return err
}

// sniffProbe returns up to the 512 bytes net/http's detector reads, drawn from
// the buffered prefix and then the pending write.
func sniffProbe(buf, pending []byte) []byte {
	const sniffLen = 512
	if len(buf) >= sniffLen || len(pending) == 0 {
		return buf
	}
	probe := make([]byte, 0, min(len(buf)+len(pending), sniffLen))
	probe = append(probe, buf...)
	return append(probe, pending[:min(len(pending), sniffLen-len(probe))]...)
}

func (w *gzipResponseWriter) flushStatus() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.ResponseWriter.WriteHeader(w.status)
}

// close finalizes the response: flush the encoder, or emit the buffered body
// uncompressed when the handler never reached the threshold.
func (w *gzipResponseWriter) close() {
	if w.closed {
		return
	}
	w.closed = true

	if w.gz != nil {
		// A failed flush means the connection is already gone; nothing to recover.
		_ = w.gz.Close()
		w.gz.Reset(nil)
		gzipPool.Put(w.gz)
		w.gz = nil
		return
	}
	if w.plain {
		return
	}
	if w.status != 0 {
		w.ResponseWriter.WriteHeader(w.status)
	}
	if len(w.buf) > 0 {
		_, _ = w.ResponseWriter.Write(w.buf)
		w.buf = nil
	}
}

// Flush on an uncommitted body passes it through uncompressed: a handler that
// flushes is streaming, and there is no plaintext left to sniff a Content-Type
// from once deflate output goes out first. Flushing straight through instead
// would let gin write its default 200 and strand the handler's real status.
func (w *gzipResponseWriter) Flush() {
	if w.gz == nil && !w.plain && !w.closed {
		w.plain = true
		w.flushStatus()
		if len(w.buf) > 0 {
			_, _ = w.ResponseWriter.Write(w.buf)
			w.buf = nil
		}
	}
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	w.ResponseWriter.Flush()
}
