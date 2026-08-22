package ginutil

import (
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
)

// BestSpeed, not the default level: on a 200-row subscription page it compresses
// 39% faster (695µs vs 1142µs) for 3 KB more on the wire (10.3x vs 12.4x). These
// endpoints exist to survive reconnect bursts, where CPU is the scarce resource
// and a few KB over a LAN is not.
var gzipPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

// Gzip compresses responses of at least minSize bytes for clients that accept
// it. Smaller bodies pass through: compressing them costs CPU and often grows
// the payload.
//
// The body length is unknown when headers are written, so the writer buffers
// until the threshold is crossed and only then commits to compression.
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

// gzipResponseWriter buffers the first minSize bytes, then either switches to
// gzip or, at close, flushes the buffer uncompressed.
//
// Written() and Size() are left delegating to the wrapped writer, so they report
// what reached the wire: false/0 while the body is still buffered, and the
// compressed length afterwards. That is what gin.Recovery needs (can it still
// write a status?) but it is not the handler's byte count.
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

// write is the shared sniff-then-commit path behind Write and WriteString.
//
// The threshold is tested before buffering, so a body that clears it in one
// write — which is every JSON render, since gin marshals whole — is handed
// straight to the encoder instead of being copied into buf first.
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
	if err := w.commit(); err != nil {
		return 0, err
	}
	return w.write(p)
}

// commit decides the encoding once the threshold is reached and drains whatever
// sub-threshold prefix was buffered. A handler that set its own Content-Encoding
// is left alone.
func (w *gzipResponseWriter) commit() error {
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

func (w *gzipResponseWriter) Flush() {
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	w.ResponseWriter.Flush()
}
