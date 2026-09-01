package ginutil

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gzipEngine builds a router with only the gzip middleware in front of h.
func gzipEngine(t *testing.T, minSize int, h gin.HandlerFunc) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(minSize))
	r.GET("/x", h)
	return r
}

// panicEngine serves /boom, which writes body and then panics, and /x, which
// serves body normally — the pair every panic test needs.
func panicEngine(t *testing.T, body string) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), Gzip(1024))
	r.GET("/boom", func(c *gin.Context) {
		writeBody(body)(c)
		panic("boom")
	})
	r.GET("/x", writeBody(body))
	return r
}

// writeBody serves body as JSON, so the sniffer sees a compressible type.
func writeBody(body string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "application/json")
		_, _ = c.Writer.WriteString(body)
	}
}

// doGet issues a GET to /x with the given Accept-Encoding and records the response.
func doGet(r *gin.Engine, acceptEncoding string) *httptest.ResponseRecorder {
	return doPath(r, "/x", acceptEncoding)
}

// doPath is doGet against an explicit route.
func doPath(r *gin.Engine, path, acceptEncoding string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// gunzip decompresses a recorded body, failing the test on malformed output.
func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	return string(out)
}

// The point of the middleware: a body past minSize goes out encoded.
func TestGzip_CompressesAboveThreshold(t *testing.T) {
	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "gzip")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", w.Header().Get("Vary"))
	assert.Empty(t, w.Header().Get("Content-Length"), "a stale Content-Length would truncate the body")
	assert.Less(t, w.Body.Len(), len(body))
	assert.Equal(t, body, gunzip(t, w.Body.Bytes()), "round-trip must be lossless")
}

// Small bodies cost more in framing than they save, so they stay plain.
func TestGzip_PassesThroughBelowThreshold(t *testing.T) {
	body := strings.Repeat("a", 100)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "gzip")

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.Equal(t, body, w.Body.String())
}

// A client that never offered gzip must not receive it.
func TestGzip_PassesThroughWithoutAcceptEncoding(t *testing.T) {
	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "")

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.Empty(t, w.Header().Get("Vary"))
	assert.Equal(t, body, w.Body.String())
}

// Accept-Encoding is a list with parameters, not a string match.
func TestGzip_AcceptEncodingParsing(t *testing.T) {
	body := strings.Repeat("a", 4096)
	tests := []struct {
		header string
		want   bool
	}{
		{"gzip", true},
		{"GZIP", true},
		{"deflate, gzip", true},
		{"gzip;q=1.0, deflate", true},
		{" gzip ", true},
		{"deflate", false},
		{"br", false},
		{"gzipped", false},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			w := doGet(gzipEngine(t, 1024, writeBody(body)), tc.header)
			if tc.want {
				assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
				return
			}
			assert.Empty(t, w.Header().Get("Content-Encoding"))
		})
	}
}

// Exactly minSize must compress: the check is >=, not >.
func TestGzip_ExactThresholdBoundary(t *testing.T) {
	w := doGet(gzipEngine(t, 1024, writeBody(strings.Repeat("a", 1024))), "gzip")
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))

	w = doGet(gzipEngine(t, 1024, writeBody(strings.Repeat("a", 1023))), "gzip")
	assert.Empty(t, w.Header().Get("Content-Encoding"))
}

// The decision is on total bytes, not per-write size.
func TestGzip_ManySmallWritesAccumulateToThreshold(t *testing.T) {
	want := strings.Repeat("b", 16*200)
	r := gzipEngine(t, 1024, func(c *gin.Context) {
		for i := 0; i < 200; i++ {
			_, _ = c.Writer.WriteString(strings.Repeat("b", 16))
		}
	})
	w := doGet(r, "gzip")

	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, want, gunzip(t, w.Body.Bytes()))
}

// The status is buffered until the encoding settles; it must survive that.
func TestGzip_PreservesStatusCode(t *testing.T) {
	body := strings.Repeat("c", 4096)
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.String(http.StatusTeapot, body) })
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusTeapot, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, body, gunzip(t, w.Body.Bytes()))
}

// Same guarantee on the plain path, where no commit ever happens.
func TestGzip_PreservesStatusCodeBelowThreshold(t *testing.T) {
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.String(http.StatusNotFound, "nope") })
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "nope", w.Body.String())
}

// A handler that writes nothing must not leave a half-built encoder.
func TestGzip_EmptyBodyIsSafe(t *testing.T) {
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.Status(http.StatusNoContent) })
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String())
	assert.Empty(t, w.Header().Get("Content-Encoding"))
}

// gin marshals JSON in one Write, the shape this endpoint actually produces.
func TestGzip_JSONRenderRoundTrips(t *testing.T) {
	payload := map[string]string{"k": strings.Repeat("v", 4096)}
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.JSON(http.StatusOK, payload) })
	w := doGet(r, "gzip")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
	assert.JSONEq(t, `{"k":"`+strings.Repeat("v", 4096)+`"}`, gunzip(t, w.Body.Bytes()))
}

// A handler that already encoded its body owns the Content-Encoding.
func TestGzip_DoesNotDoubleEncode(t *testing.T) {
	body := strings.Repeat("a", 4096)
	r := gzipEngine(t, 1024, func(c *gin.Context) {
		c.Header("Content-Encoding", "br")
		_, _ = c.Writer.WriteString(body)
	})
	w := doGet(r, "gzip")

	assert.Equal(t, "br", w.Header().Get("Content-Encoding"), "an already-encoded body must pass through")
	assert.Equal(t, body, w.Body.String())
}

// Pooled writers must be reset between requests or bodies bleed together.
func TestGzip_ReusesPooledWritersAcrossRequests(t *testing.T) {
	body := strings.Repeat("a", 4096)
	r := gzipEngine(t, 1024, writeBody(body))
	for i := 0; i < 10; i++ {
		w := doGet(r, "gzip")
		require.Equal(t, "gzip", w.Header().Get("Content-Encoding"), "request %d", i)
		require.Equal(t, body, gunzip(t, w.Body.Bytes()), "pooled writer must be fully reset, request %d", i)
	}
}

// A panicking handler unwinds through the writer's deferred close. Recovery must
// still be able to report the failure rather than the process dying.
func TestGzip_PanicIsRecoverable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), Gzip(1024))
	r.GET("/x", func(c *gin.Context) { panic("boom") })

	assert.NotPanics(t, func() {
		assert.Equal(t, http.StatusInternalServerError, doGet(r, "gzip").Code)
	})
}

// The writer must be usable again after a panic left one mid-flight, or a pooled
// writer could carry a half-finished stream into the next request.
func TestGzip_PoolSurvivesPanicMidBody(t *testing.T) {
	body := strings.Repeat("a", 4096)
	r := panicEngine(t, body)

	doPath(r, "/boom", "gzip")

	w := doGet(r, "gzip")
	assert.Equal(t, body, gunzip(t, w.Body.Bytes()), "a later request must get a clean writer")
}

// A Flush while the body is still buffered must not let gin commit its default
// 200: the handler's real status and the buffered prefix both have to survive.
func TestGzip_FlushBelowThresholdPreservesStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		c.Status(http.StatusServiceUnavailable)
		_, _ = c.Writer.WriteString("short")
		c.Writer.Flush()
	})
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusServiceUnavailable, w.Code, "the handler's status must survive a mid-body flush")
	assert.Contains(t, w.Body.String(), "short", "the buffered prefix must not be stranded")
}

// Flush has to reach the gzip writer, not just the underlying one.
func TestGzip_FlushAfterThresholdStreams(t *testing.T) {
	body := strings.Repeat("a", 4096)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		_, _ = c.Writer.WriteString(body)
		c.Writer.Flush()
		_, _ = c.Writer.WriteString(body)
	})
	w := doGet(r, "gzip")

	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, body+body, gunzip(t, w.Body.Bytes()), "a flush must not truncate the stream")
}

// The first bytes on the wire are deflate output, so net/http would sniff
// application/x-gzip. The type must be settled from the plaintext prefix.
func TestGzip_DetectsContentTypeBeforeCompressing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		_, _ = c.Writer.WriteString("<html><body>" + strings.Repeat("a", 4096) + "</body></html>")
	})
	w := doGet(r, "gzip")

	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	assert.NotContains(t, w.Header().Get("Content-Type"), "gzip")
}

// An explicit Content-Type must win over sniffing.
func TestGzip_KeepsExplicitContentType(t *testing.T) {
	w := doGet(gzipEngine(t, 1024, writeBody(strings.Repeat("a", 4096))), "gzip")
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

// A handler that flushes before writing is streaming: the body passes through
// uncompressed rather than getting an application/x-gzip label from the sniffer.
func TestGzip_FlushBeforeAnyWriteStaysUncompressed(t *testing.T) {
	body := strings.Repeat("a", 4096)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Writer.Flush()
		_, _ = c.Writer.WriteString(body)
	})
	w := doGet(r, "gzip")

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.NotContains(t, w.Header().Get("Content-Type"), "gzip")
	assert.Equal(t, body, w.Body.String())
}

// gzip;q=0 is an explicit refusal, not an offer.
func TestGzip_HonoursQZeroRefusal(t *testing.T) {
	body := strings.Repeat("a", 4096)
	tests := []struct {
		header string
		want   bool
	}{
		{"gzip;q=0", false},
		{"gzip; q=0", false},
		{"gzip;q=0.0", false},
		{"gzip;q=0.5", true},
		{"gzip;q=1", true},
		{"deflate;q=1, gzip;q=0", false},
		{"gzip;q=bogus", true},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			w := doGet(gzipEngine(t, 1024, writeBody(body)), tc.header)
			if tc.want {
				assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
				return
			}
			assert.Empty(t, w.Header().Get("Content-Encoding"), "q=0 is an explicit refusal")
			assert.Equal(t, body, w.Body.String())
		})
	}
}

// A panic after a sub-threshold write must not commit that body: gin.Recovery
// has to be able to replace it with a 500.
func TestGzip_PanicDiscardsBufferedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		c.Status(http.StatusOK)
		_, _ = c.Writer.WriteString("partial")
		panic("boom")
	})
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusInternalServerError, w.Code, "Recovery must still own the response")
	assert.NotContains(t, w.Body.String(), "partial", "a buffered body must not survive a panic")
}

// withGzipPool swaps the shared encoder pool for the duration of a test.
func withGzipPool(t *testing.T, newFn func() any) {
	t.Helper()
	saved := gzipPool.New
	gzipPool = sync.Pool{New: newFn}
	t.Cleanup(func() { gzipPool = sync.Pool{New: saved} })
}

// An encoder the pool cannot supply must cost the compression, not the response.
func TestGzip_FallsBackToPlainWhenEncoderUnavailable(t *testing.T) {
	withGzipPool(t, func() any { return nil })

	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "gzip")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("Content-Encoding"), "an unusable encoder must not be advertised")
	assert.Equal(t, body, w.Body.String(), "the body must go out uncompressed rather than fail")
}

// The buffered prefix must survive the fallback too, not just the write that
// crossed the threshold.
func TestGzip_FallbackKeepsTheBufferedPrefix(t *testing.T) {
	withGzipPool(t, func() any { return nil })

	head, tail := "prefix-", strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, func(c *gin.Context) {
		c.Header("Content-Type", "application/json")
		_, _ = c.Writer.WriteString(head)
		_, _ = c.Writer.WriteString(tail)
	}), "gzip")

	assert.Equal(t, head+tail, w.Body.String())
}

// A panic after the encoder committed must leave a broken stream. Closing it
// would stamp a valid trailer, and the client would decode a truncated body as
// a complete 200.
func TestGzip_PanicMidBodyLeavesNoValidTrailer(t *testing.T) {
	w := doPath(panicEngine(t, strings.Repeat("a", 4096)), "/boom", "gzip")

	zr, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err, "the header did reach the wire")
	_, err = io.ReadAll(zr)
	assert.Error(t, err, "a truncated body must not decode as a complete response")
}
