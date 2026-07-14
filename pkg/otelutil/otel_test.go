package otelutil_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/otelutil"
)

// MetricsServer must serve the default Prometheus registry on /metrics so
// Prometheus can scrape the OTel and promauto counters registered there.
func TestMetricsServer_ServesMetrics(t *testing.T) {
	srv := otelutil.MetricsServer()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}
