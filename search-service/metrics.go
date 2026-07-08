package main

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricESDuration is search-service-specific (Elasticsearch _search latency)
// and stays here; the generic request-path metrics now come from
// natsrouter.Metrics (pkg/rpcmetrics).
var metricESDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "search_service_es_duration_seconds",
	Help:    "Elasticsearch _search call latency in seconds.",
	Buckets: prometheus.DefBuckets,
})

func observeES() func() {
	start := time.Now()
	return func() { metricESDuration.Observe(time.Since(start).Seconds()) }
}

func metricsHandler() http.Handler { return promhttp.Handler() }
