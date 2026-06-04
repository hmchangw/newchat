package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromClient_RangeQuery_ParsesMatrix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/query_range", r.URL.Path)
		assert.NotEmpty(t, r.URL.Query().Get("query"))
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"resultType":"matrix","result":[
				{"metric":{"container_label_com_docker_compose_service":"cassandra"},
				 "values":[[100,"10.5"],[105,"11.0"]]}
			]}}`))
	}))
	defer srv.Close()

	c := newPromClient(srv.URL)
	start := time.Unix(100, 0)
	series, err := c.RangeQuery(context.Background(), `up`, start, start.Add(5*time.Second), 5*time.Second)
	require.NoError(t, err)
	require.Len(t, series, 1)
	assert.Equal(t, "cassandra", series[0].Labels["container_label_com_docker_compose_service"])
	require.Len(t, series[0].Samples, 2)
	assert.Equal(t, 10.5, series[0].Samples[0].V)
	assert.Equal(t, time.Unix(100, 0).UTC(), series[0].Samples[0].T.UTC())
	assert.Equal(t, 11.0, series[0].Samples[1].V)
	assert.Equal(t, time.Unix(105, 0).UTC(), series[0].Samples[1].T.UTC())
}

func TestPromClient_RangeQuery_NonSuccessStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"boom"}`))
	}))
	defer srv.Close()

	_, err := newPromClient(srv.URL).RangeQuery(context.Background(), `up`, time.Unix(0, 0), time.Unix(5, 0), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestPromClient_RangeQuery_NonJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("503 Service Unavailable"))
	}))
	defer srv.Close()

	_, err := newPromClient(srv.URL).RangeQuery(context.Background(), `up`, time.Unix(0, 0), time.Unix(5, 0), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode prometheus response")
}

func TestPromClient_RangeQuery_SkipsMalformedSamples(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First pair has a null timestamp (not float64) and must be skipped;
		// second pair has a non-numeric value and must be skipped; third is valid.
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"resultType":"matrix","result":[
				{"metric":{},"values":[[null,"1.0"],[100,"notanumber"],[105,"2.5"]]}
			]}}`))
	}))
	defer srv.Close()

	series, err := newPromClient(srv.URL).RangeQuery(context.Background(), `up`, time.Unix(100, 0), time.Unix(105, 0), time.Second)
	require.NoError(t, err)
	require.Len(t, series, 1)
	require.Len(t, series[0].Samples, 1)
	assert.Equal(t, 2.5, series[0].Samples[0].V)
	assert.Equal(t, time.Unix(105, 0).UTC(), series[0].Samples[0].T.UTC())
}
