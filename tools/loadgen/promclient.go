package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// promSample is one (timestamp, value) point from a Prometheus matrix result.
type promSample struct {
	T time.Time
	V float64
}

// promSeries is one labelled time-series returned by a range query.
type promSeries struct {
	Labels  map[string]string
	Samples []promSample
}

// promClient queries the Prometheus HTTP API. It satisfies the promQuerier
// interface (defined in attribution.go) used as the production querier.
type promClient struct {
	rc *resty.Client
}

// newPromClient builds a client against a Prometheus base URL (e.g.
// "http://prometheus:9090"). A short timeout keeps a slow/missing Prometheus
// from stalling the end-of-run report.
func newPromClient(baseURL string) *promClient {
	return &promClient{rc: restyutil.New(baseURL, restyutil.WithTimeout(10*time.Second))}
}

// rangeQueryResponse mirrors the subset of the query_range payload we read.
type rangeQueryResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// RangeQuery runs a PromQL range query and returns one promSeries per result.
func (c *promClient) RangeQuery(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]promSeries, error) {
	resp, err := c.rc.R().
		SetContext(ctx).
		SetQueryParams(map[string]string{
			"query": query,
			"start": strconv.FormatInt(start.Unix(), 10),
			"end":   strconv.FormatInt(end.Unix(), 10),
			"step":  strconv.FormatFloat(step.Seconds(), 'f', -1, 64),
		}).
		Get("/api/v1/query_range")
	if err != nil {
		return nil, fmt.Errorf("query prometheus: %w", err)
	}

	var parsed rangeQueryResponse
	if err := json.Unmarshal(resp.Body(), &parsed); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed: %s", parsed.Error)
	}

	out := make([]promSeries, 0, len(parsed.Data.Result))
	for _, r := range parsed.Data.Result {
		s := promSeries{Labels: r.Metric}
		for _, v := range r.Values {
			ts, ok := v[0].(float64)
			if !ok {
				continue
			}
			raw, ok := v[1].(string)
			if !ok {
				continue
			}
			val, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				continue
			}
			// Prometheus timestamps are float64 seconds; truncating to int64
			// loses at most 1s, acceptable at the attribution query step.
			s.Samples = append(s.Samples, promSample{T: time.Unix(int64(ts), 0).UTC(), V: val})
		}
		out = append(out, s)
	}
	return out, nil
}
