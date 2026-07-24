package searchengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
)

type bulkActionMeta struct {
	Index       string `json:"_index"`
	ID          string `json:"_id"`
	Version     int64  `json:"version,omitempty"`
	VersionType string `json:"version_type,omitempty"`
}

type bulkResponse struct {
	Items []map[string]bulkItemResult `json:"items"`
}

type bulkItemResult struct {
	Status int `json:"status"`
	Error  struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

type httpAdapter struct {
	transport Transporter
	// instr drives the elastic-transport OpenTelemetry hooks so the adapter's
	// raw requests produce the same ES-semantic spans the generated esapi
	// methods do (db.system=elasticsearch, db.operation, path_parts, span name
	// "elasticsearch.{op} {index}"). nil when the transport is not an
	// instrumented Elasticsearch client — an un-instrumented ES client, or an
	// OpenSearch client that does not implement elastictransport.Instrumented —
	// in which case every operation falls back to a plain Perform. This keeps
	// the backend-agnostic Transporter seam intact (CLAUDE.md observability).
	instr elastictransport.Instrumentation
}

func newAdapter(transport Transporter) *httpAdapter {
	a := &httpAdapter{transport: transport}
	// InstrumentationEnabled returns nil when the client was built without
	// instrumentation, so an un-instrumented ES client also takes the no-op path.
	if it, ok := transport.(elastictransport.Instrumented); ok {
		a.instr = it.InstrumentationEnabled()
	}
	return a
}

// do drives the elastic-transport OTel instrumentation around a raw request the
// same way a generated esapi method does: Start (span, before the request is
// built so its context is span-aware) → RecordPathPart → BeforeRequest →
// Perform (which calls AfterResponse internally) → AfterRequest → Close. When
// instrumentation is absent it builds and performs the request directly, so the
// span path is opt-in and zero-overhead for OpenSearch and un-instrumented
// clients.
//
// endpoint is the ES API id used for the span name and db.operation (e.g.
// "search", "bulk"); index is the target for db.elasticsearch.path_parts.index
// ("" to omit, e.g. _bulk and cluster-level ops). build constructs the request
// with the span-aware ctx so the response hook can resolve the span.
func (a *httpAdapter) do(ctx context.Context, endpoint, index string, build func(ctx context.Context) (*http.Request, error)) (*http.Response, error) {
	if a.instr == nil {
		req, err := build(ctx)
		if err != nil {
			return nil, err
		}
		return a.transport.Perform(req)
	}

	ctx = a.instr.Start(ctx, endpoint)
	defer a.instr.Close(ctx)

	req, err := build(ctx)
	if err != nil {
		a.instr.RecordError(ctx, err)
		return nil, err
	}
	if index != "" {
		a.instr.RecordPathPart(ctx, "index", index)
	}
	a.instr.BeforeRequest(req, endpoint)

	resp, err := a.transport.Perform(req)
	a.instr.AfterRequest(req, "elasticsearch", endpoint)
	if err != nil {
		a.instr.RecordError(ctx, err)
		return nil, err
	}
	return resp, nil
}

func (a *httpAdapter) Ping(ctx context.Context) error {
	resp, err := a.do(ctx, "ping", "", func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	})
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (a *httpAdapter) Bulk(ctx context.Context, actions []BulkAction) ([]BulkResult, error) {
	var buf bytes.Buffer
	for _, action := range actions {
		meta := bulkActionMeta{
			Index:       action.Index,
			ID:          action.DocID,
			Version:     action.Version,
			VersionType: "external",
		}
		switch action.Action {
		case ActionIndex:
			line, _ := json.Marshal(map[string]bulkActionMeta{"index": meta})
			buf.Write(line)
			buf.WriteByte('\n')
			buf.Write(action.Doc)
			buf.WriteByte('\n')
		case ActionDelete:
			line, _ := json.Marshal(map[string]bulkActionMeta{"delete": meta})
			buf.Write(line)
			buf.WriteByte('\n')
		case ActionUpdate:
			// ES 8.x / OpenSearch 2.x bulk _update DOES accept version +
			// version_type=external. We intentionally omit them because
			// user-room scripted updates already enforce ordering via the
			// painless LWW guard (`params.ts > stored` against the stored
			// roomTimestamps), so layering external versioning on top would
			// be redundant and complicate 409-handling semantics. Omit Version
			// even when the caller sets it.
			updateMeta := bulkActionMeta{Index: action.Index, ID: action.DocID}
			line, _ := json.Marshal(map[string]bulkActionMeta{"update": updateMeta})
			buf.Write(line)
			buf.WriteByte('\n')
			buf.Write(action.Doc)
			buf.WriteByte('\n')
		}
	}

	resp, err := a.do(ctx, "bulk", "", func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_bulk", &buf)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		return req, nil
	})
	if err != nil {
		return nil, fmt.Errorf("bulk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("bulk: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("bulk: status %d, body: %s", resp.StatusCode, body)
	}

	var bulkResp bulkResponse
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		return nil, fmt.Errorf("decode bulk response: %w", err)
	}

	results := make([]BulkResult, len(bulkResp.Items))
	for i, item := range bulkResp.Items {
		for _, detail := range item {
			results[i] = BulkResult{
				Status:    detail.Status,
				ErrorType: detail.Error.Type,
				Error:     detail.Error.Reason,
			}
		}
	}
	return results, nil
}

func (a *httpAdapter) UpsertTemplate(ctx context.Context, name string, body json.RawMessage) error {
	resp, err := a.do(ctx, "indices.put_index_template", "", func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("/_index_template/%s", name), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return fmt.Errorf("upsert template: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("upsert template: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("upsert template: status %d, body: %s", resp.StatusCode, respBody)
	}
	return nil
}

// UpdateMapping PUTs an additive mapping onto every index matching pattern
// (templates apply only at creation); a no-op on a fresh cluster.
func (a *httpAdapter) UpdateMapping(ctx context.Context, indexPattern string, body json.RawMessage) error {
	if indexPattern == "" {
		return fmt.Errorf("update mapping: index pattern required")
	}
	path := fmt.Sprintf("/%s/_mapping?allow_no_indices=true&ignore_unavailable=true", indexPattern)
	resp, err := a.do(ctx, "indices.put_mapping", indexPattern, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return fmt.Errorf("update mapping: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("update mapping: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("update mapping: status %d, body: %s", resp.StatusCode, respBody)
	}
	return nil
}

func (a *httpAdapter) PutScript(ctx context.Context, id string, body json.RawMessage) error {
	resp, err := a.do(ctx, "put_script", "", func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("/_scripts/%s", id), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return fmt.Errorf("put script: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("put script: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("put script: status %d, body: %s", resp.StatusCode, respBody)
	}
	return nil
}

func (a *httpAdapter) GetIndexMapping(ctx context.Context, index string) (json.RawMessage, error) {
	resp, err := a.do(ctx, "indices.get_mapping", index, func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("/%s/_mapping", index), nil)
	})
	if err != nil {
		return nil, fmt.Errorf("get index mapping: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("get index mapping: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("get index mapping: status %d, body: %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read mapping response: %w", err)
	}
	return body, nil
}

// Search executes a `_search` against the comma-joined indices with
// `ignore_unavailable=true&allow_no_indices=true` so unknown remote clusters
// and missing local indices return empty hits rather than a 404/503.
func (a *httpAdapter) Search(ctx context.Context, indices []string, body json.RawMessage) (json.RawMessage, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("search: indices required")
	}
	path := fmt.Sprintf("/%s/_search?ignore_unavailable=true&allow_no_indices=true", strings.Join(indices, ","))
	resp, err := a.do(ctx, "search", strings.Join(indices, ","), func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return nil, fmt.Errorf("search request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("search: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("search: status %d, body: %s", resp.StatusCode, respBody)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read search response: %w", err)
	}
	return data, nil
}

// GetDoc fetches a single document by id. Returns (nil, false, nil) on 404.
//
// Both `index` and `docID` are URL-path-escaped defensively — ES doc IDs
// can legally contain `/`, `?`, `#`, or whitespace, and an un-escaped
// value there would malform the request path. The caller is expected to
// pass a single index name (not a comma-joined pattern) for this endpoint.
func (a *httpAdapter) GetDoc(ctx context.Context, index, docID string) (json.RawMessage, bool, error) {
	path := fmt.Sprintf("/%s/_doc/%s", url.PathEscape(index), url.PathEscape(docID))
	resp, err := a.do(ctx, "get", index, func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	})
	if err != nil {
		return nil, false, fmt.Errorf("get-doc request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, false, fmt.Errorf("get-doc: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return nil, false, fmt.Errorf("get-doc: status %d, body: %s", resp.StatusCode, respBody)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read get-doc response: %w", err)
	}
	return data, true, nil
}
