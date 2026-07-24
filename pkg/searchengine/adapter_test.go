package searchengine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTransport struct {
	handler func(req *http.Request) (*http.Response, error)
}

func (f *fakeTransport) Perform(req *http.Request) (*http.Response, error) {
	return f.handler(req)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

func TestAdapter_Ping(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodGet, req.Method)
			assert.Equal(t, "/", req.URL.Path)
			return jsonResponse(200, `{}`), nil
		}}
		a := newAdapter(ft)
		err := a.Ping(context.Background())
		assert.NoError(t, err)
	})

	t.Run("server error", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(503, `{}`), nil
		}}
		a := newAdapter(ft)
		err := a.Ping(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "503")
	})
}

func TestAdapter_Bulk(t *testing.T) {
	t.Run("index and delete actions", func(t *testing.T) {
		var capturedBody string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, "/_bulk", req.URL.Path)
			assert.Equal(t, "application/x-ndjson", req.Header.Get("Content-Type"))
			body, _ := io.ReadAll(req.Body)
			capturedBody = string(body)
			return jsonResponse(200, `{
				"items": [
					{"index": {"status": 201}},
					{"delete": {"status": 200}}
				]
			}`), nil
		}}
		a := newAdapter(ft)
		actions := []BulkAction{
			{Action: ActionIndex, Index: "msgs-2026-01", DocID: "m1", Version: 100, Doc: json.RawMessage(`{"msg":"hello"}`)},
			{Action: ActionDelete, Index: "msgs-2026-01", DocID: "m2", Version: 200},
		}
		results, err := a.Bulk(context.Background(), actions)
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, 201, results[0].Status)
		assert.Equal(t, 200, results[1].Status)

		lines := strings.Split(strings.TrimSpace(capturedBody), "\n")
		assert.Len(t, lines, 3) // index meta + doc + delete meta

		var indexMeta map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &indexMeta))
		idx := indexMeta["index"].(map[string]any)
		assert.Equal(t, "msgs-2026-01", idx["_index"])
		assert.Equal(t, "m1", idx["_id"])
		assert.Equal(t, "external", idx["version_type"])
		assert.Equal(t, float64(100), idx["version"])

		var deleteMeta map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[2]), &deleteMeta))
		del := deleteMeta["delete"].(map[string]any)
		assert.Equal(t, "m2", del["_id"])
	})

	t.Run("update action uses update meta without version", func(t *testing.T) {
		var capturedBody string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			capturedBody = string(body)
			return jsonResponse(200, `{"items":[{"update":{"status":200}}]}`), nil
		}}
		a := newAdapter(ft)
		updateBody := json.RawMessage(`{"script":{"source":"ctx._source.rooms.add(params.rid)","params":{"rid":"r1"}},"upsert":{"userAccount":"alice","rooms":["r1"]}}`)
		results, err := a.Bulk(context.Background(), []BulkAction{
			{Action: ActionUpdate, Index: "user-room-site1", DocID: "alice", Doc: updateBody},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, 200, results[0].Status)

		lines := strings.Split(strings.TrimSpace(capturedBody), "\n")
		require.Len(t, lines, 2)

		var updateMeta map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &updateMeta))
		upd := updateMeta["update"].(map[string]any)
		assert.Equal(t, "user-room-site1", upd["_index"])
		assert.Equal(t, "alice", upd["_id"])
		// external versioning must NOT be set on update actions
		assert.NotContains(t, upd, "version")
		assert.NotContains(t, upd, "version_type")

		assert.JSONEq(t, string(updateBody), lines[1])
	})

	t.Run("version conflict treated as result not error", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(200, `{
				"items": [{"index": {"status": 409, "error": {"type": "version_conflict_engine_exception", "reason": "stale"}}}]
			}`), nil
		}}
		a := newAdapter(ft)
		results, err := a.Bulk(context.Background(), []BulkAction{
			{Action: ActionIndex, Index: "idx", DocID: "m1", Version: 1, Doc: json.RawMessage(`{}`)},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, 409, results[0].Status)
		assert.Equal(t, "version_conflict_engine_exception", results[0].ErrorType)
		assert.Equal(t, "stale", results[0].Error)
	})

	t.Run("bulk error types propagate (document_missing, index_not_found)", func(t *testing.T) {
		// Exercises both benign and fatal 404 shapes so the handler's
		// isBulkItemSuccess has a real ErrorType to key on.
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(200, `{
				"items": [
					{"update": {"status": 404, "error": {"type": "document_missing_exception", "reason": "[alice]: document missing"}}},
					{"update": {"status": 404, "error": {"type": "index_not_found_exception", "reason": "no such index [user-room-site-a]"}}}
				]
			}`), nil
		}}
		a := newAdapter(ft)
		results, err := a.Bulk(context.Background(), []BulkAction{
			{Action: ActionUpdate, Index: "user-room-site-a", DocID: "alice", Doc: json.RawMessage(`{}`)},
			{Action: ActionUpdate, Index: "user-room-site-a", DocID: "bob", Doc: json.RawMessage(`{}`)},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)

		assert.Equal(t, 404, results[0].Status)
		assert.Equal(t, "document_missing_exception", results[0].ErrorType)

		assert.Equal(t, 404, results[1].Status)
		assert.Equal(t, "index_not_found_exception", results[1].ErrorType)
	})

	t.Run("HTTP error returns error", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(503, `service unavailable`), nil
		}}
		a := newAdapter(ft)
		_, err := a.Bulk(context.Background(), []BulkAction{
			{Action: ActionIndex, Index: "idx", DocID: "m1", Version: 1, Doc: json.RawMessage(`{}`)},
		})
		assert.Error(t, err)
	})
}

func TestAdapter_UpsertTemplate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPut, req.Method)
			assert.Equal(t, "/_index_template/my_template", req.URL.Path)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			body, _ := io.ReadAll(req.Body)
			assert.JSONEq(t, `{"index_patterns":["test-*"]}`, string(body))
			return jsonResponse(200, `{"acknowledged": true}`), nil
		}}
		a := newAdapter(ft)
		err := a.UpsertTemplate(context.Background(), "my_template", json.RawMessage(`{"index_patterns":["test-*"]}`))
		assert.NoError(t, err)
	})

	t.Run("error status", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(400, `{"error":"bad request"}`), nil
		}}
		a := newAdapter(ft)
		err := a.UpsertTemplate(context.Background(), "t", json.RawMessage(`{}`))
		assert.Error(t, err)
	})
}

func TestAdapter_PutScript(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedBody string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPut, req.Method)
			assert.Equal(t, "/_scripts/user_room_add", req.URL.Path)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			body, _ := io.ReadAll(req.Body)
			capturedBody = string(body)
			return jsonResponse(200, `{"acknowledged": true}`), nil
		}}
		a := newAdapter(ft)
		body := json.RawMessage(`{"script":{"lang":"painless","source":"ctx.op='none'"}}`)
		err := a.PutScript(context.Background(), "user_room_add", body)
		require.NoError(t, err)
		assert.JSONEq(t, string(body), capturedBody)
	})

	t.Run("error status", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(400, `{"error":"bad script"}`), nil
		}}
		a := newAdapter(ft)
		err := a.PutScript(context.Background(), "bad", json.RawMessage(`{}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "400")
	})
}

func TestAdapter_UpdateMapping(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedBody string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPut, req.Method)
			assert.Equal(t, "/messages-site1-*/_mapping", req.URL.Path)
			assert.Equal(t, "true", req.URL.Query().Get("allow_no_indices"))
			assert.Equal(t, "true", req.URL.Query().Get("ignore_unavailable"))
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			body, _ := io.ReadAll(req.Body)
			capturedBody = string(body)
			return jsonResponse(200, `{"acknowledged": true}`), nil
		}}
		a := newAdapter(ft)
		body := json.RawMessage(`{"properties":{"cardData":{"type":"text"}}}`)
		err := a.UpdateMapping(context.Background(), "messages-site1-*", body)
		require.NoError(t, err)
		assert.JSONEq(t, string(body), capturedBody)
	})

	t.Run("error status", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(400, `{"error":"mapper_parsing_exception"}`), nil
		}}
		a := newAdapter(ft)
		err := a.UpdateMapping(context.Background(), "messages-*", json.RawMessage(`{}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "400")
	})

	t.Run("empty pattern rejected", func(t *testing.T) {
		a := newAdapter(&fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			t.Fatal("no request expected for empty pattern")
			return nil, nil
		}})
		err := a.UpdateMapping(context.Background(), "", json.RawMessage(`{}`))
		assert.Error(t, err)
	})
}

func TestAdapter_Search(t *testing.T) {
	t.Run("single index", func(t *testing.T) {
		var capturedPath, capturedMethod, capturedBody string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			capturedMethod = req.Method
			capturedPath = req.URL.Path
			capturedBody, _ = func() (string, error) {
				b, err := io.ReadAll(req.Body)
				return string(b), err
			}()
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			assert.Equal(t, "true", req.URL.Query().Get("ignore_unavailable"))
			assert.Equal(t, "true", req.URL.Query().Get("allow_no_indices"))
			return jsonResponse(200, `{"hits":{"total":{"value":0},"hits":[]}}`), nil
		}}
		a := newAdapter(ft)
		body := json.RawMessage(`{"query":{"match_all":{}}}`)
		raw, err := a.Search(context.Background(), []string{"spotlight"}, body)
		require.NoError(t, err)
		assert.Equal(t, http.MethodPost, capturedMethod)
		assert.Equal(t, "/spotlight/_search", capturedPath)
		assert.JSONEq(t, string(body), capturedBody)
		assert.Contains(t, string(raw), `"hits"`)
	})

	t.Run("multiple indices joined with comma", func(t *testing.T) {
		var capturedPath string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			capturedPath = req.URL.Path
			return jsonResponse(200, `{"hits":{"total":{"value":0},"hits":[]}}`), nil
		}}
		a := newAdapter(ft)
		_, err := a.Search(context.Background(), []string{"messages-*", "*:messages-*"}, json.RawMessage(`{}`))
		require.NoError(t, err)
		assert.Equal(t, "/messages-*,*:messages-*/_search", capturedPath)
	})

	t.Run("empty indices returns error", func(t *testing.T) {
		a := newAdapter(&fakeTransport{})
		_, err := a.Search(context.Background(), nil, json.RawMessage(`{}`))
		assert.Error(t, err)
	})

	t.Run("ES error status", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(500, `{"error":"boom"}`), nil
		}}
		a := newAdapter(ft)
		_, err := a.Search(context.Background(), []string{"spotlight"}, json.RawMessage(`{}`))
		assert.Error(t, err)
	})
}

func TestAdapter_GetDoc(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodGet, req.Method)
			assert.Equal(t, "/user-room/_doc/alice", req.URL.Path)
			return jsonResponse(200, `{"_index":"user-room","_id":"alice","found":true,"_source":{"userAccount":"alice","rooms":["r1"]}}`), nil
		}}
		a := newAdapter(ft)
		raw, found, err := a.GetDoc(context.Background(), "user-room", "alice")
		require.NoError(t, err)
		require.True(t, found)
		assert.Contains(t, string(raw), `"userAccount":"alice"`)
	})

	t.Run("not found via 404", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(404, `{"_index":"user-room","_id":"alice","found":false}`), nil
		}}
		a := newAdapter(ft)
		raw, found, err := a.GetDoc(context.Background(), "user-room", "alice")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, raw)
	})

	t.Run("server error", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(503, `{}`), nil
		}}
		a := newAdapter(ft)
		_, _, err := a.GetDoc(context.Background(), "user-room", "alice")
		assert.Error(t, err)
	})
}

func TestAdapter_GetIndexMapping(t *testing.T) {
	t.Run("index exists", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodGet, req.Method)
			assert.Equal(t, "/my-index/_mapping", req.URL.Path)
			return jsonResponse(200, `{"my-index":{"mappings":{"properties":{"msg":{"type":"text"}}}}}`), nil
		}}
		a := newAdapter(ft)
		mapping, err := a.GetIndexMapping(context.Background(), "my-index")
		require.NoError(t, err)
		require.NotNil(t, mapping)
		assert.Contains(t, string(mapping), "msg")
	})

	t.Run("index does not exist", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(404, `{}`), nil
		}}
		a := newAdapter(ft)
		mapping, err := a.GetIndexMapping(context.Background(), "missing")
		require.NoError(t, err)
		assert.Nil(t, mapping)
	})
}
