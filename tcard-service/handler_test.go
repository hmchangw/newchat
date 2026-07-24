package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errtest"
)

// validCardJSON is a well-formed register body; helpers override one field to
// exercise the failure paths.
const validCardJSON = `{"path":"greetings/en/welcome","_tcardVersion":"1.0.0","type":"AdaptiveCard",` +
	`"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",` +
	`"body":[{"type":"TextBlock","text":"Hi"}]}`

func doJSON(t *testing.T, r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func setupRouter(t *testing.T, h *CardHandler) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// Minimal request-id propagation so the handler sees the inbound X-Request-ID
	// under the same key production ginutil.RequestID uses.
	r.Use(func(c *gin.Context) {
		if id := c.GetHeader("X-Request-ID"); id != "" {
			c.Set("request_id", id)
		}
		c.Next()
	})
	registerRoutes(r, h)
	return r
}

func doRequest(t *testing.T, r *gin.Engine, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

// cacheWith returns a card cache populated with the given cards.
func cacheWith(cards ...card) *cardCache {
	c := newCardCache()
	c.replace(cards)
	return c
}

// deepCard exercises slash-containing card paths (a/b/c@version).
var deepCard = card{
	Path: "greetings/en/welcome", CardVersion: "0.0.1",
	Template: json.RawMessage(`{"_tcardVersion":"0.0.1","title":"Welcome"}`),
}

func TestHandleGetTemplate_SlashPath(t *testing.T) {
	r := setupRouter(t, NewCardHandler(cacheWith(deepCard), nil))

	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings/en/welcome@0.0.1.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, string(deepCard.Template), w.Body.String())

	miss := doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings/en/welcome@9.9.9.template.json")
	require.Equal(t, http.StatusNotFound, miss.Code)
	errtest.AssertCode(t, miss.Body.Bytes(), errcode.CodeNotFound)
}

func TestHandleRefresh_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard}, nil)

	cache := newCardCache()
	r := setupRouter(t, NewCardHandler(cache, store))

	w := doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusOK, w.Code)
	var resp refreshResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, refreshResponse{Status: "ok", Count: 2}, resp)

	// The refreshed docs are immediately servable.
	got := doRequest(t, r, http.MethodGet, "/api/v1/cards/home@v1.template.json")
	require.Equal(t, http.StatusOK, got.Code)
	assert.JSONEq(t, string(homeCard.Template), got.Body.String())
}

// Refresh is POST-only: a GET falls through to the wildcard route and is a
// listing lookup for the unknown prefix "refresh".
func TestHandleRefresh_GetIsNotRouted(t *testing.T) {
	r := setupRouter(t, NewCardHandler(cacheWith(), nil))
	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusNotFound, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeNotFound)
}

func TestHandleRefresh_PicksUpNewDocs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard}, nil),
		store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard}, nil),
	)

	cache := newCardCache()
	r := setupRouter(t, NewCardHandler(cache, store))

	require.Equal(t, http.StatusOK, doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)
	miss := doRequest(t, r, http.MethodGet, "/api/v1/cards/profile@v2.template.json")
	require.Equal(t, http.StatusNotFound, miss.Code)

	// A doc inserted into Mongo since the last refresh appears after the next one.
	w := doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusOK, w.Code)
	var resp refreshResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 2, resp.Count)

	hit := doRequest(t, r, http.MethodGet, "/api/v1/cards/profile@v2.template.json")
	require.Equal(t, http.StatusOK, hit.Code)
	assert.JSONEq(t, string(profileCard.Template), hit.Body.String())
}

func TestHandleRefresh_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return(nil, errors.New("mongo down"))

	r := setupRouter(t, NewCardHandler(newCardCache(), store))

	w := doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusInternalServerError, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeInternal)
}

func TestHandleRefresh_StoreErrorKeepsServingPrevious(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return(nil, errors.New("mongo down"))

	r := setupRouter(t, NewCardHandler(cacheWith(homeCard), store))

	require.Equal(t, http.StatusInternalServerError,
		doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)

	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/home@v1.template.json")
	require.Equal(t, http.StatusOK, w.Code, "a failed refresh must keep serving the previous snapshot")
	assert.JSONEq(t, string(homeCard.Template), w.Body.String())
}

func TestHandleGetTemplate_HappyPath(t *testing.T) {
	r := setupRouter(t, NewCardHandler(cacheWith(homeCard, profileCard), nil))

	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/home@v1.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
	assert.JSONEq(t, string(homeCard.Template), w.Body.String())
}

func TestHandleGetTemplate_Errors(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantCode int
		wantErr  errcode.Code
	}{
		{
			name:     "unknown (path, cardVersion) is not found",
			target:   "/api/v1/cards/missing@v9.template.json",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
		},
		{
			name:     "known path with unknown version is not found",
			target:   "/api/v1/cards/home@v9.template.json",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
		},
		{
			name:     "filename without the .template.json suffix is a listing miss",
			target:   "/api/v1/cards/home@v1.json",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
		},
		{
			name:     "missing version (no @) is a bad request",
			target:   "/api/v1/cards/home.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "empty path is a bad request",
			target:   "/api/v1/cards/@v1.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "empty version is a bad request",
			target:   "/api/v1/cards/home@.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "empty stem is a bad request",
			target:   "/api/v1/cards/.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupRouter(t, NewCardHandler(cacheWith(homeCard), nil))
			w := doRequest(t, r, http.MethodGet, tt.target)
			require.Equal(t, tt.wantCode, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), tt.wantErr)
		})
	}
}

func TestHandleGetTemplate_NotReadyIsNotFound(t *testing.T) {
	// Before the first load the cache resolves nothing; /readyz is the
	// signal for "not loaded yet", the template route just misses.
	r := setupRouter(t, NewCardHandler(newCardCache(), nil))
	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/home@v1.template.json")
	require.Equal(t, http.StatusNotFound, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeNotFound)
}

func TestHandleHealth(t *testing.T) {
	r := setupRouter(t, NewCardHandler(newCardCache(), nil))
	w := doRequest(t, r, http.MethodGet, "/healthz")
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleReady(t *testing.T) {
	t.Run("unready until first load", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(newCardCache(), nil))
		w := doRequest(t, r, http.MethodGet, "/readyz")
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})

	t.Run("ready after a load, even an empty one", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(cacheWith(), nil))
		w := doRequest(t, r, http.MethodGet, "/readyz")
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func listRouter(t *testing.T) *gin.Engine {
	t.Helper()
	return setupRouter(t, NewCardHandler(cacheWith(
		card{Path: "a/b/c", CardVersion: "0.0.1", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.2", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/d", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "a/x/y", CardVersion: "2.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "z/w/v", CardVersion: "1.2.3", Template: json.RawMessage(`{}`)},
	), nil))
}

func TestHandleList(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantBody string
	}{
		{
			name:     "root lists top-level folders",
			target:   "/api/v1/cards/",
			wantBody: `{"statusCode":200,"cards":[],"folders":["a","z"]}`,
		},
		{
			name:     "one segment lists subfolders",
			target:   "/api/v1/cards/a",
			wantBody: `{"statusCode":200,"cards":[],"folders":["a/b","a/x"]}`,
		},
		{
			name:     "trailing slash is normalized",
			target:   "/api/v1/cards/a/",
			wantBody: `{"statusCode":200,"cards":[],"folders":["a/b","a/x"]}`,
		},
		{
			name:     "two segments list cards with versions",
			target:   "/api/v1/cards/a/b",
			wantBody: `{"statusCode":200,"cards":["a/b/c@0.0.1","a/b/c@0.0.2","a/b/d@1.0.0"],"folders":[]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(t, listRouter(t), http.MethodGet, tt.target)
			require.Equal(t, http.StatusOK, w.Code)
			assert.JSONEq(t, tt.wantBody, w.Body.String())
		})
	}
}

func TestHandleList_Errors(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantCode int
		wantErr  errcode.Code
		wantMsg  string
	}{
		{
			name:     "full card path without version is a bad request",
			target:   "/api/v1/cards/a/b/c",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
			wantMsg:  `no version specified for card "a/b/c"`,
		},
		{
			name:     "unknown prefix",
			target:   "/api/v1/cards/does/not/exist",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
			wantMsg:  `given path "does/not/exist" for card list not found`,
		},
		{
			name:     "version without .template.json suffix is not a listing match",
			target:   "/api/v1/cards/a/b/c@0.0.1",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
			wantMsg:  `given path "a/b/c@0.0.1" for card list not found`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(t, listRouter(t), http.MethodGet, tt.target)
			require.Equal(t, tt.wantCode, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), tt.wantErr)
			// Assert against the decoded envelope message: the raw JSON body
			// escapes the quotes the message carries around the prefix.
			assert.Contains(t, errtest.Decode(t, w.Body.Bytes()).Message, tt.wantMsg)
		})
	}
}

func TestHandleList_CacheStates(t *testing.T) {
	t.Run("loaded but empty cache lists root as empty 200", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(cacheWith(), nil))
		w := doRequest(t, r, http.MethodGet, "/api/v1/cards/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.JSONEq(t, `{"statusCode":200,"cards":[],"folders":[]}`, w.Body.String())
	})

	t.Run("never-loaded cache is 404 at root", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(newCardCache(), nil))
		w := doRequest(t, r, http.MethodGet, "/api/v1/cards/")
		require.Equal(t, http.StatusNotFound, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeNotFound)
		assert.Contains(t, errtest.Decode(t, w.Body.Bytes()).Message, "no paths or cards exist")
	})

	t.Run("never-loaded cache is 404 for any prefix", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(newCardCache(), nil))
		w := doRequest(t, r, http.MethodGet, "/api/v1/cards/a/b")
		require.Equal(t, http.StatusNotFound, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeNotFound)
		assert.Contains(t, errtest.Decode(t, w.Body.Bytes()).Message, "no paths or cards exist")
	})
}

func TestHandleRegister_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListVersions(gomock.Any(), "greetings/en/welcome").Return(nil, nil)
	store.EXPECT().InsertCard(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetCard(gomock.Any(), "greetings/en/welcome", "1.0.0").
		Return(card{Path: "greetings/en/welcome", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)}, true, nil)

	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", validCardJSON)
	require.Equal(t, http.StatusCreated, w.Code)
	assert.JSONEq(t, `{"success":true}`, w.Body.String())
}

func TestHandleRegister_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":""`, 1)},
		{name: "single-segment path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"welcome"`, 1)},
		{name: "two-segment path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"greetings/welcome"`, 1)},
		{name: "four-segment path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a/b/c/d"`, 1)},
		{name: "empty path segment", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a//c"`, 1)},
		{name: "leading slash", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"/a/b"`, 1)},
		{name: "trailing slash", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a/b/"`, 1)},
		{name: "path with @", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a/b/c@d"`, 1)},
		{name: "missing body", body: strings.Replace(validCardJSON, `"body":[{"type":"TextBlock","text":"Hi"}]`, `"body":null`, 1)},
		{name: "body not an array", body: strings.Replace(validCardJSON, `"body":[{"type":"TextBlock","text":"Hi"}]`, `"body":{"x":1}`, 1)},
		{name: "body empty array", body: strings.Replace(validCardJSON, `"body":[{"type":"TextBlock","text":"Hi"}]`, `"body":[]`, 1)},
		{name: "non-semver _tcardVersion", body: strings.Replace(validCardJSON, `"_tcardVersion":"1.0.0"`, `"_tcardVersion":"1.0"`, 1)},
		{name: "leading-zero _tcardVersion", body: strings.Replace(validCardJSON, `"_tcardVersion":"1.0.0"`, `"_tcardVersion":"1.0.01"`, 1)},
		{name: "legacy cardVersion field is not accepted", body: strings.Replace(validCardJSON, `"_tcardVersion"`, `"cardVersion"`, 1)},
		{name: "wrong type", body: strings.Replace(validCardJSON, `"type":"AdaptiveCard"`, `"type":"Hero"`, 1)},
		{name: "wrong schema", body: strings.Replace(validCardJSON, `adaptive-card.json`, `wrong.json`, 1)},
		{name: "wrong version", body: strings.Replace(validCardJSON, `"version":"1.5"`, `"version":"1.4"`, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockCardStore(ctrl) // validation fails before any store call
			r := setupRouter(t, NewCardHandler(newCardCache(), store))
			w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", tt.body)
			require.Equal(t, http.StatusBadRequest, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		})
	}
}

func TestHandleRegister_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", `{not json`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
}

func TestHandleRegister_NotHighest(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListVersions(gomock.Any(), "greetings/en/welcome").Return([]string{"2.0.0"}, nil)

	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", validCardJSON)
	require.Equal(t, http.StatusConflict, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeConflict)
}

func TestHandleRegister_Duplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListVersions(gomock.Any(), "greetings/en/welcome").Return([]string{"0.9.0"}, nil)
	store.EXPECT().InsertCard(gomock.Any(), gomock.Any()).Return(ErrDuplicateCard)

	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", validCardJSON)
	require.Equal(t, http.StatusConflict, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeConflict)
}

// The insert succeeded; a failure fetching it back for the cache must not fail
// the request — the card is persisted and appears on the next refresh.
func TestHandleRegister_CacheAddFailureStill201(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListVersions(gomock.Any(), "greetings/en/welcome").Return(nil, nil)
	store.EXPECT().InsertCard(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetCard(gomock.Any(), "greetings/en/welcome", "1.0.0").Return(card{}, false, errors.New("mongo down"))

	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", validCardJSON)
	require.Equal(t, http.StatusCreated, w.Code)
	assert.JSONEq(t, `{"success":true}`, w.Body.String())
}
