//go:build integration

package main

// Integration tests for search.users (NATS + httptest stub for HR endpoint).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/subject"
)

type usersFixture struct {
	clientNATS *nats.Conn
	thirdParty *httptest.Server
}

func setupUsersFixture(t *testing.T, thirdPartyHandler http.Handler) *usersFixture {
	t.Helper()
	stub := httptest.NewServer(thirdPartyHandler)
	t.Cleanup(stub.Close)

	usersRC := restyutil.New(stub.URL, restyutil.WithTimeout(5*time.Second))
	h := newHandler(nil, nil, newHTTPUsersClient(usersRC, ""), newFakeCache(), handlerConfig{
		DocCounts:      25,
		MaxDocCounts:   100,
		RequestTimeout: 5 * time.Second,
	})
	clientNC := setupRouter(t, testQueueGroup, h.Register)
	return &usersFixture{clientNATS: clientNC, thirdParty: stub}
}

func TestIntegration_SearchUsers_Happy(t *testing.T) {
	stubResp := `[{"account":"alice","engName":"Alice Wang"},{"account":"alice2","engName":"Alice Chen"}]`

	f := setupUsersFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(stubResp))
	}))

	reqBytes, err := json.Marshal(model.SearchUsersRequest{Query: "alice"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchUsers("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var users []model.SearchUser
	require.NoError(t, json.Unmarshal(msg.Data, &users))

	require.Len(t, users, 2)
	assert.Equal(t, "alice", users[0].Account)
	assert.Equal(t, "Alice Wang", users[0].EngName)
}

func TestIntegration_SearchUsers_EmptyQueryReturnsBadRequest(t *testing.T) {
	// Stub should never be called for a bad-request scenario.
	f := setupUsersFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("third-party stub should not be called for empty query")
		w.WriteHeader(http.StatusInternalServerError)
	}))

	reqBytes, err := json.Marshal(model.SearchUsersRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchUsers("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}

func TestIntegration_SearchUsers_ThirdPartyErrorReturnsInternal(t *testing.T) {
	f := setupUsersFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	reqBytes, err := json.Marshal(model.SearchUsersRequest{Query: "alice"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchUsers("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeInternal, envelope.Code,
		"non-2xx from third-party must surface as internal error, not raw status")
	// Raw third-party details must not leak to the caller.
	assert.NotContains(t, envelope.Error, "503", "status code from third-party must not leak")
}
