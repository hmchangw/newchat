package drive

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getImageFixture serves the signer and, when download is non-nil, the
// presigned download endpoint.
func getImageFixture(t *testing.T, signer, download http.HandlerFunc) *Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/{groupId}/files/{fileId}", signer)
	if download != nil {
		mux.HandleFunc("/download/", download)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return NewClient(&Config{URL: srv.URL, Token: "tok"})
}

func signerJSON(w http.ResponseWriter, status int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func TestClient_GetGroupImage_ErrNotFound(t *testing.T) {
	signOK := func(w http.ResponseWriter, r *http.Request) {
		signerJSON(w, http.StatusOK, map[string]string{"url": "http://" + r.Host + "/download/f1"})
	}
	tests := []struct {
		name         string
		signer       http.HandlerFunc
		download     http.HandlerFunc
		wantNotFound bool
	}{
		{
			name: "signer 404",
			signer: func(w http.ResponseWriter, _ *http.Request) {
				signerJSON(w, http.StatusNotFound, map[string]string{"error": "no such file"})
			},
			wantNotFound: true,
		},
		{
			name:         "storage 404",
			signer:       signOK,
			download:     func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
			wantNotFound: true,
		},
		{
			name: "signer 500",
			signer: func(w http.ResponseWriter, _ *http.Request) {
				signerJSON(w, http.StatusInternalServerError, map[string]string{"error": "boom"})
			},
		},
		{
			name:     "storage 500",
			signer:   signOK,
			download: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := getImageFixture(t, tt.signer, tt.download)
			_, err := c.GetGroupImage(c.GetBaseURL(), "r1", "f1")
			require.Error(t, err)
			assert.Equal(t, tt.wantNotFound, errors.Is(err, ErrNotFound))
		})
	}
}

func TestClient_GetGroupImage_SignerErrorKeepsStatus(t *testing.T) {
	c := getImageFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		signerJSON(w, http.StatusNotFound, map[string]string{"error": "no such file"})
	}, nil)
	_, err := c.GetGroupImage(c.GetBaseURL(), "r1", "f1")
	require.ErrorIs(t, err, ErrNotFound)
	assert.Contains(t, err.Error(), "status 404")
}

// A download transport failure must not carry the presigned URL: its query is
// a bearer credential and callers log the error.
func TestClient_GetGroupImage_DownloadFailureOmitsPresignedURL(t *testing.T) {
	// Reserve a port, then close it so the download connection is refused.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := l.Addr().String()
	require.NoError(t, l.Close())

	c := getImageFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		signerJSON(w, http.StatusOK, map[string]string{"url": "http://" + dead + "/download/f1?X-Signature=SECRETSIG&X-Expires=99"})
	}, nil)
	_, err = c.GetGroupImage(c.GetBaseURL(), "r1", "f1")
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "SECRETSIG")
	assert.NotContains(t, msg, "X-Signature")
	assert.False(t, strings.Contains(msg, "/download/f1"), "path of the presigned URL must not leak: %s", msg)
	assert.Contains(t, msg, "download image")
}
