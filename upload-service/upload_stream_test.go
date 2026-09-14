package main

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/testutil"
)

// uploadStack stands up the real router against a stub Drive backend that drains
// whatever it is sent, and returns the live upload URL.
func uploadStack(t *testing.T) string {
	t.Helper()
	driveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"status":"success","object":{"objectId":"f1","fileSize":1}}]`)
	}))
	t.Cleanup(driveSrv.Close)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().MemberSiteID(gomock.Any(), gomock.Any(), gomock.Any()).Return("site1", true, nil).AnyTimes()

	h := NewHandler(store, drive.NewClient(&drive.Config{URL: driveSrv.URL, Token: "tok"}), &fakeS3{},
		0, 1, 0, 200<<20, newMediaTypeFilter("", "image/svg+xml"), testCacheMaxAge, true, &fakeDrive{})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	// The same constant main.go wires in; Gin's own 32 MiB default is what keeps
	// a whole upload resident before the handler ever runs.
	r.MaxMultipartMemory = maxMultipartMemory
	registerRoutes(r, h, authDeps{devMode: true})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv.URL + "/api/v1/file/rooms/r1/upload/file"
}

// postStreamed sends a multipart upload whose body is produced lazily, so the
// client side never holds the payload and the measured heap is the server's.
func postStreamed(t *testing.T, url, filename, contentType string, content io.Reader) *http.Response {
	t.Helper()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		hdr := make(textproto.MIMEHeader)
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
		hdr.Set("Content-Type", contentType)
		part, err := mw.CreatePart(hdr)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(part, content); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = mw.Close()
		_ = pw.Close()
	}()

	req, err := http.NewRequest(http.MethodPost, url, pr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("ssoToken", "alice")

	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	require.NoError(t, err)
	return resp
}

// A large non-image upload must reach Drive without ever being held in memory:
// buffering it is what OOMs the service under a handful of concurrent uploads.
func TestHandleUploadFile_StreamsLargeFileWithoutBuffering(t *testing.T) {
	const fileSize = 100 << 20 // FILE_UPLOAD_MAX_FILE_SIZE default
	// Both layers stream now: Gin spills the part to a temp file past its
	// (configured) 1 MiB threshold and the Drive client never copies it. A peak
	// anywhere near fileSize means one of them regressed to buffering.
	const maxPeak = 16 << 20

	url := uploadStack(t)

	var status string
	peak := testutil.PeakHeapDuring(func() {
		resp := postStreamed(t, url, "big.mp4", "application/octet-stream", &testutil.ZeroReader{N: fileSize})
		status = resp.Status
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	})

	require.Equal(t, "200 OK", status)
	require.LessOrEqualf(t, peak, uint64(maxPeak),
		"peak heap %d MiB for a %d MiB upload: the body is being buffered", peak>>20, fileSize>>20)
	t.Logf("non-image: uploaded %d MiB with a %d MiB peak heap", fileSize>>20, peak>>20)
}
