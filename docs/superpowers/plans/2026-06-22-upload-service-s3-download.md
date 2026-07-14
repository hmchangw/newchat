# upload-service: backward-compatible MinIO/S3 file download — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GET /api/v1/file-upload/:fileId/:fileName` to `upload-service`, which looks up a legacy upload's metadata in the Mongo `uploads` collection, authorizes the caller (authenticated + room member), and streams the object out of MinIO/S3 with download headers — never buffering the whole file.

**Architecture:** A new authenticated handler `HandleDownloadMinioS3File` resolves the upload doc via a new `Store.GetUpload`, reuses the existing `requireMembership` gate, then streams the object via an injected `objectStore` interface backed by `pkg/minioutil`. S3 failures surface as 503 before any body is written (GetObject + Stat probe).

**Tech Stack:** Go 1.25, Gin, MongoDB (`mongo-driver/v2`), MinIO (`minio-go/v7` via `pkg/minioutil`), `go.uber.org/mock`, `testify`, `pkg/testutil` (testcontainers).

**Spec:** `docs/superpowers/specs/2026-06-22-upload-service-s3-download-design.md`

**Conventions:** run `make fmt` before each commit (a pre-commit hook runs lint + unit tests). Do NOT put any model identifier in commit messages. Each commit must compile. Use `make` targets, never raw `go`.

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `upload-service/store.go` | modify | Add `upload` DTO, `ErrUploadNotFound`, `errIsUploadNotFound`, `GetUpload` to the `Store` interface. |
| `upload-service/store_mongo.go` | modify | Add the `uploads` collection + `GetUpload` impl (projected `FindOne`). |
| `upload-service/store_minio.go` | create | `objectStore` impl `minioObjectStore` wrapping `minio.Client` (`GetObject`+`Stat`). |
| `upload-service/handler.go` | modify | `objectStore` interface, `Handler.s3` field, `NewHandler` arg, `HandleDownloadMinioS3File`. |
| `upload-service/routes.go` | modify | Register the new route in the `/api/v1` group. |
| `upload-service/main.go` | modify | `MINIO_*` config, `minioutil.Connect`, bucket default, wire into `NewHandler`. |
| `upload-service/mock_store_test.go` | regenerate | `make generate SERVICE=upload-service`. |
| `upload-service/handler_test.go` | modify | `fakeS3`, update `newHandler`/`NewHandler` call sites, new handler unit tests. |
| `upload-service/integration_test.go` | modify | `GetUpload` (Mongo) + `minioObjectStore.Open` (MinIO) integration tests. |
| `upload-service/deploy/docker-compose.yml` | modify | Add `MINIO_*` env block. |
| `docs/client-api.md` | modify | Document the new endpoint in §2.x. |

---

## Task 1: Store surface — `upload` DTO + `GetUpload` (Mongo)

**Files:**
- Modify: `upload-service/store.go`
- Modify: `upload-service/store_mongo.go`
- Modify: `upload-service/integration_test.go`
- Regenerate: `upload-service/mock_store_test.go`

- [ ] **Step 1: Add the `upload` DTO, sentinel, and interface method to `store.go`**

Replace the body of `upload-service/store.go` with (additions: `upload` struct, `ErrUploadNotFound`, `errIsUploadNotFound`, `GetUpload`):

```go
package main

import (
	"context"
	"errors"
)

// ErrRoomNotFound is returned by GetRoomSiteID when no room matches the given ID.
var ErrRoomNotFound = errors.New("room not found")

// ErrUploadNotFound is returned by GetUpload when no upload matches the given ID.
var ErrUploadNotFound = errors.New("upload not found")

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// upload is the subset of an `uploads` document the download handler needs.
// Read DTO (bson tags only) — never serialized to clients.
type upload struct {
	ID       string `bson:"_id"`
	UserID   string `bson:"userId"`
	RID      string `bson:"rid"`
	Name     string `bson:"name"`
	Type     string `bson:"type"`
	Size     int64  `bson:"size"`
	AmazonS3 struct {
		Path string `bson:"path"`
	} `bson:"AmazonS3"`
}

// Store is the subset of persistence the upload handlers need.
type Store interface {
	// IsMember reports whether account has a subscription to roomID.
	IsMember(ctx context.Context, roomID, account string) (bool, error)
	// GetRoomSiteID returns the room's siteID, or ErrRoomNotFound (wrapped) when absent.
	GetRoomSiteID(ctx context.Context, roomID string) (string, error)
	// GetUpload returns the upload metadata for fileID, or ErrUploadNotFound (wrapped) when absent.
	GetUpload(ctx context.Context, fileID string) (*upload, error)
}

// errIsRoomNotFound reports whether err wraps ErrRoomNotFound.
func errIsRoomNotFound(err error) bool { return errors.Is(err, ErrRoomNotFound) }

// errIsUploadNotFound reports whether err wraps ErrUploadNotFound.
func errIsUploadNotFound(err error) bool { return errors.Is(err, ErrUploadNotFound) }
```

- [ ] **Step 2: Add the `uploads` collection + `GetUpload` impl to `store_mongo.go`**

In `upload-service/store_mongo.go`, add `uploads` to the struct and constructor, and append the method. Change the struct and constructor:

```go
type mongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	uploads       *mongo.Collection
}

// NewMongoStore returns a Store backed by the subscriptions, rooms, and uploads collections.
func NewMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		uploads:       db.Collection("uploads"),
	}
}
```

Append this method at the end of the file:

```go
func (s *mongoStore) GetUpload(ctx context.Context, fileID string) (*upload, error) {
	var up upload
	err := s.uploads.FindOne(ctx,
		bson.M{"_id": fileID},
		options.FindOne().SetProjection(bson.M{
			"userId": 1, "rid": 1, "name": 1, "type": 1, "size": 1, "AmazonS3.path": 1,
		}),
	).Decode(&up)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("get upload %s: %w", fileID, ErrUploadNotFound)
		}
		return nil, fmt.Errorf("get upload %s: %w", fileID, err)
	}
	return &up, nil
}
```

- [ ] **Step 3: Regenerate the mock**

Run: `make generate SERVICE=upload-service`
Expected: `upload-service/mock_store_test.go` now contains a `GetUpload` mock method (no errors).

- [ ] **Step 4: Add the integration test for `GetUpload`**

In `upload-service/integration_test.go`, append:

```go
func TestMongoStore_GetUpload(t *testing.T) {
	db := testutil.MongoDB(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.Collection("uploads").InsertOne(ctx, bson.M{
		"_id": "file_xyz789", "userId": "user_abc123", "rid": "r1",
		"name": "quarterly-report.pdf", "type": "application/pdf", "size": int64(2458624),
		"store": "AmazonS3:Uploads", "complete": true,
		"AmazonS3": bson.M{"path": "app-001/uploads/r1/user_abc123/file_xyz789"},
	})
	require.NoError(t, err)

	s := NewMongoStore(db)

	up, err := s.GetUpload(ctx, "file_xyz789")
	require.NoError(t, err)
	require.Equal(t, "r1", up.RID)
	require.Equal(t, "quarterly-report.pdf", up.Name)
	require.Equal(t, "application/pdf", up.Type)
	require.Equal(t, int64(2458624), up.Size)
	require.Equal(t, "app-001/uploads/r1/user_abc123/file_xyz789", up.AmazonS3.Path)

	_, err = s.GetUpload(ctx, "missing")
	require.True(t, errors.Is(err, ErrUploadNotFound))
}
```

- [ ] **Step 5: Verify build + integration test**

Run: `make build SERVICE=upload-service`
Expected: builds clean.
Run: `make test-integration SERVICE=upload-service`
Expected: `TestMongoStore_GetUpload` PASSES (and the existing integration test still passes).

- [ ] **Step 6: Commit**

```bash
make fmt
git add upload-service/store.go upload-service/store_mongo.go upload-service/mock_store_test.go upload-service/integration_test.go
git commit -m "feat(upload-service): add GetUpload store lookup for uploads collection"
```

---

## Task 2: MinIO object store + handler wiring (no behavior change yet)

This task introduces the `objectStore` interface, the MinIO impl, the `Handler.s3`
field, the `NewHandler` signature change, the `main.go` config/wiring, and the
test-side `fakeS3` — keeping every existing test green. No new endpoint behavior yet.

**Files:**
- Create: `upload-service/store_minio.go`
- Modify: `upload-service/handler.go`
- Modify: `upload-service/main.go`
- Modify: `upload-service/handler_test.go`

- [ ] **Step 1: Create `upload-service/store_minio.go`**

```go
package main

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
)

// minioObjectStore streams objects out of a single MinIO/S3 bucket.
type minioObjectStore struct {
	client *minio.Client
	bucket string
}

// newMinioObjectStore binds a minio client to a bucket.
func newMinioObjectStore(client *minio.Client, bucket string) *minioObjectStore {
	return &minioObjectStore{client: client, bucket: bucket}
}

// Open returns a streaming reader for the object at key. It Stats the object so
// a missing object or unreachable backend surfaces here — before any response
// body is written — letting the handler map it to 503.
func (s *minioObjectStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object %s/%s: %w", s.bucket, key, err)
	}
	// minio-go's GetObject is lazy; the request only fires on Stat/Read, so probe now.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, err)
	}
	return obj, nil
}
```

- [ ] **Step 2: Add the `objectStore` interface + `s3` field to `handler.go`**

In `upload-service/handler.go`, add `"context"`/`"io"` are already/likely imported — `context` is imported; add `io`. Add the interface near `driveClient`:

```go
// objectStore streams a stored object by key. Satisfied by *minioObjectStore.
type objectStore interface {
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}
```

Update the `Handler` struct and `NewHandler`:

```go
// Handler holds the upload-service dependencies.
type Handler struct {
	store        Store
	drive        driveClient
	s3           objectStore
	maxFiles     int
	maxImageSize int64
}

// NewHandler wires the handler dependencies. maxFiles caps the number of images
// per request; maxImageSize is the per-image upload ceiling in bytes.
func NewHandler(store Store, dc driveClient, s3 objectStore, maxFiles int, maxImageSize int64) *Handler {
	return &Handler{store: store, drive: dc, s3: s3, maxFiles: maxFiles, maxImageSize: maxImageSize}
}
```

Add `"io"` to the import block in `handler.go`.

- [ ] **Step 3: Wire MinIO config + client in `main.go`**

In `upload-service/main.go`, add to the `config` struct (after the Drive line):

```go
	MinioEndpoint  string `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey string `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey string `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL    bool   `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket    string `env:"MINIO_BUCKET"`
```

Add the import `"github.com/hmchangw/chat/pkg/minioutil"`.

After `driveClient := drive.NewClient(&cfg.Drive)`, add:

```go
	minioClient, err := minioutil.Connect(ctx, cfg.MinioEndpoint, cfg.MinioUseSSL, cfg.MinioAccessKey, cfg.MinioSecretKey)
	if err != nil {
		return fmt.Errorf("minio connect: %w", err)
	}
	bucket := cfg.MinioBucket
	if bucket == "" {
		bucket = "chat-" + cfg.SiteID
	}
	s3Store := newMinioObjectStore(minioClient, bucket)
```

Update the handler construction:

```go
	handler := NewHandler(store, driveClient, s3Store, cfg.MaxFiles, cfg.MaxImageSizeBytes)
```

- [ ] **Step 4: Add `fakeS3` and fix `NewHandler` call sites in `handler_test.go`**

In `upload-service/handler_test.go`, add the imports `"context"` and `"io"` (if not present), and add the fake after `fakeDrive`:

```go
// fakeS3 implements objectStore for handler tests.
type fakeS3 struct {
	body   string
	err    error
	gotKey string
}

func (f *fakeS3) Open(_ context.Context, key string) (io.ReadCloser, error) {
	f.gotKey = key
	if f.err != nil {
		return nil, f.err
	}
	return readCloser{strings.NewReader(f.body)}, nil
}
```

Update the `newHandler` helper to inject a default `fakeS3`:

```go
func newHandler(store Store, dc driveClient) *Handler {
	return NewHandler(store, dc, &fakeS3{}, testMaxFiles, testMaxImageSize)
}
```

Fix the two direct `NewHandler(...)` call sites (they currently pass 4 args; insert a `&fakeS3{}` as the 3rd arg):
- `TestUpload_TooManyFiles_400`: `h := NewHandler(store, &fakeDrive{}, &fakeS3{}, 1, testMaxImageSize)`
- `TestUpload_OversizeRejectedPerFile`: `h := NewHandler(store, fd, &fakeS3{}, testMaxFiles, 4)`

- [ ] **Step 5: Verify everything still compiles and passes**

Run: `make test SERVICE=upload-service`
Expected: all existing tests PASS (no behavior changed).
Run: `make build SERVICE=upload-service`
Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
make fmt
git add upload-service/store_minio.go upload-service/handler.go upload-service/main.go upload-service/handler_test.go
git commit -m "feat(upload-service): wire MinIO object store into handler dependencies"
```

---

## Task 3: `HandleDownloadMinioS3File` handler (TDD)

**Files:**
- Modify: `upload-service/handler_test.go` (tests first)
- Modify: `upload-service/handler.go` (handler)
- Modify: `upload-service/routes.go` (route)

- [ ] **Step 1: Write the failing unit tests**

In `upload-service/handler_test.go`, append the context helper and tests. The
helper builds a Gin context for the download route:

```go
func newS3DownloadCtx(t *testing.T, fileID, fileName string, user *AuthenticatedUser) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	url := "/api/v1/file-upload/" + fileID + "/" + fileName + "?download"
	c.Request = httptest.NewRequest(http.MethodGet, url, nil)
	var params gin.Params
	if fileID != "" {
		params = append(params, gin.Param{Key: "fileId", Value: fileID})
	}
	if fileName != "" {
		params = append(params, gin.Param{Key: "fileName", Value: fileName})
	}
	c.Params = params
	if user != nil {
		c.Set(ctxUserKey, user)
	}
	return c, w
}

func sampleUpload() *upload {
	up := &upload{ID: "f1", UserID: "u1", RID: "r1", Name: "réport space.pdf", Type: "application/pdf", Size: 7}
	up.AmazonS3.Path = "app-001/uploads/r1/u1/f1"
	return up
}

func TestS3Download_MissingFileID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newS3DownloadCtx(t, "", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestS3Download_NoUser_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", nil)
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestS3Download_UploadNotFound_404(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(nil, ErrUploadNotFound)
	h := newHandler(store, &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "not_found", decodeErr(t, w).Code)
}

func TestS3Download_StoreError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(nil, errors.New("boom"))
	h := newHandler(store, &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestS3Download_NotMember_403(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(sampleUpload(), nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(false, nil)
	h := newHandler(store, &fakeDrive{})
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusForbidden, w.Code)
	env := decodeErr(t, w)
	assert.Equal(t, "forbidden", env.Code)
	assert.Equal(t, "not_room_member", env.Reason)
}

func TestS3Download_S3Error_503(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(sampleUpload(), nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	h := NewHandler(store, &fakeDrive{}, &fakeS3{err: errors.New("no such key")}, testMaxFiles, testMaxImageSize)
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unavailable", decodeErr(t, w).Code)
}

func TestS3Download_Success_StreamsWithHeaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(sampleUpload(), nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	s3 := &fakeS3{body: "PDFDATA"}
	h := NewHandler(store, &fakeDrive{}, s3, testMaxFiles, testMaxImageSize)
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "PDFDATA", w.Body.String())
	assert.Equal(t, "app-001/uploads/r1/u1/f1", s3.gotKey)
	assert.Equal(t, "application/pdf", w.Header().Get("Content-Type"))
	assert.Equal(t, "7", w.Header().Get("Content-Length"))
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "max-age=31536000", w.Header().Get("Cache-Control"))
	// RFC 5987: encodeURIComponent-style, spaces as %20 (not +).
	assert.Equal(t, "attachment; filename*=UTF-8''r%C3%A9port%20space.pdf", w.Header().Get("Content-Disposition"))
}

func TestS3Download_EmptyType_DefaultsOctetStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	up := sampleUpload()
	up.Type = ""
	store.EXPECT().GetUpload(gomock.Any(), "f1").Return(up, nil)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	h := NewHandler(store, &fakeDrive{}, &fakeS3{body: "PDFDATA"}, testMaxFiles, testMaxImageSize)
	c, w := newS3DownloadCtx(t, "f1", "x.pdf", okUser())
	h.HandleDownloadMinioS3File(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `h.HandleDownloadMinioS3File` undefined.

- [ ] **Step 3: Implement the handler**

In `upload-service/handler.go`, add `"net/url"` to the imports, and append the handler after `HandleDownloadImage`:

```go
// HandleDownloadMinioS3File streams a legacy-uploaded file out of MinIO/S3. It
// resolves the upload metadata from Mongo, authorizes the caller (authenticated
// + room member), then pipes the object straight to the client with download
// headers. The :fileName path segment and the ?download query are accepted but
// ignored — the lookup is by :fileId and the response is always an attachment.
func (h *Handler) HandleDownloadMinioS3File(c *gin.Context) {
	ctx := logCtx(c)

	fileID := c.Param("fileId")
	if fileID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("fileId is required"))
		return
	}

	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}

	up, err := h.store.GetUpload(ctx, fileID)
	if err != nil {
		if errIsUploadNotFound(err) {
			errhttp.Write(ctx, c, errcode.NotFound("file not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("get upload: %w", err))
		return
	}

	if !h.requireMembership(ctx, c, up.RID, user.Account) {
		return
	}

	reader, err := h.s3.Open(ctx, up.AmazonS3.Path)
	if err != nil {
		errhttp.Write(ctx, c, errcode.Unavailable("failed to retrieve file", errcode.WithCause(err)))
		return
	}
	defer reader.Close()

	contentType := up.Type
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	// RFC 5987 filename*: percent-encode UTF-8 like encodeURIComponent (space -> %20, not +).
	encodedName := strings.ReplaceAll(url.QueryEscape(up.Name), "+", "%20")
	extraHeaders := map[string]string{
		"Content-Disposition":     fmt.Sprintf("attachment; filename*=UTF-8''%s", encodedName),
		"Content-Security-Policy": "default-src 'none'",
		"Cache-Control":           "max-age=31536000",
	}
	c.DataFromReader(http.StatusOK, up.Size, contentType, reader, extraHeaders)
}
```

(`strings` and `fmt` are already imported in `handler.go`; add only `"net/url"`.)

- [ ] **Step 4: Register the route in `routes.go`**

In `upload-service/routes.go`, add inside the `api` group (after the image route):

```go
	api.GET("/file-upload/:fileId/:fileName", h.HandleDownloadMinioS3File)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: all new `TestS3Download_*` tests PASS, existing tests still PASS.

- [ ] **Step 6: Commit**

```bash
make fmt
git add upload-service/handler.go upload-service/handler_test.go upload-service/routes.go
git commit -m "feat(upload-service): add MinIO/S3 file download endpoint"
```

---

## Task 4: Route registration + MinIO streaming integration tests

**Files:**
- Modify: `upload-service/handler_test.go` (route-guard assertion)
- Modify: `upload-service/integration_test.go` (MinIO streaming)

- [ ] **Step 1: Add a route-registration auth-guard test**

In `upload-service/handler_test.go`, append:

```go
func TestRegisterRoutes_S3DownloadAuthGuard(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, nil, true)

	// no ssoToken header -> 401 from authMiddleware before the handler runs.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/file-upload/f1/report.pdf?download", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
```

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 2: Add the MinIO streaming integration test**

In `upload-service/integration_test.go`, add `"bytes"`, `"io"`, and `"github.com/minio/minio-go/v7"` to the imports, then append:

```go
func TestMinioObjectStore_Open(t *testing.T) {
	client, bucket := testutil.MinIO(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "app-001/uploads/r1/u1/f1"
	payload := []byte("PDFDATA-binary")
	_, err := client.PutObject(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/pdf"})
	require.NoError(t, err)

	s := newMinioObjectStore(client, bucket)

	reader, err := s.Open(ctx, key)
	require.NoError(t, err)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Missing key surfaces as an error (mapped to 503 by the handler).
	_, err = s.Open(ctx, "does/not/exist")
	require.Error(t, err)
}
```

- [ ] **Step 3: Verify integration tests pass**

Run: `make test-integration SERVICE=upload-service`
Expected: `TestMinioObjectStore_Open`, `TestMongoStore_GetUpload`, and the pre-existing integration test all PASS.

- [ ] **Step 4: Commit**

```bash
make fmt
git add upload-service/handler_test.go upload-service/integration_test.go
git commit -m "test(upload-service): cover S3 download route guard and MinIO streaming"
```

---

## Task 5: Deploy config + client API docs

**Files:**
- Modify: `upload-service/deploy/docker-compose.yml`
- Modify: `docs/client-api.md`

- [ ] **Step 1: Add the `MINIO_*` env block to docker-compose**

In `upload-service/deploy/docker-compose.yml`, add under `environment:` (after the `DRIVE_*` lines):

```yaml
      - MINIO_ENDPOINT=${MINIO_ENDPOINT:-minio:9000}
      - MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
      - MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}
      - MINIO_USE_SSL=false
      - MINIO_BUCKET=${MINIO_BUCKET:-chat-site-local}
```

- [ ] **Step 2: Document the endpoint in `docs/client-api.md`**

In `docs/client-api.md`, immediately after the `GET /api/v1/rooms/:roomId/image/:fileId`
section (after its "Triggered events — error path / `None.`" block and the `---`
separator that precedes `## 3. Request/Reply Methods`), insert:

```markdown
#### GET /api/v1/file-upload/:fileId/:fileName

**Endpoint:** `GET /api/v1/file-upload/:fileId/:fileName`
**Reply:** synchronous HTTP response (raw file bytes, not JSON)

Downloads a previously-uploaded file. Metadata is resolved from the `uploads`
collection by `fileId`; the bytes are streamed straight from the MinIO/S3 bucket.
The response is always served as an attachment.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | yes | OIDC-issued SSO token. |
| `fileId` | path | string | yes | Upload ID (the `uploads._id`); used for the metadata lookup. |
| `fileName` | path | string | yes | Cosmetic — accepted but ignored; the served filename comes from the stored metadata. |
| `download` | query | flag | no | Accepted but ignored; the response is always `Content-Disposition: attachment`. |

The caller must be a member of the room the file belongs to (`uploads.rid`).

#### Success response

`HTTP 200` — raw file binary streamed directly (not JSON), with these headers:

| Header | Value |
|---|---|
| `Content-Type` | the upload's `type` (defaults to `application/octet-stream`). |
| `Content-Length` | the upload's `size`. |
| `Content-Disposition` | `attachment; filename*=UTF-8''<percent-encoded name>`. |
| `Content-Security-Policy` | `default-src 'none'`. |
| `Cache-Control` | `max-age=31536000`. |

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "fileId is required" }` |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |
| 403 | `forbidden` | `not_room_member` | `{ "code": "forbidden", "reason": "not_room_member", "error": "user alice is not in room r1" }` |
| 404 | `not_found` | — | `{ "code": "not_found", "error": "file not found" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — user missing in context. |
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "failed to retrieve file" }` — S3 GetObject/Stat failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---
```

- [ ] **Step 3: Verify docs build references are intact**

Run: `git diff --stat docs/client-api.md upload-service/deploy/docker-compose.yml`
Expected: both files modified; no other files touched.

- [ ] **Step 4: Commit**

```bash
git add upload-service/deploy/docker-compose.yml docs/client-api.md
git commit -m "docs(upload-service): document MinIO/S3 download endpoint and add deploy env"
```

---

## Final verification

- [ ] **Lint, unit, and SAST gates**

Run: `make lint`
Expected: clean.
Run: `make test SERVICE=upload-service`
Expected: all PASS, coverage ≥ 80%.
Run: `make test-integration SERVICE=upload-service`
Expected: all PASS.
Run: `make sast`
Expected: no medium+ findings.

- [ ] **Coverage spot-check**

Run: `go test -tags '' ./upload-service/... -coverprofile=/tmp/cov.out` (via the Makefile's `make test SERVICE=upload-service`) and confirm the handler/store paths are exercised by the new tests.

- [ ] **Push the branch**

```bash
git push -u origin claude/youthful-ride-nrw0y4
```

---

## Self-Review notes (addressed)

- **Spec coverage:** route mount (Task 3), 401-via-middleware (Task 4 guard test), membership authz (Task 3), `GetUpload`/404/`complete`-ignored (Task 1 + handler), S3 stream + 503 probe (Tasks 2–4), all five headers + octet-stream fallback (Task 3 tests), config incl. `chat-{SITE_ID}` default (Task 2), deploy env + client-api doc (Task 5). All covered.
- **Placeholder scan:** none — every code/test step is concrete.
- **Type consistency:** `HandleDownloadMinioS3File`, `objectStore.Open`, `minioObjectStore`/`newMinioObjectStore`, `Store.GetUpload`, `upload`/`ErrUploadNotFound`/`errIsUploadNotFound`, and the `NewHandler(store, dc, s3, maxFiles, maxImageSize)` signature are used identically across all tasks.
