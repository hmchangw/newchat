# Timestamped Drive Filenames Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send a unique filename to Drive on every upload (so neither re-uploading the same file across requests nor duplicate names within one batch collide) while returning the original filename to the client.

**Architecture:** A small `uniqueName` helper inserts `_<unixMilli>_<index>` before the file extension — the timestamp separates re-uploads across requests, the per-batch index separates duplicate names within a single request (which are processed in the same millisecond). Both `upload-service` handlers (`HandleUploadFile`, `HandleUploadImages`) apply it to the name sent to `drive.UploadGroupImages`. `HandleUploadFile` already returns the original `fh.Filename`, so it needs no strip-back. `HandleUploadImages` echoes Drive's filename, so `preprocessFiles` returns an `origNames []string` (originals in send order) used to restore the original name in the response by position. A `nowMilli func() int64` field on `Handler` makes the clock injectable for deterministic tests.

**Tech Stack:** Go 1.25, Gin, `stretchr/testify`, `go.uber.org/mock`. Run tests with `make test SERVICE=upload-service`.

---

### Task 1: `uniqueName` helper

**Files:**
- Modify: `upload-service/handler.go` (add helper near the bottom, beside `readMultipartFile`)
- Test: `upload-service/handler_test.go` (new test function)

- [ ] **Step 1: Write the failing test**

Add to `upload-service/handler_test.go`:

```go
func Test_uniqueName(t *testing.T) {
	const milli int64 = 1719312000000
	tests := []struct {
		name string
		in   string
		i    int
		want string
	}{
		{"with extension", "photo.png", 0, "photo_1719312000000_0.png"},
		{"uppercase extension", "IMG.JPG", 1, "IMG_1719312000000_1.JPG"},
		{"no extension", "README", 2, "README_1719312000000_2"},
		{"multi dot", "a.tar.gz", 0, "a.tar_1719312000000_0.gz"},
		{"dotfile (filepath.Ext semantics)", ".gitignore", 0, "_1719312000000_0.gitignore"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, uniqueName(tt.in, milli, tt.i))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: uniqueName`.

- [ ] **Step 3: Write minimal implementation**

Add to `upload-service/handler.go` (after `readMultipartFile`, before `bytesFile`):

```go
// uniqueName inserts a millisecond timestamp and a per-batch index before the
// file extension so uploads get distinct Drive object names:
// "photo.png" -> "photo_1719312000000_0.png". The timestamp separates re-uploads
// across requests; the index separates duplicate filenames within a single batch
// (which are processed in the same millisecond). A name with no extension just
// gets the suffix appended. Extension detection follows filepath.Ext semantics.
func uniqueName(name string, milli int64, i int) string {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	return fmt.Sprintf("%s_%d_%d%s", base, milli, i, ext)
}
```

(`fmt`, `path/filepath`, and `strings` are already imported in `handler.go`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "feat(upload-service): add uniqueName filename helper"
```

---

### Task 2: Injectable clock on `Handler`

**Files:**
- Modify: `upload-service/handler.go` (struct field + `NewHandler` default + `time` import)

This task adds the `nowMilli` field with a real-clock default. It introduces no behavior change yet; correctness is "the package still builds and all existing tests pass." `NewHandler`'s signature is unchanged, so no call sites change.

- [ ] **Step 1: Add the `time` import**

In `upload-service/handler.go`, add `"time"` to the standard-library import group (alongside `"strings"`):

```go
	"path/filepath"
	"strings"
	"time"
```

- [ ] **Step 2: Add the struct field**

In the `Handler` struct, add `nowMilli` after `preview`:

```go
type Handler struct {
	store        Store
	drive        driveClient
	maxFiles     int
	maxImageSize int64
	maxFileSize  int64
	mimeFilter   *mediaTypeFilter
	preview      previewFunc
	nowMilli     func() int64
}
```

- [ ] **Step 3: Default it in `NewHandler`**

Update the returned struct literal in `NewHandler`:

```go
	return &Handler{
		store: store, drive: dc, maxFiles: maxFiles, maxImageSize: maxImageSize,
		maxFileSize: maxFileSize, mimeFilter: mimeFilter, preview: preview,
		nowMilli: func() int64 { return time.Now().UTC().UnixMilli() },
	}
```

- [ ] **Step 4: Run tests to verify nothing broke**

Run: `make test SERVICE=upload-service`
Expected: PASS (all existing tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add upload-service/handler.go
git commit -m "feat(upload-service): add injectable nowMilli clock to Handler"
```

---

### Task 3: Capture uploaded filenames in `fakeDrive` (test infra)

**Files:**
- Modify: `upload-service/handler_test.go` (`fakeDrive` struct + `UploadGroupImages` method)

The current fake records only the file count. Capture the sent filenames so the next two tasks can assert Drive receives timestamped names. This is test-only; no production change.

- [ ] **Step 1: Add a capture slice to the fake**

In `upload-service/handler_test.go`, extend the `uploadGot` anonymous struct inside `fakeDrive`:

```go
	uploadGot  struct {
		userID, username, email, groupID, origin string
		n                                        int
		filenames                                []string
	}
```

- [ ] **Step 2: Record filenames in the method**

Update `fakeDrive.UploadGroupImages` to capture each sent name:

```go
func (f *fakeDrive) UploadGroupImages(userID, username, email, groupID, origin string, files []drive.MultipartFile) ([]drive.UploadGroupImageResponse, error) {
	f.uploadGot.userID, f.uploadGot.username, f.uploadGot.email = userID, username, email
	f.uploadGot.groupID, f.uploadGot.origin, f.uploadGot.n = groupID, origin, len(files)
	f.uploadGot.filenames = nil
	for _, mf := range files {
		f.uploadGot.filenames = append(f.uploadGot.filenames, mf.Filename)
	}
	return f.uploadResp, f.uploadErr
}
```

- [ ] **Step 3: Run tests to verify nothing broke**

Run: `make test SERVICE=upload-service`
Expected: PASS (no assertions on the new field yet).

- [ ] **Step 4: Commit**

```bash
git add upload-service/handler_test.go
git commit -m "test(upload-service): capture uploaded filenames in fakeDrive"
```

---

### Task 4: Unique filename in `HandleUploadFile`

**Files:**
- Modify: `upload-service/handler.go` (`HandleUploadFile`, around line 245-246)
- Test: `upload-service/handler_test.go` (new test)

- [ ] **Step 1: Write the failing test**

Add to `upload-service/handler_test.go`. The constructor mirrors the existing file tests: `NewHandler(store, fd, 0, 0, 100<<20, newMediaTypeFilter("", "image/svg+xml"), imagePreview)`.

```go
func TestHandleUploadFile_SendsUniqueName_ReturnsOriginal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "http://drive",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "f1", GroupID: "r1", Filename: "photo_1719312000000_0.png", FileSize: 3}},
		},
	}
	h := NewHandler(store, fd, 0, 0, 100<<20, newMediaTypeFilter("", "image/svg+xml"), imagePreview)
	h.nowMilli = func() int64 { return 1719312000000 }

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	w, err := mw.CreateFormFile("file", "photo.png")
	require.NoError(t, err)
	_, _ = w.Write([]byte("xxx"))
	require.NoError(t, mw.Close())

	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/r1/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	c.Request = req
	c.Params = gin.Params{{Key: "roomId", Value: "r1"}}
	c.Set(ctxUserKey, okUser())

	h.HandleUploadFile(c)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"photo_1719312000000_0.png"}, fd.uploadGot.filenames, "drive receives the unique name")

	var got struct {
		Attachments []model.Attachment `json:"attachments"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Attachments, 1)
	assert.Equal(t, "photo.png", got.Attachments[0].Title, "response keeps the original name")
}
```

Note: the uploaded filename surfaces as `Attachment.Title` (json `title`) via `buildAttachment` (`meta.name` → `att.Title`); `model.Attachment` has no `Name` field.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=upload-service -run TestHandleUploadFile_SendsUniqueName`
Expected: FAIL — `fd.uploadGot.filenames` is `["photo.png"]`, not the unique name.

- [ ] **Step 3: Implement**

In `upload-service/handler.go` `HandleUploadFile`, change the Drive call (currently lines 245-246) to send the unique name (single file → index `0`). The original `fh.Filename` is still used for `meta.name` below, so the response is unaffected:

```go
	responses, err := h.drive.UploadGroupImages(user.Account, user.DisplayName(), user.Email, roomID, siteID,
		[]drive.MultipartFile{{File: driveFile, Filename: uniqueName(fh.Filename, h.nowMilli(), 0)}})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=upload-service`
Expected: PASS (new test passes; all existing file/image tests still pass).

- [ ] **Step 5: Commit**

```bash
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "feat(upload-service): unique Drive filename in HandleUploadFile"
```

---

### Task 5: Unique names + strip-back in `HandleUploadImages`

**Files:**
- Modify: `upload-service/handler.go` (`preprocessFiles` signature + body; `HandleUploadImages` call site + response loop)
- Test: `upload-service/handler_test.go` (new tests)

- [ ] **Step 1: Write the failing tests**

Add to `upload-service/handler_test.go`. The fake echoes the unique name (as real Drive does); the response strip-back comes from `origNames[i]`:

```go
func TestHandleUploadImages_SendsUniqueNames_ReturnsOriginals(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "img-1", GroupID: "r1", Filename: "a_1719312000000_0.png"}},
		},
	}
	h := newHandler(store, fd)
	h.nowMilli = func() int64 { return 1719312000000 }

	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"a_1719312000000_0.png"}, fd.uploadGot.filenames, "drive receives the unique name")

	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 1)
	assert.Equal(t, "success", got.Results[0].Status)
	assert.Equal(t, "a.png", got.Results[0].Name, "response shows the original name")
	assert.Equal(t, "api/v1/rooms/r1/file/img-1?drive_host=https://drive.example.com", got.Results[0].RelativePath)
}

// Two files with the SAME name in one batch must get distinct indexed names so
// they don't collide in Drive; both response items keep the original name.
func TestHandleUploadImages_DuplicateNamesInBatch_GetDistinctNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "success", File: drive.GroupImageObject{FileID: "img-0", GroupID: "r1", Filename: "a_1719312000000_0.png"}},
			{Status: "success", File: drive.GroupImageObject{FileID: "img-1", GroupID: "r1", Filename: "a_1719312000000_1.png"}},
		},
	}
	h := newHandler(store, fd)
	h.nowMilli = func() int64 { return 1719312000000 }

	// Two parts under the same field with the same filename.
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	for i := 0; i < 2; i++ {
		w, err := mw.CreateFormFile("images", "a.png")
		require.NoError(t, err)
		_, _ = w.Write([]byte("x"))
	}
	require.NoError(t, mw.Close())

	c, w := newUploadCtx(t, "r1", body, mw.FormDataContentType(), okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"a_1719312000000_0.png", "a_1719312000000_1.png"}, fd.uploadGot.filenames, "duplicate names get distinct indexed names")

	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 2)
	assert.Equal(t, "a.png", got.Results[0].Name)
	assert.Equal(t, "a.png", got.Results[1].Name)
}

func TestHandleUploadImages_DriveErrorEmptyFilename_KeepsOriginalName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-x", nil)
	// Drive reports a per-file failure: status "failure", empty File (so
	// resp.File.Filename == "").
	fd := &fakeDrive{
		baseURL: "https://drive.example.com",
		uploadResp: []drive.UploadGroupImageResponse{
			{Status: "failure", Error: "drive exploded", File: drive.GroupImageObject{}},
		},
	}
	h := newHandler(store, fd)
	h.nowMilli = func() int64 { return 1719312000000 }

	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, okUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Results []uploadResultItem `json:"results"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Results, 1)
	assert.Equal(t, "failure", got.Results[0].Status)
	assert.Equal(t, "drive exploded", got.Results[0].Error)
	assert.Equal(t, "a.png", got.Results[0].Name, "name falls back to original even when drive returns empty filename")
	assert.Empty(t, got.Results[0].RelativePath)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=upload-service -run 'TestHandleUploadImages_SendsUniqueNames|TestHandleUploadImages_DuplicateNamesInBatch|TestHandleUploadImages_DriveErrorEmptyFilename'`
Expected: FAIL — `fd.uploadGot.filenames` is `["a.png"]` (not unique); once unique naming is added without strip-back, the success `Name` would be the unique name; and the error case `Name` would be `""`.

- [ ] **Step 3: Change `preprocessFiles` to apply the unique name and return the originals**

Replace `preprocessFiles` in `upload-service/handler.go` with:

```go
// preprocessFiles runs the per-file size/extension/open checks. Rejected files
// become failure result items; accepted files become MultipartFiles whose open
// handles the caller is responsible for closing. Each accepted file is uploaded
// under a unique name (timestamp + accepted-file index, so neither re-uploads
// across requests nor duplicate names within a batch collide in Drive); origNames
// lists the caller-facing originals in send order so the response can show them
// (Drive echoes the unique name, and an empty name on a per-file failure).
func preprocessFiles(files []*multipart.FileHeader, maxSize, milli int64) (results []uploadResultItem, fileHeaders []drive.MultipartFile, origNames []string) {
	for _, fh := range files {
		if fh.Size > maxSize {
			results = append(results, uploadResultItem{Name: fh.Filename, Status: statusFailure, Error: "file size exceeds limit"})
			continue
		}
		if !drive.AllowedImageFileTypes[strings.ToLower(filepath.Ext(fh.Filename))] {
			results = append(results, uploadResultItem{Name: fh.Filename, Status: statusFailure, Error: "file has an invalid file type"})
			continue
		}
		f, err := fh.Open()
		if err != nil {
			results = append(results, uploadResultItem{Name: fh.Filename, Status: statusFailure, Error: "failed to open file"})
			continue
		}
		fileHeaders = append(fileHeaders, drive.MultipartFile{File: f, Filename: uniqueName(fh.Filename, milli, len(origNames))})
		origNames = append(origNames, fh.Filename)
	}
	return results, fileHeaders, origNames
}
```

The index passed to `uniqueName` is `len(origNames)` *before* the append — i.e. the accepted-file index — so it stays aligned with the `responses[i]` / `origNames[i]` correlation and skips rejected files.

- [ ] **Step 4: Update the `HandleUploadImages` call site and response loop**

In `HandleUploadImages`, change the `preprocessFiles` call (currently line 131) to pass the clock and receive the originals:

```go
	results, fileHeaders, origNames := preprocessFiles(files, h.maxImageSize, h.nowMilli())
```

Then update the response loop (currently lines 150-156) to set the name from send
order, ignoring Drive's (unique or empty) echo:

```go
	driveHost := h.drive.GetBaseURLFromRoomOrigin(siteID)
	for i, resp := range responses {
		name := resp.File.Filename
		if i < len(origNames) {
			name = origNames[i]
		}
		item := uploadResultItem{Name: name, Status: resp.Status, Error: resp.Error}
		if resp.Status == driveStatusSuccess {
			item.RelativePath = fileURL(resp.File.GroupID, resp.File.FileID, driveHost)
		}
		results = append(results, item)
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS — the three new tests pass; `TestUpload_MixedSuccessAndFailure_Merges` still passes (one valid file → one response at index 0 → `origNames[0]` is `"a.png"`).

- [ ] **Step 6: Commit**

```bash
git add upload-service/handler.go upload-service/handler_test.go
git commit -m "feat(upload-service): unique Drive filenames in HandleUploadImages, return originals"
```

---

### Task 6: Lint, full verification, push

**Files:** none (verification only)

- [ ] **Step 1: Lint**

Run: `make lint`
Expected: no findings in `upload-service`.

- [ ] **Step 2: Full unit test run with race detector**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 3: Confirm coverage floor (≥80%) for the package**

Run: `make test SERVICE=upload-service` and review coverage output (the Makefile reports it); confirm `upload-service` stays ≥80%. The new branches are exercised by Tasks 4-5 tests.

- [ ] **Step 4: Push**

```bash
git push -u origin claude/amazing-albattani-pt99o4
```

---

## Self-Review

**Spec coverage:**
- Unique name (timestamp + index) on Drive filename → Tasks 1, 4, 5. ✅
- Both handlers (`HandleUploadFile`, `HandleUploadImages`) → Tasks 4, 5. ✅
- Response excludes the suffix → Task 4 (uses original `fh.Filename`), Task 5 (`origNames[i]` by send order). ✅
- Within-batch duplicate names get distinct names → Task 1 (index param), Task 5 (`uniqueName(..., len(origNames))` + `TestHandleUploadImages_DuplicateNamesInBatch_GetDistinctNames`). ✅
- Response `Name` never empty on Drive per-file failure (empty echo) → Task 5 (`origNames[i]` + `TestHandleUploadImages_DriveErrorEmptyFilename_KeepsOriginalName`). ✅
- Testable clock → Task 2. ✅
- Edge cases (no ext, multi-dot, dotfile) → Task 1 table test. ✅
- No `docs/client-api.md` change required (per spec; response schema unchanged). ✅

**Placeholder scan:** No TBD/TODO; all steps contain concrete code and commands. Task 4 asserts on `Attachment.Title` (confirmed via `attachment.go`: `meta.name` → `att.Title`).

**Type consistency:** `uniqueName(string, int64, int) string`, `nowMilli func() int64`, and `preprocessFiles(..., milli int64) (results []uploadResultItem, fileHeaders []drive.MultipartFile, origNames []string)` are used consistently across Tasks 1-5 (the call site in Task 5 Step 4 destructures all three returns; the index passed to `uniqueName` is `len(origNames)` before the append). The `fakeDrive.uploadGot.filenames []string` field defined in Task 3 is read in Tasks 4-5.
