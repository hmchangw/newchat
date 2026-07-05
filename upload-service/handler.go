package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

// Per-file result status values for the upload response.
const (
	statusFailure      = "failure" // pre-check rejection
	driveStatusSuccess = "success" // Drive's success marker
)

// imageFormField is the multipart form field carrying the uploaded images.
const imageFormField = "images"

// fileFormField is the multipart form field carrying the single-endpoint upload.
const fileFormField = "file"

// defaultUploadContentType is the fallback MIME for the single-file endpoint when
// the multipart part carries no Content-Type.
const defaultUploadContentType = "application/octet-stream"

// driveClient is the subset of the Drive client the handlers use.
type driveClient interface {
	UploadGroupImages(userID, username, email, groupID, origin string, files []drive.MultipartFile) ([]drive.UploadGroupImageResponse, error)
	GetGroupImage(host, groupID, fileID string) (*drive.GetGroupImageResponse, error)
	GetBaseURLFromRoomOrigin(origin string) string
}

// previewFunc decodes an image once, returning a base64 preview and the source
// dimensions; injected for testability.
type previewFunc func(data []byte, mime string) (string, *model.ImageDimensions, error)

// objectStore streams a stored object by key. Satisfied by *minioObjectStore.
type objectStore interface {
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}

// Handler holds the upload-service dependencies.
type Handler struct {
	store          Store
	drive          driveClient
	s3             objectStore
	maxImages      int
	maxAttachments int
	maxImageSize   int64
	maxFileSize    int64
	mimeFilter     *mediaTypeFilter
	preview        previewFunc
	nowMilli       func() int64
	cacheMaxAge    int
}

// NewHandler wires the handler dependencies. maxImages/maxImageSize gate the image
// endpoint; maxAttachments/maxFileSize/mimeFilter/preview gate the file endpoint; s3
// backs the MinIO/S3 download endpoint; cacheMaxAge is its Cache-Control max-age in seconds.
func NewHandler(store Store, dc driveClient, s3 objectStore, maxImages, maxAttachments int, maxImageSize, maxFileSize int64,
	mimeFilter *mediaTypeFilter, preview previewFunc, cacheMaxAge int) *Handler {
	return &Handler{
		store: store, drive: dc, s3: s3, maxImages: maxImages, maxAttachments: maxAttachments,
		maxImageSize: maxImageSize, maxFileSize: maxFileSize, mimeFilter: mimeFilter,
		preview: preview, cacheMaxAge: cacheMaxAge,
		nowMilli: func() int64 { return time.Now().UTC().UnixMilli() },
	}
}

// uploadResultItem is one per-file entry in the partial-success upload response.
type uploadResultItem struct {
	Name         string `json:"name"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
	RelativePath string `json:"relativePath,omitempty"`
}

// logCtx returns a context carrying the request ID so errhttp.Write/Classify
// logs the failure once with correlation.
func logCtx(c *gin.Context) context.Context {
	return errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
}

// HandleHealth is the liveness probe.
func (h *Handler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HandleSetCookie issues the (already auth-validated) ssoToken as a cross-site session
// cookie so the browser can authenticate <img>-driven downloads that cannot send headers.
// SameSite=None + Partitioned require the hand-built http.Cookie; c.SetCookie cannot set them.
func (h *Handler) HandleSetCookie(c *gin.Context) {
	token := tokenFromRequest(c)
	// #nosec G124 -- SameSite=None is required for the cross-site <img> download flow; it is mitigated by Secure + HttpOnly + Partitioned.
	http.SetCookie(c.Writer, &http.Cookie{
		Name:        ssoTokenName,
		Value:       token,
		Path:        "/",
		HttpOnly:    true,
		Secure:      true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	})
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// HandleUploadImages uploads one or more images for a room on behalf of the
// authenticated user, returning per-file success/failure in a single 200.
func (h *Handler) HandleUploadImages(c *gin.Context) {
	ctx := logCtx(c)

	roomID := c.Param("roomId")
	if roomID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("roomId is required"))
		return
	}

	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}
	if user.Email == "" {
		errhttp.Write(ctx, c, errcode.Internal("the user has no email provided"))
		return
	}

	if !h.requireMembership(ctx, c, roomID, user.Account) {
		return
	}

	siteID, err := h.store.GetRoomSiteID(ctx, roomID)
	if err != nil {
		if errIsRoomNotFound(err) {
			errhttp.Write(ctx, c, errcode.NotFound("room not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("get room: %w", err))
		return
	}

	form, err := c.MultipartForm()
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("request must be multipart/form-data"))
		return
	}
	files := form.File[imageFormField]
	if len(files) > h.maxImages {
		errhttp.Write(ctx, c, errcode.BadRequest("too many files"))
		return
	}

	results, fileHeaders, origNames := preprocessFiles(files, h.maxImageSize, h.nowMilli())
	defer func() {
		for _, mf := range fileHeaders {
			_ = mf.File.Close()
		}
	}()

	if len(fileHeaders) == 0 {
		c.JSON(http.StatusOK, gin.H{"results": results})
		return
	}

	responses, err := h.drive.UploadGroupImages(user.Account, user.DisplayName(), user.Email, roomID, siteID, fileHeaders)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("upload images to drive: %w", err))
		return
	}

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

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// HandleUploadFile uploads one file for a room on behalf of the authenticated
// user and returns a render-ready attachment. It does not publish a message.
func (h *Handler) HandleUploadFile(c *gin.Context) {
	ctx := logCtx(c)

	roomID := c.Param("roomId")
	if roomID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("roomId is required"))
		return
	}
	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}
	if user.Email == "" {
		errhttp.Write(ctx, c, errcode.Internal("the user has no email provided"))
		return
	}

	if !h.requireMembership(ctx, c, roomID, user.Account) {
		return
	}

	siteID, err := h.store.GetRoomSiteID(ctx, roomID)
	if err != nil {
		if errIsRoomNotFound(err) {
			errhttp.Write(ctx, c, errcode.NotFound("room not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("get room: %w", err))
		return
	}

	form, err := c.MultipartForm()
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("request must be multipart/form-data"))
		return
	}
	files := form.File[fileFormField]
	if len(files) == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("file is required"))
		return
	}
	if len(files) > h.maxAttachments {
		errhttp.Write(ctx, c, errcode.BadRequest("too many files"))
		return
	}
	fh := files[0]
	if h.maxFileSize >= 0 && fh.Size > h.maxFileSize {
		errhttp.Write(ctx, c, errcode.BadRequest("file size exceeds limit"))
		return
	}

	// Normalize the (client-controlled) declared type: lowercase + strip params so
	// the filter and the image branch see a clean value.
	mime := normalizeMediaType(fh.Header.Get("Content-Type"))
	if mime == "" {
		mime = defaultUploadContentType
	}
	if !h.mimeFilter.allowed(mime) {
		errhttp.Write(ctx, c, errcode.BadRequest("file type is not allowed"))
		return
	}

	// Images are buffered once (for preview + dimensions) and the same bytes are
	// reused for the Drive upload, so the file is read exactly once. Non-image
	// types are streamed straight to Drive without buffering (a large video must
	// not be held in memory).
	var data []byte
	var driveFile multipart.File
	if strings.HasPrefix(mime, "image/") {
		if data, err = readMultipartFile(fh); err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("read uploaded file: %w", err))
			return
		}
		driveFile = bytesFile{bytes.NewReader(data)}
	} else if driveFile, err = fh.Open(); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("open uploaded file: %w", err))
		return
	}
	defer driveFile.Close()

	// Build the preview + dimensions BEFORE the Drive upload so a preview failure
	// can't leave an orphaned Drive file. data is non-nil only for images.
	var preview string
	var dims *model.ImageDimensions
	if data != nil {
		if preview, dims, err = h.preview(data, mime); err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("build image preview: %w", err))
			return
		}
	}

	responses, err := h.drive.UploadGroupImages(user.Account, user.DisplayName(), user.Email, roomID, siteID,
		[]drive.MultipartFile{{File: driveFile, Filename: uniqueName(fh.Filename, h.nowMilli(), 0)}})
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("upload file to drive: %w", err))
		return
	}
	if len(responses) == 0 || responses[0].Status != driveStatusSuccess {
		var cause error
		if len(responses) == 0 {
			cause = errors.New("drive returned no upload response")
		} else {
			cause = fmt.Errorf("drive upload status %q: %s", responses[0].Status, responses[0].Error)
		}
		errhttp.Write(ctx, c, errcode.Unavailable("drive upload failed", errcode.WithCause(cause)))
		return
	}
	obj := responses[0].File

	meta := fileMeta{id: obj.FileID, name: fh.Filename, mime: mime, size: obj.FileSize}
	url := fileURL(roomID, obj.FileID, h.drive.GetBaseURLFromRoomOrigin(siteID))

	att := buildAttachment(meta, c.PostForm("description"), url, preview, dims)
	c.JSON(http.StatusOK, gin.H{"success": true, "attachments": []model.Attachment{att}})
}

// HandleDownloadFile proxies a protected file: it resolves a signed URL from
// Drive, fetches the bytes, and streams them straight to the client.
func (h *Handler) HandleDownloadFile(c *gin.Context) {
	ctx := logCtx(c)

	roomID := c.Param("roomId")
	if roomID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("roomId is required"))
		return
	}
	fileID := c.Param("fileId")
	if fileID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("fileId is required"))
		return
	}
	driveHost := c.Query("drive_host")
	if driveHost == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("drive_host is required"))
		return
	}

	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}

	if !h.requireMembership(ctx, c, roomID, user.Account) {
		return
	}

	img, err := h.drive.GetGroupImage(driveHost, roomID, fileID)
	if err != nil {
		errhttp.Write(ctx, c, errcode.Unavailable("failed to retrieve file", errcode.WithCause(err)))
		return
	}
	defer img.Reader.Close()

	// Download headers mirror the MinIO/S3 path: force-download, lock down
	// execution, and allow private (per-user) caching only — auth + membership gated.
	extraHeaders := map[string]string{
		"Content-Disposition":     contentDisposition(img.Filename),
		"Content-Security-Policy": "default-src 'none'",
		"Cache-Control":           fmt.Sprintf("private, max-age=%d", h.cacheMaxAge),
	}
	c.DataFromReader(http.StatusOK, img.ContentLength, img.ContentType, img.Reader, extraHeaders)
}

// HandleDownloadMinioS3File streams a legacy-uploaded file out of MinIO/S3. It
// resolves the upload metadata from Mongo, authorizes the caller (authenticated
// + room member), then pipes the object straight to the client with download
// headers. The :fileName path segment is cosmetic (accepted but ignored); the
// lookup is by :fileId and the response is always an attachment.
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
	extraHeaders := map[string]string{
		"Content-Disposition":     contentDisposition(up.Name),
		"Content-Security-Policy": "default-src 'none'",
		// private: this response is authorization-gated (auth + room membership),
		// so only the user agent may cache it — never a shared/intermediary cache.
		"Cache-Control": fmt.Sprintf("private, max-age=%d", h.cacheMaxAge),
	}
	c.DataFromReader(http.StatusOK, up.Size, contentType, reader, extraHeaders)
}

// requireMembership verifies the account is a member of roomID, writing the
// appropriate error response and returning false when it is not (or on a store
// error). Both room-scoped handlers gate on this.
func (h *Handler) requireMembership(ctx context.Context, c *gin.Context, roomID, account string) bool {
	member, err := h.store.IsMember(ctx, roomID, account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("check room membership: %w", err))
		return false
	}
	if !member {
		errhttp.Write(ctx, c, errcode.Forbidden(
			fmt.Sprintf("user %s is not in room %s", account, roomID),
			errcode.WithReason(errcode.RoomNotMember)))
		return false
	}
	return true
}

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

// readMultipartFile opens, reads, and closes a multipart file header's content.
func readMultipartFile(fh *multipart.FileHeader) ([]byte, error) {
	f, err := fh.Open()
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	return io.ReadAll(f)
}

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

// contentDisposition builds an attachment Content-Disposition value. A non-empty
// name is appended as an RFC 5987 filename* (percent-encoded, space -> %20, not +).
func contentDisposition(name string) string {
	if name == "" {
		return "attachment"
	}
	encodedName := strings.ReplaceAll(url.QueryEscape(name), "+", "%20")
	return fmt.Sprintf("attachment; filename*=UTF-8''%s", encodedName)
}

// bytesFile adapts a *bytes.Reader (Read/ReadAt/Seek) to multipart.File by adding
// a no-op Close, so already-buffered image bytes can be handed to Drive without
// re-reading the upload.
type bytesFile struct{ *bytes.Reader }

func (bytesFile) Close() error { return nil }
