package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

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

// defaultUploadContentType marks a declared Content-Type as generic (so
// resolveMediaType knows to look past it) and is the final fallback when
// nothing else — sniff or extension — can name the file.
const defaultUploadContentType = "application/octet-stream"

// driveClient is the subset of the Drive client the handlers use.
type driveClient interface {
	UploadGroupImages(userID, username, email, groupID, origin string, files []drive.MultipartFile) ([]drive.UploadGroupImageResponse, error)
	GetGroupImage(host, groupID, fileID string) (*drive.GetGroupImageResponse, error)
	GetBaseURLFromRoomOrigin(origin string) string
}

// objectStore streams a stored object by key. Satisfied by *minioObjectStore.
type objectStore interface {
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}

// Handler holds the upload-service dependencies.
type Handler struct {
	store          Store
	drive          driveClient
	legacyDrive    driveClient
	s3             objectStore
	maxImages      int
	maxAttachments int
	maxImageSize   int64
	maxFileSize    int64
	mimeFilter     *mediaTypeFilter
	cacheMaxAge    int

	setCookiePartitioned bool
}

// NewHandler wires the handler dependencies. maxImages/maxImageSize gate the image
// endpoint; maxAttachments/maxFileSize/mimeFilter gate the file endpoint; s3
// backs the MinIO/S3 download endpoint; cacheMaxAge is its Cache-Control max-age in
// seconds; setCookiePartitioned gates the Partitioned attribute on HandleSetCookie;
// legacyDrive serves the /api/v3 download from a separate (legacy) Drive backend.
func NewHandler(store Store, dc driveClient, s3 objectStore, maxImages, maxAttachments int, maxImageSize, maxFileSize int64,
	mimeFilter *mediaTypeFilter, cacheMaxAge int, setCookiePartitioned bool, legacyDrive driveClient) *Handler {
	return &Handler{
		store: store, drive: dc, legacyDrive: legacyDrive, s3: s3, maxImages: maxImages, maxAttachments: maxAttachments,
		maxImageSize: maxImageSize, maxFileSize: maxFileSize, mimeFilter: mimeFilter,
		cacheMaxAge: cacheMaxAge, setCookiePartitioned: setCookiePartitioned,
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
	ctx := logCtx(c)

	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}
	// Session callers have no ssoToken to mirror; without this guard the cookie
	// would be issued empty and fail confusingly on the next download.
	if user.Session != nil {
		errhttp.Write(ctx, c, errcode.BadRequest(
			"setCookie requires an ssoToken; session-token callers send credentials as headers"))
		return
	}

	token := tokenFromRequest(c)
	// #nosec G124 -- SameSite=None is required for the cross-site <img> download flow; mitigated by Secure + HttpOnly (and Partitioned when SETCOOKIE_PARTITIONED is enabled).
	http.SetCookie(c.Writer, &http.Cookie{
		Name:        ssoTokenName,
		Value:       token,
		Path:        "/",
		HttpOnly:    true,
		Secure:      true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: h.setCookiePartitioned,
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
	// A blank email on an SSO caller is a broken token. Session callers legitimately
	// have none unless BOT_EMAIL_DOMAIN is set.
	if user.Email == "" && user.Session == nil {
		errhttp.Write(ctx, c, errcode.Internal("the user has no email provided"))
		return
	}

	siteID, ok := h.requireMembership(ctx, c, roomID, user.Account)
	if !ok {
		return
	}

	form, err := c.MultipartForm()
	if err != nil {
		// Cause distinguishes a read deadline or client disconnect from a genuinely
		// non-multipart request; it is logged server-side, never sent to the client.
		errhttp.Write(ctx, c, errcode.BadRequest("request must be multipart/form-data", errcode.WithCause(err)))
		return
	}
	files := form.File[imageFormField]
	if len(files) > h.maxImages {
		errhttp.Write(ctx, c, errcode.BadRequest("too many files"))
		return
	}

	results, fileHeaders := preprocessFiles(files, h.maxImageSize)
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
		// The name we sent is the source of truth (Drive echoes it on success and
		// returns an empty name on a per-file failure).
		name := resp.File.Filename
		if i < len(fileHeaders) {
			name = fileHeaders[i].Filename
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
	// A blank email on an SSO caller is a broken token. Session callers legitimately
	// have none unless BOT_EMAIL_DOMAIN is set.
	if user.Email == "" && user.Session == nil {
		errhttp.Write(ctx, c, errcode.Internal("the user has no email provided"))
		return
	}

	siteID, ok := h.requireMembership(ctx, c, roomID, user.Account)
	if !ok {
		return
	}

	form, err := c.MultipartForm()
	if err != nil {
		// Cause distinguishes a read deadline or client disconnect from a genuinely
		// non-multipart request; it is logged server-side, never sent to the client.
		errhttp.Write(ctx, c, errcode.BadRequest("request must be multipart/form-data", errcode.WithCause(err)))
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

	// The upload is handed to the resolver, the header read and Drive as a reader,
	// so the file is never held in memory whatever its type or size.
	driveFile, err := fh.Open()
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("open uploaded file: %w", err))
		return
	}
	defer driveFile.Close()

	// The declared Content-Type is a client-controlled hint — browsers send
	// application/octet-stream for any file the OS cannot type — so the real type
	// comes from the bytes and the name. Filtering THAT rather than the declared
	// value is what stops a blacklisted upload arriving under a generic label.
	mime, err := resolveMediaType(fh.Header.Get("Content-Type"), fh.Filename, driveFile)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("resolve uploaded file media type: %w", err))
		return
	}
	if !h.mimeFilter.allowed(mime) {
		errhttp.Write(errcode.WithLogValues(ctx, "media_type", mime), c,
			errcode.BadRequest("file type is not allowed"))
		return
	}

	// Read the dimensions BEFORE the Drive upload so a read failure can't leave an
	// orphaned Drive file. imageDimensions rewinds driveFile for us and no-ops on
	// a non-image MIME, so the caller needs no image check of its own.
	dims, err := imageDimensions(driveFile, mime)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("read image dimensions: %w", err))
		return
	}

	responses, err := h.drive.UploadGroupImages(user.Account, user.DisplayName(), user.Email, roomID, siteID,
		[]drive.MultipartFile{{File: driveFile, Filename: fh.Filename}})
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

	att := buildAttachment(meta, c.PostForm("description"), url, dims)
	c.JSON(http.StatusOK, gin.H{"success": true, "attachments": []model.Attachment{att}})
}

// HandleDownloadFile proxies a protected file: it resolves a signed URL from
// Drive, fetches the bytes, and streams them straight to the client.
func (h *Handler) HandleDownloadFile(c *gin.Context) {
	h.downloadFrom(c, h.drive)
}

// HandleDownloadProtectedImageV3 serves the backward-compatible /api/v3 download for inline images in legacy message data, proxied from the legacy Drive backend (its own URL/api-token).
func (h *Handler) HandleDownloadProtectedImageV3(c *gin.Context) {
	h.downloadFrom(c, h.legacyDrive)
}

// downloadFrom streams a protected file from the given Drive client; the v1 and v3 download endpoints share it, differing only in which backend serves the request.
func (h *Handler) downloadFrom(c *gin.Context, dc driveClient) {
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

	if _, ok := h.requireMembership(ctx, c, roomID, user.Account); !ok {
		return
	}

	img, err := dc.GetGroupImage(driveHost, roomID, fileID)
	if err != nil {
		if errors.Is(err, drive.ErrHostNotAllowed) {
			// A host outside the configured Drive base URLs is a malformed
			// request, not a broken dependency — 400, not 503. The rejected
			// value rides the log line (WithLogValues, so Classify still logs
			// exactly once) because it is the signal that someone is probing
			// the credential boundary; it is never echoed to the client.
			ctx = errcode.WithLogValues(ctx, "drive_host", driveHost)
			errhttp.Write(ctx, c, errcode.BadRequest("drive_host is not a configured Drive host"))
			return
		}
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

	if _, ok := h.requireMembership(ctx, c, up.RID, user.Account); !ok {
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
// appropriate error response and returning ok=false when it is not (or on a
// store error). On success it also returns the room's home siteID from the
// subscription — the local source that exists even for cross-site rooms.
func (h *Handler) requireMembership(ctx context.Context, c *gin.Context, roomID, account string) (string, bool) {
	siteID, member, err := h.store.MemberSiteID(ctx, roomID, account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("check room membership: %w", err))
		return "", false
	}
	if !member {
		errhttp.Write(ctx, c, errcode.Forbidden(
			fmt.Sprintf("user %s is not in room %s", account, roomID),
			errcode.WithReason(errcode.RoomNotMember)))
		return "", false
	}
	return siteID, true
}

// preprocessFiles runs the per-file size/extension/open checks. Rejected files
// become failure result items; accepted files become MultipartFiles whose open
// handles the caller is responsible for closing. Files keep their original name
// (Drive addresses them by FileID).
func preprocessFiles(files []*multipart.FileHeader, maxSize int64) (results []uploadResultItem, fileHeaders []drive.MultipartFile) {
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
		fileHeaders = append(fileHeaders, drive.MultipartFile{File: f, Filename: fh.Filename})
	}
	return results, fileHeaders
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
