package main

import (
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// Per-file result status values for the upload response.
const (
	statusFailure      = "failure" // pre-check rejection
	driveStatusSuccess = "Success" // Drive's success marker
)

// imageFormField is the multipart form field carrying the uploaded images.
const imageFormField = "images"

// driveClient is the subset of the Drive client the handlers use.
type driveClient interface {
	UploadGroupImages(userID, username, email, groupID, origin string, files []drive.MultipartFile) ([]drive.UploadGroupImageResponse, error)
	GetGroupImage(host, groupID, fileID string) (*drive.GetGroupImageResponse, error)
	GetBaseURLFromRoomOrigin(origin string) string
}

// Handler holds the upload-service dependencies.
type Handler struct {
	store        Store
	drive        driveClient
	maxFiles     int
	maxImageSize int64
}

// NewHandler wires the handler dependencies. maxFiles caps the number of images
// per request; maxImageSize is the per-image upload ceiling in bytes.
func NewHandler(store Store, dc driveClient, maxFiles int, maxImageSize int64) *Handler {
	return &Handler{store: store, drive: dc, maxFiles: maxFiles, maxImageSize: maxImageSize}
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
	if len(files) > h.maxFiles {
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
	for _, resp := range responses {
		item := uploadResultItem{Name: resp.File.Filename, Status: resp.Status, Error: resp.Error}
		if resp.Status == driveStatusSuccess {
			item.RelativePath = fmt.Sprintf("api/v1/rooms/%s/image/%s?drive_host=%s",
				resp.File.GroupID, resp.File.FileID, driveHost)
		}
		results = append(results, item)
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// HandleDownloadImage proxies a protected image: it resolves a signed URL from
// Drive, fetches the bytes, and streams them straight to the client.
func (h *Handler) HandleDownloadImage(c *gin.Context) {
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
		errhttp.Write(ctx, c, errcode.Unavailable("failed to retrieve image", errcode.WithCause(err)))
		return
	}
	defer img.Reader.Close()

	// GetGroupImage already defaults ContentType to application/octet-stream, so
	// stream the body straight through with no intermediate buffering.
	c.DataFromReader(http.StatusOK, img.ContentLength, img.ContentType, img.Reader, map[string]string{})
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
// handles the caller is responsible for closing.
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
