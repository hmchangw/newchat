package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

const (
	objectPrefix     = "chat.go/chat-versions/"
	configFileField  = "configFile"
	executeFileField = "executeFile"
)

// objectKey namespaces a version file within the shared bucket.
func objectKey(fileName string) string { return objectPrefix + fileName }

// validFileName rejects empty or path-unsafe names (traversal / separators).
func validFileName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return false
	}
	return true
}

// HandleUpload stores a configFile (.yaml/.yml) + executeFile pair, streaming each
// straight to MinIO. No size cap — streaming keeps memory bounded regardless of size.
func (h *Handler) HandleUpload(c *gin.Context) {
	ctx := c.Request.Context()

	cfgFile, err := c.FormFile(configFileField)
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("configFile is required"))
		return
	}
	exeFile, err := c.FormFile(executeFileField)
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("executeFile is required"))
		return
	}
	if cfgFile.Size == 0 || exeFile.Size == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("uploaded files must not be empty"))
		return
	}
	if !hasYAMLExt(cfgFile.Filename) {
		errhttp.Write(ctx, c, errcode.BadRequest("configFile must be a .yaml or .yml file"))
		return
	}

	if err := h.storeFormFile(ctx, cfgFile, "application/x-yaml"); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	if err := h.storeFormFile(ctx, exeFile, "application/octet-stream"); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}

// storeFormFile streams one multipart part to MinIO and drops any stale cached copy.
// The stored content type comes from the uploaded part's Content-Type header;
// fallbackContentType is used only when the client did not declare one.
func (h *Handler) storeFormFile(ctx context.Context, fh *multipart.FileHeader, fallbackContentType string) error {
	f, err := fh.Open()
	if err != nil {
		return fmt.Errorf("open upload %q: %w", fh.Filename, err)
	}
	defer f.Close()
	contentType := fh.Header.Get("Content-Type")
	if contentType == "" {
		contentType = fallbackContentType
	}
	key := objectKey(fh.Filename)
	if err := h.store.Put(ctx, key, f, fh.Size, contentType); err != nil {
		return fmt.Errorf("store upload %q: %w", fh.Filename, err)
	}
	h.cache.remove(key)
	return nil
}

func hasYAMLExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".yaml" || ext == ".yml"
}

// HandleDownload serves an artifact by name from the cache, or MinIO on a miss.
func (h *Handler) HandleDownload(c *gin.Context) {
	ctx := c.Request.Context()
	fileName := c.Param("fileName")
	if !validFileName(fileName) {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid fileName"))
		return
	}
	key := objectKey(fileName)

	if blob, ok := h.cache.get(key); ok {
		serveBytes(c, fileName, blob)
		return
	}

	blob, cacheable, err := h.cache.loadCacheable(key, func() (cachedBlob, bool, error) {
		return h.loadObject(ctx, key)
	})
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("version not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("load version %q: %w", fileName, err))
		return
	}
	if cacheable {
		serveBytes(c, fileName, blob)
		return
	}
	h.streamObject(ctx, c, fileName, key)
}

// loadObject opens key and, when it fits the cache cap, reads its whole body into a
// cachedBlob (cacheable=true). Oversized objects return cacheable=false with only the
// content-type, so the caller streams them instead.
func (h *Handler) loadObject(ctx context.Context, key string) (cachedBlob, bool, error) {
	rc, info, err := h.store.Open(ctx, key)
	if err != nil {
		return cachedBlob{}, false, err
	}
	defer rc.Close()
	if info.Size > h.cache.maxObjectBytes {
		return cachedBlob{contentType: info.ContentType}, false, nil
	}
	body := make([]byte, info.Size)
	if _, err := io.ReadFull(rc, body); err != nil {
		return cachedBlob{}, false, fmt.Errorf("read object body: %w", err)
	}
	return cachedBlob{body: body, contentType: info.ContentType}, true, nil
}

// streamObject re-opens key and streams it straight to the client, uncached.
func (h *Handler) streamObject(ctx context.Context, c *gin.Context, fileName, key string) {
	rc, info, err := h.store.Open(ctx, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("version not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("open version %q: %w", fileName, err))
		return
	}
	defer rc.Close()
	c.DataFromReader(http.StatusOK, info.Size, info.ContentType, rc, map[string]string{
		"Content-Disposition": contentDisposition(fileName),
	})
}

// serveBytes writes a cached blob with the same headers the streamed path sets:
// Content-Type, an explicit Content-Length, and the download disposition.
func serveBytes(c *gin.Context, fileName string, blob cachedBlob) {
	c.Header("Content-Disposition", contentDisposition(fileName))
	c.Header("Content-Length", strconv.Itoa(len(blob.body)))
	c.Data(http.StatusOK, blob.contentType, blob.body)
}

func contentDisposition(fileName string) string {
	return fmt.Sprintf("attachment; filename=%q", fileName)
}
