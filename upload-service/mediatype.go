package main

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
)

// mediaTypeFilter decides whether an uploaded MIME type is allowed: blacklist
// first (deny wins), then whitelist (when non-empty, the type must match). Each
// list is split into an exact-match set (O(1)) and a slice of wildcard patterns.
type mediaTypeFilter struct {
	whitelistExact    map[string]struct{}
	whitelistWildcard []string
	blacklistExact    map[string]struct{}
	blacklistWildcard []string
}

func newMediaTypeFilter(whitelist, blacklist string) *mediaTypeFilter {
	we, ww := parseMediaTypes(whitelist)
	be, bw := parseMediaTypes(blacklist)
	return &mediaTypeFilter{
		whitelistExact:    we,
		whitelistWildcard: ww,
		blacklistExact:    be,
		blacklistWildcard: bw,
	}
}

// parseMediaTypes splits a CSV into an exact-match set and a wildcard slice
// ("type/*" or bare "*"/"*/*"). Entries are normalized; blanks are dropped.
func parseMediaTypes(csv string) (exact map[string]struct{}, wildcard []string) {
	exact = make(map[string]struct{})
	for _, p := range strings.Split(csv, ",") {
		p = normalizeMediaType(p)
		if p == "" {
			continue
		}
		if p == "*" || p == "*/*" || strings.HasSuffix(p, "/*") {
			wildcard = append(wildcard, p)
			continue
		}
		exact[p] = struct{}{}
	}
	return exact, wildcard
}

func (f *mediaTypeFilter) allowed(mime string) bool {
	m := normalizeMediaType(mime)
	if matchSet(f.blacklistExact, f.blacklistWildcard, m) {
		return false
	}
	if len(f.whitelistExact) == 0 && len(f.whitelistWildcard) == 0 {
		return true
	}
	return matchSet(f.whitelistExact, f.whitelistWildcard, m)
}

// matchSet returns true if mime is in the exact set (O(1)) or matches any
// wildcard pattern in the slice.
func matchSet(exact map[string]struct{}, wildcard []string, mime string) bool {
	if _, ok := exact[mime]; ok {
		return true
	}
	for _, w := range wildcard {
		if matchMediaType(w, mime) {
			return true
		}
	}
	return false
}

// matchMediaType supports "type/*" prefix wildcard and bare "*".
func matchMediaType(pattern, mime string) bool {
	if pattern == "*" || pattern == "*/*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		return strings.HasPrefix(mime, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == mime
}

// normalizeMediaType lowercases, trims, and drops any parameters after the first
// ";" (e.g. "Image/SVG+XML; charset=utf-8" → "image/svg+xml") so a parameterized
// Content-Type can't slip past an exact-match allow/deny rule.
func normalizeMediaType(v string) string {
	if base, _, ok := strings.Cut(v, ";"); ok {
		v = base
	}
	return strings.ToLower(strings.TrimSpace(v))
}

// extensionMediaTypes maps a lowercase extension to its MIME type for the types
// Go's own table lacks. The runtime image is bare alpine with no /etc/mime.types,
// so mime.TypeByExtension knows only ~16 builtin entries — without this table
// every Office document, archive and media file resolves to nothing.
var extensionMediaTypes = map[string]string{
	".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	".doc":  "application/msword",
	".xls":  "application/vnd.ms-excel",
	".ppt":  "application/vnd.ms-powerpoint",
	".odt":  "application/vnd.oasis.opendocument.text",
	".ods":  "application/vnd.oasis.opendocument.spreadsheet",
	".odp":  "application/vnd.oasis.opendocument.presentation",
	".zip":  "application/zip",
	".7z":   "application/x-7z-compressed",
	".rar":  "application/vnd.rar",
	".gz":   "application/gzip",
	".tar":  "application/x-tar",
	".txt":  "text/plain",
	".log":  "text/plain",
	".csv":  "text/csv",
	".md":   "text/markdown",
	".rtf":  "application/rtf",
	".svg":  "image/svg+xml",
	".heic": "image/heic",
	".heif": "image/heif",
	".bmp":  "image/bmp",
	".tif":  "image/tiff",
	".tiff": "image/tiff",
	".mp3":  "audio/mpeg",
	".m4a":  "audio/mp4",
	".wav":  "audio/wav",
	".ogg":  "audio/ogg",
	".aac":  "audio/aac",
	".mp4":  "video/mp4",
	".mov":  "video/quicktime",
	".webm": "video/webm",
	".mkv":  "video/x-matroska",
	".avi":  "video/x-msvideo",
}

// mediaTypeByExtension resolves a file name's extension to a MIME type: our own
// table first, then Go's builtin one. Returns "" when neither knows it.
func mediaTypeByExtension(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" {
		return ""
	}
	if mt, ok := extensionMediaTypes[ext]; ok {
		return mt
	}
	return normalizeMediaType(mime.TypeByExtension(ext))
}

// sniffLen is the number of leading bytes http.DetectContentType inspects.
const sniffLen = 512

// sniffMediaType detects a MIME type from a file's first bytes and rewinds r so
// the caller can still upload it. Only the header is read, so a large upload is
// never held in memory; a reader we cannot rewind is unusable for the upload
// that follows, which makes it the one real error here (as in imageDimensions).
func sniffMediaType(r io.ReadSeeker) (string, error) {
	head := make([]byte, sniffLen)
	n, err := io.ReadFull(r, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", fmt.Errorf("read file header for type detection: %w", err)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind file after type detection: %w", err)
	}
	return normalizeMediaType(http.DetectContentType(head[:n])), nil
}
