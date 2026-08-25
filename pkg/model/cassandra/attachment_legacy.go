package cassandra

import (
	"encoding/json"
	"mime"
	"net/url"
	"path"
	"strings"
)

const (
	// legacyAPIPrefix is the route prefix every converted legacy URL is rewritten
	// onto: upload-service's GET /api/v1/file-upload/:fileId/:fileName.
	legacyAPIPrefix = "api/v1/"
	// legacyUploadSegment precedes the file id in a legacy download path.
	legacyUploadSegment = "file-upload"
	defaultFileType     = "application/octet-stream"
)

// legacyAttachment is the pre-migration snake_case attachment shape still
// present in Cassandra rows written by the old stack. Only the fields that
// survive conversion to Attachment are declared.
type legacyAttachment struct {
	Title             string `json:"title"`
	Type              string `json:"type"`
	Description       string `json:"description"`
	TitleLink         string `json:"title_link"`
	TitleLinkDownload bool   `json:"title_link_download"`

	ImageURL  string `json:"image_url"`
	ImageType string `json:"image_type"`
	AudioURL  string `json:"audio_url"`
	AudioType string `json:"audio_type"`
	VideoURL  string `json:"video_url"`
	VideoType string `json:"video_type"`
}

// sourceURL is the legacy download URL, from whichever media field carries it.
func (l *legacyAttachment) sourceURL() string {
	for _, u := range []string{l.TitleLink, l.ImageURL, l.AudioURL, l.VideoURL} {
		if u != "" {
			return u
		}
	}
	return ""
}

// mediaType is the declared MIME type, from whichever media family carries it.
func (l *legacyAttachment) mediaType() string {
	for _, t := range []string{l.ImageType, l.AudioType, l.VideoType} {
		if t != "" {
			return t
		}
	}
	return ""
}

// normalizeAttachment rewrites a in place when raw holds a legacy snake_case
// blob. An attachment carrying id or titleLink is already in the current shape,
// so it is left untouched — which is what keeps future Attachment fields safe
// and makes the conversion idempotent.
func normalizeAttachment(raw []byte, a *Attachment) {
	if a.ID != "" || a.TitleLink != "" {
		return
	}
	var l legacyAttachment
	if err := json.Unmarshal(raw, &l); err != nil {
		return
	}
	src := l.sourceURL()
	if src == "" {
		return
	}
	*a = convertLegacy(&l, src)
}

// convertLegacy builds the current-shape attachment from a legacy blob and its
// download URL. Legacy image geometry and thumbnails are intentionally dropped.
func convertLegacy(l *legacyAttachment, src string) Attachment {
	p := legacyURLPath(src)
	att := Attachment{
		ID:                legacyFileID(p),
		Title:             l.Title,
		Type:              l.Type,
		Description:       l.Description,
		TitleLink:         legacyDownloadURL(p),
		TitleLinkDownload: l.TitleLinkDownload,
		FileType:          strings.ToLower(strings.TrimSpace(l.mediaType())),
	}
	if att.Title == "" {
		att.Title = legacyFileName(p)
	}
	if att.Type == "" {
		att.Type = "file"
	}
	if att.FileType == "" {
		att.FileType = fileTypeFromName(att.Title)
	}
	return att
}

// legacyURLPath reduces an absolute legacy URL to its path, preserving
// percent-encoding. A relative URL is returned unchanged.
func legacyURLPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme == "" && u.Host == "") {
		return raw
	}
	return u.EscapedPath()
}

// legacyDownloadURL rewrites a legacy path onto the current download route. A
// path already carrying the prefix is returned as-is, so converting twice is a
// no-op.
func legacyDownloadURL(p string) string {
	trimmed := strings.TrimPrefix(p, "/")
	if strings.HasPrefix(trimmed, legacyAPIPrefix) {
		return trimmed
	}
	return legacyAPIPrefix + trimmed
}

// legacyFileID extracts the file id: the segment after "file-upload". Falls back
// to the second-to-last segment for an unrecognized layout, since the id always
// precedes the file name.
func legacyFileID(p string) string {
	segs := strings.Split(strings.Trim(p, "/"), "/")
	for i, s := range segs {
		if s == legacyUploadSegment && i+1 < len(segs) {
			return segs[i+1]
		}
	}
	if len(segs) >= 2 {
		return segs[len(segs)-2]
	}
	return ""
}

// legacyFileName is the percent-decoded last path segment, used when the legacy
// blob carries no title.
func legacyFileName(p string) string {
	base := path.Base(p)
	if name, err := url.PathUnescape(base); err == nil {
		return name
	}
	return base
}

// fileTypeFromName derives a MIME type from the file name's extension. Media
// type parameters (charset) are stripped so the result is the bare type.
func fileTypeFromName(name string) string {
	ext := strings.ToLower(path.Ext(name))
	if ext == "" {
		return defaultFileType
	}
	parsed, _, err := mime.ParseMediaType(mime.TypeByExtension(ext))
	if err != nil {
		return defaultFileType
	}
	return parsed
}
