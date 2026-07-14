package main

import (
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// fileMeta is the processed metadata for an uploaded file.
type fileMeta struct {
	id   string
	name string
	mime string
	size int64
}

// fileURL builds the relative download URL for the protected file-download route.
func fileURL(roomID, fileID, driveHost string) string {
	return fmt.Sprintf("api/v1/file/rooms/%s/file/%s?drive_host=%s", roomID, fileID, driveHost)
}

// buildAttachment assembles the render-ready attachment, adding media-specific
// fields based on the MIME prefix.
func buildAttachment(m fileMeta, description, url, imagePreview string, dims *model.ImageDimensions) model.Attachment {
	mime := strings.ToLower(strings.TrimSpace(m.mime))
	att := model.Attachment{
		ID:                m.id,
		Title:             m.name,
		Type:              "file",
		Description:       description,
		TitleLink:         url,
		TitleLinkDownload: true,
		FileType:          mime,
	}
	switch {
	case strings.HasPrefix(mime, "image/"):
		att.ImageURL = url
		att.ImageType = mime
		att.ImageSize = m.size
		att.ImageDimensions = dims
		att.ImagePreview = imagePreview
	case strings.HasPrefix(mime, "audio/"):
		att.AudioURL = url
		att.AudioType = mime
		att.AudioSize = m.size
	case strings.HasPrefix(mime, "video/"):
		att.VideoURL = url
		att.VideoType = mime
		att.VideoSize = m.size
	}
	return att
}
