package cassandra

import "encoding/json"

// ImageDimensions is the pixel size of an uploaded image attachment.
type ImageDimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// Attachment is the render-ready descriptor for an uploaded file. upload-service
// returns it over HTTP; the frontend base64-encodes its JSON into each
// Message.Attachments blob; history-service decodes those blobs back into this
// type. ID is the Drive file id; Title is the file name (no separate name).
type Attachment struct {
	ID                string `json:"id"`
	Title             string `json:"title"`
	Type              string `json:"type"`
	Description       string `json:"description,omitempty"`
	TitleLink         string `json:"titleLink"`
	TitleLinkDownload bool   `json:"titleLinkDownload"`

	// FileType is the canonical lowercased MIME type, present on every attachment
	// family (image/audio/video/generic). The media-specific *Type fields remain
	// for the existing frontend.
	FileType string `json:"fileType,omitempty"`

	ImageURL        string           `json:"imageUrl,omitempty"`
	ImageType       string           `json:"imageType,omitempty"`
	ImageSize       int64            `json:"imageSize,omitempty"`
	ImageDimensions *ImageDimensions `json:"imageDimensions,omitempty"`
	ImagePreview    string           `json:"imagePreview,omitempty"`

	AudioURL  string `json:"audioUrl,omitempty"`
	AudioType string `json:"audioType,omitempty"`
	AudioSize int64  `json:"audioSize,omitempty"`

	VideoURL  string `json:"videoUrl,omitempty"`
	VideoType string `json:"videoType,omitempty"`
	VideoSize int64  `json:"videoSize,omitempty"`
}

// DecodeAttachments decodes a LIST<BLOB> attachments column (each blob is one
// JSON-encoded Attachment) into typed objects. It is lenient: a malformed blob
// is skipped and counted (returned as skipped) rather than failing the batch, so
// one bad row can't break a history load or a live delivery. Returns (nil, 0)
// for empty input.
func DecodeAttachments(raw [][]byte) (out []Attachment, skipped int) {
	if len(raw) == 0 {
		return nil, 0
	}
	out = make([]Attachment, 0, len(raw))
	for _, b := range raw {
		var a Attachment
		if err := json.Unmarshal(b, &a); err != nil {
			skipped++
			continue
		}
		out = append(out, a)
	}
	return out, skipped
}

// EncodeAttachments is the inverse of DecodeAttachments: it marshals each
// Attachment into one JSON blob for the LIST<BLOB> attachments column. Returns
// nil for empty input. A marshal error on a plain struct is not expected; such
// an element is skipped so one bad entry can't fail the batch.
func EncodeAttachments(atts []Attachment) [][]byte {
	if len(atts) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(atts))
	for i := range atts {
		b, err := json.Marshal(atts[i])
		if err != nil {
			continue
		}
		out = append(out, b)
	}
	return out
}
