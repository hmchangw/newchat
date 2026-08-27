package cassandra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyIssue374Blob is the exact payload reported in issue #374.
const legacyIssue374Blob = `{
  "image_dimensions": {"height": 215, "width": 426},
  "image_preview": "(base64 thumbnail)",
  "image_size": 29283,
  "image_type": "image/png",
  "image_url": "/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "title": "image (2).png",
  "title_link": "/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
  "title_link_download": true,
  "type": "file"
}`

func TestDecodeAttachments_LegacyIssue374(t *testing.T) {
	out, skipped := DecodeAttachments([][]byte{[]byte(legacyIssue374Blob)})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)

	assert.Equal(t, Attachment{
		ID:                "xh3e4jnJDhEvEy7rk",
		Title:             "image (2).png",
		Type:              "file",
		TitleLink:         "api/v1/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png",
		TitleLinkDownload: true,
		FileType:          "image/png",
	}, out[0])
}

// The legacy thumbnail and geometry are dropped by design (issue #374), so the
// serialized attachment must not carry them back to the frontend.
func TestDecodeAttachments_LegacyDropsImageExtras(t *testing.T) {
	out, _ := DecodeAttachments([][]byte{[]byte(legacyIssue374Blob)})
	require.Len(t, out, 1)

	raw, err := json.Marshal(out[0])
	require.NoError(t, err)
	for _, key := range []string{"imageUrl", "imageType", "imageSize", "imageDimensions", "imagePreview"} {
		assert.NotContains(t, string(raw), key)
	}
}

func TestDecodeAttachments_LegacyVariants(t *testing.T) {
	tests := []struct {
		name string
		blob string
		want Attachment
	}{
		{
			name: "generic file without a media type falls back to the extension",
			blob: `{"title":"report.pdf","type":"file","title_link":"/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "unknown extension falls back to octet-stream",
			blob: `{"title":"CHANGELOG","type":"file","title_link":"/file-upload/abc123/CHANGELOG","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "CHANGELOG", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/CHANGELOG",
				TitleLinkDownload: true, FileType: defaultFileType,
			},
		},
		{
			name: "audio uses audio_url and audio_type",
			blob: `{"title":"clip.mp3","type":"file","audio_url":"/file-upload/aud1/clip.mp3","audio_type":"audio/mpeg","title_link_download":true}`,
			want: Attachment{
				ID: "aud1", Title: "clip.mp3", Type: "file",
				TitleLink:         "api/v1/file-upload/aud1/clip.mp3",
				TitleLinkDownload: true, FileType: "audio/mpeg",
			},
		},
		{
			name: "absolute URL is reduced to its path",
			blob: `{"title":"report.pdf","type":"file","title_link":"https://legacy.example.com/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "an already-prefixed path is not prefixed twice",
			blob: `{"title":"report.pdf","type":"file","title_link":"/api/v1/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "unrecognized layout takes the segment before the file name",
			blob: `{"title":"report.pdf","type":"file","title_link":"/ufs/uploads/xyz789/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "xyz789", Title: "report.pdf", Type: "file",
				TitleLink:         "api/v1/ufs/uploads/xyz789/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "missing title is taken from the path and percent-decoded",
			blob: `{"type":"file","title_link":"/file-upload/abc123/my%20report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "my report.pdf", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/my%20report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "missing type defaults to file",
			blob: `{"title":"report.pdf","title_link":"/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "description survives the conversion",
			blob: `{"title":"report.pdf","type":"file","description":"Q3 numbers","title_link":"/file-upload/abc123/report.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "report.pdf", Type: "file", Description: "Q3 numbers",
				TitleLink:         "api/v1/file-upload/abc123/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "declared media type is lowercased",
			blob: `{"title":"photo.PNG","type":"file","image_type":"IMAGE/PNG","image_url":"/file-upload/abc123/photo.PNG","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "photo.PNG", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/photo.PNG",
				TitleLinkDownload: true, FileType: "image/png",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, skipped := DecodeAttachments([][]byte{[]byte(tt.blob)})
			require.Len(t, out, 1)
			assert.Zero(t, skipped)
			assert.Equal(t, tt.want, out[0])
		})
	}
}

// A current-shape attachment must survive decoding byte-for-byte, including the
// image fields the legacy conversion drops.
func TestDecodeAttachments_CurrentFormatUntouched(t *testing.T) {
	current := Attachment{
		ID: "f1", Title: "photo.png", Type: "file", Description: "team photo",
		TitleLink: "api/v1/file/rooms/r1/file/f1?drive_host=h", TitleLinkDownload: true,
		FileType: "image/png",
		ImageURL: "api/v1/file/rooms/r1/file/f1?drive_host=h", ImageType: "image/png",
		ImageSize: 1234, ImageDimensions: &ImageDimensions{Width: 800, Height: 600},
		ImagePreview: "b64",
	}
	raw, err := json.Marshal(current)
	require.NoError(t, err)

	out, skipped := DecodeAttachments([][]byte{raw})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)
	assert.Equal(t, current, out[0])
}

// An attachment with a titleLink but no id is still current-format, not legacy.
func TestDecodeAttachments_TitleLinkOnlyIsNotLegacy(t *testing.T) {
	raw := []byte(`{"title":"a.png","type":"file","titleLink":"api/v1/file/rooms/r1/file/f1"}`)

	out, skipped := DecodeAttachments([][]byte{raw})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)
	assert.Empty(t, out[0].ID)
	assert.Equal(t, "api/v1/file/rooms/r1/file/f1", out[0].TitleLink)
}

// A blob with neither a current-shape nor a legacy URL has nothing to convert
// and must be returned as decoded rather than mangled.
func TestDecodeAttachments_NoURLLeftAsDecoded(t *testing.T) {
	raw := []byte(`{"title":"a.png","type":"file"}`)

	out, skipped := DecodeAttachments([][]byte{raw})
	require.Len(t, out, 1)
	assert.Zero(t, skipped)
	assert.Equal(t, Attachment{Title: "a.png", Type: "file"}, out[0])
}

// A mixed column keeps input order, converts only the legacy entry, and still
// counts the malformed blob as skipped.
func TestDecodeAttachments_MixedFormats(t *testing.T) {
	current, err := json.Marshal(Attachment{ID: "f2", Title: "b.pdf", Type: "file",
		TitleLink: "api/v1/file/rooms/r1/file/f2", TitleLinkDownload: true})
	require.NoError(t, err)

	out, skipped := DecodeAttachments([][]byte{
		[]byte(legacyIssue374Blob),
		current,
		[]byte("{not json"),
	})

	require.Len(t, out, 2)
	assert.Equal(t, 1, skipped)
	assert.Equal(t, "xh3e4jnJDhEvEy7rk", out[0].ID)
	assert.Equal(t, "api/v1/file-upload/xh3e4jnJDhEvEy7rk/image%20(2).png", out[0].TitleLink)
	assert.Equal(t, "f2", out[1].ID)
	assert.Equal(t, "api/v1/file/rooms/r1/file/f2", out[1].TitleLink)
}

// Re-encoding a converted attachment and decoding it again must be a fixed
// point — message-worker re-encodes quoted-parent snapshots this way.
func TestDecodeAttachments_LegacyConversionIsIdempotent(t *testing.T) {
	first, _ := DecodeAttachments([][]byte{[]byte(legacyIssue374Blob)})
	require.Len(t, first, 1)

	second, skipped := DecodeAttachments(EncodeAttachments(first))
	require.Len(t, second, 1)
	assert.Zero(t, skipped)
	assert.Equal(t, first[0], second[0])
}

// Degenerate legacy blobs must degrade gracefully: never a panic, never a
// half-converted attachment presented as if it were complete.
func TestDecodeAttachments_LegacyDegenerate(t *testing.T) {
	tests := []struct {
		name string
		blob string
		want Attachment
	}{
		{
			name: "wrong-typed legacy field leaves the plain decode in place",
			blob: `{"title":"a.png","type":"file","title_link":123}`,
			want: Attachment{Title: "a.png", Type: "file"},
		},
		{
			name: "single-segment path yields no file id",
			blob: `{"title":"report.pdf","type":"file","title_link":"/report.pdf","title_link_download":true}`,
			want: Attachment{
				Title: "report.pdf", Type: "file",
				TitleLink:         "api/v1/report.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "undecodable escape in the file name is kept verbatim as the title",
			blob: `{"type":"file","title_link":"/file-upload/abc123/bad%zz.pdf","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "bad%zz.pdf", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/bad%zz.pdf",
				TitleLinkDownload: true, FileType: "application/pdf",
			},
		},
		{
			name: "unmapped extension falls back to octet-stream",
			blob: `{"title":"archive.zzz","type":"file","title_link":"/file-upload/abc123/archive.zzz","title_link_download":true}`,
			want: Attachment{
				ID: "abc123", Title: "archive.zzz", Type: "file",
				TitleLink:         "api/v1/file-upload/abc123/archive.zzz",
				TitleLinkDownload: true, FileType: defaultFileType,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, skipped := DecodeAttachments([][]byte{[]byte(tt.blob)})
			require.Len(t, out, 1)
			assert.Zero(t, skipped)
			assert.Equal(t, tt.want, out[0])
		})
	}
}

// Every converted URL points at HandleDownloadMinioS3File, which always answers
// with Content-Disposition: attachment, so titleLinkDownload must hold the
// "always true" contract in docs/client-api.md regardless of the legacy value.
func TestDecodeAttachments_LegacyAlwaysDownloadable(t *testing.T) {
	tests := []struct {
		name string
		blob string
	}{
		{
			name: "legacy flag absent",
			blob: `{"title":"report.pdf","type":"file","title_link":"/file-upload/abc123/report.pdf"}`,
		},
		{
			name: "legacy flag explicitly false",
			blob: `{"title":"report.pdf","type":"file","title_link":"/file-upload/abc123/report.pdf","title_link_download":false}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, skipped := DecodeAttachments([][]byte{[]byte(tt.blob)})
			require.Len(t, out, 1)
			assert.Zero(t, skipped)
			assert.True(t, out[0].TitleLinkDownload)
		})
	}
}
