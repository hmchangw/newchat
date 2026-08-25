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
