package cassandra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachment_RoundTrip(t *testing.T) {
	att := Attachment{
		ID: "drive-file-1", Title: "photo.png", Type: "file", Description: "team photo",
		TitleLink: "api/v1/rooms/r1/image/drive-file-1?drive_host=h", TitleLinkDownload: true,
		ImageURL: "api/v1/rooms/r1/image/drive-file-1?drive_host=h", ImageType: "image/png",
		ImageSize: 1234, ImageDimensions: &ImageDimensions{Width: 800, Height: 600}, ImagePreview: "b64",
	}
	data, err := json.Marshal(att)
	require.NoError(t, err)
	var got Attachment
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, att, got)

	audio := Attachment{ID: "f2", Title: "a.mp3", Type: "file", TitleLink: "u", TitleLinkDownload: true,
		AudioURL: "u", AudioType: "audio/mpeg", AudioSize: 99}
	ab, err := json.Marshal(audio)
	require.NoError(t, err)
	assert.NotContains(t, string(ab), "imageUrl")
	assert.Contains(t, string(ab), `"audioUrl":"u"`)
	assert.Contains(t, string(ab), `"id":"f2"`)
}

func TestAttachment_FileTypeRoundTrip(t *testing.T) {
	in := Attachment{ID: "f1", Title: "a.pdf", Type: "file", FileType: "application/pdf"}
	raw, err := json.Marshal(in)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"fileType":"application/pdf"`)
	var out Attachment
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, "application/pdf", out.FileType)
}

func TestEncodeAttachments_RoundTrip(t *testing.T) {
	in := []Attachment{
		{ID: "f1", Title: "a.png", Type: "file"},
		{ID: "f2", Title: "b.pdf", Type: "file"},
	}
	raw := EncodeAttachments(in)
	require.Len(t, raw, 2)
	out, skipped := DecodeAttachments(raw)
	assert.Zero(t, skipped)
	assert.Equal(t, in, out)
}

func TestEncodeAttachments_Empty(t *testing.T) {
	assert.Nil(t, EncodeAttachments(nil))
	assert.Nil(t, EncodeAttachments([]Attachment{}))
}

func TestDecodeAttachments(t *testing.T) {
	good, err := json.Marshal(Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)

	t.Run("good blob", func(t *testing.T) {
		out, skipped := DecodeAttachments([][]byte{good})
		require.Len(t, out, 1)
		assert.Equal(t, "f1", out[0].ID)
		assert.Equal(t, 0, skipped)
	})
	t.Run("malformed skipped", func(t *testing.T) {
		out, skipped := DecodeAttachments([][]byte{good, []byte("{not json")})
		require.Len(t, out, 1)
		assert.Equal(t, 1, skipped)
	})
	t.Run("empty", func(t *testing.T) {
		out, skipped := DecodeAttachments(nil)
		assert.Nil(t, out)
		assert.Equal(t, 0, skipped)
	})
}
