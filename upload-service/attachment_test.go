package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestFileURL(t *testing.T) {
	assert.Equal(t, "api/v1/file/rooms/room-1/file/f1?drive_host=http://drive",
		fileURL("room-1", "f1", "http://drive"))
}

func TestBuildAttachment_Generic(t *testing.T) {
	m := fileMeta{id: "f1", name: "report.pdf", mime: "application/pdf", size: 10}
	att := buildAttachment(m, "Q2", "http://link", "", nil)
	assert.Equal(t, "f1", att.ID)
	assert.Equal(t, "report.pdf", att.Title)
	assert.Equal(t, "file", att.Type)
	assert.Equal(t, "Q2", att.Description)
	assert.Equal(t, "http://link", att.TitleLink)
	assert.True(t, att.TitleLinkDownload)
	assert.Empty(t, att.ImageURL)
	assert.Empty(t, att.AudioURL)
}

func TestBuildAttachment_Image(t *testing.T) {
	m := fileMeta{id: "f1", name: "p.png", mime: "image/png", size: 99}
	dims := &model.ImageDimensions{Width: 800, Height: 600}
	att := buildAttachment(m, "", "http://link", "b64preview", dims)
	assert.Equal(t, "http://link", att.ImageURL)
	assert.Equal(t, "image/png", att.ImageType)
	assert.Equal(t, int64(99), att.ImageSize)
	assert.Equal(t, "b64preview", att.ImagePreview)
	require.NotNil(t, att.ImageDimensions)
	assert.Equal(t, 800, att.ImageDimensions.Width)
	assert.Empty(t, att.AudioURL)
}

func TestBuildAttachment_Audio(t *testing.T) {
	m := fileMeta{id: "f1", name: "a.mp3", mime: "audio/mpeg", size: 5}
	att := buildAttachment(m, "", "http://link", "", nil)
	assert.Equal(t, "http://link", att.AudioURL)
	assert.Equal(t, "audio/mpeg", att.AudioType)
	assert.Equal(t, int64(5), att.AudioSize)
	assert.Empty(t, att.ImageURL)
}

func TestBuildAttachment_Video(t *testing.T) {
	m := fileMeta{id: "f1", name: "v.mp4", mime: "video/mp4", size: 7}
	att := buildAttachment(m, "", "http://link", "", nil)
	assert.Equal(t, "http://link", att.VideoURL)
	assert.Equal(t, "video/mp4", att.VideoType)
	assert.Equal(t, int64(7), att.VideoSize)
}

func TestBuildAttachment_FileTypeAllFamilies(t *testing.T) {
	cases := []struct{ mime string }{
		{"application/pdf"}, {"image/png"}, {"audio/mpeg"}, {"video/mp4"},
	}
	for _, tc := range cases {
		t.Run(tc.mime, func(t *testing.T) {
			att := buildAttachment(fileMeta{id: "f1", name: "x", mime: tc.mime, size: 1}, "", "http://link", "", nil)
			assert.Equal(t, tc.mime, att.FileType)
		})
	}
	// Mixed case is normalized like the other media fields.
	att := buildAttachment(fileMeta{id: "f1", name: "x", mime: "Image/PNG", size: 1}, "", "http://link", "", nil)
	assert.Equal(t, "image/png", att.FileType)
}

func TestBuildAttachment_MixedCaseMIME(t *testing.T) {
	img := buildAttachment(fileMeta{id: "f1", name: "p.png", mime: "Image/PNG", size: 1}, "", "http://link", "", nil)
	assert.Equal(t, "http://link", img.ImageURL)
	assert.Equal(t, "image/png", img.ImageType)

	aud := buildAttachment(fileMeta{id: "f2", name: "a.mp3", mime: "AUDIO/MPEG", size: 2}, "", "http://link", "", nil)
	assert.Equal(t, "http://link", aud.AudioURL)
	assert.Equal(t, "audio/mpeg", aud.AudioType)
}
