package main

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makePNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{uint8(x * 4), uint8(y * 4), 128, 255})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

func TestImagePreview_PNG(t *testing.T) {
	out, dims, err := imagePreview(makePNG(t, 64, 48), "image/png")
	require.NoError(t, err)
	require.NotEmpty(t, out)
	require.NotNil(t, dims)
	assert.Equal(t, 64, dims.Width)
	assert.Equal(t, 48, dims.Height)

	raw, err := base64.StdEncoding.DecodeString(out)
	require.NoError(t, err)
	cfg, format, err := image.DecodeConfig(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.Equal(t, "jpeg", format)
	assert.Equal(t, 32, cfg.Width)
	assert.Equal(t, 32, cfg.Height)
}

func TestImagePreview_NonImage(t *testing.T) {
	out, dims, err := imagePreview([]byte("not an image"), "application/pdf")
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.Nil(t, dims)
}

func TestImagePreview_Undecodable(t *testing.T) {
	out, dims, err := imagePreview([]byte{0, 1, 2}, "image/heic")
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.Nil(t, dims)
}
