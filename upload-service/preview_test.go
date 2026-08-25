package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"strings"
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
	out, dims, err := imagePreview(bytes.NewReader(makePNG(t, 64, 48)), "image/png")
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
	out, dims, err := imagePreview(strings.NewReader("not an image"), "application/pdf")
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.Nil(t, dims)
}

func TestImagePreview_Undecodable(t *testing.T) {
	out, dims, err := imagePreview(bytes.NewReader([]byte{0, 1, 2}), "image/heic")
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.Nil(t, dims)
}

// The caller uploads the same reader straight after previewing it, so every exit
// path must leave it rewound — otherwise the file arrives at Drive truncated.
func TestImagePreview_RewindsReader(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		mime string
	}{
		{name: "decodable image", data: makePNG(t, 64, 48), mime: "image/png"},
		{name: "undecodable image", data: []byte{0, 1, 2}, mime: "image/heic"},
		{name: "non-image mime", data: []byte("not an image"), mime: "application/pdf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			_, _, err := imagePreview(r, tt.mime)
			require.NoError(t, err)

			got, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tt.data, got, "reader must be left at the start for the upload")
		})
	}
}

// seekFailReader wraps a real io.ReadSeeker but fails every Seek call from
// the failFrom'th call onward (1-indexed), so tests can trigger imagePreview's
// rewind-error branches without a genuinely broken reader.
type seekFailReader struct {
	io.ReadSeeker
	failFrom int
	seeks    int
	seekErr  error
}

func (r *seekFailReader) Seek(offset int64, whence int) (int64, error) {
	r.seeks++
	if r.seeks >= r.failFrom {
		return 0, r.seekErr
	}
	return r.ReadSeeker.Seek(offset, whence)
}

// imagePreview seeks r twice on the decodable-image path: once explicitly
// before image.Decode (undoing image.DecodeConfig's header read) and once
// more in the deferred rewind-on-exit. Either can fail independently, and a
// failing deferred rewind must never clobber an error that already occurred.
func TestImagePreview_RewindErrors(t *testing.T) {
	seekErr := errors.New("seek boom")

	tests := []struct {
		name       string
		failFrom   int
		wantErr    string
		wantNotErr string
		wantEmpty  bool
	}{
		{
			name:      "deferred rewind after preview fails",
			failFrom:  2,
			wantErr:   "rewind image after preview",
			wantEmpty: false,
		},
		{
			name:       "earlier error survives a failing deferred rewind",
			failFrom:   1,
			wantErr:    "rewind image before decode",
			wantNotErr: "rewind image after preview",
			wantEmpty:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &seekFailReader{
				ReadSeeker: bytes.NewReader(makePNG(t, 64, 48)),
				failFrom:   tt.failFrom,
				seekErr:    seekErr,
			}

			out, dims, err := imagePreview(r, "image/png")

			require.Error(t, err)
			assert.ErrorIs(t, err, seekErr)
			assert.ErrorContains(t, err, tt.wantErr)
			if tt.wantNotErr != "" {
				assert.NotContains(t, err.Error(), tt.wantNotErr)
			}
			assert.Equal(t, 2, r.seeks, "both the before-decode and deferred rewinds must be attempted")

			if tt.wantEmpty {
				assert.Empty(t, out)
				assert.Nil(t, dims)
			} else {
				assert.NotEmpty(t, out, "a deferred rewind failure must not erase an already-computed preview")
				assert.NotNil(t, dims)
			}
		})
	}
}
