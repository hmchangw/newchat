package main

import (
	"bytes"
	_ "embed"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// The fixtures are embedded rather than encoded here so that NO test file in
// this package imports image/png or image/jpeg: those blank imports live in
// dimensions.go alone, and encoding a fixture here would re-register both
// formats for the test binary — letting a deleted import pass every test while
// every production upload of that format silently lost its dimensions.
//
//go:embed testdata/64x48.png
var png64x48 []byte

//go:embed testdata/32x32.jpg
var jpeg32x32 []byte

func TestImageDimensions(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		mime string
		want *model.ImageDimensions
	}{
		{
			name: "png",
			data: png64x48,
			mime: "image/png",
			want: &model.ImageDimensions{Width: 64, Height: 48},
		},
		{
			// Guards the blank image/jpeg import: without it DecodeConfig does not
			// know JPEG, and the most common photo format loses its dimensions.
			name: "jpeg",
			data: jpeg32x32,
			mime: "image/jpeg",
			want: &model.ImageDimensions{Width: 32, Height: 32},
		},
		{
			name: "mixed-case mime",
			data: png64x48,
			mime: "Image/PNG",
			want: &model.ImageDimensions{Width: 64, Height: 48},
		},
		{name: "non-image mime", data: []byte("not an image"), mime: "application/pdf"},
		{name: "undecodable image", data: []byte{0, 1, 2}, mime: "image/heic"},
		{name: "empty image", data: []byte{}, mime: "image/png"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			got, err := imageDimensions(r, tt.mime)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// The caller uploads this same reader straight after measuring it, so
			// every exit path must leave it rewound — a reader left mid-header
			// sends Drive a truncated file.
			rest, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tt.data, rest, "reader must be left at the start for the upload")
		})
	}
}

// seekFailReader is a readable image whose Seek always fails.
type seekFailReader struct {
	io.Reader
	seeks   int
	seekErr error
}

func (r *seekFailReader) Seek(int64, int) (int64, error) {
	r.seeks++
	return 0, r.seekErr
}

// A reader that cannot be rewound is unusable for the upload that follows, so
// the read must surface an error rather than hand back a half-consumed file.
func TestImageDimensions_RewindError(t *testing.T) {
	seekErr := errors.New("seek boom")
	r := &seekFailReader{Reader: bytes.NewReader(png64x48), seekErr: seekErr}

	_, err := imageDimensions(r, "image/png")

	require.Error(t, err)
	assert.ErrorIs(t, err, seekErr)
	assert.ErrorContains(t, err, "rewind image after reading dimensions")
	assert.Equal(t, 1, r.seeks, "the header read is the only thing that moves the reader")
}

// A non-image mime returns before any read or seek, so it must not touch the
// reader at all.
func TestImageDimensions_NonImageNeverSeeks(t *testing.T) {
	r := &seekFailReader{Reader: strings.NewReader("not an image"), seekErr: errors.New("seek boom")}
	got, err := imageDimensions(r, "application/pdf")
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Zero(t, r.seeks)
}
