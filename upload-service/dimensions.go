package main

import (
	"fmt"
	"image"
	_ "image/jpeg" // register JPEG decoder for image.DecodeConfig
	_ "image/png"  // register PNG decoder for image.DecodeConfig
	"io"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// imageDimensions returns an uploaded image's pixel size. It reads only the
// header via image.DecodeConfig — never the pixels — so a large upload is never
// held in memory, and it always rewinds r before returning so the caller can
// hand the same reader to the upload. A non-image MIME type, undecodable bytes
// (e.g. heic) and a degenerate header all yield (nil, nil): dimensions are
// best-effort metadata, not a reason to fail an upload.
//
// The blank image/jpeg and image/png imports above are what teach
// image.DecodeConfig those two formats; dropping either silently costs every
// upload of that format its dimensions.
func imageDimensions(r io.ReadSeeker, mime string) (*model.ImageDimensions, error) {
	if !strings.HasPrefix(strings.ToLower(mime), "image/") {
		return nil, nil
	}
	cfg, _, decodeErr := image.DecodeConfig(r)
	// Rewind before acting on decodeErr: DecodeConfig consumed part of r either
	// way, and the caller still has to upload it. A reader we cannot rewind is
	// unusable for that upload, which makes it the one real error here.
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind image after reading dimensions: %w", err)
	}
	// image/jpeg does not reject a zero-sized SOF the way image/png rejects a
	// zero-sized IHDR, so the degenerate case has to be caught here.
	if decodeErr != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		//nolint:nilerr // an image we cannot read yields no dimensions, not an error
		return nil, nil
	}
	return &model.ImageDimensions{Width: cfg.Width, Height: cfg.Height}, nil
}
