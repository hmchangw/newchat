package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	_ "image/png" // register PNG decoder for image.Decode
	"strings"

	xdraw "golang.org/x/image/draw"

	"github.com/hmchangw/chat/pkg/model"
)

const previewDim = 32

// previewMaxPixels caps the decoded pixel count for untrusted uploads:
// image.Decode allocates by decoded dimensions, so a decompression-bomb image
// (small compressed, enormous decoded) could spike memory before the 32x32
// resize. ~50 MP covers any legitimate photo.
const previewMaxPixels = 50_000_000

// imagePreview decodes an image once and returns a 32x32 blurred JPEG preview
// (base64) plus the source pixel dimensions. Non-image MIME types, undecodable
// bytes (e.g. heic), and over-large images yield ("", nil, nil).
func imagePreview(data []byte, mime string) (string, *model.ImageDimensions, error) {
	if !strings.HasPrefix(strings.ToLower(mime), "image/") {
		return "", nil, nil
	}
	// Check dimensions from the header before the full (allocating) decode.
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		//nolint:nilerr // an undecodable image yields no preview, not an error
		return "", nil, nil
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > previewMaxPixels {
		return "", nil, nil
	}
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		//nolint:nilerr // an undecodable image (e.g. heic) yields no preview, not an error
		return "", nil, nil
	}
	dims := &model.ImageDimensions{Width: src.Bounds().Dx(), Height: src.Bounds().Dy()}
	dst := image.NewRGBA(image.Rect(0, 0, previewDim, previewDim))
	xdraw.CatmullRom.Scale(dst, dst.Bounds(), src, src.Bounds(), xdraw.Over, nil)
	boxBlur(dst) // PERF INVARIANT: blur runs on the 32x32 dst, never the full-size src
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 50}); err != nil {
		return "", nil, fmt.Errorf("encode preview jpeg: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), dims, nil
}

// boxBlur applies one 3x3 averaging pass in place. Cost is O(pixels): on the
// 32x32 preview it is ~68us, but it is dimension-bound, so it MUST only ever be
// called on the downscaled preview, never on a full-size decoded upload.
func boxBlur(img *image.RGBA) {
	src := image.NewRGBA(img.Bounds())
	copy(src.Pix, img.Pix)
	b := img.Bounds()
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			var r, g, bl, a, n int
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					nx, ny := x+dx, y+dy
					if nx < b.Min.X || nx >= b.Max.X || ny < b.Min.Y || ny >= b.Max.Y {
						continue
					}
					c := src.RGBAAt(nx, ny)
					r += int(c.R)
					g += int(c.G)
					bl += int(c.B)
					a += int(c.A)
					n++
				}
			}
			// #nosec G115 -- r/g/bl/a are sums of uint8 pixel channels over n pixels, so each average (sum/n) is necessarily within 0–255
			img.SetRGBA(x, y, color.RGBA{uint8(r / n), uint8(g / n), uint8(bl / n), uint8(a / n)})
		}
	}
}
