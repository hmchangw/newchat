package main

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMediaTypeFilter_Allowed(t *testing.T) {
	tests := []struct {
		name, whitelist, blacklist, mime string
		want                             bool
	}{
		{"empty allows all", "", "", "application/pdf", true},
		{"blacklist blocks", "", "image/svg+xml", "image/svg+xml", false},
		{"blacklist blocks with params", "", "image/svg+xml", "image/svg+xml; charset=utf-8", false},
		{"blacklist case-insensitive", "", "image/svg+xml", "IMAGE/SVG+XML", false},
		{"whitelist allows match", "image/png", "", "image/png", true},
		{"whitelist excludes others", "image/png", "", "image/jpeg", false},
		{"whitelist wildcard", "image/*", "", "image/jpeg", true},
		{"blacklist beats whitelist", "image/*", "image/svg+xml", "image/svg+xml", false},
		{"bare star", "*", "", "anything/here", true},
		{"trims spaces", " image/png , image/jpeg ", "", "image/jpeg", true},
		{"exact map hit among mixed list", "image/png,text/*", "", "image/png", true},
		{"wildcard hit among mixed list", "image/png,text/*", "", "text/csv", true},
		{"exact miss with no wildcard match", "image/png,text/*", "", "image/gif", false},
		{"deny wins over allow-all", "*", "application/x-msdownload", "application/x-msdownload", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := newMediaTypeFilter(tc.whitelist, tc.blacklist).allowed(tc.mime); got != tc.want {
				t.Fatalf("allowed(%q) = %v, want %v", tc.mime, got, tc.want)
			}
		})
	}
}

func TestMediaTypeByExtension(t *testing.T) {
	tests := []struct {
		name, filename, want string
	}{
		{"docx from our table", "report.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{"xlsx from our table", "budget.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		{"txt is absent from Go's builtin table", "notes.txt", "text/plain"},
		{"zip", "bundle.zip", "application/zip"},
		{"svg", "logo.svg", "image/svg+xml"},
		{"mp4", "clip.mp4", "video/mp4"},
		{"mp3", "song.mp3", "audio/mpeg"},
		{"uppercase extension", "REPORT.DOCX", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{"falls through to Go's builtin table", "photo.png", "image/png"},
		{"builtin charset parameter is stripped", "page.html", "text/html"},
		{"last extension wins", "archive.tar.gz", "application/gzip"},
		{"dots in the stem", "my.report.final.pdf", "application/pdf"},
		{"unknown extension", "data.zzz", ""},
		{"no extension", "README", ""},
		{"empty name", "", ""},
		{"dotfile has no extension to speak of", ".gitignore", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, mediaTypeByExtension(tc.filename))
		})
	}
}

// pdfBytes and zipBytes are the magic-number prefixes http.DetectContentType
// keys on; the rest of a real file is irrelevant to the sniff.
var (
	pdfBytes = []byte("%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\n")
	zipBytes = []byte("PK\x03\x04\x14\x00\x06\x00\x08\x00\x00\x00!\x00")
	svgBytes = []byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>`)
)

func TestSniffMediaType(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"png fixture", png64x48, "image/png"},
		{"jpeg fixture", jpeg32x32, "image/jpeg"},
		{"pdf", pdfBytes, "application/pdf"},
		{"zip prefix, as every OOXML file sniffs", zipBytes, "application/zip"},
		{"svg sniffs as xml, not as an image", svgBytes, "text/xml"},
		{"plain text", []byte("hello, world"), "text/plain"},
		{"charset parameter is stripped", []byte("hello"), "text/plain"},
		{"empty file", []byte{}, "text/plain"},
		{"opaque binary", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}, "application/octet-stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			got, err := sniffMediaType(r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			rest, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tc.data, rest, "reader must be left at the start for the upload")
		})
	}
}

// A file larger than the sniff window must still be fully readable afterwards.
func TestSniffMediaType_RewindsPastSniffWindow(t *testing.T) {
	data := append(append([]byte{}, pdfBytes...), bytes.Repeat([]byte("a"), 4096)...)
	r := bytes.NewReader(data)

	got, err := sniffMediaType(r)
	require.NoError(t, err)
	assert.Equal(t, "application/pdf", got)

	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, rest)
}

// A reader that cannot be rewound is unusable for the upload that follows, so
// the sniff must surface an error rather than hand back a half-consumed file.
func TestSniffMediaType_RewindError(t *testing.T) {
	seekErr := errors.New("seek boom")
	r := &seekFailReader{Reader: bytes.NewReader(png64x48), seekErr: seekErr}

	_, err := sniffMediaType(r)
	require.Error(t, err)
	assert.ErrorIs(t, err, seekErr)
}

func TestSniffMediaType_ReadError(t *testing.T) {
	readErr := errors.New("read boom")
	r := &seekFailReader{Reader: iotest.ErrReader(readErr)}

	_, err := sniffMediaType(r)
	require.Error(t, err)
	assert.ErrorIs(t, err, readErr)
}

func TestResolveMediaType(t *testing.T) {
	tests := []struct {
		name, declared, filename string
		data                     []byte
		want                     string
	}{
		{
			name:     "a specific declared type wins over the bytes",
			declared: "text/csv", filename: "data.bin", data: pdfBytes, want: "text/csv",
		},
		{
			name:     "declared type keeps only its base, not its parameters",
			declared: "text/csv; charset=utf-8", filename: "data.csv", data: []byte("a,b\n1,2"), want: "text/csv",
		},
		{
			name:     "octet-stream is resolved from the bytes",
			declared: "application/octet-stream", filename: "photo.png", data: png64x48, want: "image/png",
		},
		{
			name:     "octet-stream is matched case-insensitively",
			declared: "APPLICATION/OCTET-STREAM", filename: "photo.png", data: png64x48, want: "image/png",
		},
		{
			name:     "the generic default is still generic when it carries parameters",
			declared: "application/octet-stream; charset=binary", filename: "photo.png", data: png64x48, want: "image/png",
		},
		{
			name:     "an absent declared type is resolved from the bytes",
			declared: "", filename: "photo.jpg", data: jpeg32x32, want: "image/jpeg",
		},
		{
			name:     "a zip sniff defers to the extension, so docx is not application/zip",
			declared: "application/octet-stream", filename: "report.docx", data: zipBytes,
			want: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		},
		{
			name:     "a genuine zip still resolves to zip via its own extension",
			declared: "application/octet-stream", filename: "bundle.zip", data: zipBytes, want: "application/zip",
		},
		{
			name:     "an xml sniff defers to the extension, so svg is not text/xml",
			declared: "application/octet-stream", filename: "logo.svg", data: svgBytes, want: "image/svg+xml",
		},
		{
			name:     "a text sniff defers to the extension",
			declared: "application/octet-stream", filename: "notes.csv", data: []byte("a,b\n1,2"), want: "text/csv",
		},
		{
			name:     "a conclusive sniff beats a lying extension",
			declared: "application/octet-stream", filename: "report.docx", data: pdfBytes, want: "application/pdf",
		},
		{
			name:     "an inconclusive sniff with an unknown extension keeps the sniff result",
			declared: "application/octet-stream", filename: "notes.zzz", data: []byte("hello"), want: "text/plain",
		},
		{
			name:     "opaque bytes with no usable extension stay octet-stream",
			declared: "application/octet-stream", filename: "blob", data: []byte{0x00, 0x01, 0xff}, want: "application/octet-stream",
		},
		{
			name:     "an empty file falls back to its extension",
			declared: "", filename: "empty.docx", data: []byte{},
			want: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			got, err := resolveMediaType(tc.declared, tc.filename, r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			rest, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tc.data, rest, "reader must be left at the start for the upload")
		})
	}
}

// A specific declared type is answered without touching the reader at all.
func TestResolveMediaType_SpecificDeclaredTypeSkipsTheSniff(t *testing.T) {
	r := &seekFailReader{Reader: bytes.NewReader(pdfBytes), seekErr: errors.New("seek boom")}

	got, err := resolveMediaType("application/pdf", "report.pdf", r)
	require.NoError(t, err)
	assert.Equal(t, "application/pdf", got)
	assert.Zero(t, r.seeks, "a specific declared type must not read the file")
}

func TestResolveMediaType_RewindError(t *testing.T) {
	seekErr := errors.New("seek boom")
	r := &seekFailReader{Reader: bytes.NewReader(png64x48), seekErr: seekErr}

	_, err := resolveMediaType("application/octet-stream", "photo.png", r)
	require.Error(t, err)
	assert.ErrorIs(t, err, seekErr)
}
