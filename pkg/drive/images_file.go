package drive

import "io"

// defaultContentType is used when a file's content type is unknown.
const defaultContentType = "application/octet-stream"

// AllowedImageFileTypes is the set of accepted lowercase file extensions.
var AllowedImageFileTypes = map[string]bool{
	".png":  true,
	".jpeg": true,
	".jpg":  true,
	".heic": true,
}

// GroupImageObject is Drive's per-file descriptor in a bulk-upload response.
type GroupImageObject struct {
	FileID   string `json:"objectId"`
	GroupID  string `json:"groupId"`
	Filename string `json:"fileName"`
	FileSize int64  `json:"fileSize"`
}

// UploadGroupImageResponse is one item in the Drive bulk-upload response.
type UploadGroupImageResponse struct {
	Status string           `json:"status"`
	File   GroupImageObject `json:"object"`
	Error  string           `json:"error,omitempty"`
}

// GetGroupImageResponse carries a streamed download body plus metadata.
type GetGroupImageResponse struct {
	Reader        io.ReadCloser
	ContentType   string
	ContentLength int64
}

// File is one staged upload file.
type File struct {
	Reader      io.Reader
	Filename    string
	ContentType string
}

// ImagesFormData accumulates files staged for upload.
type ImagesFormData struct {
	files []File
}

// NewImagesForm returns an empty ImagesFormData.
func NewImagesForm() *ImagesFormData {
	return &ImagesFormData{files: []File{}}
}

// AddFile appends a file built from the provided filename, content type and reader.
func (f *ImagesFormData) AddFile(filename, contentType string, r io.Reader) {
	f.files = append(f.files, File{Reader: r, Filename: filename, ContentType: contentType})
}

// FileCount returns the number of staged files.
func (f *ImagesFormData) FileCount() int {
	return len(f.files)
}

// ClearFiles re-initializes the internal slice to empty.
func (f *ImagesFormData) ClearFiles() {
	f.files = []File{}
}

// Files returns the staged files.
func (f *ImagesFormData) Files() []File {
	return f.files
}
