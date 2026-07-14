package drive

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGroupImageObject_FileSizeJSON(t *testing.T) {
	const body = `{"objectId":"f1","groupId":"g1","fileName":"a.pdf","fileSize":4096}`
	var obj GroupImageObject
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if obj.FileID != "f1" {
		t.Fatalf("FileID = %q, want f1", obj.FileID)
	}
	if obj.FileSize != 4096 {
		t.Fatalf("FileSize = %d, want 4096", obj.FileSize)
	}
}

func TestImagesFormData_AddCountClearFiles(t *testing.T) {
	f := NewImagesForm()
	if f.FileCount() != 0 {
		t.Fatalf("new form should be empty, got %d", f.FileCount())
	}
	f.AddFile("a.png", "image/png", strings.NewReader("aaa"))
	f.AddFile("b.jpg", "image/jpeg", strings.NewReader("bbb"))
	if f.FileCount() != 2 {
		t.Fatalf("want 2 files, got %d", f.FileCount())
	}
	if got := f.Files(); got[0].Filename != "a.png" || got[1].ContentType != "image/jpeg" {
		t.Fatalf("unexpected files slice: %+v", got)
	}
	f.ClearFiles()
	if f.FileCount() != 0 {
		t.Fatalf("ClearFiles should empty the slice, got %d", f.FileCount())
	}
}

func TestUploadGroupImageResponse_JSONTags(t *testing.T) {
	// File must marshal under "object"; GroupImageObject fields under objectId/groupId/fileName.
	in := UploadGroupImageResponse{
		Status: "Success",
		File:   GroupImageObject{FileID: "f1", GroupID: "r1", Filename: "a.png"},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{`"object":`, `"objectId":"f1"`, `"groupId":"r1"`, `"fileName":"a.png"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("marshaled JSON %s missing %s", s, want)
		}
	}
}

func TestAllowedImageFileTypes(t *testing.T) {
	for _, ext := range []string{".png", ".jpeg", ".jpg", ".heic"} {
		if !AllowedImageFileTypes[ext] {
			t.Fatalf("%s should be allowed", ext)
		}
	}
	if AllowedImageFileTypes[".exe"] {
		t.Fatal(".exe must not be allowed")
	}
}
