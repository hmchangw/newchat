package searchindex

import "testing"

func TestFileTypeCategory(t *testing.T) {
	cases := []struct {
		mime string
		want string
	}{
		{"image/png", "image"},
		{"image/jpeg", "image"},
		{"application/pdf", "pdf"},
		{"application/vnd.ms-excel", "excel"},
		{"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "excel"},
		{"application/vnd.ms-powerpoint", "powerpoint"},
		{"application/vnd.openxmlformats-officedocument.presentationml.presentation", "powerpoint"},
		{"application/msword", "word"},
		{"application/vnd.openxmlformats-officedocument.wordprocessingml.document", "word"},
		{"application/zip", "zip"},
		{"application/x-zip-compressed", "zip"},
		{"application/x-rar-compressed", "others"},
		{"text/plain", "others"},
		{"", "others"},
	}
	for _, tc := range cases {
		t.Run(tc.mime, func(t *testing.T) {
			if got := fileTypeCategory(tc.mime); got != tc.want {
				t.Errorf("fileTypeCategory(%q) = %q, want %q", tc.mime, got, tc.want)
			}
		})
	}
}

func TestDedupeStrings(t *testing.T) {
	got := dedupeStrings([]string{"a", "b", "a", "c", "b"})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("dedupeStrings = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeStrings = %v, want %v", got, want)
		}
	}
	if dedupeStrings(nil) != nil {
		t.Error("dedupeStrings(nil) must return nil")
	}
}
