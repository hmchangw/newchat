package main

import "testing"

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
