package searchindex

import "strings"

// fileTypeCategory maps an attachment's canonical lowercased MIME (Attachment.FileType)
// to one of the search filter categories. Unrecognized/empty MIME falls back to "others"
// rather than being dropped, so every attachment stays filterable.
func fileTypeCategory(mime string) string {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return "image"
	case mime == "application/pdf":
		return "pdf"
	case mime == "application/zip", mime == "application/x-zip-compressed":
		return "zip"
	case isExcelMIME(mime):
		return "excel"
	case isPowerPointMIME(mime):
		return "powerpoint"
	case isWordMIME(mime):
		return "word"
	default:
		return "others"
	}
}

func isExcelMIME(mime string) bool {
	return mime == "application/vnd.ms-excel" ||
		mime == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

func isPowerPointMIME(mime string) bool {
	return mime == "application/vnd.ms-powerpoint" ||
		mime == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
}

func isWordMIME(mime string) bool {
	return mime == "application/msword" ||
		mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
}

// dedupeStrings returns in's distinct values in first-seen order; nil for empty input.
func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
