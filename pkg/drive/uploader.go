package drive

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// driveTimeout is the whole-request ceiling on a Drive transfer. Sized for the
// internal leg only — 2 GiB moves in tens of seconds over the private network —
// so it does not track the public client leg's server timeouts. Overrides
// restyutil's 30s default, which large streamed bodies blow through.
const driveTimeout = 5 * time.Minute

// Drive's per-file upload modes and the markers that identify a name conflict
// in its per-file result. Both are matched case-insensitively — Drive's casing
// is not contractual, and the status arrives spelled either way.
const (
	modeNormal   = "Normal"
	modeKeepBoth = "KeepBoth"

	statusFailure  = "failure"
	conflictMarker = "file conflict"
)

// MultipartFile is an opened multipart file plus its name, ready to upload.
type MultipartFile struct {
	File     multipart.File
	Filename string
}

// Client talks to the internal Drive API.
type Client struct {
	uploadHTTP     *http.Client
	downloadClient *resty.Client
	baseURLMap     map[string]string
	baseURL        string
	apiToken       string
}

// NewClient builds a Drive client. Both underlying clients skip TLS verification
// (the Drive is reached over a private network) and share driveTimeout.
//
// The upload side keeps only the *http.Client — a deliberate exception to the
// Resty-for-outbound-HTTP guideline: the bulk upload streams its body, and
// Resty v2 materializes any io.Reader body it cannot natively replay
// (createHTTPRequest -> getBodyCopy -> io.ReadAll; only in-memory bytes/strings
// readers escape), which is the very OOM this path exists to avoid. Building it
// through restyutil anyway preserves the shared transport, OTel instrumentation
// and timeout — but not resty's request/response log hooks; upload failures are
// logged once at the caller's errhttp boundary instead.
func NewClient(cfg *Config) *Client {
	// #nosec G402 -- internal Drive over a private network; TLS verification is intentionally skipped per deployment.
	insecure := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}}
	return &Client{
		uploadHTTP:     restyutil.New(cfg.URL, restyutil.WithTransport(insecure), restyutil.WithTimeout(driveTimeout)).GetClient(),
		downloadClient: restyutil.New(cfg.URL, restyutil.WithTransport(insecure), restyutil.WithTimeout(driveTimeout)),
		baseURLMap:     cfg.BaseURLMap,
		baseURL:        cfg.URL,
		apiToken:       cfg.Token,
	}
}

// GetBaseURL returns the default Drive base URL.
func (c *Client) GetBaseURL() string { return c.baseURL }

// GetBaseURLFromRoomOrigin returns the Drive base URL for a room-origin siteID,
// falling back to the default base URL when the origin is unknown.
func (c *Client) GetBaseURLFromRoomOrigin(origin string) string {
	if url, ok := c.baseURLMap[origin]; ok && url != "" {
		return url
	}
	return c.baseURL
}

// UploadGroupImages uploads files to a Drive group in one bulk multipart call,
// then re-sends any file Drive rejected as a name conflict under KeepBoth.
//
// Drive reports conflicts per file inside a 200 response, so the retry is
// scoped to exactly those entries: files that uploaded on the first attempt are
// never sent again, which is what keeps a KeepBoth retry from storing a second
// copy of them. Retried once only — a KeepBoth upload has no name to collide
// with, so a repeat would only burn the bytes again.
//
// A retry that never lands leaves the first attempt's results standing rather
// than failing the whole batch: the successes are real, and the conflicting
// entries stay per-file failures marked as retried, with the cause logged
// server-side rather than handed to the client.
func (c *Client) UploadGroupImages(userID, username, email, groupID, origin string, files []MultipartFile) ([]UploadGroupImageResponse, error) {
	results, err := c.uploadBulk(userID, username, email, groupID, origin, files, modeNormal)
	if err != nil {
		return nil, err
	}

	conflicts := conflictedIndexes(results, len(files))
	if len(conflicts) == 0 {
		return results, nil
	}

	retryFiles := make([]MultipartFile, 0, len(conflicts))
	for _, i := range conflicts {
		retryFiles = append(retryFiles, files[i])
	}
	retried, err := c.uploadBulk(userID, username, email, groupID, origin, retryFiles, modeKeepBoth)
	if err != nil {
		// Logged here because returning a nil error means no boundary handler
		// will. The client is told only that the retry ran — the Drive status
		// and body snippet stay server-side.
		slog.Error("drive keepboth retry failed",
			"error", err, "groupId", groupID, "files", len(retryFiles))
		for _, i := range conflicts {
			results[i].Error = fmt.Sprintf("%s; %s retry failed", results[i].Error, modeKeepBoth)
		}
		return results, nil
	}
	for n, i := range conflicts {
		if n < len(retried) {
			results[i] = retried[n]
		}
	}
	return results, nil
}

// conflictedIndexes returns the positions of the results Drive failed with a
// name conflict. An entry past the end of what was sent is skipped: there is no
// file to re-upload for it, whatever it says.
func conflictedIndexes(results []UploadGroupImageResponse, sent int) []int {
	var conflicts []int
	for i, r := range results {
		if i >= sent {
			break
		}
		if strings.EqualFold(r.Status, statusFailure) &&
			strings.Contains(strings.ToLower(r.Error), conflictMarker) {
			conflicts = append(conflicts, i)
		}
	}
	return conflicts
}

// uploadBulk performs one bulk multipart call with every file under mode.
// userID/userName/email are sent as form fields; each file is attached with the
// indexed naming convention files[i].file / files[i].fileName / files[i].mode.
// The body is streamed (see buildStreamedBody), so memory does not scale with
// upload size, and a caller may run it twice over the same files — each build
// rewinds them.
func (c *Client) uploadBulk(userID, username, email, groupID, origin string, files []MultipartFile, mode string) ([]UploadGroupImageResponse, error) {
	fields := []formField{
		{"userId", userID},
		{"userName", username},
		{"email", email},
	}
	for i, f := range files {
		fields = append(fields,
			formField{fmt.Sprintf("files[%d].fileName", i), f.Filename},
			formField{fmt.Sprintf("files[%d].mode", i), mode})
	}
	body, err := buildStreamedBody(fields, files)
	if err != nil {
		return nil, fmt.Errorf("build upload body: %w", err)
	}

	endpoint := fmt.Sprintf("%s/api/v1/groups/%s/files/bulk?bypass=true",
		c.GetBaseURLFromRoomOrigin(origin), url.PathEscape(groupID))
	req, err := http.NewRequest(http.MethodPost, endpoint, body.reader)
	if err != nil {
		return nil, fmt.Errorf("build upload request: %w", err)
	}
	// Set explicitly: net/http cannot infer the length of a streamed body and
	// would fall back to chunked encoding without it.
	req.ContentLength = body.length
	req.Header.Set("api-token", c.apiToken)
	req.Header.Set("Content-Type", body.contentType)

	resp, err := c.uploadHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upload group images: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		// Bounded snippet so a Drive rejection is diagnosable without resty's
		// response logging, and without ever logging a full body. A read error
		// here is irrelevant next to the status and intentionally dropped.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("drive bulk upload returned status %d: %s", resp.StatusCode, snippet)
	}
	var result []UploadGroupImageResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode drive bulk upload response: %w", err)
	}
	return result, nil
}

// fetchPresignedURL asks the Drive signer for a temporary download URL.
func (c *Client) fetchPresignedURL(host, groupID, fileID string) (string, error) {
	type presignedURL struct {
		URL   string `json:"url"`
		Error string `json:"error,omitempty"`
	}
	var result presignedURL
	resp, err := c.downloadClient.R().
		SetHeader("api-token", c.apiToken).
		SetResult(&result).
		SetPathParam("groupId", groupID).
		SetPathParam("fileId", fileID).
		Get(fmt.Sprintf("%s/api/v1/groups/{groupId}/files/{fileId}", host))
	if err != nil {
		return "", fmt.Errorf("network error calling signer service: %w", err)
	}
	if resp.IsError() {
		return "", fmt.Errorf("signer service returned status %d: %s", resp.StatusCode(), result.Error)
	}
	if result.URL == "" {
		return "", fmt.Errorf("empty download url returned from signer")
	}
	return result.URL, nil
}

// GetGroupImage resolves a presigned URL then streams the image bytes. The
// returned Reader is the raw response body and must be closed by the caller.
func (c *Client) GetGroupImage(host, groupID, fileID string) (*GetGroupImageResponse, error) {
	signedURL, err := c.fetchPresignedURL(host, groupID, fileID)
	if err != nil {
		return nil, fmt.Errorf("fetch presigned url: %w", err)
	}
	resp, err := c.downloadClient.R().
		SetDoNotParseResponse(true).
		Get(signedURL)
	if err != nil {
		return nil, fmt.Errorf("download image: %w", err)
	}
	if resp.IsError() {
		defer resp.RawBody().Close()
		if resp.StatusCode() == http.StatusNotFound {
			return nil, fmt.Errorf("image not found")
		}
		return nil, fmt.Errorf("failed to fetch image from storage, status: %d", resp.StatusCode())
	}
	contentType := resp.Header().Get("Content-Type")
	if contentType == "" {
		contentType = defaultContentType
	}
	var contentLength int64
	if resp.RawResponse != nil {
		contentLength = resp.RawResponse.ContentLength
	}
	return &GetGroupImageResponse{
		Reader:        resp.RawBody(),
		ContentType:   contentType,
		ContentLength: contentLength,
		Filename:      filenameFromDisposition(resp.Header().Get("Content-Disposition")),
	}, nil
}

// filenameFromDisposition parses the filename from a Content-Disposition
// header value, preferring RFC 5987 filename*; returns "" when absent or unparseable.
func filenameFromDisposition(v string) string {
	if v == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(v)
	if err != nil {
		return ""
	}
	if name := params["filename*"]; name != "" {
		return name
	}
	return params["filename"]
}
