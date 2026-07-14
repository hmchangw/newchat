package drive

import (
	"crypto/tls"
	"fmt"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// MultipartFile is an opened multipart file plus its name, ready to upload.
type MultipartFile struct {
	File     multipart.File
	Filename string
}

// Client talks to the internal Drive API.
type Client struct {
	uploadClient   *resty.Client
	downloadClient *resty.Client
	baseURLMap     map[string]string
	baseURL        string
	apiToken       string
}

// NewClient builds a Drive client. Both underlying Resty clients skip TLS
// verification (the Drive is reached over a private network); the download
// client uses a 5-minute timeout to allow large streamed bodies.
func NewClient(cfg *Config) *Client {
	// #nosec G402 -- internal Drive over a private network; TLS verification is intentionally skipped per deployment.
	insecure := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}}
	return &Client{
		uploadClient:   restyutil.New(cfg.URL, restyutil.WithTransport(insecure)),
		downloadClient: restyutil.New(cfg.URL, restyutil.WithTransport(insecure), restyutil.WithTimeout(5*time.Minute)),
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

// UploadGroupImages uploads files to a Drive group in one bulk multipart call.
// userID/userName/email are sent as form fields; each file is attached with the
// indexed naming convention files[i].file / files[i].fileName / files[i].mode.
func (c *Client) UploadGroupImages(userID, username, email, groupID, origin string, files []MultipartFile) ([]UploadGroupImageResponse, error) {
	req := c.uploadClient.R().SetHeader("api-token", c.apiToken)
	formData := map[string]string{
		"userId":   userID,
		"userName": username,
		"email":    email,
	}
	for i, f := range files {
		req.SetMultipartField(fmt.Sprintf("files[%d].file", i), f.Filename, defaultContentType, f.File)
		formData[fmt.Sprintf("files[%d].fileName", i)] = f.Filename
		formData[fmt.Sprintf("files[%d].mode", i)] = "Normal"
	}
	req.SetFormData(formData)

	var result []UploadGroupImageResponse
	resp, err := req.
		SetResult(&result).
		SetPathParam("groupId", groupID).
		Post(fmt.Sprintf("%s/api/v1/groups/{groupId}/files/bulk?bypass=true", c.GetBaseURLFromRoomOrigin(origin)))
	if err != nil {
		return nil, fmt.Errorf("upload group images: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("drive bulk upload returned status %d", resp.StatusCode())
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
	}, nil
}
