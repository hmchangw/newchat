package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

var (
	errBlobMissing = errors.New("attachment no longer at source")
	errBlobLegacy  = errors.New("legacy MinIO attachment is not archived")
)

// blobSource opens an attachment's bytes. size is -1 when unknown.
type blobSource interface {
	Open(ctx context.Context, roomID string, att cassandra.Attachment) (body io.ReadCloser, size int64, contentType string, err error)
}

// driveHostOf parses the Drive host out of an attachment link written by
// upload-service: "api/v1/file/rooms/{room}/file/{file}?drive_host={host}".
// The legacy "api/v1/file-upload/{file}/{name}" form points at the upload
// MinIO bucket through a Mongo lookup this worker does not have; it is
// reported as legacy so the lane records it as skipped.
func driveHostOf(titleLink string) (host string, legacy bool, err error) {
	if strings.HasPrefix(strings.TrimPrefix(titleLink, "/"), "api/v1/file-upload/") {
		return "", true, nil
	}
	u, err := url.Parse(titleLink)
	if err != nil {
		return "", false, fmt.Errorf("parse attachment link: %w", err)
	}
	host = u.Query().Get("drive_host")
	if host == "" {
		return "", false, errors.New("attachment link has no drive_host")
	}
	return host, false, nil
}

// driveSource fetches attachments through pkg/drive exactly as upload-service
// resolves them: the host comes from the link's drive_host, which the client
// checks against its configured base URLs before it attaches the api-token.
type driveSource struct{ client *drive.Client }

func (s *driveSource) Open(_ context.Context, roomID string, att cassandra.Attachment) (io.ReadCloser, int64, string, error) { //nolint:gocritic // hugeParam: signature fixed by the blobSource interface
	host, legacy, err := driveHostOf(att.TitleLink)
	if err != nil {
		return nil, 0, "", fmt.Errorf("%w: %w", errBlobMissing, err)
	}
	if legacy {
		return nil, 0, "", errBlobLegacy
	}
	resp, err := s.client.GetGroupImage(host, roomID, att.ID)
	if err != nil {
		if errors.Is(err, drive.ErrHostNotAllowed) || driveNotFound(err) {
			return nil, 0, "", fmt.Errorf("%w: %w", errBlobMissing, err)
		}
		return nil, 0, "", fmt.Errorf("fetch attachment %s from drive: %w", att.ID, err)
	}
	return resp.Reader, resp.ContentLength, resp.ContentType, nil
}

// driveNotFound reports a Drive 404 so a deleted file is recorded as missing
// instead of being retried until the redelivery budget runs out. pkg/drive
// exposes no sentinel for it: the signer's 404 surfaces as "status 404" and a
// storage 404 as "image not found", so this is the one place errors are
// matched by text. A wording change in pkg/drive fails safe, because the file
// is then retried rather than skipped.
func driveNotFound(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "signer service returned status 404") || strings.Contains(msg, "image not found")
}
