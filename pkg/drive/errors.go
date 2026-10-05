package drive

import "errors"

// ErrNotFound reports that Drive no longer has the file: the signer or the
// storage behind the presigned URL answered 404. It is permanent, unlike a 5xx
// or a transport failure, so a caller that archives or mirrors files can record
// the file as gone instead of retrying it.
var ErrNotFound = errors.New("drive: file not found")
