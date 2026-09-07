// Package testdata holds the fixture for remoteenvelope.yml.
//
// `semgrep scan --test` reads the annotations: a `ruleid:` comment names the
// rule that must fire on the following line; an unannotated line is a negative
// assertion — a rule firing there is reported as a false positive.
package testdata

import (
	"errors"
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
)

type reply struct{ Data []byte }

// The two shapes this rule exists to stop, and the one it wants instead.
func readRemoteReply(msg reply) error {
	// The success-swallowing shape: an unrecognised code is not Valid, so the
	// whole branch is skipped and the envelope decodes as a zero-value success.
	// ruleid: remote-envelope-must-use-fromreply
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return ee
	}

	// The relay shape: an unrecognised code reaches an API that assumes the
	// closed set, and errcode.New panics on one.
	// ruleid: remote-envelope-must-use-fromreply
	if ee, ok := errcode.Parse(msg.Data); ok {
		return ee
	}

	// Assignment rather than declaration is the same defect.
	var ee *errcode.Error
	var ok bool
	// ruleid: remote-envelope-must-use-fromreply
	ee, ok = errcode.Parse(msg.Data)
	if ok {
		return ee
	}

	return nil
}

// The correct forms stay clean: a plain relay, and a branch on one code.
func readRemoteReplyCorrectly(msg reply) error {
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return remoteErr
	}
	return nil
}

func readRemoteReplyAndBranch(msg reply) error {
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		var ee *errcode.Error
		if errors.As(remoteErr, &ee) && ee.Code == errcode.CodeNotFound {
			return nil // absence is this route's success
		}
		return fmt.Errorf("remote: %w", remoteErr)
	}
	return nil
}
