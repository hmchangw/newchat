package errcode

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanent_PanicsOnNil(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Permanent(nil) must panic")
		}
	}()
	Permanent(nil)
}

func TestPermanent_UnwrapReachesErrcode(t *testing.T) {
	inner := NotFound("room not found", WithReason("room_not_found"))
	p := Permanent(inner)
	var got *Error
	if !errors.As(p, &got) {
		t.Fatal("errors.As must reach the wrapped *Error")
	}
	if got.Code != CodeNotFound || got.Reason != "room_not_found" {
		t.Fatalf("wrapped *Error lost: %+v", got)
	}
}

func TestPermanent_IsMatchesSentinel(t *testing.T) {
	p := Permanent(Internal("boom"))
	if !errors.Is(p, ErrPermanent) {
		t.Fatal("errors.Is(p, ErrPermanent) must hold")
	}
	wrapped := fmt.Errorf("publish: %w", p)
	if !errors.Is(wrapped, ErrPermanent) {
		t.Fatal("errors.Is must traverse the wrap")
	}
}

func TestIsPermanent_DetectsWrapper(t *testing.T) {
	inner := Forbidden("denied")
	p := Permanent(inner)
	ec, ok := IsPermanent(p)
	if !ok {
		t.Fatal("IsPermanent must return true on wrapped")
	}
	if ec.Code != CodeForbidden {
		t.Fatalf("wrapped *Error lost: %+v", ec)
	}
}

func TestIsPermanent_FalseOnPlainErrcode(t *testing.T) {
	if _, ok := IsPermanent(Internal("boom")); ok {
		t.Fatal("plain *Error is not permanent")
	}
	if _, ok := IsPermanent(errors.New("raw")); ok {
		t.Fatal("raw error is not permanent")
	}
	if _, ok := IsPermanent(nil); ok {
		t.Fatal("nil is not permanent")
	}
}

func TestTerminal(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     bool
		wantCode Code
	}{
		{"not found is terminal", NotFound("gone"), true, CodeNotFound},
		{"forbidden is terminal", Forbidden("nope"), true, CodeForbidden},
		{"bad request is terminal", BadRequest("bad"), true, CodeBadRequest},
		{"wrapped terminal is still terminal", fmt.Errorf("fetch: %w", NotFound("gone")), true, CodeNotFound},
		// The remote may recover, so these must keep their retry budget. These are
		// states of the world, not facts about this message.
		{"unavailable is transient", Unavailable("history down"), false, ""},
		{"internal is transient", Internal("boom"), false, ""},
		// "retry shortly" must never mean "drop" — that is what BackpressureBackoff
		// exists for. pkg/ginutil/limit.go sheds load with exactly this code.
		{"too many requests is transient", TooManyRequests("server is at capacity"), false, ""},
		// A credential problem hits every message at once; dropping them would be
		// mass data loss, not poison rejection.
		{"unauthenticated is transient", Unauthenticated("token expired"), false, ""},
		// Infra failures carry no errcode and are always retryable.
		{"bare error is transient", errors.New("dial tcp: timeout"), false, ""},
		{"nil is transient", nil, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ee, got := Terminal(tc.err)
			if got != tc.want {
				t.Fatalf("Terminal(%v) = %v, want %v", tc.err, got, tc.want)
			}
			if !tc.want {
				if ee != nil {
					t.Fatalf("non-terminal must return a nil *Error, got %+v", ee)
				}
				return
			}
			if ee == nil {
				t.Fatal("terminal must return the typed *Error")
			}
			if ee.Code != tc.wantCode {
				t.Fatalf("Code = %v, want %v", ee.Code, tc.wantCode)
			}
		})
	}
}
