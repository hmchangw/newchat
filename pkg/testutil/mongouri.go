package testutil

import (
	"fmt"
	"net"
	"testing"
)

// ReserveAddr returns a 127.0.0.1 address with nothing listening on it: bind an
// ephemeral port, read it back, then release it.
//
// The reservation is advisory — once released, nothing stops the OS handing the
// port to another process. A caller that only needs "connection refused" is
// safe either way; one that later binds the address itself (see outageProxy)
// must tolerate the bind failing and retry.
func ReserveAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve local address: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release reserved address %s: %v", addr, err)
	}
	return addr
}

// UnreachableMongoURI returns a mongodb:// URI for a reserved address with
// nothing listening — what a pod restarting into a MongoDB outage sees. The
// short serverSelectionTimeoutMS keeps the unreachable paths failing in test
// time rather than the driver's 30s default.
func UnreachableMongoURI(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("mongodb://%s/?directConnection=true&serverSelectionTimeoutMS=200", ReserveAddr(t))
}
