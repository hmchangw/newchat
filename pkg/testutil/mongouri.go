package testutil

import (
	"fmt"
	"net"
	"testing"
)

// ReserveAddr returns a local address with nothing listening on it: it opens a
// port, notes the number, then closes it again.
//
// Nothing keeps that port free afterwards — the operating system may hand it to
// another program. That is fine for a caller who only wants connections to be
// refused. A caller who later wants to listen on it (see MongoOutage) has to
// expect that and retry.
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

// UnreachableMongoURI returns a MongoDB address where nothing is listening —
// what a service sees when MongoDB is down.
//
// Both settings in the URI matter. directConnection=true stops the driver
// asking MongoDB for its real address and going there instead, which would
// defeat the point. serverSelectionTimeoutMS shortens how long the driver waits
// before giving up, so these tests take a fraction of a second each instead of
// 30 seconds; queries still fail the same way, just sooner.
func UnreachableMongoURI(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("mongodb://%s/?directConnection=true&serverSelectionTimeoutMS=200", ReserveAddr(t))
}
