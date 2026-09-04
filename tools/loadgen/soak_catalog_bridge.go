package main

import (
	"time"

	soakcatalog "github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
)

// These aliases keep the root runtime stable while its lanes move into
// internal/soak packages in later commits. New package code uses the concise
// catalog names directly.
type soakCatalogAction = soakcatalog.Action

const (
	soakCatalogEdit         = soakcatalog.ActionEdit
	soakCatalogDelete       = soakcatalog.ActionDelete
	soakCatalogPin          = soakcatalog.ActionPin
	soakCatalogReaction     = soakcatalog.ActionReaction
	soakCatalogThreadParent = soakcatalog.ActionThreadParent
	soakCatalogThreadRead   = soakcatalog.ActionThreadRead
	soakCatalogReadReceipt  = soakcatalog.ActionReadReceipt
)

type soakClock = soakcatalog.TimeProvider
type soakRealClock = soakcatalog.RealClock
type soakCatalogCandidate = soakcatalog.Candidate
type soakCatalogMessage = soakcatalog.Message
type soakCatalog = soakcatalog.Catalog

func newSoakCatalog(
	perRoomCap int,
	globalCap int,
	persistGrace time.Duration,
	clock soakClock,
) *soakCatalog {
	return soakcatalog.New(perRoomCap, globalCap, persistGrace, clock)
}

func soakContentDigest(body string) string {
	return soakcatalog.ContentDigest(body)
}

func searchProbeTerm(content string) string {
	return soakcatalog.SearchTerm(content)
}
