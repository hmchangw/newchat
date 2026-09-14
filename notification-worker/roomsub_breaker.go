package main

import "github.com/hmchangw/chat/pkg/mongoutil"

// memberBreakerFailure is the failure predicate the room-member breaker must be
// built with. Not a not-found rule: roomsubcache's Mongo loader lists members,
// so an empty room yields an empty slice rather than mongo.ErrNoDocuments, and
// the breaker never sees one. What this buys is the context.Canceled exemption
// — a cancelled caller is evidence about the caller, not about Mongo, and
// counting it would open the breaker against a healthy database, leaving every
// room whose member list is not already warm in L2 without notifications.
//
// context.DeadlineExceeded deliberately still counts: the driver bounds server
// selection with MONGO_SERVER_SELECTION_TIMEOUT and reports an unreachable
// MongoDB as an error matching it, so exempting it would hold the breaker closed
// through exactly the outage the fence exists for.
var memberBreakerFailure = mongoutil.BreakerFailure()
