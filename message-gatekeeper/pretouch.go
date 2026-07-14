package main

import (
	"reflect"

	"github.com/hmchangw/chat/pkg/jsonwarm"
	"github.com/hmchangw/chat/pkg/model"
)

// pretouchTypes are the hot types whose sonic codecs are warmed at startup.
// SendMessageRequest is the untrusted client-input decode; MessageEvent is the
// canonical publish. The quoted-parent fetch (fetcher_history.go) decodes a
// narrow projection, not the full cassandra.Message, so it needs no warm-up here.
var pretouchTypes = []reflect.Type{
	reflect.TypeOf(model.SendMessageRequest{}),
	reflect.TypeOf(model.MessageEvent{}),
	reflect.TypeOf(model.Message{}),
}

func pretouchJSON() { jsonwarm.Pretouch(pretouchTypes...) }
