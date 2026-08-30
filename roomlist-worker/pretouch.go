package main

import (
	"reflect"

	"github.com/hmchangw/chat/pkg/jsonwarm"
)

// pretouchTypes are the hot event types whose sonic codecs are warmed at
// startup. It is the projection, not model.MessageEvent: warming a codec this
// service never decodes would cost startup time and warm nothing on the path.
var pretouchTypes = []reflect.Type{
	reflect.TypeOf(eventProjection{}),
}

func pretouchJSON() { jsonwarm.Pretouch(pretouchTypes...) }
