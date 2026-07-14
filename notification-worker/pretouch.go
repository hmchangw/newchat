package main

import (
	"reflect"

	"github.com/hmchangw/chat/pkg/jsonwarm"
	"github.com/hmchangw/chat/pkg/model"
)

// pretouchTypes are the hot event types whose sonic codecs are warmed at startup.
var pretouchTypes = []reflect.Type{
	reflect.TypeOf(model.MessageEvent{}),
	reflect.TypeOf(model.PushNotificationEvent{}),
	reflect.TypeOf(model.CanonicalMemberEvent{}),
	reflect.TypeOf(model.PresenceSnapshotRequest{}),
	reflect.TypeOf(model.PresenceSnapshotReply{}),
}

func pretouchJSON() { jsonwarm.Pretouch(pretouchTypes...) }
