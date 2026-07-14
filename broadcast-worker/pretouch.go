package main

import (
	"reflect"

	"github.com/hmchangw/chat/pkg/jsonwarm"
	"github.com/hmchangw/chat/pkg/model"
)

// pretouchTypes are the hot event types whose sonic codecs are warmed at startup.
var pretouchTypes = []reflect.Type{
	reflect.TypeOf(model.MessageEvent{}),
	reflect.TypeOf(model.RoomEvent{}),
	reflect.TypeOf(model.EditRoomEvent{}),
	reflect.TypeOf(model.DeleteRoomEvent{}),
	reflect.TypeOf(model.PinStateRoomEvent{}),
	reflect.TypeOf(model.ReactRoomEvent{}),
	reflect.TypeOf(model.ThreadMetadataUpdatedEvent{}),
}

func pretouchJSON() { jsonwarm.Pretouch(pretouchTypes...) }
