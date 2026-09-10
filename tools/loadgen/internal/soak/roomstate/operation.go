package roomstate

import (
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/failure"
)

type MemberIntent struct {
	RoomID    string
	Account   string
	Requester string
	Add       bool
}

func (i MemberIntent) OperationType() failure.OperationType {
	if i.Add {
		return failure.OperationMemberAdd
	}
	return failure.OperationMemberRemove
}

type RenameIntent struct {
	RoomID    string
	Requester string
	NewName   string
}

type MuteIntent struct {
	RoomID      string
	Account     string
	TargetMuted bool
}

type ReadIntent struct {
	RoomID   string
	Account  string
	Baseline time.Time
	Known    bool
}

type RoomProbe struct {
	RoomID  string
	Account string
	Mute    bool
}
