package failure

import "time"

type Event struct {
	SchemaVersion     int                                 `json:"schemaVersion,omitempty"`
	Type              string                              `json:"type"`
	Operation         *Operation                          `json:"operation,omitempty"`
	OperationID       string                              `json:"operationId,omitempty"`
	Observer          Observer                            `json:"observer,omitempty"`
	Observation       Observation                         `json:"observation,omitempty"`
	Reason            Reason                              `json:"reason,omitempty"`
	Result            Result                              `json:"result,omitempty"`
	Results           map[Result]uint64                   `json:"results,omitempty"`
	ObservationCounts map[Observer]map[Observation]uint64 `json:"observationCounts,omitempty"`
	NotSent           []string                            `json:"notSent,omitempty"`
	InvalidReason     string                              `json:"invalidReason,omitempty"`
	At                time.Time                           `json:"at"`
}

const (
	EventStarted     = "started"
	EventActivated   = "activated"
	EventObserved    = "observed"
	EventFinalized   = "finalized"
	EventCheckpoint  = "checkpoint"
	EventInvariant   = "accounting_invariant"
	EventInvalidated = "invalidated"
)

type Journal interface {
	Replay() ([]Event, error)
	Append(*Event) error
	Compact([]Event) error
	Size() int64
	Close() error
}

type StreamingJournal interface {
	ReplayEach(func(*Event) error) error
}

type BufferedJournal interface {
	Journal
	AppendBuffered(*Event) error
	Sync() error
}

type ObserverContractJournal interface {
	ConfigureObserverContract(ObserverContract, []Operation) error
}

type UpgradeJournal interface {
	NeedsUpgrade() bool
}

type WALFlushRecorder interface {
	RecordWALFlush(duration time.Duration, batchSize int, err error)
}
