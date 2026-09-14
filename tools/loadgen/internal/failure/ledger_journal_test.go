package failure

import "errors"

type memoryFailureJournal struct {
	events     []Event
	refuseNext bool
}

func (j *memoryFailureJournal) Replay() ([]Event, error) {
	return append([]Event(nil), j.events...), nil
}

func (j *memoryFailureJournal) Append(event *Event) error {
	if j.refuseNext {
		return errors.New("no space left on device")
	}
	j.events = append(j.events, *event)
	return nil
}

func (j *memoryFailureJournal) Compact(events []Event) error {
	j.events = append([]Event(nil), events...)
	return nil
}

func (*memoryFailureJournal) Size() int64  { return 0 }
func (*memoryFailureJournal) Close() error { return nil }
