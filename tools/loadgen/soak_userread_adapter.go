package main

import soakuserread "github.com/hmchangw/chat/tools/loadgen/internal/soak/userread"

// soakUserReadRecorderAdapter connects the extracted lane's sample to the
// shared recorder still owned by the root soak engine. It remains at the
// composition boundary until the other read lanes move to their packages.
type soakUserReadRecorderAdapter struct {
	recorder soakReadSampleRecorder
}

func (a soakUserReadRecorderAdapter) Record(sample *soakuserread.Sample) {
	if a.recorder == nil {
		return
	}
	a.recorder.Record(&soakReadSample{
		Action: sample.Action, Latency: sample.Latency,
		Messages: sample.Messages, RowsCounted: sample.RowsCounted,
		ReplyBytes: sample.ReplyBytes, ErrorClass: sample.ErrorClass,
		ErrorReason: sample.ErrorReason, Retries: sample.Retries,
		Skipped: sample.Skipped,
	})
}
