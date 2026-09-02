package main

import soakuserread "github.com/hmchangw/chat/tools/loadgen/internal/soak/userread"

// soakUserReadRecorderAdapter connects the extracted lane's sample to the
// shared recorder still owned by the root soak engine. It remains at the
// composition boundary until the other read lanes move to their packages.
type soakUserReadRecorderAdapter struct {
	recorder soakReadSampleRecorder
}

// Record converts rather than copies field by field: the two sample types have
// identical underlying types, so the conversion is what pins them together. A
// field added to one and not the other stops compiling here instead of being
// silently dropped on the way to the recorder.
func (a soakUserReadRecorderAdapter) Record(sample *soakuserread.Sample) {
	if a.recorder == nil {
		return
	}
	a.recorder.Record((*soakReadSample)(sample))
}
