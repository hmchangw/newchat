package main

import "github.com/hmchangw/chat/pkg/model"

type soakPresenceMetricsAdapter struct {
	metrics *Metrics
}

func (a *soakPresenceMetricsAdapter) CountSignal(signal string) {
	a.metrics.SoakPresenceSignals.WithLabelValues(signal).Inc()
}

func (a *soakPresenceMetricsAdapter) CountCheck(result string, count int) {
	a.metrics.SoakPresenceChecks.WithLabelValues(result).Add(float64(count))
}

func (a *soakPresenceMetricsAdapter) SetConnections(
	status model.PresenceStatus,
	count int,
) {
	a.metrics.SoakPresenceConnections.WithLabelValues(string(status)).Set(float64(count))
}
