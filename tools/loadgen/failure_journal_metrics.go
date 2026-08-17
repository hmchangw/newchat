package main

import "time"

type failureJournalMetrics struct {
	journal failureJournal
	metrics *Metrics
}

func newFailureJournalMetrics(journal failureJournal, metrics *Metrics) *failureJournalMetrics {
	return &failureJournalMetrics{journal: journal, metrics: metrics}
}

func (j *failureJournalMetrics) Replay() ([]failureLedgerEvent, error) {
	return j.journal.Replay()
}

func (j *failureJournalMetrics) Append(event *failureLedgerEvent) error {
	startedAt := time.Now()
	err := j.journal.Append(event)
	if j.metrics != nil {
		j.metrics.FailureWALAppendDuration.Observe(time.Since(startedAt).Seconds())
		result := "success"
		if err != nil {
			result = "error"
		}
		j.metrics.FailureWALAppends.WithLabelValues(result).Inc()
	}
	return err
}

func (j *failureJournalMetrics) Compact(events []failureLedgerEvent) error {
	return j.journal.Compact(events)
}

func (j *failureJournalMetrics) Size() int64 { return j.journal.Size() }

func (j *failureJournalMetrics) Close() error { return j.journal.Close() }

func (j *failureJournalMetrics) ConfigureObserverContract(
	contract failureObserverContract,
	active []failureOperation,
) error {
	configurer, ok := j.journal.(interface {
		ConfigureObserverContract(failureObserverContract, []failureOperation) error
	})
	if !ok {
		return nil
	}
	return configurer.ConfigureObserverContract(contract, active)
}

func (j *failureJournalMetrics) NeedsUpgrade() bool {
	upgrade, ok := j.journal.(interface{ NeedsUpgrade() bool })
	return ok && upgrade.NeedsUpgrade()
}
