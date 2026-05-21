//go:build integration

package testutil

// TerminateAll stops every process-shared container. Each TerminateXxx
// is a no-op if its container was never started.
func TerminateAll() {
	TerminateMongo()
	TerminateCassandra()
	TerminateMinIO()
	TerminateElasticsearch()
	TerminateNATS()
	TerminateValkey()
}
