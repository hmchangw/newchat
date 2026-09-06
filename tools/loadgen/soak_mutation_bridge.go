package main

import (
	"math/rand"

	soakmutation "github.com/hmchangw/chat/tools/loadgen/internal/soak/mutation"
)

type soakMutationConfig = soakmutation.Config
type soakMutationSample = soakmutation.Sample
type soakMutationSampleRecorder = soakmutation.SampleRecorder
type soakMutationOutcome = soakmutation.Outcome
type soakMutator = soakmutation.Mutator

func newSoakMutator(
	cfg *soakMutationConfig,
	topology *soakTopology,
	catalog *soakCatalog,
	rpc *soakRPCClient,
	recorder soakMutationSampleRecorder,
	rng *rand.Rand,
	clock soakClock,
	sleeper soakSleeper,
) *soakMutator {
	return soakmutation.New(
		cfg,
		topology,
		catalog,
		rpc,
		recorder,
		rng,
		clock,
		sleeper,
	)
}
