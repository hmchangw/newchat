package main

// stage is one node in a workload's pipeline. The bottleneck engine walks
// stages in flow order, mapping loadgen's signals (durable backlog, latency
// series) and cAdvisor's container metrics onto each one.
type stage struct {
	Name          string   // logical component name, used in the verdict
	Container     string   // cAdvisor compose-service label value
	Durable       string   // durable consumer fronting this stage; "" if none
	LatencySeries string   // loadgen latency series measuring this stage; "" if none
	DependsOn     []string // external dependencies this stage calls into (e.g. databases)
}

// messagesStageGraph describes the messages pipeline:
// publish -> message-gatekeeper -> MESSAGES_CANONICAL -> {message-worker (Cassandra),
// broadcast-worker (MongoDB membership + Valkey keys)}. E1 latency measures the
// gatekeeper front door; E2 is the end-to-end publish->broadcast time.
func messagesStageGraph() []stage {
	return []stage{
		{Name: "message-gatekeeper", Container: "message-gatekeeper", LatencySeries: "E1"},
		{Name: "message-worker", Container: "message-worker", Durable: "message-worker", DependsOn: []string{"cassandra"}},
		{Name: "broadcast-worker", Container: "broadcast-worker", Durable: "broadcast-worker", LatencySeries: "E2", DependsOn: []string{"mongodb", "valkey"}},
	}
}

// dependencyDisplayName maps an internal dependency key to a human label for
// the verdict ("message-worker (Cassandra-bound)"). Unknown keys pass through.
func dependencyDisplayName(dep string) string {
	switch dep {
	case "cassandra":
		return "Cassandra"
	case "mongodb":
		return "MongoDB"
	default:
		return dep
	}
}
