//go:build integration

package main

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	containertypes "github.com/moby/moby/api/types/container"
	networktypes "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

const mongoOutageContainerLabel = "newchat.test.loadgen-mongo-outage"

func startMongoOutageContainer(
	t *testing.T,
	ctx context.Context,
) (*mongocontainer.MongoDBContainer, string) {
	t.Helper()
	const attempts = 5
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		hostPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		require.NoError(t, listener.Close())

		mongoContainer, err := mongocontainer.Run(
			ctx,
			testimages.Mongo,
			testcontainers.WithLabels(map[string]string{
				mongoOutageContainerLabel: "true",
			}),
			testcontainers.WithHostConfigModifier(func(config *containertypes.HostConfig) {
				config.PortBindings = networktypes.PortMap{
					networktypes.MustParsePort("27017/tcp"): []networktypes.PortBinding{{
						HostIP:   netip.MustParseAddr("127.0.0.1"),
						HostPort: hostPort,
					}},
				}
			}),
		)
		if err != nil {
			lastErr = err
			if mongoContainer != nil {
				_ = mongoContainer.Terminate(ctx)
			}
			if isMongoOutagePortConflict(err) {
				continue
			}
			t.Fatalf("start dedicated Mongo outage container: %v", err)
		}

		uri, err := mongoContainer.ConnectionString(ctx)
		if err != nil {
			_ = mongoContainer.Terminate(ctx)
			t.Fatalf("get dedicated Mongo outage URI: %v", err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if err := mongoContainer.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate dedicated Mongo outage container: %v", err)
			}
		})
		return mongoContainer, uri
	}

	t.Fatalf(
		"start dedicated Mongo outage container after %d fixed-port attempts: %v; "+
			"if this recurs locally, inspect containers left by an interrupted test with "+
			"docker ps -a --filter label=%s=true",
		attempts, lastErr, mongoOutageContainerLabel,
	)
	return nil, ""
}

func isMongoOutagePortConflict(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "port is already allocated") ||
		strings.Contains(message, "address already in use") ||
		strings.Contains(message, "bind:")
}

type mongoOutageHeartbeatAttempt struct {
	outcome  soakHeartbeatOutcome
	degraded bool
}

type mongoOutageHeartbeatObserver struct {
	status   *soakHeartbeatStatus
	attempts chan mongoOutageHeartbeatAttempt
}

func (o *mongoOutageHeartbeatObserver) RecordHeartbeatAttempt(
	outcome soakHeartbeatOutcome,
	degraded bool,
	completedAt time.Time,
) {
	o.status.RecordHeartbeatAttempt(outcome, degraded, completedAt)
	o.attempts <- mongoOutageHeartbeatAttempt{outcome: outcome, degraded: degraded}
}

func awaitMongoOutageHeartbeatAttempt(
	t *testing.T,
	attempts <-chan mongoOutageHeartbeatAttempt,
) mongoOutageHeartbeatAttempt {
	t.Helper()
	select {
	case attempt := <-attempts:
		return attempt
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for soak heartbeat attempt")
		return mongoOutageHeartbeatAttempt{}
	}
}

func awaitMongoOutagePrimary(
	t *testing.T,
	ctx context.Context,
	client *mongo.Client,
) {
	t.Helper()
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRecovery()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		attemptCtx, cancelAttempt := context.WithTimeout(recoveryCtx, 750*time.Millisecond)
		lastErr = client.Ping(attemptCtx, readpref.Primary())
		cancelAttempt()
		if lastErr == nil {
			return
		}
		select {
		case <-recoveryCtx.Done():
			t.Fatalf("original mongo.Client did not recover before the deadline: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func TestMongoOutageRecovery_ExternalStopStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, uri := startMongoOutageContainer(t, ctx)
	client, err := mongo.Connect(options.Client().
		ApplyURI(uri).
		SetConnectTimeout(500 * time.Millisecond).
		SetServerSelectionTimeout(30 * time.Second))
	require.NoError(t, err)
	t.Cleanup(func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer disconnectCancel()
		_ = client.Disconnect(disconnectCtx)
	})
	require.NoError(t, client.Ping(ctx, readpref.Primary()))

	db := client.Database("loadgen_mongo_outage")
	store := &mongoSoakStore{db: db}
	runID := "mongo-outage-recovery"
	roomID := "room-outage"
	account := "member-outage"
	now := time.Now().UTC()
	require.NoError(t, store.PutManifest(ctx, &soakManifest{
		ID: runID, State: soakManifestRunning, StartedAt: now, UpdatedAt: now,
	}))
	_, err = db.Collection("room_members").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "member-outage-id"},
		{Key: "rid", Value: roomID},
		{Key: "member", Value: bson.D{{Key: "account", Value: account}}},
	})
	require.NoError(t, err)

	metrics := NewMetrics()
	status := newSoakHeartbeatStatus(metrics)
	observer := &mongoOutageHeartbeatObserver{
		status: status, attempts: make(chan mongoOutageHeartbeatAttempt, 4),
	}
	healthyTicks := make(chan time.Time, 2)
	retryTicks := make(chan time.Time, 2)
	waitRetry := func(ctx context.Context, _ time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retryTicks:
			return nil
		}
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatErr := make(chan error, 1)
	heartbeatDone := false
	go func() {
		heartbeatErr <- runSoakHeartbeat(
			heartbeatCtx, store, runID, healthyTicks,
			time.Second, soakHeartbeatRetryInterval, waitRetry, time.Now, observer,
		)
	}()
	defer func() {
		cancelHeartbeat()
		if !heartbeatDone {
			select {
			case <-heartbeatErr:
			case <-time.After(5 * time.Second):
				t.Error("soak heartbeat did not stop")
			}
		}
	}()

	healthyTicks <- now
	attempt := awaitMongoOutageHeartbeatAttempt(t, observer.attempts)
	assert.Equal(t, soakHeartbeatSuccess, attempt.outcome)
	assert.False(t, attempt.degraded)

	stopTimeout := 10 * time.Second
	require.NoError(t, container.Stop(ctx, &stopTimeout))
	healthyTicks <- time.Now().UTC()
	attempt = awaitMongoOutageHeartbeatAttempt(t, observer.attempts)
	assert.Equal(t, soakHeartbeatError, attempt.outcome)
	assert.True(t, attempt.degraded)
	assert.True(t, status.Snapshot().Degraded)

	probe := newSoakMongoProbe(client, 750*time.Millisecond, time.Now)
	require.Error(t, probe.Probe(ctx))
	assert.False(t, probe.Snapshot().Up)

	verifier := newSoakRoomStateVerifier(nil, store, "site-outage", nil, nil, time.Now)
	operation := &failureOperation{
		ID: "member-add-outage", OperationType: failureOperationMemberAdd,
		Targets: map[string]string{"roomId": roomID},
		Attributes: map[string]string{
			soakFailureAttributeTargetAccount:  account,
			soakFailureAttributeExpectedMember: "true",
		},
	}
	verifyStarted := time.Now()
	result, reason, err := verifier.Verify(ctx, operation)
	require.NoError(t, err)
	assert.Equal(t, soakRoomStateUnknown, result)
	assert.Equal(t, failureReasonNone, reason)
	assert.Less(t, time.Since(verifyStarted), soakRoomStateTimeout+time.Second,
		"verifier must enforce its own bound before the driver's server-selection timeout")

	require.NoError(t, container.Start(ctx))
	restartedURI, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	assert.Equal(t, uri, restartedURI, "fixed host port must survive container restart")
	awaitMongoOutagePrimary(t, ctx, client)
	retryTicks <- time.Now().UTC()
	attempt = awaitMongoOutageHeartbeatAttempt(t, observer.attempts)
	assert.Equal(t, soakHeartbeatSuccess, attempt.outcome)
	assert.False(t, attempt.degraded)
	assert.False(t, status.Snapshot().Degraded)
	require.NoError(t, probe.Probe(ctx))
	assert.True(t, probe.Snapshot().Up)

	result, reason, err = verifier.Verify(ctx, operation)
	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, failureReasonNone, reason)

	_, err = db.Collection(soakManifestCollection).UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: runID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: soakManifestStopped}}}},
	)
	require.NoError(t, err)
	healthyTicks <- time.Now().UTC()
	attempt = awaitMongoOutageHeartbeatAttempt(t, observer.attempts)
	assert.Equal(t, soakHeartbeatNotActive, attempt.outcome)
	assert.False(t, attempt.degraded)

	err = <-heartbeatErr
	heartbeatDone = true
	assert.ErrorIs(t, err, errSoakRunNotActive)
}
