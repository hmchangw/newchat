package failure

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureWAL_RequiresPath(t *testing.T) {
	_, err := OpenWAL("")
	require.ErrorContains(t, err, "path is required")
}

func TestFailureWAL_ReportsPathSetupErrors(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(parent, []byte("file"), 0o600))
	_, err := OpenWAL(filepath.Join(parent, "run.wal"))
	require.ErrorContains(t, err, "create failure WAL directory")

	directory := filepath.Join(t.TempDir(), "run.wal")
	require.NoError(t, os.Mkdir(directory, 0o750))
	_, err = OpenWAL(directory, nil)
	require.ErrorContains(t, err, "open failure WAL")
}

func TestFailureWAL_AppendsAndReplaysInOrder(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.ConfigureObserverContract(NewObserverContract(false, false), nil))

	started := Event{Type: EventStarted, Operation: testJournalOperation("operation-1"), At: testJournalTime()}
	observed := Event{
		Type: EventObserved, OperationID: "operation-1", Observer: ObserverAdmission,
		Observation: ObservationGood, At: testJournalTime().Add(time.Second),
	}
	require.NoError(t, wal.Append(&started))
	require.NoError(t, wal.Append(&observed))
	assert.Equal(t, WALSchemaVersion, started.SchemaVersion)

	var streamed []string
	require.NoError(t, wal.ReplayEach(func(event *Event) error {
		streamed = append(streamed, event.Type)
		return nil
	}))
	buffered, err := wal.Replay()
	require.NoError(t, err)
	require.Len(t, buffered, 2)
	assert.Equal(t, []string{EventStarted, EventObserved}, streamed)
	assert.Equal(t, "operation-1", buffered[0].Operation.ID)
}

func TestFailureWAL_ReplayEachStopsAtApplyError(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.Append(&Event{Type: EventCheckpoint, At: testJournalTime()}))
	require.NoError(t, wal.Append(&Event{Type: EventCheckpoint, At: testJournalTime()}))

	seen := 0
	err = wal.ReplayEach(func(*Event) error {
		seen++
		return assert.AnError
	})

	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, seen)
}

func TestFailureWAL_ReplayReportsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := OpenWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	require.NoError(t, os.Remove(path))

	_, err = wal.Replay()
	require.ErrorContains(t, err, "open failure WAL for replay")
}

func TestFailureWAL_EmptyReplayIsNotLegacy(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	events, err := wal.Replay()
	require.NoError(t, err)
	assert.Empty(t, events)
	assert.False(t, wal.NeedsUpgrade())
}

func TestFailureWAL_RepairsTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := OpenWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append(&Event{Type: EventCheckpoint, At: testJournalTime()}))
	require.NoError(t, wal.Close())

	// #nosec G304 -- test-owned temporary path
	// nosemgrep: gosec.G304-1
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString(`{"type":"observed"`)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reopened, err := OpenWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	events, err := reopened.Replay()
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NoError(t, reopened.Append(&Event{Type: EventCheckpoint, At: testJournalTime()}))
	events, err = reopened.Replay()
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestFailureWAL_RejectsInvalidVersionFraming(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{"unsupported header", `{"recordType":"header","schemaVersion":99}` + "\n"},
		{"legacy record after header", `{"recordType":"header","schemaVersion":2}` + "\n" +
			`{"type":"checkpoint","at":"2026-09-09T01:02:03Z"}` + "\n"},
		{"versioned record without header", `{"schemaVersion":2,"type":"checkpoint","at":"2026-09-09T01:02:03Z"}` + "\n"},
		{"malformed complete record", `{"type":]` + "\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "run.wal")
			require.NoError(t, os.WriteFile(path, []byte(tc.contents), 0o600))
			wal, err := OpenWAL(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			_, err = wal.Replay()
			require.Error(t, err)
		})
	}
}

func TestFailureWAL_LegacyContractUpgradeAndMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.wal")
	legacyOperation := testJournalOperation("legacy")
	legacyOperation.SchemaVersion = 0
	legacyOperation.Effects = nil
	legacyOperation.Expected = []Observer{ObserverAdmission, ObserverHistory}
	legacy, err := json.Marshal(Event{Type: EventStarted, Operation: legacyOperation, At: testJournalTime()})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(legacy, '\n'), 0o600))

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	_, err = wal.Replay()
	require.NoError(t, err)
	assert.True(t, wal.NeedsUpgrade())
	contract := NewObserverContract(false, false)
	require.NoError(t, wal.ConfigureObserverContract(contract, []Operation{*legacyOperation}))
	require.NoError(t, wal.Compact([]Event{{Type: EventStarted, Operation: legacyOperation, At: testJournalTime()}}))
	assert.False(t, wal.NeedsUpgrade())

	require.NoError(t, wal.ConfigureObserverContract(contract, nil))
	err = wal.ConfigureObserverContract(NewObserverContract(true, false), nil)
	require.ErrorIs(t, err, ErrObserverContractMismatch)
}

func TestFailureWAL_RejectsLegacyOperationsOutsideConfiguredContract(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	operation := testJournalOperation("legacy")
	operation.Expected = append(operation.Expected, ObserverRecipient)

	err = wal.ConfigureObserverContract(NewObserverContract(false, false), []Operation{*operation})
	require.ErrorIs(t, err, ErrObserverContractMismatch)
}

func TestFailureWAL_RejectsInvalidObserverContract(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	err = wal.ConfigureObserverContract(ObserverContract{}, nil)
	require.ErrorContains(t, err, "unsupported observer contract schema")
}

func TestFailureWAL_CompactionSyncsContainingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	var synced []string
	wal, err := OpenWAL(path, WithDirectorySync(func(directory string) error {
		synced = append(synced, directory)
		return nil
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	require.NoError(t, wal.Compact(nil))
	assert.Equal(t, []string{filepath.Dir(path), filepath.Dir(path)}, synced)
}

func TestFailureWAL_RestoresBackupWhenPrimaryIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	backup := path + ".bak"
	require.NoError(t, os.WriteFile(backup, nil, 0o600))

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	_, err = os.Stat(path)
	require.NoError(t, err)
	_, err = os.Stat(backup)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestFailureWAL_ClosedWriterRejectsMutations(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	require.NoError(t, wal.Close())

	assert.Error(t, wal.AppendBuffered(&Event{Type: EventCheckpoint}))
	assert.Error(t, wal.Sync())
	assert.Error(t, wal.Compact(nil))
}

func TestFailureWAL_ReportsDirectorySyncFailure(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"), WithDirectorySync(func(string) error {
		return assert.AnError
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	err = wal.ConfigureObserverContract(NewObserverContract(false, false), nil)
	require.ErrorIs(t, err, assert.AnError)
}

func TestFailureWAL_CompactionReportsBothDirectorySyncBarriers(t *testing.T) {
	tests := []struct {
		name   string
		failAt int
	}{
		{name: "installed file", failAt: 1},
		{name: "removed backup", failAt: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"), WithDirectorySync(func(string) error {
				calls++
				if calls == tc.failAt {
					return assert.AnError
				}
				return nil
			}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })

			err = wal.Compact([]Event{{Type: EventCheckpoint, At: testJournalTime()}})
			require.ErrorIs(t, err, assert.AnError)
			assert.Equal(t, tc.failAt, calls)
		})
	}
}

func TestFailureWAL_CompactionRecoversFromFilesystemConflicts(t *testing.T) {
	t.Run("stale backup cannot be removed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "run.wal")
		wal, err := OpenWAL(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		require.NoError(t, os.Mkdir(path+".bak", 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(path+".bak", "keep"), nil, 0o600))

		err = wal.Compact(nil)
		require.ErrorContains(t, err, "remove stale failure WAL backup")
		require.NoError(t, wal.Append(&Event{Type: EventCheckpoint, At: testJournalTime()}))
	})

	t.Run("primary disappears before backup", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "run.wal")
		wal, err := OpenWAL(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		wal.path = filepath.Join(directory, "missing.wal")

		err = wal.Compact(nil)
		require.ErrorContains(t, err, "backup failure WAL")
		require.NoError(t, wal.Append(&Event{Type: EventCheckpoint, At: testJournalTime()}))
	})

	t.Run("compaction temporary path is a directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "run.wal")
		wal, err := OpenWAL(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		require.NoError(t, os.Mkdir(path+".compact", 0o750))

		err = wal.Compact(nil)
		require.ErrorContains(t, err, "create compacted failure WAL")
	})
}

func TestFailureWAL_ReportsClosedUnderlyingFile(t *testing.T) {
	t.Run("header write", func(t *testing.T) {
		wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
		require.NoError(t, err)
		require.NoError(t, wal.file.Close())
		err = wal.ConfigureObserverContract(NewObserverContract(false, false), nil)
		require.ErrorContains(t, err, "append failure WAL header")
		_ = wal.Close()
	})

	t.Run("append sync", func(t *testing.T) {
		wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
		require.NoError(t, err)
		require.NoError(t, wal.ConfigureObserverContract(NewObserverContract(false, false), nil))
		require.NoError(t, wal.file.Close())
		err = wal.Append(&Event{Type: EventCheckpoint, At: testJournalTime()})
		require.ErrorContains(t, err, "append failure WAL event")
		err = wal.Close()
		require.ErrorContains(t, err, "close failure WAL")
	})

	t.Run("compaction closes writer", func(t *testing.T) {
		wal, err := OpenWAL(filepath.Join(t.TempDir(), "run.wal"))
		require.NoError(t, err)
		require.NoError(t, wal.file.Close())
		err = wal.Compact(nil)
		require.ErrorContains(t, err, "close failure WAL before compaction")
		_ = wal.Close()
	})
}

func testJournalTime() time.Time {
	return time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
}

func testJournalOperation(id string) *Operation {
	now := testJournalTime()
	return &Operation{
		SchemaVersion: WALSchemaVersion,
		ID:            id, RunID: "run-1", Scenario: ScenarioMessageSoak, Lane: LaneMessageSend,
		OperationType: OperationMessageCreate, LifecycleState: OperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"messageId": id},
		Effects: MessageCreateExpectedEffects(false, false, 0, ""),
	}
}
