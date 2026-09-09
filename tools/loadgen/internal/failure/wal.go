package failure

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const WALSchemaVersion = 2

type WALOption func(*WAL)

func WithDirectorySync(syncDirectory func(string) error) WALOption {
	return func(wal *WAL) {
		if syncDirectory != nil {
			wal.syncDirectory = syncDirectory
		}
	}
}

type WAL struct {
	mu               sync.Mutex
	path             string
	file             *os.File
	size             int64
	legacy           bool
	observerContract *ObserverContract
	syncDirectory    func(string) error
}

type walHeader struct {
	RecordType       string            `json:"recordType"`
	SchemaVersion    int               `json:"schemaVersion"`
	ObserverContract *ObserverContract `json:"observerContract,omitempty"`
}

func OpenWAL(path string, options ...WALOption) (*WAL, error) {
	if path == "" {
		return nil, fmt.Errorf("failure WAL path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create failure WAL directory: %w", err)
	}
	backupPath := path + ".bak"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if _, backupErr := os.Stat(backupPath); backupErr == nil {
			if err := os.Rename(backupPath, path); err != nil {
				return nil, fmt.Errorf("restore failure WAL backup: %w", err)
			}
		}
	}
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open failure WAL %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat failure WAL %q: %w", path, err)
	}
	wal := &WAL{
		path: path, file: file, size: info.Size(), syncDirectory: SyncWALDirectory,
	}
	for _, option := range options {
		if option != nil {
			option(wal)
		}
	}
	return wal, nil
}

// Replay buffers the whole journal. Recovery should prefer ReplayEach so a
// long-lived run's complete journal does not need to fit in memory.
func (w *WAL) Replay() ([]Event, error) {
	events := make([]Event, 0)
	if err := w.ReplayEach(func(event *Event) error {
		events = append(events, *event)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("buffer replayed failure WAL: %w", err)
	}
	return events, nil
}

// ReplayEach emits durable records in write order without retaining them.
func (w *WAL) ReplayEach(emit func(*Event) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("open failure WAL for replay: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			// A replay error is more useful than a later close error.
			_ = file.Close()
		}
	}()

	reader := bufio.NewReader(file)
	line := 0
	headerSeen := false
	w.observerContract = nil
	durableBytes := int64(0)
	tornFinalRecord := false
	for {
		encoded, readErr := reader.ReadBytes('\n')
		if errors.Is(readErr, io.EOF) {
			// Append writes JSON and its newline together. An unterminated final
			// record is a torn crash write, not a reason to lose prior records.
			tornFinalRecord = len(encoded) > 0
			break
		}
		if readErr != nil {
			return fmt.Errorf("read failure WAL line %d: %w", line+1, readErr)
		}
		line++
		var header walHeader
		if err := json.Unmarshal(encoded, &header); err == nil && header.RecordType != "" {
			if line != 1 || header.RecordType != "header" || header.SchemaVersion != WALSchemaVersion {
				return fmt.Errorf("decode failure WAL line %d: unsupported header version %d", line, header.SchemaVersion)
			}
			durableBytes += int64(len(encoded))
			headerSeen = true
			w.observerContract = CloneObserverContract(header.ObserverContract)
			continue
		}
		if line == 1 {
			w.legacy = true
		}
		var event Event
		if err := json.Unmarshal(encoded, &event); err != nil {
			return fmt.Errorf("decode failure WAL line %d: %w", line, err)
		}
		switch {
		case headerSeen && event.SchemaVersion != WALSchemaVersion:
			return fmt.Errorf(
				"decode failure WAL line %d: versioned WAL requires record version %d",
				line,
				WALSchemaVersion,
			)
		case !headerSeen && event.SchemaVersion != 0:
			return fmt.Errorf(
				"decode failure WAL line %d: record version %d requires a versioned header",
				line,
				event.SchemaVersion,
			)
		}
		if err := emit(&event); err != nil {
			return fmt.Errorf("apply failure WAL line %d: %w", line, err)
		}
		durableBytes += int64(len(encoded))
	}
	if line == 0 && !headerSeen {
		w.legacy = false
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close replayed failure WAL: %w", err)
	}
	closed = true
	if tornFinalRecord {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close failure WAL before repair: %w", err)
		}
		w.file = nil
		if err := os.Truncate(w.path, durableBytes); err != nil {
			return w.reopenAfterCompactFailure(
				fmt.Errorf("truncate torn failure WAL record: %w", err),
			)
		}
		// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
		// nosemgrep: gosec.G304-1
		reopened, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("reopen repaired failure WAL: %w", err)
		}
		w.file = reopened
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync repaired failure WAL: %w", err)
		}
		w.size = durableBytes
	}
	return nil
}

func (w *WAL) NeedsUpgrade() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.legacy
}

func (w *WAL) ConfigureObserverContract(contract ObserverContract, active []Operation) error {
	if err := ValidateObserverContract(contract); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.observerContract != nil {
		if !EqualObserverContract(*w.observerContract, contract) {
			return fmt.Errorf("%w: stored observer contract differs from current configuration", ErrObserverContractMismatch)
		}
		return nil
	}
	for index := range active {
		if !OperationMatchesObserverContract(&active[index], contract) {
			return fmt.Errorf(
				"%w: pending operation %q is incompatible with the current observer contract",
				ErrObserverContractMismatch,
				active[index].ID,
			)
		}
	}
	w.observerContract = CloneObserverContract(&contract)
	if w.size == 0 {
		return w.writeHeaderLocked()
	}
	w.legacy = true
	return nil
}

func (w *WAL) writeHeaderLocked() error {
	header, err := json.Marshal(walHeader{
		RecordType: "header", SchemaVersion: WALSchemaVersion,
		ObserverContract: CloneObserverContract(w.observerContract),
	})
	if err != nil {
		return fmt.Errorf("encode failure WAL header: %w", err)
	}
	header = append(header, '\n')
	written, err := w.file.Write(header)
	if err != nil {
		return fmt.Errorf("append failure WAL header: %w", err)
	}
	if written != len(header) {
		return fmt.Errorf("append failure WAL header: wrote %d of %d bytes", written, len(header))
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL header: %w", err)
	}
	if err := w.syncDirectory(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("sync failure WAL header directory: %w", err)
	}
	w.size += int64(written)
	return nil
}

func (w *WAL) Append(event *Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendBufferedLocked(event); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL event: %w", err)
	}
	return nil
}

func (w *WAL) AppendBuffered(event *Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendBufferedLocked(event)
}

func (w *WAL) appendBufferedLocked(event *Event) error {
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
	}
	if w.size == 0 {
		if err := w.writeHeaderLocked(); err != nil {
			return err
		}
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = WALSchemaVersion
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode failure WAL event: %w", err)
	}
	encoded = append(encoded, '\n')
	written, err := w.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("append failure WAL event: %w", err)
	}
	if written != len(encoded) {
		return fmt.Errorf("append failure WAL event: wrote %d of %d bytes", written, len(encoded))
	}
	w.size += int64(written)
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL event: %w", err)
	}
	return nil
}

func (w *WAL) Compact(events []Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
	}
	temporaryPath := w.path + ".compact"
	backupPath := w.path + ".bak"
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	temporary, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create compacted failure WAL: %w", err)
	}
	closeTemporary := true
	defer func() {
		if closeTemporary {
			_ = temporary.Close()
		}
	}()
	var compactedSize int64
	header, err := json.Marshal(walHeader{
		RecordType: "header", SchemaVersion: WALSchemaVersion,
		ObserverContract: CloneObserverContract(w.observerContract),
	})
	if err != nil {
		return fmt.Errorf("encode compacted failure WAL header: %w", err)
	}
	header = append(header, '\n')
	written, err := temporary.Write(header)
	if err != nil {
		return fmt.Errorf("write compacted failure WAL header: %w", err)
	}
	if written != len(header) {
		return fmt.Errorf("write compacted failure WAL header: wrote %d of %d bytes", written, len(header))
	}
	compactedSize += int64(written)
	for index := range events {
		event := &events[index]
		if event.SchemaVersion == 0 {
			event.SchemaVersion = WALSchemaVersion
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode compacted failure WAL event: %w", err)
		}
		encoded = append(encoded, '\n')
		written, err := temporary.Write(encoded)
		if err != nil {
			return fmt.Errorf("write compacted failure WAL event: %w", err)
		}
		if written != len(encoded) {
			return fmt.Errorf("write compacted failure WAL event: wrote %d of %d bytes", written, len(encoded))
		}
		compactedSize += int64(written)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync compacted failure WAL: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close compacted failure WAL: %w", err)
	}
	closeTemporary = false

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close failure WAL before compaction: %w", err)
	}
	w.file = nil
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return w.reopenAfterCompactFailure(fmt.Errorf("remove stale failure WAL backup: %w", err))
	}
	if err := os.Rename(w.path, backupPath); err != nil {
		return w.reopenAfterCompactFailure(fmt.Errorf("backup failure WAL: %w", err))
	}
	if err := os.Rename(temporaryPath, w.path); err != nil {
		if restoreErr := os.Rename(backupPath, w.path); restoreErr != nil {
			return fmt.Errorf("install compacted failure WAL: %w; restore backup: %v", err, restoreErr)
		}
		return w.reopenAfterCompactFailure(fmt.Errorf("install compacted failure WAL: %w", err))
	}
	if err := w.syncDirectory(filepath.Dir(w.path)); err != nil {
		return w.reopenAfterCompactFailure(fmt.Errorf("sync installed failure WAL directory: %w", err))
	}
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen compacted failure WAL: %w", err)
	}
	w.file = file
	w.size = compactedSize
	w.legacy = false
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove failure WAL backup: %w", err)
	}
	if err := w.syncDirectory(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("sync failure WAL directory: %w", err)
	}
	return nil
}

func SyncWALDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		// Windows does not support fsync on directory handles. Production PVCs
		// run on Linux, where this makes rename-based compaction durable.
		return nil
	}
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	defer directoryFile.Close()
	if err := directoryFile.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}

func (w *WAL) reopenAfterCompactFailure(compactErr error) error {
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, reopenErr := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if reopenErr == nil {
		w.file = file
		if info, statErr := file.Stat(); statErr == nil {
			w.size = info.Size()
		}
		return compactErr
	}
	return fmt.Errorf("%v; reopen failure WAL: %w", compactErr, reopenErr)
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close failure WAL: %w", err)
	}
	w.file = nil
	return nil
}
