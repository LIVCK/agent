package buffer

import (
	"fmt"
	"os"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
	"google.golang.org/protobuf/proto"
)

// spoolPerm is the mode for the spool file: owner read/write only, it holds host
// metrics.
const spoolPerm os.FileMode = 0o600

// Spool persists the ring and pending events to a single file on the state
// directory at shutdown and reloads them on start. It reuses the MetricBatch
// wire message as the on-disk container (reports plus events), so the spool
// format is exactly what would have gone on the wire. The file is consumed on
// load (removed) so a crash after load does not replay stale data twice.
type Spool struct {
	fs   platform.FS
	path string
	// maxBytes caps the serialized spool, matching the ring byte cap.
	maxBytes int
}

// NewSpool builds a spool at path. maxBytes <= 0 uses DefaultMaxBytes.
func NewSpool(fs platform.FS, path string, maxBytes int) *Spool {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	return &Spool{fs: fs, path: path, maxBytes: maxBytes}
}

// Save marshals reports and events into one MetricBatch and writes it atomically
// at 0600. If the serialized batch would exceed the byte cap, the oldest reports
// are dropped until it fits (events are kept: they are the stronger class and
// tiny). An empty batch removes any stale spool file instead of writing an empty
// one.
func (s *Spool) Save(reports []*wire.Report, events []*wire.Event) error {
	if len(reports) == 0 && len(events) == 0 {
		return s.fs.Remove(s.path)
	}
	batch := &wire.MetricBatch{Reports: reports, Events: events}
	for proto.Size(batch) > s.maxBytes && len(batch.Reports) > 0 {
		// Drop the oldest report (reports are oldest-first).
		batch.Reports = batch.Reports[1:]
	}
	data, err := proto.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal spool: %w", err)
	}
	return s.fs.WriteFileAtomic(s.path, data, spoolPerm)
}

// Load reads and removes the spool file, returning the persisted reports and
// events. A missing file returns no data and no error.
func (s *Spool) Load() (reports []*wire.Report, events []*wire.Event, err error) {
	data, err := s.fs.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("read spool: %w", err)
	}
	// Consume-once: remove before returning so a later crash does not double-load.
	_ = s.fs.Remove(s.path)

	var batch wire.MetricBatch
	if err := proto.Unmarshal(data, &batch); err != nil {
		return nil, nil, fmt.Errorf("unmarshal spool: %w", err)
	}
	return batch.Reports, batch.Events, nil
}
