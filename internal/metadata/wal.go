package metadata

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// wal is an append-only log of resolved mutation records.
//
// Frame: [u32 length][u32 crc32c(payload)][payload JSON]
//
// Records describe EFFECTS (the version with its assigned sequence number,
// timestamps and IDs), not REQUESTS, so replay is deterministic and never
// consults the clock or random sources.
//
// A torn final frame (crash mid-append) is detected by length/CRC and
// truncated on open. Any append error makes the shard fail-stop (read-only):
// continuing to append after a partial write could put valid records behind
// a corrupt one, and replay stops at the first corrupt frame.
type wal struct {
	dir     string
	f       *os.File
	w       *bufio.Writer
	sync    bool
	records int
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	walName      = "wal.log"
	snapshotName = "snapshot.json"
	maxFrame     = 64 << 20
)

func openWAL(dir string, sync bool) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{dir: dir, f: f, sync: sync}, nil
}

// replay calls fn for each intact record and truncates any torn tail.
func (w *wal) replay(fn func(*record) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(w.f, 1<<20)
	var good int64
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // clean EOF or torn header
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		if n > maxFrame {
			break
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			break
		}
		if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(hdr[4:]) {
			break
		}
		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("wal record at offset %d: %w", good, err)
		}
		if err := fn(&rec); err != nil {
			return err
		}
		good += int64(8 + n)
		w.records++
	}
	if err := w.f.Truncate(good); err != nil {
		return err
	}
	if _, err := w.f.Seek(good, io.SeekStart); err != nil {
		return err
	}
	w.w = bufio.NewWriterSize(w.f, 64<<10)
	return nil
}

func (w *wal) append(rec *record) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.Checksum(b, crcTable))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	w.records++
	return nil
}

// writeSnapshot atomically replaces the snapshot, then truncates the WAL.
// Crash between the two steps is safe: replay skips records whose LSN is
// already covered by the snapshot.
func (w *wal) writeSnapshot(snap any) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(w.dir, snapshotName+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if w.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, snapshotName)); err != nil {
		return err
	}
	if w.sync {
		if err := syncFile(w.dir); err != nil {
			return err
		}
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.w.Reset(w.f)
	w.records = 0
	return nil
}

func readSnapshot(dir string, into any) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(b, into)
}

func (w *wal) close() error {
	if w.w != nil {
		w.w.Flush()
	}
	return w.f.Close()
}

func syncFile(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
