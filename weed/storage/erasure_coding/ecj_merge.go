package erasure_coding

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ReadEcjIds reads the id set from an .ecj journal file. A missing file
// reads as empty. A torn trailing partial record is ignored — it was never
// readable and keeping it would misalign later appends.
func ReadEcjIds(path string) (map[types.NeedleId]struct{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[types.NeedleId]struct{}), nil
		}
		return nil, err
	}
	count := len(data) / types.NeedleIdSize
	ids := make(map[types.NeedleId]struct{}, count)
	for i := 0; i < count; i++ {
		ids[types.BytesToNeedleId(data[i*types.NeedleIdSize:(i+1)*types.NeedleIdSize])] = struct{}{}
	}
	return ids, nil
}

// MergeEcjUnion writes localPath ∪ incomingPath into destPath as a
// deduplicated, sorted journal (8-byte big-endian records) via
// <dest>.tmp + fsync + rename + fsync-dir. It returns the distinct id count.
//
// All three paths may coincide (local == dest is the normal copy case).
// Missing inputs read as empty. Failures before the rename leave dest
// untouched; the caller removes its own staging files.
func MergeEcjUnion(localPath, incomingPath, destPath string) (int, error) {
	union, err := ReadEcjIds(localPath)
	if err != nil {
		return 0, fmt.Errorf("read local ecj %s: %w", localPath, err)
	}
	if incomingPath != localPath {
		incoming, err := ReadEcjIds(incomingPath)
		if err != nil {
			return 0, fmt.Errorf("read incoming ecj %s: %w", incomingPath, err)
		}
		for id := range incoming {
			union[id] = struct{}{}
		}
	}
	sorted := make([]types.NeedleId, 0, len(union))
	for id := range union {
		sorted = append(sorted, id)
	}
	slices.Sort(sorted)

	tmpPath := destPath + ".tmp"
	if err := writeEcjIdsSorted(tmpPath, sorted); err != nil {
		os.Remove(tmpPath)
		return 0, err
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("commit merged ecj %s: %w", destPath, err)
	}
	if err := util.FsyncDir(filepath.Dir(destPath)); err != nil {
		return 0, fmt.Errorf("fsync dir for %s: %w", destPath, err)
	}
	return len(sorted), nil
}

func writeEcjIdsSorted(tmpPath string, ids []types.NeedleId) error {
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create merged ecj %s: %w", tmpPath, err)
	}
	buf := make([]byte, types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(buf, id)
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return fmt.Errorf("write merged ecj %s: %w", tmpPath, err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync merged ecj %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close merged ecj %s: %w", tmpPath, err)
	}
	return nil
}

// AdoptMergedEcj adopts a merged .ecj that replaced this volume's journal on
// disk (copy/recover dedup union). It extends the in-memory deleted set with
// the merged ids and reopens the journal handle on the new inode, so later
// deletes land in the live file and never an unlinked one. Call after the
// atomic rename has published FileName(".ecj").
func (ev *EcVolume) AdoptMergedEcj(merged map[types.NeedleId]struct{}) error {
	ev.deletedNeedlesLock.Lock()
	for id := range merged {
		ev.deletedNeedles[id] = struct{}{}
	}
	ev.deletedNeedlesLock.Unlock()

	ev.ecjFileAccessLock.Lock()
	defer ev.ecjFileAccessLock.Unlock()
	if ev.ecjFile != nil {
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
	}
	ecjPath := ev.FileName(".ecj")
	f, err := os.OpenFile(ecjPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("reopen merged ecj %s: %w", ecjPath, err)
	}
	if fi, statErr := f.Stat(); statErr == nil {
		ev.ecjFileSize = fi.Size()
	} else {
		ev.ecjFileSize = 0
	}
	ev.ecjFile = f
	return nil
}
