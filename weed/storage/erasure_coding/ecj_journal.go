package erasure_coding

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// EcjCompactTmpSuffix names the temp file mount-time compaction writes before
// renaming it over the journal. A leftover one is only ever an unpublished
// rewrite: the journal it would have replaced is still intact.
const EcjCompactTmpSuffix = ".ecj.compact.tmp"

// EcjIncomingSuffix names the staging file a peer's journal is copied into
// before MergeEcjFile folds it into the local one.
const EcjIncomingSuffix = ".ecj.incoming"

// ecjJournal serialises the process's path-level work on one .ecj file and
// counts the EcVolume instances holding it open.
//
// Several EcVolumes can hold the same journal: with -dir.idx every disk
// resolves it to one path, and cross-disk reconcile (#9212) mounts a disk's
// orphan shards against a sibling disk's index. Compaction replaces the file's
// inode, so it may only run while nothing else has the old inode open for
// append. mu is held across an EcVolume's open + load + compact and across
// every MergeEcjFile, so a holder registered under it is known to every later
// compaction, and a merge never straddles one.
type ecjJournal struct {
	key     string
	mu      sync.Mutex
	holders int // EcVolumes holding the journal open; guarded by mu
	refs    int // holders plus in-flight lockers; guarded by ecjJournalsMu
}

var (
	ecjJournalsMu sync.Mutex
	ecjJournals   = map[string]*ecjJournal{}
)

func ecjJournalKey(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return filepath.Clean(path)
}

func retainEcjJournal(path string) *ecjJournal {
	key := ecjJournalKey(path)
	ecjJournalsMu.Lock()
	defer ecjJournalsMu.Unlock()
	j := ecjJournals[key]
	if j == nil {
		j = &ecjJournal{key: key}
		ecjJournals[key] = j
	}
	j.refs++
	return j
}

func (j *ecjJournal) release() {
	ecjJournalsMu.Lock()
	defer ecjJournalsMu.Unlock()
	j.refs--
	if j.refs == 0 {
		delete(ecjJournals, j.key)
	}
}

// withEcjJournal runs fn with path's journal lock held.
func withEcjJournal(path string, fn func(j *ecjJournal) error) error {
	j := retainEcjJournal(path)
	defer j.release()
	j.mu.Lock()
	defer j.mu.Unlock()
	return fn(j)
}

// addHolder records an EcVolume holding the journal open. Called with j.mu
// held; the holder keeps its own reference until dropHolder.
func (j *ecjJournal) addHolder() {
	j.holders++
	ecjJournalsMu.Lock()
	j.refs++
	ecjJournalsMu.Unlock()
}

func (j *ecjJournal) dropHolder() {
	j.mu.Lock()
	j.holders--
	j.mu.Unlock()
	j.release()
}

// MergeEcjFile folds the journal at srcPath into dstPath as a set union:
// only ids that dstPath does not already hold are appended, once each.
//
// The journal is a set of deleted needle ids. Appending a peer's whole journal
// onto the local one (what shard copy and index recovery used to do) is
// correct but never dedupes, so a volume whose shards move back and forth
// grows its journal geometrically. A union keeps it at one entry per id.
//
// A missing srcPath is a no-op. A torn tail on either side is ignored on the
// source and truncated on the destination, since a partial record there would
// misalign the append. Runs under the journal lock, so it never interleaves
// with a mount's load and compaction of the same file.
func MergeEcjFile(dstPath, srcPath string) (added int, err error) {
	incoming, err := readEcjIds(srcPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	err = withEcjJournal(dstPath, func(*ecjJournal) error {
		added, err = mergeEcjLocked(dstPath, incoming)
		return err
	})
	return added, err
}

func mergeEcjLocked(dstPath string, incoming []types.NeedleId) (int, error) {
	dst, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer dst.Close()
	fi, err := dst.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	if ragged := size % int64(types.NeedleIdSize); ragged != 0 {
		size -= ragged
		if err := dst.Truncate(size); err != nil {
			return 0, fmt.Errorf("repair torn tail of %s: %w", dstPath, err)
		}
	}

	have := make(map[types.NeedleId]struct{})
	if err := scanEcj(io.NewSectionReader(dst, 0, size), func(id types.NeedleId) {
		have[id] = struct{}{}
	}); err != nil {
		return 0, fmt.Errorf("read %s: %w", dstPath, err)
	}
	out := make([]byte, 0, len(incoming)*types.NeedleIdSize)
	rec := make([]byte, types.NeedleIdSize)
	for _, id := range incoming {
		if _, ok := have[id]; ok {
			continue
		}
		have[id] = struct{}{}
		types.NeedleIdToBytes(rec, id)
		out = append(out, rec...)
	}
	if len(out) == 0 {
		return 0, nil
	}
	if _, err := dst.WriteAt(out, size); err != nil {
		_ = dst.Truncate(size)
		return 0, fmt.Errorf("append to %s: %w", dstPath, err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Truncate(size)
		return 0, fmt.Errorf("sync %s: %w", dstPath, err)
	}
	return len(out) / types.NeedleIdSize, nil
}

// readEcjIds reads every whole record of a journal, in file order.
func readEcjIds(path string) ([]types.NeedleId, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ids []types.NeedleId
	if err := scanEcj(f, func(id types.NeedleId) { ids = append(ids, id) }); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return ids, nil
}

// scanEcj calls fn for each whole record in r, ignoring a torn tail.
func scanEcj(r io.Reader, fn func(types.NeedleId)) error {
	br := bufio.NewReaderSize(r, ecjLoadChunkBytes)
	rec := make([]byte, types.NeedleIdSize)
	for {
		if _, err := io.ReadFull(br, rec); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		fn(types.BytesToNeedleId(rec))
	}
}
