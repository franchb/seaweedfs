package erasure_coding

import (
	"errors"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// override swaps *seam for fn until the test ends.
func override[T any](t *testing.T, seam *T, fn T) {
	old := *seam
	*seam = fn
	t.Cleanup(func() { *seam = old })
}

func ecjJournalCount() int {
	ecjJournalsMu.Lock()
	defer ecjJournalsMu.Unlock()
	return len(ecjJournals)
}

// A failed rename (pre-publication) must keep the original journal working:
// mount succeeds, the set loads, and the file is untouched.
func TestEcjCompactRenameFailureKeepsOriginal(t *testing.T) {
	dir := t.TempDir()
	base, ids := seedBloatedEcVolume(t, dir, 4096)
	before, err := os.Stat(base + ".ecj")
	require.NoError(t, err)

	override(t, &ecjRename, func(_, _ string) error { return errors.New("injected rename failure") })

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err, "pre-rename failure must be non-fatal")
	defer ev.Close()

	assert.NotNil(t, ev.ecjFile, "handle must be restored after failed rename")
	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	after, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before.Size(), after.Size(), "original journal must be untouched")
	assert.NoFileExists(t, base+EcjCompactTmpSuffix)
}

// When the rename fails AND the original cannot be reopened, the mount fails
// and the error names both causes rather than claiming a compaction happened.
func TestEcjCompactRenameAndReopenFailureReportsBoth(t *testing.T) {
	dir := t.TempDir()
	seedBloatedEcVolume(t, dir, 4096)

	override(t, &ecjRename, func(_, _ string) error { return errors.New("injected rename failure") })
	override(t, &ecjOpenVolumeFile, func(string, int) (*os.File, error) {
		return nil, errors.New("injected reopen failure")
	})

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.Error(t, err)
	assert.Nil(t, ev)
	assert.Contains(t, err.Error(), "injected rename failure")
	assert.Contains(t, err.Error(), "injected reopen failure")
	assert.NotContains(t, err.Error(), "was compacted")
	assert.Zero(t, ecjJournalCount(), "a failed mount must not stay registered as a holder")
}

// A failed reopen (post-publication) must be a mount error, never a volume
// holding a nil or unlinked handle.
func TestEcjCompactReopenFailureIsMountError(t *testing.T) {
	dir := t.TempDir()
	seedBloatedEcVolume(t, dir, 4096)

	override(t, &ecjOpenVolumeFile, func(string, int) (*os.File, error) {
		return nil, errors.New("injected reopen failure")
	})

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.Error(t, err, "post-rename reopen failure must fail the mount")
	assert.Nil(t, ev)
}

// A failed directory sync happens after the rename published the new journal,
// so it must fail the mount like a failed reopen.
func TestEcjCompactFsyncDirFailureIsMountError(t *testing.T) {
	dir := t.TempDir()
	seedBloatedEcVolume(t, dir, 4096)

	override(t, &ecjFsyncDir, func(string) error { return errors.New("injected fsync failure") })

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.Error(t, err, "post-rename directory sync failure must fail the mount")
	assert.Nil(t, ev)
	assert.Contains(t, err.Error(), "injected fsync failure")
}

// A load that fails part way leaves a partial set. Compacting from it would
// rewrite the journal without every id past the failure, so the journal must
// be left exactly as it was.
func TestEcjLoadFailureSkipsCompaction(t *testing.T) {
	dir := t.TempDir()
	base, ids := seedBloatedEcVolume(t, dir, 4096)
	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)

	override(t, &ecjLoad, func(ev *EcVolume) error {
		// Only the first half of the distinct ids make it into the set.
		for _, id := range ids[:len(ids)/2] {
			ev.deletedNeedles[id] = struct{}{}
		}
		return errors.New("injected read error")
	})

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err, "a load error is logged, not fatal")
	defer ev.Close()

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after, "a partial set must never be written back as the journal")
}

// A temp file left by a crash before the rename is removed at mount, even
// when the journal does not need compacting.
func TestEcjStaleCompactTmpRemovedOnMount(t *testing.T) {
	dir := t.TempDir()
	base, _ := seedBloatedEcVolume(t, dir, 1)
	require.NoError(t, os.WriteFile(base+EcjCompactTmpSuffix, []byte("partial"), 0644))

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev.Close()
	assert.NoFileExists(t, base+EcjCompactTmpSuffix)
}

// Every mounted volume registers as a holder of its journal and drops out on
// Close, so the registry neither leaks entries nor forgets live holders.
func TestEcjHolderRegistration(t *testing.T) {
	dir := t.TempDir()
	base, _ := seedBloatedEcVolume(t, dir, 1)
	key := ecjJournalKey(base + ".ecj")
	holders := func() int {
		ecjJournalsMu.Lock()
		j := ecjJournals[key]
		ecjJournalsMu.Unlock()
		if j == nil {
			return 0
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.holders
	}

	ev1, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	ev2, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	assert.Equal(t, 2, holders())

	ev1.Close()
	ev1.Close() // idempotent
	assert.Equal(t, 1, holders())
	ev2.Close()
	assert.Zero(t, holders())
	assert.Zero(t, ecjJournalCount())
}

// MergeEcjFile appends only ids the destination lacks, once each, and repairs
// a torn destination tail before appending.
func TestMergeEcjFileIsSetUnion(t *testing.T) {
	dir := t.TempDir()
	dst, src := dir+"/7.ecj", dir+"/7"+EcjIncomingSuffix

	writeRepeatedEcj(t, dst, []types.NeedleId{1, 2, 3}, 2)
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xAB, 0xCD})
	require.NoError(t, err)
	require.NoError(t, f.Close())
	writeRepeatedEcj(t, src, []types.NeedleId{3, 4, 5, 4}, 3)

	added, err := MergeEcjFile(dst, src)
	require.NoError(t, err)
	assert.Equal(t, 2, added)

	got, err := readEcjIds(dst)
	require.NoError(t, err)
	assert.Equal(t, []types.NeedleId{1, 2, 3, 1, 2, 3, 4, 5}, got)

	// Merging the same journal again adds nothing.
	added, err = MergeEcjFile(dst, src)
	require.NoError(t, err)
	assert.Zero(t, added)
	fi, err := os.Stat(dst)
	require.NoError(t, err)
	assert.Equal(t, int64(8*types.NeedleIdSize), fi.Size())
	assert.Zero(t, ecjJournalCount())
}

// A peer without a journal leaves the local one alone, and does not create
// one either.
func TestMergeEcjFileMissingSourceIsNoop(t *testing.T) {
	dir := t.TempDir()
	added, err := MergeEcjFile(dir+"/7.ecj", dir+"/missing")
	require.NoError(t, err)
	assert.Zero(t, added)
	assert.NoFileExists(t, dir+"/7.ecj")
}
