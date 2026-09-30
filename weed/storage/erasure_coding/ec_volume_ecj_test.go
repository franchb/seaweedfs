package erasure_coding_test

import (
	"os"
	"testing"

	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ecjBytes(ids ...types.NeedleId) []byte {
	b := make([]byte, 0, len(ids)*types.NeedleIdSize)
	rec := make([]byte, types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(rec, id)
		b = append(b, rec...)
	}
	return b
}

func mountEcVolume(t *testing.T, dir string, ecx, ecj []byte) (*erasure_coding.EcVolume, string) {
	t.Helper()
	base := erasure_coding.EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", ecx, 0644))
	if ecj != nil {
		require.NoError(t, os.WriteFile(base+".ecj", ecj, 0644))
	}
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	return ev, base
}

// A crash mid-append leaves a partial record at the end of .ecj. Mount must
// drop it: deletes append at the physical end, so keeping the fragment would
// misalign every later record and lose those deletes on the next mount.
func TestEcjTornTailTruncatedOnMount(t *testing.T) {
	dir := t.TempDir()

	ecx := append(makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(0), types.Size(100)),
		makeNeedleMapEntry(types.NeedleId(4), types.ToOffset(8), types.Size(100))...)
	ecj := append(ecjBytes(1, 2, 3), []byte{1, 2, 3, 4, 5}...)

	ev, base := mountEcVolume(t, dir, ecx, ecj)

	fi, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, int64(3*types.NeedleIdSize), fi.Size())
	for _, id := range []types.NeedleId{1, 2, 3} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	assert.False(t, ev.IsNeedleDeleted(4))

	require.NoError(t, ev.DeleteNeedleFromEcx(4))
	ev.Close()

	ev, _ = mountEcVolume(t, dir, ecx, nil)
	defer ev.Close()
	for _, id := range []types.NeedleId{1, 2, 3, 4} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
}

// A journal larger than one load chunk must seed every record, including the
// ones straddling and following the chunk boundary.
func TestEcjLoadsAcrossChunkBoundary(t *testing.T) {
	dir := t.TempDir()

	const count = types.NeedleId(200_000) // 1.6 MiB > 1 MiB chunk
	ecj := make([]byte, 0, int(count)*types.NeedleIdSize)
	for id := types.NeedleId(1); id <= count; id++ {
		rec := make([]byte, types.NeedleIdSize)
		types.NeedleIdToBytes(rec, id)
		ecj = append(ecj, rec...)
	}

	ev, _ := mountEcVolume(t, dir, nil, ecj)
	defer ev.Close()

	for _, id := range []types.NeedleId{1, 131072, 131073, count} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	assert.False(t, ev.IsNeedleDeleted(count+1))
}

func ecjSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

// A bloated journal (many repeats of few ids) must mount to exactly those ids
// and be rewritten down to one entry per id.
func TestEcjBloatedIsCompactedOnMount(t *testing.T) {
	dir := t.TempDir()
	base, ids := erasure_coding.SeedBloatedEcVolume(t, dir, 4096)
	before := ecjSize(t, base+".ecj")
	require.Equal(t, int64(100*4096*types.NeedleIdSize), before)

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)

	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	after := ecjSize(t, base+".ecj")
	assert.Equal(t, int64(len(ids)*types.NeedleIdSize), after)
	assert.NoFileExists(t, base+erasure_coding.EcjCompactTmpSuffix)
	ev.Close()

	// Remount is idempotent.
	ev2, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev2.Close()
	for _, id := range ids {
		assert.True(t, ev2.IsNeedleDeleted(id), "id %d", id)
	}
	assert.Equal(t, after, ecjSize(t, base+".ecj"))
}

// A delete taken after compaction must land in the replacement journal.
func TestEcjDeleteAfterCompactionPersists(t *testing.T) {
	dir := t.TempDir()
	base, ids := erasure_coding.SeedBloatedEcVolume(t, dir, 4096)
	require.NoError(t, os.WriteFile(base+".ecx",
		makeNeedleMapEntry(types.NeedleId(7), types.ToOffset(8), types.Size(10)), 0644))

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	require.NoError(t, ev.DeleteNeedleFromEcx(7))
	ev.Close()
	assert.Equal(t, int64((len(ids)+1)*types.NeedleIdSize), ecjSize(t, base+".ecj"))

	ev2, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev2.Close()
	assert.True(t, ev2.IsNeedleDeleted(7))
}

// A healthy small journal must never be rewritten.
func TestEcjHealthyIsNotRewritten(t *testing.T) {
	dir := t.TempDir()
	// 100 ids three times over: 2.4 KB, under the 1 MiB floor.
	base, _ := erasure_coding.SeedBloatedEcVolume(t, dir, 3)
	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev.Close()

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after, "small journal must not be rewritten")
}

// A journal over the size floor but under the ratio is ordinary slack, not
// bloat, and must be left alone.
func TestEcjOverFloorUnderRatioIsNotRewritten(t *testing.T) {
	dir := t.TempDir()
	base := erasure_coding.EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	ids := make([]types.NeedleId, 0, 50_000)
	for id := types.NeedleId(1); id <= 50_000; id++ {
		ids = append(ids, id)
	}
	// 400 KB of ids three times over: 1.2 MB on disk, 3x the set.
	erasure_coding.WriteRepeatedEcj(t, base+".ecj", ids, 3)
	before := ecjSize(t, base+".ecj")
	require.Greater(t, before, int64(1<<20))

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev.Close()
	assert.Equal(t, before, ecjSize(t, base+".ecj"), "a 3x journal is under the 4x ratio")
}

// A torn tail plus an oversized journal → repaired and compacted.
func TestEcjTornTailAndBloatedRepairedAndCompacted(t *testing.T) {
	dir := t.TempDir()
	base, ids := erasure_coding.SeedBloatedEcVolume(t, dir, 4096)
	f, err := os.OpenFile(base+".ecj", os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xAB, 0xCD, 0xEF})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev.Close()

	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	assert.Equal(t, int64(len(ids)*types.NeedleIdSize), ecjSize(t, base+".ecj"))
}

// A journal in a shared index dir is compacted when this volume is its only
// holder: nothing else has the old inode open.
func TestEcjSharedIndexDirSoleHolderIsCompacted(t *testing.T) {
	dataDir, idxDir := t.TempDir(), t.TempDir()
	idxBase, ids := erasure_coding.SeedBloatedEcVolume(t, idxDir, 4096)

	ev, err := erasure_coding.NewEcVolume("hdd", dataDir, idxDir, "", 7)
	require.NoError(t, err)
	defer ev.Close()

	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	assert.Equal(t, int64(len(ids)*types.NeedleIdSize), ecjSize(t, idxBase+".ecj"))
}

// A journal another EcVolume already holds must not be replaced under it —
// that holder's later deletes would land on the unlinked inode. Once it is
// closed, the next mount compacts.
func TestEcjHeldJournalIsNotCompacted(t *testing.T) {
	idxDir, dataA, dataB := t.TempDir(), t.TempDir(), t.TempDir()
	idxBase := erasure_coding.EcShardFileName("", idxDir, 7)
	require.NoError(t, os.WriteFile(idxBase+".ecx",
		makeNeedleMapEntry(types.NeedleId(7), types.ToOffset(8), types.Size(10)), 0644))
	require.NoError(t, os.WriteFile(idxBase+".vif", []byte{}, 0644))

	// Disk A mounts first, while the journal is still small.
	evA, err := erasure_coding.NewEcVolume("hdd", dataA, idxDir, "", 7)
	require.NoError(t, err)

	// The journal then bloats (a peer's journal appended whole, say) and disk
	// B mounts against the same file, as cross-disk reconcile does.
	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	erasure_coding.WriteRepeatedEcj(t, idxBase+".ecj", ids, 4096)
	bloated := ecjSize(t, idxBase+".ecj")

	evB, err := erasure_coding.NewEcVolume("hdd", dataB, idxDir, "", 7)
	require.NoError(t, err)
	assert.Equal(t, bloated, ecjSize(t, idxBase+".ecj"), "held journal must not be replaced")

	// A's delete still reaches the file every later mount reads.
	require.NoError(t, evA.DeleteNeedleFromEcx(7))
	evA.Close()
	evB.Close()

	evC, err := erasure_coding.NewEcVolume("hdd", dataA, idxDir, "", 7)
	require.NoError(t, err)
	defer evC.Close()
	assert.True(t, evC.IsNeedleDeleted(7), "a holder's delete was stranded")
	assert.Equal(t, int64((len(ids)+1)*types.NeedleIdSize), ecjSize(t, idxBase+".ecj"),
		"sole holder compacts")
}
