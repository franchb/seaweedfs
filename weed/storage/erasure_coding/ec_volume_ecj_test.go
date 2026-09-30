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

func writeBloatedEcj(t *testing.T, base string, ids []types.NeedleId, repeats int) {
	t.Helper()
	rec := make([]byte, types.NeedleIdSize)
	one := make([]byte, 0, len(ids)*types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(rec, id)
		one = append(one, rec...)
	}
	f, err := os.Create(base + ".ecj")
	require.NoError(t, err)
	for i := 0; i < repeats; i++ {
		_, err := f.Write(one)
		require.NoError(t, err)
	}
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

// A bloated journal (many repeats of few ids) must mount to exactly those ids
// and be rewritten down to one entry per id.
func TestEcjBloatedIsCompactedOnMount(t *testing.T) {
	dir := t.TempDir()

	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	base := erasure_coding.EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	writeBloatedEcj(t, base, ids, 4096) // 100*4096*8 = 3.1 MiB

	before, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	require.Equal(t, int64(100*4096*types.NeedleIdSize), before.Size())

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev.Close()

	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}

	after, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, int64(len(ids)*types.NeedleIdSize), after.Size())
	assert.Less(t, after.Size(), before.Size())
	assert.NoFileExists(t, base+".ecj.compact.tmp")

	// Remount is idempotent.
	ev.Close()
	ev2, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev2.Close()
	for _, id := range ids {
		assert.True(t, ev2.IsNeedleDeleted(id), "id %d", id)
	}
	again, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, after.Size(), again.Size())
}

// A healthy small journal must never be rewritten.
func TestEcjHealthyIsNotRewritten(t *testing.T) {
	dir := t.TempDir()

	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1); id <= 100; id++ {
		ids = append(ids, id)
	}
	ev, base := mountEcVolume(t, dir, nil, ecjBytes(ids...))
	ev.Close()
	// Rewrite the 100-id journal 3x: 2.4 KB, under the 1 MiB floor.
	writeBloatedEcj(t, base, ids, 3)

	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	beforeStat, err := os.Stat(base + ".ecj")
	require.NoError(t, err)

	ev2, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	defer ev2.Close()

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after, "small journal must not be rewritten")
	afterStat, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, beforeStat.Size(), afterStat.Size())
}

// A torn tail plus an oversized journal → repaired and compacted.
func TestEcjTornTailAndBloatedRepairedAndCompacted(t *testing.T) {
	dir := t.TempDir()

	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	base := erasure_coding.EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	writeBloatedEcj(t, base, ids, 4096)
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
	fi, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, int64(len(ids)*types.NeedleIdSize), fi.Size())
}

// A journal in a shared index dir must not be compacted: a sibling holder may
// have the same file open.
func TestEcjSharedIndexDirIsNotCompacted(t *testing.T) {
	dataDir := t.TempDir()
	idxDir := t.TempDir()

	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	idxBase := erasure_coding.EcShardFileName("", idxDir, 7)
	require.NoError(t, os.WriteFile(idxBase+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(idxBase+".vif", []byte{}, 0644))
	writeBloatedEcj(t, idxBase, ids, 4096)

	before, err := os.Stat(idxBase + ".ecj")
	require.NoError(t, err)

	ev, err := erasure_coding.NewEcVolume("hdd", dataDir, idxDir, "", 7)
	require.NoError(t, err)
	defer ev.Close()

	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	after, err := os.Stat(idxBase + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before.Size(), after.Size(), "shared journal must not be replaced")
}
