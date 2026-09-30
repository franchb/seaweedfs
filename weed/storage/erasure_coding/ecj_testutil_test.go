package erasure_coding

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/require"
)

// Shared by the internal and the external (_test package) .ecj tests; the
// external ones reach these through export_test.go.

// bloatedEcjIds are the distinct ids seedBloatedEcVolume writes.
func bloatedEcjIds() []types.NeedleId {
	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	return ids
}

// writeRepeatedEcj writes ids to path, the whole list repeated `repeats` times.
func writeRepeatedEcj(t *testing.T, path string, ids []types.NeedleId, repeats int) {
	t.Helper()
	rec := make([]byte, types.NeedleIdSize)
	one := make([]byte, 0, len(ids)*types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(rec, id)
		one = append(one, rec...)
	}
	f, err := os.Create(path)
	require.NoError(t, err)
	for i := 0; i < repeats; i++ {
		_, err := f.Write(one)
		require.NoError(t, err)
	}
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

// seedBloatedEcVolume lays out volume 7 in dir: an empty .ecx, an empty .vif,
// and a journal repeating bloatedEcjIds `repeats` times. 4096 repeats is
// 3.1 MiB for 800 bytes of ids, over both compaction thresholds.
func seedBloatedEcVolume(t *testing.T, dir string, repeats int) (base string, ids []types.NeedleId) {
	t.Helper()
	base = EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	ids = bloatedEcjIds()
	writeRepeatedEcj(t, base+".ecj", ids, repeats)
	return base, ids
}
