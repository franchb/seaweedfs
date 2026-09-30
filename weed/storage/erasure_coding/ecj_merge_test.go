package erasure_coding_test

import (
	"os"
	"path/filepath"
	"testing"

	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeEcjIds(t *testing.T, path string, ids ...types.NeedleId) {
	t.Helper()
	buf := make([]byte, 0, len(ids)*types.NeedleIdSize)
	rec := make([]byte, types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(rec, id)
		buf = append(buf, rec...)
	}
	require.NoError(t, os.WriteFile(path, buf, 0644))
}

func readEcjIdsSorted(t *testing.T, path string) []types.NeedleId {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Zero(t, len(data)%types.NeedleIdSize, "journal must stay aligned")
	var ids []types.NeedleId
	for i := 0; i+types.NeedleIdSize <= len(data); i += types.NeedleIdSize {
		ids = append(ids, types.BytesToNeedleId(data[i:i+types.NeedleIdSize]))
	}
	return ids
}

// Destination {1,2,3} + source {3,4} => exactly {1,2,3,4}, 4x8 bytes.
func TestMergeEcjUnion_DedupesUnion(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "vol.ecj")
	incoming := filepath.Join(dir, "vol.ecj.incoming")
	dest := filepath.Join(dir, "vol.ecj")
	writeEcjIds(t, local, 1, 2, 3)
	writeEcjIds(t, incoming, 3, 4)
	writeEcjIds(t, dest, 1, 2, 3)

	n, err := erasure_coding.MergeEcjUnion(local, incoming, dest)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []types.NeedleId{1, 2, 3, 4}, readEcjIdsSorted(t, dest))
	fi, err := os.Stat(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(4*types.NeedleIdSize), fi.Size())
}

// Copying the same shard A->B->A->B 20 times keeps the size constant;
// the old append path grew it geometrically.
func TestMergeEcjUnion_RoundTripStaysConstant(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "vol.ecj")
	incoming := filepath.Join(dir, "vol.ecj.incoming")
	writeEcjIds(t, dest, 1, 2, 3, 4)
	writeEcjIds(t, incoming, 3, 4)
	for i := 0; i < 20; i++ {
		n, err := erasure_coding.MergeEcjUnion(dest, incoming, dest)
		require.NoError(t, err)
		assert.Equal(t, 4, n)
	}
	assert.Equal(t, []types.NeedleId{1, 2, 3, 4}, readEcjIdsSorted(t, dest))
}

// Missing inputs read as empty; merging nothing yields an empty journal.
func TestMergeEcjUnion_MissingInputsReadAsEmpty(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "vol.ecj")
	n, err := erasure_coding.MergeEcjUnion(
		filepath.Join(dir, "no-local.ecj"),
		filepath.Join(dir, "no-incoming.ecj"),
		dest,
	)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	fi, err := os.Stat(dest)
	require.NoError(t, err)
	assert.Zero(t, fi.Size())
}

// A torn trailing partial record is ignored, never promoted into an id.
func TestMergeEcjUnion_TornTailIgnored(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "vol.ecj")
	incoming := filepath.Join(dir, "vol.ecj.incoming")
	dest := filepath.Join(dir, "vol.ecj")
	writeEcjIds(t, local, 1, 2)
	writeEcjIds(t, incoming, 2, 3)
	f, err := os.OpenFile(incoming, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{9, 9, 9})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ids, err := erasure_coding.ReadEcjIds(incoming)
	require.NoError(t, err)
	assert.Len(t, ids, 2)

	n, err := erasure_coding.MergeEcjUnion(local, incoming, dest)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []types.NeedleId{1, 2, 3}, readEcjIdsSorted(t, dest))
}
