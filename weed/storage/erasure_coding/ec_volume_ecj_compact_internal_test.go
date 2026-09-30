package erasure_coding

import (
	"errors"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeBloatedEcjInternal(t *testing.T, base string, ids []types.NeedleId, repeats int) {
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

// A failed rename (pre-publication) must keep the original journal working:
// mount succeeds, the set loads, and the file is untouched.
func TestEcjCompactRenameFailureKeepsOriginal(t *testing.T) {
	dir := t.TempDir()
	base := EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	writeBloatedEcjInternal(t, base, ids, 4096)
	before, err := os.Stat(base + ".ecj")
	require.NoError(t, err)

	oldRename := ecjRename
	ecjRename = func(_, _ string) error { return errors.New("injected rename failure") }
	defer func() { ecjRename = oldRename }()

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
	assert.NoFileExists(t, base+".ecj.compact.tmp")
}

// A failed reopen (post-publication) must be a mount error, never a volume
// holding a nil or unlinked handle.
func TestEcjCompactReopenFailureIsMountError(t *testing.T) {
	dir := t.TempDir()
	base := EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	ids := make([]types.NeedleId, 0, 100)
	for id := types.NeedleId(1000); id < 1100; id++ {
		ids = append(ids, id)
	}
	writeBloatedEcjInternal(t, base, ids, 4096)

	oldOpen := ecjOpenVolumeFile
	ecjOpenVolumeFile = func(fileName string, flag int) (*os.File, error) {
		return nil, errors.New("injected reopen failure")
	}
	defer func() { ecjOpenVolumeFile = oldOpen }()

	ev, err := NewEcVolume("hdd", dir, dir, "", 7)
	require.Error(t, err, "post-rename reopen failure must fail the mount")
	assert.Nil(t, ev)
}
