package ecbalancer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// FORK TEST (not upstream). Companion to patches/0001, which caps TOTAL
// (data+parity) EC shards per rack inside ecbalancer.Plan. Plan governs
// ec.balance and shell ec.encode, but the WORKER auto-EC encode path places via
// Topology.Place / PlaceDurabilityFirst, which upstream caps only per SHARD TYPE
// (ceil(10/racks) data AND ceil(4/racks) parity, independently) plus parityShards
// per logical disk.
//
// On this cluster a "rack" is one PHYSICAL HDD (8 spindles modelled as 8 racks,
// two volume servers per spindle at /srv/hddN/seaweedfs/{a,b}), so independent
// per-type caps permit 2 data + 1 parity = 3+ shards on ONE spindle. Losing any
// 2 spindles then strands >4 shards and a 10+4 volume becomes unreadable — the
// exact durability claim the 2-disk-loss rehearsal certifies.
//
// The invariant: no rack may hold more than ceil(TotalShards/racks) TOTAL shards.
const forkRacks = 8

// buildForkTopo mirrors production: 8 racks (one per physical HDD), two volume
// servers per rack, one disk each.
func buildForkTopo(perDiskFree int) *Topology {
	return buildPlaceTopo(forkRacks, 2, perDiskFree)
}

func assertTotalPerRackCapped(t *testing.T, res *PlaceResult, mode string) {
	t.Helper()
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("%s: placed %d shards, want %d", mode, len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	totalPerRack := map[string]int{}
	for _, d := range res.Destinations {
		totalPerRack[d.Rack]++
	}
	maxAllowed := ceilDivide(erasure_coding.TotalShardsCount, forkRacks)
	t.Logf("%s: total shards per rack: %v (max allowed: %d)", mode, totalPerRack, maxAllowed)
	for rk, n := range totalPerRack {
		if n > maxAllowed {
			t.Errorf("%s: rack %s has %d TOTAL shards, expected max %d (2-disk-loss survival needs <=%d/disk)",
				mode, rk, n, maxAllowed, maxAllowed)
		}
	}
}

func TestPlaceTotalShardsPerRackCap(t *testing.T) {
	topo := buildForkTopo(50)
	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	assertTotalPerRackCapped(t, res, "PlaceStrict")
}

func TestPlaceDurabilityFirstTotalShardsPerRackCap(t *testing.T) {
	topo := buildForkTopo(50)
	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	assertTotalPerRackCapped(t, res, "PlaceDurabilityFirst")
}
