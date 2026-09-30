package ecbalancer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Total-shards-per-rack durability cap for Place / PlaceDurabilityFirst.
//
// Place used to cap each shard TYPE independently (ceil(data/racks) data AND
// ceil(parity/racks) parity) plus parityShards per disk. On a topology where
// one rack is one failure domain that permits e.g. 2 data + 1 parity on a
// single rack; losing two racks then strands more than parityShards shards
// and a 10+4 volume becomes unreadable. No rack may hold more than
// ceil(totalShards/eligibleRacks) TOTAL shards, under both PlacementModes and
// even when ReplicaPlacement is nil.
const rackCapRacks = 8

func assertTotalPerRackCapped(t *testing.T, res *PlaceResult, mode string, numRacks int) {
	t.Helper()
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("%s: placed %d shards, want %d", mode, len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	totalPerRack := map[string]int{}
	for _, d := range res.Destinations {
		totalPerRack[d.Rack]++
	}
	maxAllowed := ceilDivide(erasure_coding.TotalShardsCount, numRacks)
	t.Logf("%s: total shards per rack: %v (max allowed: %d)", mode, totalPerRack, maxAllowed)
	for rk, n := range totalPerRack {
		if n > maxAllowed {
			t.Errorf("%s: rack %s has %d TOTAL shards, expected max %d (2-disk-loss survival needs <=%d/rack)",
				mode, rk, n, maxAllowed, maxAllowed)
		}
	}
}

// TestPlaceTotalShardsPerRackCap: 10+4 over 8 racks (two nodes each) must cap
// TOTAL shards at ceil(14/8)=2 per rack under strict placement.
func TestPlaceTotalShardsPerRackCap(t *testing.T) {
	topo := buildPlaceTopo(rackCapRacks, 2, 50)
	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	assertTotalPerRackCapped(t, res, "PlaceStrict", rackCapRacks)
}

// TestPlaceDurabilityFirstTotalShardsPerRackCap: same invariant under the
// worker auto-encode path, which relaxes per-type caps, anti-affinity and
// ReplicaPlacement by design. The total cap is never relaxed.
func TestPlaceDurabilityFirstTotalShardsPerRackCap(t *testing.T) {
	topo := buildPlaceTopo(rackCapRacks, 2, 50)
	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	assertTotalPerRackCapped(t, res, "PlaceDurabilityFirst", rackCapRacks)
}

// TestPlaceTotalCapWithConstrainedRack: the cap divides by racks that still
// have a free disk, so one nearly-full rack must not make placement fail when
// a feasible placement exists. Rack0 keeps a single free slot; the other 7
// racks are roomy (1 + 7*2 = 15 >= 14 slots under the cap).
func TestPlaceTotalCapWithConstrainedRack(t *testing.T) {
	topo := buildPlaceTopo(rackCapRacks, 2, 50)
	// Starve rack0 to a single free slot: the first node keeps one slot on
	// disk 0, every other disk in the rack goes to zero. Node freeSlots and
	// rack freeSlots both derive from these, so keep them consistent.
	kept := false
	for _, n := range topo.nodes {
		if n.rack != "dc1:rack0" {
			continue
		}
		if !kept {
			for diskID, d := range n.disks {
				if diskID == 0 {
					d.freeSlots = 1
				} else {
					d.freeSlots = 0
				}
			}
			n.freeSlots = 1
			kept = true
		} else {
			for _, d := range n.disks {
				d.freeSlots = 0
			}
			n.freeSlots = 0
		}
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place with one constrained rack should still succeed when feasible: %v", err)
	}
	assertTotalPerRackCapped(t, res, "PlaceDurabilityFirst-constrained", rackCapRacks)
	if got := len(res.Destinations); got != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d shards, want %d", got, erasure_coding.TotalShardsCount)
	}
}
