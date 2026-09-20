package ec

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// countTotalShardsPerRack counts ALL shards (data + parity) of vid per rack.
// 2-disk-loss survival depends on TOTAL shards per physical disk (=rack), not on
// data/parity counted separately.
func countTotalShardsPerRack(ecNodes []*EcNode, vid needle.VolumeId) map[string]int {
	totalPerRack := make(map[string]int)
	for _, ecNode := range ecNodes {
		// NOTE: the field is EcNode.Rack (exported) at 4.42 -- the #10760 refactor
		// exported what was `rack` in weed/shell. FindEcVolumeShardsInfo returns an
		// empty ShardsInfo (never nil) on a miss, so len(si.Ids()) is safe.
		si := FindEcVolumeShardsInfo(ecNode, vid, types.HardDriveType)
		totalPerRack[string(ecNode.Rack)] += len(si.Ids())
	}
	return totalPerRack
}

// TestCommandEcBalanceTotalShardsPerRackCap models the production HDD topology
// (8 physical disks = 8 racks) and asserts no rack holds more than
// CeilDivide(14, 8) = 2 TOTAL shards. This fails on stock SeaweedFS because data
// and parity are capped separately (2 data + 1 parity = 3 on a rack).
func TestCommandEcBalanceTotalShardsPerRackCap(t *testing.T) {
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1",
				[]erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
			newEcNode("dc1", "rack2", "dn2", 100),
			newEcNode("dc1", "rack3", "dn3", 100),
			newEcNode("dc1", "rack4", "dn4", 100),
			newEcNode("dc1", "rack5", "dn5", 100),
			newEcNode("dc1", "rack6", "dn6", 100),
			newEcNode("dc1", "rack7", "dn7", 100),
			newEcNode("dc1", "rack8", "dn8", 100),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balance([]string{"c1"})

	vid := needle.VolumeId(1)
	totalShards := erasure_coding.DataShardsCount + erasure_coding.ParityShardsCount // 14
	numRacks := 8
	maxTotalPerRack := CeilDivide(totalShards, numRacks) // 2

	totalPerRack := countTotalShardsPerRack(ecb.ecNodes, vid)
	t.Logf("total shards per rack: %v (max allowed: %d)", totalPerRack, maxTotalPerRack)

	sum := 0
	for rackId, count := range totalPerRack {
		sum += count
		if count > maxTotalPerRack {
			t.Errorf("rack %s has %d TOTAL shards, expected max %d (2-disk-loss survival needs <=2/disk)",
				rackId, count, maxTotalPerRack)
		}
	}
	if sum != totalShards {
		t.Errorf("total shards placed = %d, expected %d", sum, totalShards)
	}
}

// TestCommandEcBalanceTotalShardsPerRackCapTwoNodesPerRack models the REAL prod
// topology: 16 volume servers, TWO per physical disk (2 nodes share each
// rack=hddN). It asserts <=2 TOTAL shards per RACK (=physical disk). This is the
// discriminator between a correct per-rack cap and an incorrect per-node cap: a
// per-node cap of 2 would allow 2+2=4 on one disk yet pass the 1-node/rack test.
func TestCommandEcBalanceTotalShardsPerRackCapTwoNodesPerRack(t *testing.T) {
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1",
				[]erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
			newEcNode("dc1", "rack1", "dn2", 100),
			newEcNode("dc1", "rack2", "dn3", 100),
			newEcNode("dc1", "rack2", "dn4", 100),
			newEcNode("dc1", "rack3", "dn5", 100),
			newEcNode("dc1", "rack3", "dn6", 100),
			newEcNode("dc1", "rack4", "dn7", 100),
			newEcNode("dc1", "rack4", "dn8", 100),
			newEcNode("dc1", "rack5", "dn9", 100),
			newEcNode("dc1", "rack5", "dn10", 100),
			newEcNode("dc1", "rack6", "dn11", 100),
			newEcNode("dc1", "rack6", "dn12", 100),
			newEcNode("dc1", "rack7", "dn13", 100),
			newEcNode("dc1", "rack7", "dn14", 100),
			newEcNode("dc1", "rack8", "dn15", 100),
			newEcNode("dc1", "rack8", "dn16", 100),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balance([]string{"c1"})

	vid := needle.VolumeId(1)
	totalShards := erasure_coding.DataShardsCount + erasure_coding.ParityShardsCount // 14
	numRacks := 8
	maxTotalPerRack := CeilDivide(totalShards, numRacks) // 2

	totalPerRack := countTotalShardsPerRack(ecb.ecNodes, vid)
	t.Logf("total shards per rack (2 nodes/rack): %v (max allowed: %d)", totalPerRack, maxTotalPerRack)

	sum := 0
	for rackId, count := range totalPerRack {
		sum += count
		if count > maxTotalPerRack {
			t.Errorf("rack %s has %d TOTAL shards across its 2 nodes, expected max %d (per-DISK cap, not per-node)",
				rackId, count, maxTotalPerRack)
		}
	}
	if sum != totalShards {
		t.Errorf("total shards placed = %d, expected %d", sum, totalShards)
	}
}

// TestCommandEcBalanceTotalCapFromClumpedStart reproduces the reviewer's exact
// repro: data already at the per-type cap on 5 racks (2,2,2,2,2,0,0,0) with all 4
// parity clumped on rack1. The parity pass sheds rack1's over-cap parity, but the
// 4th parity finds no destination under the per-type parity cap (=ceil(4/8)=1) once
// racks 6-8 each take one, while racks 2-5 are at the TOTAL cap. Without the total-cap
// destination fallback the 4th parity strands on rack1 (2 data + 1 parity = 3 total),
// silently, with the sum still 14. The fallback relaxes the per-type evenness cap up
// to the hard total cap so the shard lands on a rack with real room.
func TestCommandEcBalanceTotalCapFromClumpedStart(t *testing.T) {
	// data 2,2,2,2,2,0,0,0 across racks; all 4 parity clumped on rack1.
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 10, 11, 12, 13}),
			newEcNode("dc1", "rack2", "dn2", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{2, 3}),
			newEcNode("dc1", "rack3", "dn3", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{4, 5}),
			newEcNode("dc1", "rack4", "dn4", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{6, 7}),
			newEcNode("dc1", "rack5", "dn5", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{8, 9}),
			newEcNode("dc1", "rack6", "dn6", 100),
			newEcNode("dc1", "rack7", "dn7", 100),
			newEcNode("dc1", "rack8", "dn8", 100),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}
	ecb.balance([]string{"c1"})
	vid := needle.VolumeId(1)
	maxTotalPerRack := CeilDivide(erasure_coding.DataShardsCount+erasure_coding.ParityShardsCount, 8) // 2
	totalPerRack := countTotalShardsPerRack(ecb.ecNodes, vid)
	t.Logf("clumped-start total shards per rack: %v (max %d)", totalPerRack, maxTotalPerRack)
	sum := 0
	for rackId, c := range totalPerRack {
		sum += c
		if c > maxTotalPerRack {
			t.Errorf("rack %s has %d total shards, expected <=%d", rackId, c, maxTotalPerRack)
		}
	}
	if sum != 14 {
		t.Errorf("sum=%d, expected 14 (no shard dropped)", sum)
	}
}

// TestCommandEcBalanceTotalCapAllRacksBearData proves anti-affinity stays a
// PREFERENCE, not a hard gate, in the total-cap destination fallback. Every one of
// the 8 racks holds >=1 data shard (data 0-9 spread across all racks) and the 4
// parity are clumped on rack1; parity therefore MUST coexist with data on some
// data-bearing rack. The same per-disk cap (<=2 total/rack) and full-placement
// (sum 14) invariants must hold. Like the other cap tests it FAILS on stock
// SeaweedFS (the separate per-type caps leave 3 on a rack) and PASSES patched --
// validating that the fallback relaxes the per-type cap up to the total cap while
// keeping anti-affinity a preference, so it never re-strands on a data-bearing rack.
func TestCommandEcBalanceTotalCapAllRacksBearData(t *testing.T) {
	// data 0-9 across all 8 racks (r1{0,1}, r2{2,3}, r3{4}, r4{5}, r5{6}, r6{7},
	// r7{8}, r8{9}); parity 10-13 clumped on r1.
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 10, 11, 12, 13}),
			newEcNode("dc1", "rack2", "dn2", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{2, 3}),
			newEcNode("dc1", "rack3", "dn3", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{4}),
			newEcNode("dc1", "rack4", "dn4", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{5}),
			newEcNode("dc1", "rack5", "dn5", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{6}),
			newEcNode("dc1", "rack6", "dn6", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{7}),
			newEcNode("dc1", "rack7", "dn7", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{8}),
			newEcNode("dc1", "rack8", "dn8", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{9}),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}
	ecb.balance([]string{"c1"})
	vid := needle.VolumeId(1)
	maxTotalPerRack := CeilDivide(erasure_coding.DataShardsCount+erasure_coding.ParityShardsCount, 8) // 2
	totalPerRack := countTotalShardsPerRack(ecb.ecNodes, vid)
	t.Logf("all-racks-bear-data total shards per rack: %v (max %d)", totalPerRack, maxTotalPerRack)
	sum := 0
	for rackId, c := range totalPerRack {
		sum += c
		if c > maxTotalPerRack {
			t.Errorf("rack %s has %d total shards, expected <=%d", rackId, c, maxTotalPerRack)
		}
	}
	if sum != 14 {
		t.Errorf("sum=%d, expected 14 (no shard dropped)", sum)
	}
}
