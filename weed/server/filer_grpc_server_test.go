package weed_server

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newAssignTestServer(fc *filer.FilerConf, diskType string) *FilerServer {
	return &FilerServer{
		option: &FilerOption{
			DiskType: diskType,
		},
		filer: &filer.Filer{
			DirBucketsPath:    "/buckets",
			FilerConf:         fc,
			MaxFilenameLength: 255,
		},
	}
}

func TestResolveAssignStorageOptionUsesBucketRuleBeforeFilerDiskDefault(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/zot",
		DiskType:       "disk",
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	fs := newAssignTestServer(fc, "hdd")

	so, err := fs.resolveAssignStorageOption(context.Background(), &filer_pb.AssignVolumeRequest{
		Path: "/buckets/zot/.uploads/upload-id/0001_part.part",
	})
	if err != nil {
		t.Fatalf("resolve assign storage option: %v", err)
	}

	if got, want := so.Collection, "zot"; got != want {
		t.Fatalf("collection = %q, want %q", got, want)
	}
	if got, want := so.DiskType, "disk"; got != want {
		t.Fatalf("disk type = %q, want %q", got, want)
	}
}

func TestResolveAssignStorageOptionFallsBackToFilerDiskDefault(t *testing.T) {
	fs := newAssignTestServer(filer.NewFilerConf(), "hdd")

	so, err := fs.resolveAssignStorageOption(context.Background(), &filer_pb.AssignVolumeRequest{
		Path: "/tmp/unmatched/file.bin",
	})
	if err != nil {
		t.Fatalf("resolve assign storage option: %v", err)
	}

	if got, want := so.DiskType, "hdd"; got != want {
		t.Fatalf("disk type = %q, want %q", got, want)
	}
}

func TestAssignVolumeReadOnlyReturnsFailedPrecondition(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/overquota",
		ReadOnly:       true,
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	fs := newAssignTestServer(fc, "")

	_, err := fs.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
		Path: "/buckets/overquota/x",
	})
	if err == nil {
		t.Fatal("AssignVolume err = nil, want FailedPrecondition")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("AssignVolume code = %v, want %v", got, codes.FailedPrecondition)
	}
	if !strings.Contains(err.Error(), ErrReadOnly.Error()) {
		t.Fatalf("AssignVolume err = %v, want it to carry %q", err, ErrReadOnly.Error())
	}
}
