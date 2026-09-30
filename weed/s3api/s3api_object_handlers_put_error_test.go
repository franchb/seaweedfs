package s3api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapChunkedUploadErrorToS3Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want s3err.ErrorCode
	}{
		{
			// A truncated body (client abort or reverse-proxy timeout) reaches
			// putToFiler tagged exactly like UploadReaderInChunks reports it.
			name: "truncated source read maps to IncompleteBody",
			err:  fmt.Errorf("%w: read chunk at offset %d (got %d bytes): %w", operation.ErrTruncatedBody, 0, 8056500, io.ErrUnexpectedEOF),
			want: s3err.ErrIncompleteBody,
		},
		{
			// A volume-server upload dropping mid-write is a server fault, not a
			// client truncation, even though it also carries io.ErrUnexpectedEOF.
			name: "volume upload unexpected EOF maps to InternalError",
			err:  fmt.Errorf("upload chunk: %w", io.ErrUnexpectedEOF),
			want: s3err.ErrInternalError,
		},
		{
			name: "payload checksum mismatch maps to InvalidDigest",
			err:  errors.New(s3err.ErrMsgPayloadChecksumMismatch),
			want: s3err.ErrInvalidDigest,
		},
		{
			name: "other errors map to InternalError",
			err:  errors.New("assign volume: no free volumes"),
			want: s3err.ErrInternalError,
		},
		{
			// Over-quota (read-only) buckets must be 403, not retryable 500.
			// See franchb/seaweedfs#12: ErrReadOnly was lost at the filer
			// AssignVolume gRPC boundary.
			name: "wrapped read-only maps to AccessDenied",
			err:  fmt.Errorf("assign volume: %w: read only: /buckets/q (e.g. bucket over quota)", weed_server.ErrReadOnly),
			want: s3err.ErrAccessDenied,
		},
		{
			name: "old filer free-text read-only maps to AccessDenied",
			err:  errors.New("assign volume: assign volume: read only: /buckets/q (e.g. bucket over quota)"),
			want: s3err.ErrAccessDenied,
		},
		{
			name: "gRPC FailedPrecondition read-only maps to AccessDenied",
			err:  status.Errorf(codes.FailedPrecondition, "assign volume: read only: /buckets/q (e.g. bucket over quota)"),
			want: s3err.ErrAccessDenied,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapChunkedUploadErrorToS3Error(context.Background(), tt.err); got != tt.want {
				t.Errorf("mapChunkedUploadErrorToS3Error(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// A body that ends early because the peer vanished and one that ends early while the
// peer is still connected arrive as the same read error. Only the request context
// tells them apart, and they point at opposite causes, so they must not share a code.
func TestMapChunkedUploadErrorToS3ErrorClientDisconnect(t *testing.T) {
	truncated := fmt.Errorf("%w: read chunk at offset %d (got %d bytes): %w", operation.ErrTruncatedBody, 0, 0, io.ErrUnexpectedEOF)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want s3err.ErrorCode
	}{
		{
			name: "peer gone maps to ClientDisconnected",
			ctx:  canceled,
			err:  truncated,
			want: s3err.ErrClientDisconnected,
		},
		{
			name: "peer still connected stays IncompleteBody",
			ctx:  context.Background(),
			err:  truncated,
			want: s3err.ErrIncompleteBody,
		},
		{
			// A deadline is the server giving up, not the peer leaving, so it must
			// not be laundered into a client-side code.
			name: "expired deadline stays IncompleteBody",
			ctx:  deadline,
			err:  truncated,
			want: s3err.ErrIncompleteBody,
		},
		{
			// Cancellation must not reclassify faults that are not truncations.
			name: "server fault under a canceled context stays InternalError",
			ctx:  canceled,
			err:  errors.New("assign volume: no free volumes"),
			want: s3err.ErrInternalError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapChunkedUploadErrorToS3Error(tt.ctx, tt.err); got != tt.want {
				t.Errorf("mapChunkedUploadErrorToS3Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsReadOnlyAssignError(t *testing.T) {
	readOnlyStatus := status.Errorf(codes.FailedPrecondition, "assign volume: %v", weed_server.ErrReadOnly)
	wrapped := fmt.Errorf("assign volume: %w: %v", weed_server.ErrReadOnly, readOnlyStatus)
	otherStatus := status.Errorf(codes.Unavailable, "filer down")
	plain := errors.New("assign volume: no free volumes")

	if !isReadOnlyAssignError(readOnlyStatus) {
		t.Error("FailedPrecondition read-only status should match")
	}
	if !isReadOnlyAssignError(wrapped) {
		t.Error("wrapped ErrReadOnly should match")
	}
	if isReadOnlyAssignError(otherStatus) {
		t.Error("Unavailable should not match as read-only")
	}
	if isReadOnlyAssignError(plain) {
		t.Error("generic assign error should not match as read-only")
	}
	if isReadOnlyAssignError(nil) {
		t.Error("nil should not match as read-only")
	}
}

// 499 must stay distinguishable from the 400 it was split out of.
func TestClientDisconnectedAPIError(t *testing.T) {
	got := s3err.GetAPIError(s3err.ErrClientDisconnected)
	if got.HTTPStatusCode != 499 {
		t.Errorf("ClientDisconnected status = %d, want 499", got.HTTPStatusCode)
	}
	if got.Code != "ClientDisconnected" {
		t.Errorf("ClientDisconnected code = %q, want %q", got.Code, "ClientDisconnected")
	}
	if s3err.GetAPIError(s3err.ErrIncompleteBody).HTTPStatusCode != 400 {
		t.Error("IncompleteBody must remain a 400")
	}
}
