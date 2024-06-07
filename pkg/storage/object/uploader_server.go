package object

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type uploaderServer struct {
	uploader Uploader[GlobalReference, []byte]
}

// NewUploaderServer creates a gRPC server object for exposing an
// Uploader. This can be used to make objects contained in an object
// store accessible via the network.
func NewUploaderServer(uploader Uploader[GlobalReference, []byte]) object.UploaderServer {
	return &uploaderServer{
		uploader: uploader,
	}
}

func (s *uploaderServer) UploadObject(ctx context.Context, request *object.UploadObjectRequest) (*object.UploadObjectResponse, error) {
	namespace, err := NewNamespace(request.Namespace)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid namespace")
	}
	reference, err := namespace.NewGlobalReference(request.Reference)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid reference")
	}
	var contents *Contents
	if request.Contents != nil {
		contents, err = NewContentsFromProto(reference.LocalReference, request.Contents)
		if err != nil {
			return nil, util.StatusWrap(err, "Invalid object contents")
		}
	}

	degree := reference.GetDegree()
	if l := len(request.OutgoingReferencesLeases); l != 0 && l != degree {
		return nil, status.Errorf(codes.InvalidArgument, "Client provided %d leases, even though the object has a degree of %d", l, degree)
	}

	if request.WantContentsIfIncomplete {
		if contents != nil {
			return nil, status.Error(codes.InvalidArgument, "Client requested the object's contents, even though it is already in possession of them")
		}
		if degree == 0 {
			return nil, status.Error(codes.InvalidArgument, "Client requested the object's contents, even though the object does not have any outgoing references")
		}
	}

	result, err := s.uploader.UploadObject(
		ctx,
		reference,
		contents,
		request.OutgoingReferencesLeases,
		request.WantContentsIfIncomplete,
	)
	if err != nil {
		return nil, err
	}

	switch resultData := result.(type) {
	case UploadObjectComplete[[]byte]:
		return &object.UploadObjectResponse{
			Type: &object.UploadObjectResponse_Complete_{
				Complete: &object.UploadObjectResponse_Complete{
					Lease: resultData.Lease,
				},
			},
		}, nil
	case UploadObjectIncomplete[[]byte]:
		var contentsMessage *object.Contents
		if resultData.Contents != nil {
			contentsMessage = resultData.Contents.ToProto()
		}
		wantLeases := make([]uint32, 0, len(resultData.WantOutgoingReferencesLeases))
		for _, lease := range resultData.WantOutgoingReferencesLeases {
			wantLeases = append(wantLeases, uint32(lease))
		}
		return &object.UploadObjectResponse{
			Type: &object.UploadObjectResponse_Incomplete_{
				Incomplete: &object.UploadObjectResponse_Incomplete{
					Contents:                     contentsMessage,
					WantOutgoingReferencesLeases: wantLeases,
				},
			},
		}, nil
	case UploadObjectMissing[[]byte]:
		return nil, status.Error(codes.NotFound, "Object not found")
	default:
		panic("unexpected upload object result")
	}
}
