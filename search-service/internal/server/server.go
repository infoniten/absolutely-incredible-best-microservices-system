package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/quantara/search-service/internal/domainconfig"
	"github.com/quantara/search-service/internal/errdefs"
	"github.com/quantara/search-service/internal/service"
	pb "github.com/quantara/search-service/proto"
)

type Server struct {
	pb.UnimplementedSearchServiceServer

	searchService *service.SearchService
	configLoader  *domainconfig.Loader
	tracer        trace.Tracer
}

func New(
	searchService *service.SearchService,
	configLoader *domainconfig.Loader,
	tracer trace.Tracer,
) *Server {
	return &Server{
		searchService: searchService,
		configLoader:  configLoader,
		tracer:        tracer,
	}
}

func (s *Server) GetObjectRevisionById(
	ctx context.Context,
	req *pb.GetObjectRevisionByIdRequest,
) (*pb.GetObjectRevisionByIdResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetObjectRevisionById")
	defer span.End()

	content, err := s.searchService.GetObjectRevisionByID(ctx, req.Id, req.ObjectClass)
	if err != nil {
		return nil, mapError(err)
	}
	contentValue, err := jsonToProtoValue(content)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to serialize content: %v", err)
	}
	return &pb.GetObjectRevisionByIdResponse{Content: contentValue}, nil
}

func (s *Server) GetObjectByGlobalId(
	ctx context.Context,
	req *pb.GetObjectByGlobalIdRequest,
) (*pb.GetObjectByGlobalIdResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetObjectByGlobalId")
	defer span.End()

	content, err := s.searchService.GetObjectByGlobalID(
		ctx,
		req.GlobalId,
		req.ObjectRootClass,
		req.DraftStatus,
		req.ActualDate,
	)
	if err != nil {
		return nil, mapError(err)
	}
	contentValue, err := jsonToProtoValue(content)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to serialize content: %v", err)
	}
	return &pb.GetObjectByGlobalIdResponse{Content: contentValue}, nil
}

func (s *Server) GetObjectCollectionByParentId(
	ctx context.Context,
	req *pb.GetObjectCollectionByParentIdRequest,
) (*pb.GetObjectCollectionByParentIdResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetObjectCollectionByParentId")
	defer span.End()

	contents, err := s.searchService.GetObjectCollectionByParentID(
		ctx,
		req.ParentId,
		req.ObjectRootClass,
	)
	if err != nil {
		return nil, mapError(err)
	}
	contentValues, err := jsonSliceToProtoValues(contents)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to serialize contents: %v", err)
	}
	return &pb.GetObjectCollectionByParentIdResponse{Contents: contentValues}, nil
}

func (s *Server) GetObjectGlobalIdByAltId(
	ctx context.Context,
	req *pb.GetObjectGlobalIdByAltIdRequest,
) (*pb.GetObjectGlobalIdByAltIdResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetObjectGlobalIdByAltId")
	defer span.End()

	globalID, err := s.searchService.GetObjectGlobalIDByAltID(
		ctx,
		req.AltId,
		req.SourceAlias,
		req.ObjectRootClass,
	)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.GetObjectGlobalIdByAltIdResponse{GlobalId: globalID}, nil
}

func (s *Server) GetObjectCollectionByFilter(
	ctx context.Context,
	req *pb.GetObjectCollectionByFilterRequest,
) (*pb.GetObjectCollectionByFilterResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetObjectCollectionByFilter")
	defer span.End()

	var limit *int
	if req.Limit != nil {
		value := int(req.Limit.Value)
		limit = &value
	}

	var offset *int
	if req.Offset != nil {
		value := int(req.Offset.Value)
		offset = &value
	}

	contents, err := s.searchService.GetObjectCollectionByFilter(
		ctx,
		req.ObjectRootClass,
		req.DraftStatus,
		req.ActualDate,
		req.Filter,
		req.Sort,
		limit,
		offset,
	)
	if err != nil {
		return nil, mapError(err)
	}
	contentValues, err := jsonSliceToProtoValues(contents)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to serialize contents: %v", err)
	}
	return &pb.GetObjectCollectionByFilterResponse{Contents: contentValues}, nil
}

func (s *Server) GetDomainConfig(
	ctx context.Context,
	_ *pb.GetDomainConfigRequest,
) (*pb.GetDomainConfigResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetDomainConfig")
	defer span.End()

	info, err := s.configLoader.Info()
	if err != nil {
		return nil, mapError(err)
	}

	configJSON, err := json.Marshal(info.Config)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to serialize domain config: %v", err)
	}

	return &pb.GetDomainConfigResponse{
		Source:     info.Source,
		Location:   info.Location,
		Hash:       info.Hash,
		LoadedAt:   timestamppb.New(info.LoadedAt),
		ConfigJson: string(configJSON),
	}, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, errdefs.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, errdefs.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, errdefs.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func jsonToProtoValue(raw string) (*structpb.Value, error) {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}

	protoValue, err := structpb.NewValue(value)
	if err != nil {
		return nil, fmt.Errorf("failed to convert json to proto value: %w", err)
	}
	return protoValue, nil
}

func jsonSliceToProtoValues(raw []string) ([]*structpb.Value, error) {
	result := make([]*structpb.Value, 0, len(raw))
	for _, item := range raw {
		value, err := jsonToProtoValue(item)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}
