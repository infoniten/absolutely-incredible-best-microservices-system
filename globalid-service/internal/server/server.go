package server

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/quantara/globalid-service/internal/idpool"
	pb "github.com/quantara/globalid-service/proto"
)

type Server struct {
	pb.UnimplementedGlobalIDServiceServer
	pool   *idpool.Pool
	tracer trace.Tracer
}

func New(pool *idpool.Pool, tracer trace.Tracer) *Server {
	return &Server{
		pool:   pool,
		tracer: tracer,
	}
}

func (s *Server) GetGlobalID(ctx context.Context, req *pb.GetGlobalIDRequest) (*pb.GetGlobalIDResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetGlobalID")
	defer span.End()

	globalID, err := s.pool.GetGlobalID(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "failed to get GlobalID: %v", err)
	}

	return &pb.GetGlobalIDResponse{GlobalId: globalID}, nil
}

func (s *Server) GetBatchGlobalID(ctx context.Context, req *pb.GetBatchGlobalIDRequest) (*pb.GetBatchGlobalIDResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetBatchGlobalID")
	defer span.End()

	if req.BatchSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "batch_size must be positive")
	}

	globalIDs, err := s.pool.GetBatchGlobalID(ctx, int(req.BatchSize))
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "failed to get batch GlobalIDs: %v", err)
	}

	return &pb.GetBatchGlobalIDResponse{GlobalIds: globalIDs}, nil
}
