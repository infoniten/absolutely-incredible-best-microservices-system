package server

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/quantara/id-service/internal/idpool"
	pb "github.com/quantara/id-service/proto"
)

type Server struct {
	pb.UnimplementedIDServiceServer
	pool   *idpool.Pool
	tracer trace.Tracer
}

func New(pool *idpool.Pool, tracer trace.Tracer) *Server {
	return &Server{
		pool:   pool,
		tracer: tracer,
	}
}

func (s *Server) GetID(ctx context.Context, req *pb.GetIDRequest) (*pb.GetIDResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetID")
	defer span.End()

	id, err := s.pool.GetID(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "failed to get ID: %v", err)
	}

	return &pb.GetIDResponse{Id: id}, nil
}

func (s *Server) GetBatchID(ctx context.Context, req *pb.GetBatchIDRequest) (*pb.GetBatchIDResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.GetBatchID")
	defer span.End()

	if req.BatchSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "batch_size must be positive")
	}

	ids, err := s.pool.GetBatchID(ctx, int(req.BatchSize))
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "failed to get batch IDs: %v", err)
	}

	return &pb.GetBatchIDResponse{Ids: ids}, nil
}
