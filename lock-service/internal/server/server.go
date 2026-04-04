package server

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/quantara/lock-service/internal/repository"
	pb "github.com/quantara/lock-service/proto"
)

type Server struct {
	pb.UnimplementedLockServiceServer
	repo       *repository.LockRepository
	tracer     trace.Tracer
	defaultTTL time.Duration
}

func New(repo *repository.LockRepository, tracer trace.Tracer, defaultTTL time.Duration) *Server {
	return &Server{
		repo:       repo,
		tracer:     tracer,
		defaultTTL: defaultTTL,
	}
}

func (s *Server) Lock(ctx context.Context, req *pb.LockRequest) (*pb.LockResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.Lock")
	defer span.End()

	span.SetAttributes(attribute.Int64("global_id", req.GlobalId))

	return s.acquireLock(ctx, req.GlobalId, s.defaultTTL)
}

func (s *Server) LockWithTTL(ctx context.Context, req *pb.LockWithTTLRequest) (*pb.LockResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.LockWithTTL")
	defer span.End()

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if ttl <= 0 {
		ttl = s.defaultTTL
	}

	span.SetAttributes(
		attribute.Int64("global_id", req.GlobalId),
		attribute.Int64("ttl_ms", req.TtlMs),
	)

	return s.acquireLock(ctx, req.GlobalId, ttl)
}

func (s *Server) acquireLock(ctx context.Context, globalID int64, ttl time.Duration) (*pb.LockResponse, error) {
	token := uuid.New().String()
	expireAt := time.Now().Add(ttl)

	err := s.repo.AcquireLock(ctx, globalID, token, expireAt)
	if err != nil {
		if errors.Is(err, repository.ErrLockAlreadyExists) {
			return &pb.LockResponse{
				Success:      false,
				ErrorMessage: "lock already exists for this global_id",
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to acquire lock: %v", err)
	}

	return &pb.LockResponse{
		Success:  true,
		Token:    token,
		ExpireAt: timestamppb.New(expireAt),
	}, nil
}

func (s *Server) Renew(ctx context.Context, req *pb.RenewRequest) (*pb.RenewResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.Renew")
	defer span.End()

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if ttl <= 0 {
		ttl = s.defaultTTL
	}

	span.SetAttributes(
		attribute.Int64("global_id", req.GlobalId),
		attribute.String("token", req.Token),
		attribute.Int64("ttl_ms", req.TtlMs),
	)

	newExpireAt := time.Now().Add(ttl)

	err := s.repo.RenewLock(ctx, req.GlobalId, req.Token, newExpireAt)
	if err != nil {
		if errors.Is(err, repository.ErrLockNotFound) {
			return &pb.RenewResponse{
				Success:      false,
				ErrorMessage: "lock not found",
			}, nil
		}
		if errors.Is(err, repository.ErrInvalidToken) {
			return &pb.RenewResponse{
				Success:      false,
				ErrorMessage: "invalid token for this lock",
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to renew lock: %v", err)
	}

	return &pb.RenewResponse{
		Success:  true,
		ExpireAt: timestamppb.New(newExpireAt),
	}, nil
}

func (s *Server) CheckLock(ctx context.Context, req *pb.CheckLockRequest) (*pb.CheckLockResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.CheckLock")
	defer span.End()

	span.SetAttributes(attribute.Int64("global_id", req.GlobalId))

	lock, err := s.repo.GetLockByGlobalID(ctx, req.GlobalId)
	if err != nil {
		if errors.Is(err, repository.ErrLockNotFound) {
			return &pb.CheckLockResponse{
				Exists: false,
				Status: pb.LockStatus_LOCK_STATUS_FREE,
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to check lock: %v", err)
	}

	ttlRemaining := time.Until(lock.ExpireAt).Milliseconds()
	if ttlRemaining < 0 {
		ttlRemaining = 0
	}

	return &pb.CheckLockResponse{
		Exists:         true,
		Status:         pb.LockStatus_LOCK_STATUS_ACQUIRED,
		Token:          lock.Token,
		ExpireAt:       timestamppb.New(lock.ExpireAt),
		TtlRemainingMs: ttlRemaining,
	}, nil
}

func (s *Server) CheckLockByToken(ctx context.Context, req *pb.CheckLockByTokenRequest) (*pb.CheckLockResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.CheckLockByToken")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", req.GlobalId),
		attribute.String("token", req.Token),
	)

	lock, err := s.repo.GetLockByGlobalIDAndToken(ctx, req.GlobalId, req.Token)
	if err != nil {
		if errors.Is(err, repository.ErrLockNotFound) {
			return &pb.CheckLockResponse{
				Exists: false,
				Status: pb.LockStatus_LOCK_STATUS_FREE,
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to check lock: %v", err)
	}

	ttlRemaining := time.Until(lock.ExpireAt).Milliseconds()
	if ttlRemaining < 0 {
		ttlRemaining = 0
	}

	return &pb.CheckLockResponse{
		Exists:         true,
		Status:         pb.LockStatus_LOCK_STATUS_ACQUIRED,
		Token:          lock.Token,
		ExpireAt:       timestamppb.New(lock.ExpireAt),
		TtlRemainingMs: ttlRemaining,
	}, nil
}

func (s *Server) Unlock(ctx context.Context, req *pb.UnlockRequest) (*pb.UnlockResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Server.Unlock")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", req.GlobalId),
		attribute.String("token", req.Token),
	)

	err := s.repo.ReleaseLock(ctx, req.GlobalId, req.Token)
	if err != nil {
		if errors.Is(err, repository.ErrLockNotFound) {
			return &pb.UnlockResponse{
				Success:      false,
				ErrorMessage: "lock not found or invalid token",
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to unlock: %v", err)
	}

	return &pb.UnlockResponse{
		Success: true,
	}, nil
}
