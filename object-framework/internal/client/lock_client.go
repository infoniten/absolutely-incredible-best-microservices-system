package client

import (
	"context"
	"fmt"

	pb "github.com/quantara/object-framework/proto/lockservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LockClient wraps the gRPC client for Lock Service
type LockClient struct {
	conn   *grpc.ClientConn
	client pb.LockServiceClient
}

// NewLockClient creates a new LockClient
func NewLockClient(addr string) (*LockClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lock-service: %w", err)
	}

	return &LockClient{
		conn:   conn,
		client: pb.NewLockServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *LockClient) Close() error {
	return c.conn.Close()
}

// LockResult contains the result of a lock operation
type LockResult struct {
	Success bool
	Token   string
	Error   string
}

// Lock acquires a lock for the given globalID with default TTL
func (c *LockClient) Lock(ctx context.Context, globalID int64) (*LockResult, error) {
	resp, err := c.client.Lock(ctx, &pb.LockRequest{GlobalId: globalID})
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	return &LockResult{
		Success: resp.Success,
		Token:   resp.Token,
		Error:   resp.ErrorMessage,
	}, nil
}

// LockWithTTL acquires a lock for the given globalID with custom TTL
func (c *LockClient) LockWithTTL(ctx context.Context, globalID int64, ttlMs int64) (*LockResult, error) {
	resp, err := c.client.LockWithTTL(ctx, &pb.LockWithTTLRequest{
		GlobalId: globalID,
		TtlMs:    ttlMs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock with TTL: %w", err)
	}
	return &LockResult{
		Success: resp.Success,
		Token:   resp.Token,
		Error:   resp.ErrorMessage,
	}, nil
}

// Unlock releases the lock for the given globalID and token
func (c *LockClient) Unlock(ctx context.Context, globalID int64, token string) error {
	resp, err := c.client.Unlock(ctx, &pb.UnlockRequest{
		GlobalId: globalID,
		Token:    token,
	})
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to release lock: %s", resp.ErrorMessage)
	}
	return nil
}
