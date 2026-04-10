package client

import (
	"context"
	"fmt"

	pb "github.com/quantara/object-framework/proto/globalidservice"
	"google.golang.org/grpc"
)

// GlobalIDClient wraps the gRPC client for GlobalID Service
type GlobalIDClient struct {
	conn   *grpc.ClientConn
	client pb.GlobalIDServiceClient
}

// NewGlobalIDClient creates a new GlobalIDClient
func NewGlobalIDClient(addr string) (*GlobalIDClient, error) {
	conn, err := newGRPCConn(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to globalid-service: %w", err)
	}

	return &GlobalIDClient{
		conn:   conn,
		client: pb.NewGlobalIDServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *GlobalIDClient) Close() error {
	return c.conn.Close()
}

// GetGlobalID returns a single unique GlobalID
func (c *GlobalIDClient) GetGlobalID(ctx context.Context) (int64, error) {
	resp, err := c.client.GetGlobalID(ctx, &pb.GetGlobalIDRequest{})
	if err != nil {
		return 0, fmt.Errorf("failed to get GlobalID: %w", err)
	}
	return resp.GlobalId, nil
}

// GetBatchGlobalID returns a batch of unique GlobalIDs
func (c *GlobalIDClient) GetBatchGlobalID(ctx context.Context, batchSize int32) ([]int64, error) {
	resp, err := c.client.GetBatchGlobalID(ctx, &pb.GetBatchGlobalIDRequest{BatchSize: batchSize})
	if err != nil {
		return nil, fmt.Errorf("failed to get batch GlobalID: %w", err)
	}
	return resp.GlobalIds, nil
}
