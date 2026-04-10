package client

import (
	"context"
	"fmt"

	pb "github.com/quantara/object-framework/proto/idservice"
	"google.golang.org/grpc"
)

// IDClient wraps the gRPC client for ID Service
type IDClient struct {
	conn   *grpc.ClientConn
	client pb.IDServiceClient
}

// NewIDClient creates a new IDClient
func NewIDClient(addr string) (*IDClient, error) {
	conn, err := newGRPCConn(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to id-service: %w", err)
	}

	return &IDClient{
		conn:   conn,
		client: pb.NewIDServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *IDClient) Close() error {
	return c.conn.Close()
}

// GetID returns a single unique ID
func (c *IDClient) GetID(ctx context.Context) (int64, error) {
	resp, err := c.client.GetID(ctx, &pb.GetIDRequest{})
	if err != nil {
		return 0, fmt.Errorf("failed to get ID: %w", err)
	}
	return resp.Id, nil
}

// GetBatchID returns a batch of unique IDs
func (c *IDClient) GetBatchID(ctx context.Context, batchSize int32) ([]int64, error) {
	resp, err := c.client.GetBatchID(ctx, &pb.GetBatchIDRequest{BatchSize: batchSize})
	if err != nil {
		return nil, fmt.Errorf("failed to get batch ID: %w", err)
	}
	return resp.Ids, nil
}
