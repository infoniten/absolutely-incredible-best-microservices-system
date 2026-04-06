package client

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/quantara/object-framework/proto/transaction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TransactionClient wraps the gRPC client for Transaction Service
type TransactionClient struct {
	conn   *grpc.ClientConn
	client pb.TransactionServiceClient
}

// NewTransactionClient creates a new TransactionClient
func NewTransactionClient(addr string) (*TransactionClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to transaction-service: %w", err)
	}

	return &TransactionClient{
		conn:   conn,
		client: pb.NewTransactionServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *TransactionClient) Close() error {
	return c.conn.Close()
}

// BeginTransaction starts a new transaction
func (c *TransactionClient) BeginTransaction(ctx context.Context, timeoutSeconds *int32) (string, error) {
	req := &pb.BeginTxRequest{}
	if timeoutSeconds != nil {
		req.TimeoutSeconds = timeoutSeconds
	}

	resp, err := c.client.BeginTransaction(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	return resp.TransactionId, nil
}

// SaveObject saves an object to the transaction
func (c *TransactionClient) SaveObject(ctx context.Context, txID string, headers *ObjectHeaders, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	pbHeaders := &pb.ObjectHeaders{
		Id:         headers.ID,
		GlobalId:   headers.GlobalID,
		ObjectType: headers.ObjectType,
		LockId:     headers.LockID,
		Revision:   headers.Revision,
	}
	if headers.Version != nil {
		pbHeaders.Version = headers.Version
	}
	if headers.ActualFrom != nil {
		pbHeaders.ActualFrom = headers.ActualFrom
	}
	if headers.DraftStatus != nil {
		pbHeaders.DraftStatus = headers.DraftStatus
	}
	if headers.ParentID != nil {
		pbHeaders.ParentId = headers.ParentID
	}

	resp, err := c.client.Save(ctx, &pb.SaveRequest{
		TransactionId: txID,
		Headers:       pbHeaders,
		Payload:       payloadBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to save object: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("failed to save object: %s", resp.Error)
	}
	return nil
}

// Commit commits the transaction
func (c *TransactionClient) Commit(ctx context.Context, txID string) (*CommitResult, error) {
	resp, err := c.client.Commit(ctx, &pb.CommitRequest{TransactionId: txID})
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return &CommitResult{
		Success:      resp.Success,
		Error:        resp.Error,
		ObjectsSaved: resp.ObjectsSaved,
	}, nil
}

// Rollback rolls back the transaction
func (c *TransactionClient) Rollback(ctx context.Context, txID string) error {
	resp, err := c.client.Rollback(ctx, &pb.RollbackRequest{TransactionId: txID})
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("rollback failed")
	}
	return nil
}

// ObjectHeaders represents headers for a saved object
type ObjectHeaders struct {
	ID          int64
	GlobalID    int64
	ObjectType  string
	LockID      string
	Revision    int32
	Version     *int32
	ActualFrom  *string
	DraftStatus *string
	ParentID    *int64
}

// CommitResult represents the result of a commit operation
type CommitResult struct {
	Success      bool
	Error        string
	ObjectsSaved int32
}
