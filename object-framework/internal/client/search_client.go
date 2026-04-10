package client

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/quantara/object-framework/proto/searchservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// SearchClient wraps the gRPC client for Search Service
type SearchClient struct {
	conn   *grpc.ClientConn
	client pb.SearchServiceClient
}

// NewSearchClient creates a new SearchClient
func NewSearchClient(addr string) (*SearchClient, error) {
	conn, err := newGRPCConn(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to search-service: %w", err)
	}

	return &SearchClient{
		conn:   conn,
		client: pb.NewSearchServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *SearchClient) Close() error {
	return c.conn.Close()
}

// GetObjectByGlobalID retrieves an object by its GlobalID
// Returns nil if object not found
func (c *SearchClient) GetObjectByGlobalID(ctx context.Context, objectClass string, globalID int64, draftStatus, actualDate string) (map[string]interface{}, error) {
	resp, err := c.client.GetObjectByGlobalId(ctx, &pb.GetObjectByGlobalIdRequest{
		ObjectRootClass: objectClass,
		GlobalId:        globalID,
		DraftStatus:     draftStatus,
		ActualDate:      actualDate,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object by GlobalID: %w", err)
	}

	if resp.Content == nil {
		return nil, nil // Not found
	}

	return structValueToMap(resp.Content)
}

// GetObjectRevisionByID retrieves an object by its ID
func (c *SearchClient) GetObjectRevisionByID(ctx context.Context, objectClass string, id int64) (map[string]interface{}, error) {
	resp, err := c.client.GetObjectRevisionById(ctx, &pb.GetObjectRevisionByIdRequest{
		ObjectClass: objectClass,
		Id:          id,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object by ID: %w", err)
	}

	if resp.Content == nil {
		return nil, nil
	}

	return structValueToMap(resp.Content)
}

// GetObjectCollectionByParentID retrieves objects by parent ID
func (c *SearchClient) GetObjectCollectionByParentID(ctx context.Context, objectClass string, parentID int64) ([]map[string]interface{}, error) {
	resp, err := c.client.GetObjectCollectionByParentId(ctx, &pb.GetObjectCollectionByParentIdRequest{
		ObjectRootClass: objectClass,
		ParentId:        parentID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get objects by parent ID: %w", err)
	}

	result := make([]map[string]interface{}, 0, len(resp.Contents))
	for _, content := range resp.Contents {
		m, err := structValueToMap(content)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}

	return result, nil
}

// GetObjectGlobalIDByAltID retrieves GlobalID by alternative ID (external ID)
func (c *SearchClient) GetObjectGlobalIDByAltID(ctx context.Context, objectClass, altID, sourceAlias string) (int64, error) {
	resp, err := c.client.GetObjectGlobalIdByAltId(ctx, &pb.GetObjectGlobalIdByAltIdRequest{
		ObjectRootClass: objectClass,
		AltId:           altID,
		SourceAlias:     sourceAlias,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get GlobalID by alt ID: %w", err)
	}
	return resp.GlobalId, nil
}

// structValueToMap converts protobuf Value to map
func structValueToMap(v *structpb.Value) (map[string]interface{}, error) {
	if v == nil {
		return nil, nil
	}

	// Convert to JSON and back to map for type safety
	jsonBytes, err := v.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal protobuf value: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to map: %w", err)
	}

	return result, nil
}

// IsNotFoundError checks if the error is a gRPC NotFound error
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.NotFound
}
