package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/quantara/object-framework/internal/domain"
)

// GlobalIDMappingRepository handles persistence of external ID to GlobalID mappings
type GlobalIDMappingRepository struct {
	db *sql.DB
}

// NewGlobalIDMappingRepository creates a new GlobalIDMappingRepository
func NewGlobalIDMappingRepository(db *sql.DB) *GlobalIDMappingRepository {
	return &GlobalIDMappingRepository{db: db}
}

// FindByExternalID finds a mapping by external ID, source, and source object type
func (r *GlobalIDMappingRepository) FindByExternalID(ctx context.Context, externalID, source, sourceObjectType string) (*domain.GlobalIDMapping, error) {
	query := `
		SELECT id, external_id, source, source_object_type, global_id
		FROM globalid_mappings
		WHERE external_id = $1 AND source = $2 AND source_object_type = $3
	`

	var mapping domain.GlobalIDMapping
	err := r.db.QueryRowContext(ctx, query, externalID, source, sourceObjectType).Scan(
		&mapping.ID,
		&mapping.ExternalID,
		&mapping.Source,
		&mapping.SourceObjectType,
		&mapping.GlobalID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find globalid mapping: %w", err)
	}

	return &mapping, nil
}

// Create inserts a new GlobalID mapping
func (r *GlobalIDMappingRepository) Create(ctx context.Context, mapping *domain.GlobalIDMapping) error {
	query := `
		INSERT INTO globalid_mappings (id, external_id, source, source_object_type, global_id)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := r.db.ExecContext(ctx, query,
		mapping.ID,
		mapping.ExternalID,
		mapping.Source,
		mapping.SourceObjectType,
		mapping.GlobalID,
	)
	if err != nil {
		return fmt.Errorf("failed to insert globalid mapping: %w", err)
	}
	return nil
}

// GetOrCreate finds an existing mapping or creates a new one
// Returns the mapping and a boolean indicating if it was created (true) or found (false)
func (r *GlobalIDMappingRepository) GetOrCreate(ctx context.Context, externalID, source, sourceObjectType string, newID, newGlobalID int64) (*domain.GlobalIDMapping, bool, error) {
	// Try to find existing
	existing, err := r.FindByExternalID(ctx, externalID, source, sourceObjectType)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return existing, false, nil
	}

	// Create new mapping
	mapping := &domain.GlobalIDMapping{
		ID:               newID,
		ExternalID:       externalID,
		Source:           source,
		SourceObjectType: sourceObjectType,
		GlobalID:         newGlobalID,
	}

	if err := r.Create(ctx, mapping); err != nil {
		// Check if it was created by another process (race condition)
		existing, findErr := r.FindByExternalID(ctx, externalID, source, sourceObjectType)
		if findErr != nil {
			return nil, false, err // Return original error
		}
		if existing != nil {
			return existing, false, nil
		}
		return nil, false, err
	}

	return mapping, true, nil
}

// CreateTableIfNotExists creates the globalid_mappings table if it doesn't exist
func (r *GlobalIDMappingRepository) CreateTableIfNotExists(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS globalid_mappings (
			id BIGINT PRIMARY KEY,
			external_id VARCHAR(256) NOT NULL,
			source VARCHAR(64) NOT NULL,
			source_object_type VARCHAR(64) NOT NULL,
			global_id BIGINT NOT NULL,
			UNIQUE(external_id, source, source_object_type)
		);
		CREATE INDEX IF NOT EXISTS idx_globalid_mappings_lookup
			ON globalid_mappings(external_id, source, source_object_type);
		CREATE INDEX IF NOT EXISTS idx_globalid_mappings_global_id
			ON globalid_mappings(global_id);
	`

	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create globalid_mappings table: %w", err)
	}
	return nil
}
