package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrLockAlreadyExists = errors.New("lock already exists for this global_id")
	ErrLockNotFound      = errors.New("lock not found")
	ErrInvalidToken      = errors.New("invalid token for this lock")
)

type Lock struct {
	GlobalID int64
	Token    string
	ExpireAt time.Time
}

type LockRepository struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewLockRepository(db *sql.DB, tracer trace.Tracer) *LockRepository {
	return &LockRepository{
		db:     db,
		tracer: tracer,
	}
}

// AcquireLock attempts to acquire a lock with transaction isolation
func (r *LockRepository) AcquireLock(ctx context.Context, globalID int64, token string, expireAt time.Time) error {
	ctx, span := r.tracer.Start(ctx, "LockRepository.AcquireLock")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", globalID),
		attribute.String("token", token),
	)

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if active lock exists
	var existingExpire time.Time
	err = tx.QueryRowContext(ctx,
		"SELECT expire FROM murex.object_lock WHERE global_id = $1 AND expire > NOW() FOR UPDATE",
		globalID,
	).Scan(&existingExpire)

	if err == nil {
		// Active lock exists
		span.SetAttributes(attribute.Bool("lock_exists", true))
		return ErrLockAlreadyExists
	} else if !errors.Is(err, sql.ErrNoRows) {
		span.RecordError(err)
		return fmt.Errorf("failed to check existing lock: %w", err)
	}

	// Delete expired lock if exists
	_, err = tx.ExecContext(ctx,
		"DELETE FROM murex.object_lock WHERE global_id = $1",
		globalID,
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete expired lock: %w", err)
	}

	// Insert new lock
	_, err = tx.ExecContext(ctx,
		"INSERT INTO murex.object_lock (global_id, token, expire) VALUES ($1, $2, $3)",
		globalID, token, expireAt,
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to insert lock: %w", err)
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	span.SetAttributes(attribute.Bool("success", true))
	return nil
}

// RenewLock extends the lock expiration time
func (r *LockRepository) RenewLock(ctx context.Context, globalID int64, token string, newExpireAt time.Time) error {
	ctx, span := r.tracer.Start(ctx, "LockRepository.RenewLock")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", globalID),
		attribute.String("token", token),
	)

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx,
		"UPDATE murex.object_lock SET expire = $1 WHERE global_id = $2 AND token = $3 AND expire > NOW()",
		newExpireAt, globalID, token,
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to renew lock: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Check if lock exists with different token or expired
		var existingToken string
		err = tx.QueryRowContext(ctx,
			"SELECT token FROM murex.object_lock WHERE global_id = $1",
			globalID,
		).Scan(&existingToken)

		if errors.Is(err, sql.ErrNoRows) {
			return ErrLockNotFound
		}
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to check lock: %w", err)
		}
		return ErrInvalidToken
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	span.SetAttributes(attribute.Bool("success", true))
	return nil
}

// GetLockByGlobalID retrieves lock information by global_id
func (r *LockRepository) GetLockByGlobalID(ctx context.Context, globalID int64) (*Lock, error) {
	ctx, span := r.tracer.Start(ctx, "LockRepository.GetLockByGlobalID")
	defer span.End()

	span.SetAttributes(attribute.Int64("global_id", globalID))

	var lock Lock
	err := r.db.QueryRowContext(ctx,
		"SELECT global_id, token, expire FROM murex.object_lock WHERE global_id = $1 AND expire > NOW()",
		globalID,
	).Scan(&lock.GlobalID, &lock.Token, &lock.ExpireAt)

	if errors.Is(err, sql.ErrNoRows) {
		span.SetAttributes(attribute.Bool("found", false))
		return nil, ErrLockNotFound
	}
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get lock: %w", err)
	}

	span.SetAttributes(attribute.Bool("found", true))
	return &lock, nil
}

// GetLockByGlobalIDAndToken retrieves lock information by global_id and token
func (r *LockRepository) GetLockByGlobalIDAndToken(ctx context.Context, globalID int64, token string) (*Lock, error) {
	ctx, span := r.tracer.Start(ctx, "LockRepository.GetLockByGlobalIDAndToken")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", globalID),
		attribute.String("token", token),
	)

	var lock Lock
	err := r.db.QueryRowContext(ctx,
		"SELECT global_id, token, expire FROM murex.object_lock WHERE global_id = $1 AND token = $2 AND expire > NOW()",
		globalID, token,
	).Scan(&lock.GlobalID, &lock.Token, &lock.ExpireAt)

	if errors.Is(err, sql.ErrNoRows) {
		span.SetAttributes(attribute.Bool("found", false))
		return nil, ErrLockNotFound
	}
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get lock: %w", err)
	}

	span.SetAttributes(attribute.Bool("found", true))
	return &lock, nil
}

// ReleaseLock releases the lock
func (r *LockRepository) ReleaseLock(ctx context.Context, globalID int64, token string) error {
	ctx, span := r.tracer.Start(ctx, "LockRepository.ReleaseLock")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", globalID),
		attribute.String("token", token),
	)

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx,
		"DELETE FROM murex.object_lock WHERE global_id = $1 AND token = $2",
		globalID, token,
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to release lock: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrLockNotFound
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	span.SetAttributes(attribute.Bool("success", true))
	return nil
}

// CleanupExpiredLocks removes all expired locks
func (r *LockRepository) CleanupExpiredLocks(ctx context.Context) (int64, error) {
	ctx, span := r.tracer.Start(ctx, "LockRepository.CleanupExpiredLocks")
	defer span.End()

	result, err := r.db.ExecContext(ctx, "DELETE FROM murex.object_lock WHERE expire < NOW()")
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to cleanup expired locks: %w", err)
	}

	rowsDeleted, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get rows deleted: %w", err)
	}

	span.SetAttributes(attribute.Int64("rows_deleted", rowsDeleted))
	return rowsDeleted, nil
}
