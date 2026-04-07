package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/quantara/lock-service/internal/cache"
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
	cache  *cache.LockCache
	tracer trace.Tracer
}

func NewLockRepository(db *sql.DB, lockCache *cache.LockCache, tracer trace.Tracer) *LockRepository {
	return &LockRepository{
		db:     db,
		cache:  lockCache,
		tracer: tracer,
	}
}

// AcquireLock attempts to acquire a lock using atomic upsert pattern
func (r *LockRepository) AcquireLock(ctx context.Context, globalID int64, token string, expireAt time.Time) error {
	ctx, span := r.tracer.Start(ctx, "LockRepository.AcquireLock")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("global_id", globalID),
		attribute.String("token", token),
	)

	// Use READ COMMITTED with row-level locking instead of SERIALIZABLE
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Try to lock the row if it exists (FOR UPDATE NOWAIT to fail fast)
	var existingToken string
	var existingExpire time.Time
	err = tx.QueryRowContext(ctx,
		"SELECT token, expire FROM murex.object_lock WHERE global_id = $1 FOR UPDATE NOWAIT",
		globalID,
	).Scan(&existingToken, &existingExpire)

	if err == nil {
		// Row exists - check if lock is still active
		if existingExpire.After(time.Now()) {
			// Active lock exists
			span.SetAttributes(attribute.Bool("lock_exists", true))
			return ErrLockAlreadyExists
		}
		// Lock expired - update with new token
		_, err = tx.ExecContext(ctx,
			"UPDATE murex.object_lock SET token = $1, expire = $2 WHERE global_id = $3",
			token, expireAt, globalID,
		)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to update expired lock: %w", err)
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		// No lock exists - insert new one using ON CONFLICT to handle race
		result, err := tx.ExecContext(ctx,
			`INSERT INTO murex.object_lock (global_id, token, expire)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (global_id) DO UPDATE
			 SET token = EXCLUDED.token, expire = EXCLUDED.expire
			 WHERE murex.object_lock.expire <= NOW()`,
			globalID, token, expireAt,
		)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to insert lock: %w", err)
		}
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			// Another transaction inserted an active lock
			span.SetAttributes(attribute.Bool("lock_exists", true))
			return ErrLockAlreadyExists
		}
	} else {
		// Check if it's a lock timeout error (NOWAIT)
		if err.Error() == "pq: could not obtain lock on row in relation \"object_lock\"" {
			span.SetAttributes(attribute.Bool("lock_contention", true))
			return ErrLockAlreadyExists
		}
		span.RecordError(err)
		return fmt.Errorf("failed to check existing lock: %w", err)
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Cache the lock (best effort - DB is source of truth)
	if r.cache != nil {
		r.cache.Set(ctx, globalID, token, expireAt)
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

	// Use READ COMMITTED - the UPDATE itself is atomic
	result, err := r.db.ExecContext(ctx,
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
		err = r.db.QueryRowContext(ctx,
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

	// Update cache TTL (best effort)
	if r.cache != nil {
		r.cache.UpdateTTL(ctx, globalID, token, newExpireAt)
	}

	span.SetAttributes(attribute.Bool("success", true))
	return nil
}

// GetLockByGlobalID retrieves lock information by global_id
func (r *LockRepository) GetLockByGlobalID(ctx context.Context, globalID int64) (*Lock, error) {
	ctx, span := r.tracer.Start(ctx, "LockRepository.GetLockByGlobalID")
	defer span.End()

	span.SetAttributes(attribute.Int64("global_id", globalID))

	// Check cache first
	if r.cache != nil {
		cached, err := r.cache.Get(ctx, globalID)
		if err == nil && cached != nil {
			span.SetAttributes(attribute.Bool("cache_hit", true))
			return &Lock{
				GlobalID: globalID,
				Token:    cached.Token,
				ExpireAt: cached.ExpireAt,
			}, nil
		}
		span.SetAttributes(attribute.Bool("cache_hit", false))
	}

	// Cache miss - query DB
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

	// Cache the result
	if r.cache != nil {
		r.cache.Set(ctx, globalID, lock.Token, lock.ExpireAt)
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

	// DELETE is atomic, no transaction needed
	result, err := r.db.ExecContext(ctx,
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

	// Delete from cache AFTER DB (DB is source of truth)
	// If DB delete succeeded but cache delete fails - cache will expire via TTL
	if r.cache != nil {
		r.cache.Delete(ctx, globalID)
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
