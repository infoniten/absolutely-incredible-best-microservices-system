package cron

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/quantara/lock-service/internal/repository"
)

type CleanupJob struct {
	repo     *repository.LockRepository
	tracer   trace.Tracer
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewCleanupJob(repo *repository.LockRepository, tracer trace.Tracer, interval time.Duration) *CleanupJob {
	return &CleanupJob{
		repo:     repo,
		tracer:   tracer,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (j *CleanupJob) Start() {
	log.Printf("starting cleanup job with interval: %v", j.interval)

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	go func() {
		defer close(j.doneCh)

		// Run immediately on start
		j.runCleanup()

		for {
			select {
			case <-ticker.C:
				j.runCleanup()
			case <-j.stopCh:
				log.Println("cleanup job stopped")
				return
			}
		}
	}()
}

func (j *CleanupJob) Stop() {
	close(j.stopCh)
	<-j.doneCh
}

func (j *CleanupJob) runCleanup() {
	ctx, span := j.tracer.Start(context.Background(), "CleanupJob.runCleanup")
	defer span.End()

	startTime := time.Now()
	rowsDeleted, err := j.repo.CleanupExpiredLocks(ctx)
	duration := time.Since(startTime)

	span.SetAttributes(
		attribute.Int64("rows_deleted", rowsDeleted),
		attribute.Int64("duration_ms", duration.Milliseconds()),
	)

	if err != nil {
		span.RecordError(err)
		log.Printf("cleanup job failed: %v", err)
		return
	}

	log.Printf("cleanup job completed: deleted %d expired locks in %v", rowsDeleted, duration)
}
