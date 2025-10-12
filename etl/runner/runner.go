package runner

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/sql/etl"
)

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
)

// Job represents an ETL job to be executed
type Job struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Config      interface{}            `json:"config"` // ETL config
	Status      JobStatus              `json:"status"`
	CreatedAt   time.Time              `json:"created_at"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Result      *JobResult             `json:"result,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// JobResult contains the result of a completed job
type JobResult struct {
	Metrics          etl.Metrics   `json:"metrics"`
	Summary          etl.Summary   `json:"summary"`
	Duration         time.Duration `json:"duration"`
	RecordsProcessed int           `json:"records_processed"`
}

// Runner defines the interface for running ETL jobs
type Runner interface {
	// Submit submits a job for execution
	Submit(ctx context.Context, job *etl.Job) error

	// Cancel cancels a running job
	Cancel(jobID string) error

	// GetJob returns the job status and result
	GetJob(jobID string) (*etl.Job, error)

	// ListJobs returns all jobs with optional status filter
	ListJobs(status etl.JobStatus) ([]*etl.Job, error)

	// Start starts the runner
	Start(ctx context.Context) error

	// Stop stops the runner
	Stop() error
}

// DefaultRunner is the default implementation of Runner
type DefaultRunner struct {
	mu         sync.RWMutex
	jobs       map[string]*etl.Job
	queue      chan *etl.Job
	workers    int
	workerPool chan struct{}
	running    bool
	cancel     context.CancelFunc
	etlManager *etl.Manager
}

// NewDefaultRunner creates a new DefaultRunner
func NewDefaultRunner(workers int, etlManager *etl.Manager) *DefaultRunner {
	if workers <= 0 {
		workers = 1
	}

	return &DefaultRunner{
		jobs:       make(map[string]*etl.Job),
		queue:      make(chan *etl.Job, 100), // buffered channel for job queue
		workers:    workers,
		workerPool: make(chan struct{}, workers),
		etlManager: etlManager,
	}
}

// Submit submits a job for execution
func (r *DefaultRunner) Submit(ctx context.Context, job *etl.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return fmt.Errorf("runner is not running")
	}

	if _, exists := r.jobs[job.ID]; exists {
		return fmt.Errorf("job with ID %s already exists", job.ID)
	}

	job.Status = etl.JobStatusPending
	job.CreatedAt = time.Now()
	r.jobs[job.ID] = job

	// Try to queue the job
	select {
	case r.queue <- job:
		log.Printf("Job %s submitted to runner", job.ID)
		return nil
	default:
		// Queue is full, remove from jobs map
		delete(r.jobs, job.ID)
		return fmt.Errorf("job queue is full")
	}
}

// Cancel cancels a running job
func (r *DefaultRunner) Cancel(jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, exists := r.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	if job.Status != etl.JobStatusPending && job.Status != etl.JobStatusRunning {
		return fmt.Errorf("job %s cannot be cancelled (status: %s)", jobID, job.Status)
	}

	job.Status = etl.JobStatusCancelled
	log.Printf("Job %s cancelled", jobID)
	return nil
}

// GetJob returns the job status and result
func (r *DefaultRunner) GetJob(jobID string) (*etl.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	job, exists := r.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	return job, nil
}

// ListJobs returns all jobs with optional status filter
func (r *DefaultRunner) ListJobs(status etl.JobStatus) ([]*etl.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var jobs []*etl.Job
	for _, job := range r.jobs {
		if status == "" || job.Status == status {
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

// Start starts the runner
func (r *DefaultRunner) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("runner is already running")
	}
	r.running = true
	r.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	log.Printf("Starting ETL runner with %d workers", r.workers)

	// Start worker goroutines
	for i := 0; i < r.workers; i++ {
		go r.worker(ctx, i)
	}

	return nil
}

// Stop stops the runner
func (r *DefaultRunner) Stop() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = false
	r.mu.Unlock()

	if r.cancel != nil {
		r.cancel()
	}

	log.Println("ETL runner stopped")
	return nil
}

// worker processes jobs from the queue
func (r *DefaultRunner) worker(ctx context.Context, workerID int) {
	log.Printf("Worker %d started", workerID)

	for {
		select {
		case job := <-r.queue:
			r.processJob(ctx, job, workerID)
		case <-ctx.Done():
			log.Printf("Worker %d stopping", workerID)
			return
		}
	}
}

// processJob processes a single job
func (r *DefaultRunner) processJob(ctx context.Context, job *etl.Job, workerID int) {
	r.mu.Lock()
	job.Status = etl.JobStatusRunning
	now := time.Now()
	job.StartedAt = &now
	r.mu.Unlock()

	log.Printf("Worker %d processing job %s", workerID, job.ID)

	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if job.Status == etl.JobStatusRunning {
			job.Status = etl.JobStatusFailed
			job.Error = "job processing failed unexpectedly"
		}
	}()

	// Use the config directly since job.Config is already *config.Config
	cfg := job.Config
	if cfg == nil {
		r.updateJobStatus(job.ID, etl.JobStatusFailed, "invalid ETL config: nil")
		return
	}

	// Prepare ETL using manager
	jobIDs, err := r.etlManager.Prepare(cfg)
	if err != nil {
		r.updateJobStatus(job.ID, etl.JobStatusFailed, fmt.Sprintf("failed to prepare ETL: %v", err))
		return
	}

	if len(jobIDs) == 0 {
		r.updateJobStatus(job.ID, etl.JobStatusFailed, "no ETL jobs prepared")
		return
	}

	startTime := time.Now()
	var totalRecords int
	var allMetrics etl.Metrics

	// Run all prepared ETL jobs
	for _, etlID := range jobIDs {
		// Check if job was cancelled
		r.mu.RLock()
		if job.Status == etl.JobStatusCancelled {
			r.mu.RUnlock()
			r.updateJobStatus(job.ID, etl.JobStatusCancelled, "job was cancelled")
			return
		}
		r.mu.RUnlock()

		// Start ETL job
		if err := r.etlManager.Start(ctx, etlID); err != nil {
			r.updateJobStatus(job.ID, etl.JobStatusFailed, fmt.Sprintf("ETL job %s failed: %v", etlID, err))
			return
		}

		// Get metrics from completed ETL
		if etlInstance, exists := r.etlManager.GetETL(etlID); exists {
			metrics := etlInstance.GetMetrics()
			totalRecords += int(metrics.Loaded)
			allMetrics.Extracted += metrics.Extracted
			allMetrics.Mapped += metrics.Mapped
			allMetrics.Transformed += metrics.Transformed
			allMetrics.Loaded += metrics.Loaded
			allMetrics.Errors += metrics.Errors
			allMetrics.WorkerActivities = append(allMetrics.WorkerActivities, metrics.WorkerActivities...)
		}
	}

	duration := time.Since(startTime)

	// Create job result
	result := &etl.JobResult{
		Metrics: etl.Metrics{
			Extracted:        allMetrics.Extracted,
			Mapped:           allMetrics.Mapped,
			Transformed:      allMetrics.Transformed,
			Loaded:           allMetrics.Loaded,
			Errors:           allMetrics.Errors,
			WorkerActivities: allMetrics.WorkerActivities,
		},
		Duration:         duration,
		RecordsProcessed: totalRecords,
	}

	// Get summary from last ETL job
	if len(jobIDs) > 0 {
		if etlInstance, exists := r.etlManager.GetETL(jobIDs[len(jobIDs)-1]); exists {
			result.Summary = etlInstance.GetSummary()
		}
	}

	r.mu.Lock()
	job.Status = etl.JobStatusCompleted
	now = time.Now()
	job.CompletedAt = &now
	job.Result = result
	r.mu.Unlock()

	log.Printf("Job %s completed successfully in %v", job.ID, duration)
}

// updateJobStatus updates a job's status and error message
func (r *DefaultRunner) updateJobStatus(jobID string, status etl.JobStatus, errorMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if job, exists := r.jobs[jobID]; exists {
		job.Status = status
		job.Error = errorMsg
		if status == etl.JobStatusFailed || status == etl.JobStatusCancelled {
			now := time.Now()
			job.CompletedAt = &now
		}
	}
}

// etlConfig wraps the ETL configuration for job submission
type etlConfig struct {
	Config interface{} `json:"config"`
}

// NewETLJob creates a new ETL job
func NewETLJob(id, name string, config interface{}) *Job {
	return &Job{
		ID:       id,
		Name:     name,
		Config:   &etlConfig{Config: config},
		Status:   JobStatusPending,
		Metadata: make(map[string]interface{}),
	}
}
