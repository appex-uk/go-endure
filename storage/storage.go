// Package storage defines the persistence contract used by go-endure engines.
package storage

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrLeaseLost means another worker took over the workflow.
	ErrLeaseLost = errors.New("endure: lease lost")
	// ErrNotFound means the workflow does not exist.
	ErrNotFound = errors.New("endure: workflow not found")
	// ErrNameMismatch means a workflow ID already belongs to another workflow definition.
	ErrNameMismatch = errors.New("endure: workflow id already used by a different workflow")
	// ErrInvalidConfig means a built-in storage configuration is incomplete.
	ErrInvalidConfig = errors.New("endure: invalid storage configuration")
	// ErrUnsupportedBackend means no built-in adapter exists for the configured backend.
	ErrUnsupportedBackend = errors.New("endure: unsupported storage backend")
)

// WorkflowStatus is a persisted workflow state.
type WorkflowStatus string

const (
	WorkflowQueued    WorkflowStatus = "queued"
	WorkflowRunning   WorkflowStatus = "running"
	WorkflowWaiting   WorkflowStatus = "waiting"
	WorkflowCompleted WorkflowStatus = "completed"
	WorkflowFailed    WorkflowStatus = "failed"
)

// StepStatus is a persisted step state.
type StepStatus string

const (
	StepRunning   StepStatus = "running"
	StepRetrying  StepStatus = "retrying"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
)

// Lease identifies one exclusive claim of a workflow. All lease-fenced writes must compare
// both ID and Token and return ErrLeaseLost when the claim is no longer current.
type Lease struct {
	ID    string
	Token string
}

// WorkflowID returns the workflow ID protected by this lease.
func (l Lease) WorkflowID() string { return l.ID }

// Claim is a workflow made runnable by a worker.
type Claim struct {
	Lease
	Name    string
	Input   []byte
	Attempt int
}

// ClaimRequest controls an atomic claim operation.
type ClaimRequest struct {
	WorkerID string
	LeaseFor time.Duration
	Names    []string
	Limit    int
}

// StepRecord is the durable checkpoint for one step.
type StepRecord struct {
	Status    StepStatus
	Output    []byte
	Error     string
	Attempts  int
	NotBefore *time.Time
}

// StepWrite describes a durable step checkpoint.
type StepWrite struct {
	Name      string
	Status    StepStatus
	Attempts  int
	Output    []byte
	Error     string
	NotBefore *time.Time
}

// WorkflowRecord is the persisted terminal state of a workflow.
type WorkflowRecord struct {
	Status WorkflowStatus
	Output []byte
	Error  string
}

// Storage is the adapter contract used by an Engine. Implementations are responsible for
// atomic claim, suspension and signal transitions, as well as fencing every lease-protected
// write. Implementations must be safe for concurrent use.
//
// The durable lease semantics are intentionally part of this contract: adapters may use
// database transactions, file locks or another equivalent synchronization mechanism.
type Storage interface {
	// Migrate idempotently creates or upgrades the adapter's durable state.
	Migrate(ctx context.Context) error

	// CreateWorkflow is idempotent for the same ID and name. Reusing an ID for a different
	// name must return ErrNameMismatch.
	CreateWorkflow(ctx context.Context, workflowID, name string, input []byte) error
	// ClaimWorkflows atomically claims due queued workflows and expired running workflows
	// whose names are in the request. Every claim receives a fresh lease token and increments
	// the workflow attempt count.
	ClaimWorkflows(ctx context.Context, request ClaimRequest) ([]Claim, error)
	// RenewLease extends a current lease and returns ErrLeaseLost for an expired or replaced token.
	RenewLease(ctx context.Context, lease Lease, leaseFor time.Duration) error

	// GetStep returns nil when the workflow or step does not exist.
	GetStep(ctx context.Context, workflowID, name string) (*StepRecord, error)
	// WriteStep atomically upserts a step only while lease is current.
	WriteStep(ctx context.Context, lease Lease, step StepWrite) error

	// GetSignal returns false when the named signal has not been stored.
	GetSignal(ctx context.Context, workflowID, name string) (payload []byte, ok bool, err error)
	// SignalWorkflow preserves the first payload and wakes a workflow waiting on the same name.
	// It returns ErrNotFound when the workflow does not exist.
	SignalWorkflow(ctx context.Context, workflowID, name string, payload []byte) error

	// FinishWorkflow atomically records a terminal state only while lease is current.
	FinishWorkflow(ctx context.Context, lease Lease, status WorkflowStatus, output []byte, errMsg string) error
	// RequeueWorkflow releases the current lease and schedules another attempt at runAt.
	RequeueWorkflow(ctx context.Context, lease Lease, runAt time.Time) error
	// WaitForSignal atomically parks a current lease or immediately requeues it when the
	// requested signal already exists.
	WaitForSignal(ctx context.Context, lease Lease, name string) error
	// GetWorkflow returns ErrNotFound when the workflow does not exist.
	GetWorkflow(ctx context.Context, workflowID string) (*WorkflowRecord, error)
}
