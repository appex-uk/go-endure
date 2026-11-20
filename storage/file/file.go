// Package file provides a filesystem-backed go-endure storage adapter.
package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"

	"github.com/appex-uk/go-endure/storage"
)

const (
	stateFileName = "endure.json"
	lockFileName  = ".endure.lock"
	stateVersion  = 1
)

// Store persists workflows in an atomically replaced JSON file. Operations use an OS file lock,
// so multiple processes can safely share the same directory.
type Store struct {
	root      string
	statePath string
	lockPath  string
	lock      *flock.Flock
	gate      chan struct{}
	valid     bool
}

var _ storage.Storage = (*Store)(nil)

type stateFile struct {
	Version   int                       `json:"version"`
	Workflows map[string]*workflowState `json:"workflows"`
}

type workflowState struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	Input          []byte                 `json:"input"`
	Status         storage.WorkflowStatus `json:"status"`
	Output         []byte                 `json:"output,omitempty"`
	Error          string                 `json:"error,omitempty"`
	RunAt          time.Time              `json:"run_at"`
	WaitingOn      string                 `json:"waiting_on,omitempty"`
	WorkerID       string                 `json:"worker_id,omitempty"`
	LeaseToken     string                 `json:"lease_token,omitempty"`
	LeaseExpiresAt *time.Time             `json:"lease_expires_at,omitempty"`
	Attempt        int                    `json:"attempt"`
	Steps          map[string]*stepState  `json:"steps,omitempty"`
	Signals        map[string][]byte      `json:"signals,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

type stepState struct {
	Status      storage.StepStatus `json:"status"`
	Output      []byte             `json:"output,omitempty"`
	Error       string             `json:"error,omitempty"`
	Attempts    int                `json:"attempts"`
	NotBefore   *time.Time         `json:"not_before,omitempty"`
	StartedAt   time.Time          `json:"started_at"`
	CompletedAt *time.Time         `json:"completed_at,omitempty"`
}

// New creates a filesystem adapter for root.
func New(root string) *Store {
	valid := root != ""
	root = filepath.Clean(root)
	return &Store{
		root:      root,
		statePath: filepath.Join(root, stateFileName),
		lockPath:  filepath.Join(root, lockFileName),
		lock:      flock.New(filepath.Join(root, lockFileName)),
		gate:      makeGate(),
		valid:     valid,
	}
}

// Open validates config and creates a filesystem adapter. Migrate initializes its directory.
func Open(config storage.FileConfig) (*Store, error) {
	if strings.TrimSpace(config.Path) == "" {
		return nil, fmt.Errorf("%w: file path is required", storage.ErrInvalidConfig)
	}
	return New(config.Path), nil
}

func makeGate() chan struct{} {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return gate
}

// Migrate initializes the filesystem state file. It is safe to call concurrently.
func (s *Store) Migrate(ctx context.Context) error {
	if !s.valid {
		return fmt.Errorf("%w: file path is required", storage.ErrInvalidConfig)
	}
	if err := os.MkdirAll(s.root, 0o700); err != nil {
		return fmt.Errorf("endure: create filesystem storage: %w", err)
	}

	return s.withLock(ctx, func() error {
		_, err := os.Stat(s.statePath)
		switch {
		case err == nil:
			_, err = s.load()
			return err
		case !errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("endure: inspect filesystem state: %w", err)
		default:
			return s.save(&stateFile{Version: stateVersion, Workflows: map[string]*workflowState{}})
		}
	})
}

// Close releases any lock held by the adapter.
func (s *Store) Close() error {
	return s.lock.Close()
}

// CreateWorkflow creates an idempotent workflow record.
func (s *Store) CreateWorkflow(ctx context.Context, workflowID, name string, input []byte) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		if existing, ok := state.Workflows[workflowID]; ok {
			if existing.Name != name {
				return false, storage.ErrNameMismatch
			}
			return false, nil
		}
		now := time.Now().UTC()
		state.Workflows[workflowID] = &workflowState{
			ID:        workflowID,
			Name:      name,
			Input:     cloneBytes(input),
			Status:    storage.WorkflowQueued,
			RunAt:     now,
			Steps:     map[string]*stepState{},
			Signals:   map[string][]byte{},
			CreatedAt: now,
			UpdatedAt: now,
		}
		return true, nil
	})
}

// ClaimWorkflows atomically claims due workflows and returns their inputs.
func (s *Store) ClaimWorkflows(ctx context.Context, request storage.ClaimRequest) ([]storage.Claim, error) {
	if request.Limit <= 0 || len(request.Names) == 0 {
		return nil, nil
	}

	var claims []storage.Claim
	err := s.update(ctx, func(state *stateFile) (bool, error) {
		names := make(map[string]struct{}, len(request.Names))
		for _, name := range request.Names {
			names[name] = struct{}{}
		}

		now := time.Now().UTC()
		candidates := make([]*workflowState, 0, request.Limit)
		for _, workflow := range state.Workflows {
			if _, ok := names[workflow.Name]; !ok {
				continue
			}
			dueQueued := workflow.Status == storage.WorkflowQueued && !workflow.RunAt.After(now)
			expiredRunning := workflow.Status == storage.WorkflowRunning &&
				workflow.LeaseExpiresAt != nil && !workflow.LeaseExpiresAt.After(now)
			if dueQueued || expiredRunning {
				candidates = append(candidates, workflow)
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].RunAt.Equal(candidates[j].RunAt) {
				return candidates[i].ID < candidates[j].ID
			}
			return candidates[i].RunAt.Before(candidates[j].RunAt)
		})
		if len(candidates) > request.Limit {
			candidates = candidates[:request.Limit]
		}
		for _, workflow := range candidates {
			token := uuid.NewString()
			expiresAt := now.Add(request.LeaseFor)
			workflow.Status = storage.WorkflowRunning
			workflow.WorkerID = request.WorkerID
			workflow.LeaseToken = token
			workflow.LeaseExpiresAt = &expiresAt
			workflow.Attempt++
			workflow.WaitingOn = ""
			workflow.UpdatedAt = now
			claims = append(claims, storage.Claim{
				Lease:   storage.Lease{ID: workflow.ID, Token: token},
				Name:    workflow.Name,
				Input:   cloneBytes(workflow.Input),
				Attempt: workflow.Attempt,
			})
		}
		return len(claims) > 0, nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// RenewLease extends a current lease.
func (s *Store) RenewLease(ctx context.Context, lease storage.Lease, leaseFor time.Duration) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		workflow, err := state.workflowForLease(lease)
		if err != nil {
			return false, err
		}
		expiresAt := time.Now().UTC().Add(leaseFor)
		workflow.LeaseExpiresAt = &expiresAt
		workflow.UpdatedAt = time.Now().UTC()
		return true, nil
	})
}

// GetStep returns a step checkpoint or nil when it does not exist.
func (s *Store) GetStep(ctx context.Context, workflowID, name string) (*storage.StepRecord, error) {
	var result *storage.StepRecord
	err := s.read(ctx, func(state *stateFile) error {
		workflow, ok := state.Workflows[workflowID]
		if !ok {
			return nil
		}
		step, ok := workflow.Steps[name]
		if !ok {
			return nil
		}
		result = &storage.StepRecord{
			Status:    step.Status,
			Output:    cloneBytes(step.Output),
			Error:     step.Error,
			Attempts:  step.Attempts,
			NotBefore: cloneTime(step.NotBefore),
		}
		return nil
	})
	return result, err
}

// WriteStep atomically upserts a fenced step checkpoint.
func (s *Store) WriteStep(ctx context.Context, lease storage.Lease, write storage.StepWrite) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		workflow, err := state.workflowForLease(lease)
		if err != nil {
			return false, err
		}
		now := time.Now().UTC()
		previous, exists := workflow.Steps[write.Name]
		step := &stepState{
			Status:    write.Status,
			Output:    cloneBytes(write.Output),
			Error:     write.Error,
			Attempts:  write.Attempts,
			NotBefore: cloneTime(write.NotBefore),
		}
		if exists {
			step.StartedAt = previous.StartedAt
		} else {
			step.StartedAt = now
		}
		if write.Status == storage.StepCompleted || write.Status == storage.StepFailed {
			step.CompletedAt = &now
		}
		workflow.Steps[write.Name] = step
		workflow.UpdatedAt = now
		return true, nil
	})
}

// GetSignal returns a buffered signal when present.
func (s *Store) GetSignal(ctx context.Context, workflowID, name string) ([]byte, bool, error) {
	var payload []byte
	var ok bool
	err := s.read(ctx, func(state *stateFile) error {
		workflow, exists := state.Workflows[workflowID]
		if !exists {
			return nil
		}
		payload, ok = workflow.Signals[name]
		payload = cloneBytes(payload)
		return nil
	})
	return payload, ok, err
}

// SignalWorkflow stores the first signal for name and wakes a matching parked workflow.
func (s *Store) SignalWorkflow(ctx context.Context, workflowID, name string, payload []byte) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		workflow, ok := state.Workflows[workflowID]
		if !ok {
			return false, storage.ErrNotFound
		}
		changed := false
		if _, exists := workflow.Signals[name]; !exists {
			workflow.Signals[name] = cloneBytes(payload)
			changed = true
		}
		if workflow.Status == storage.WorkflowWaiting && workflow.WaitingOn == name {
			now := time.Now().UTC()
			workflow.Status = storage.WorkflowQueued
			workflow.RunAt = now
			workflow.WaitingOn = ""
			workflow.UpdatedAt = now
			changed = true
		}
		return changed, nil
	})
}

// FinishWorkflow atomically stores a fenced terminal outcome.
func (s *Store) FinishWorkflow(ctx context.Context, lease storage.Lease, status storage.WorkflowStatus, output []byte, errMsg string) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		workflow, err := state.workflowForLease(lease)
		if err != nil {
			return false, err
		}
		workflow.Status = status
		workflow.Output = cloneBytes(output)
		workflow.Error = errMsg
		workflow.clearLease(time.Now().UTC())
		return true, nil
	})
}

// RequeueWorkflow releases a fenced workflow until runAt.
func (s *Store) RequeueWorkflow(ctx context.Context, lease storage.Lease, runAt time.Time) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		workflow, err := state.workflowForLease(lease)
		if err != nil {
			return false, err
		}
		workflow.Status = storage.WorkflowQueued
		workflow.RunAt = runAt.UTC()
		workflow.clearLease(time.Now().UTC())
		return true, nil
	})
}

// WaitForSignal parks or immediately requeues a fenced workflow based on buffered signals.
func (s *Store) WaitForSignal(ctx context.Context, lease storage.Lease, name string) error {
	return s.update(ctx, func(state *stateFile) (bool, error) {
		workflow, err := state.workflowForLease(lease)
		if err != nil {
			return false, err
		}
		now := time.Now().UTC()
		workflow.RunAt = now
		workflow.UpdatedAt = now
		if _, ok := workflow.Signals[name]; ok {
			workflow.Status = storage.WorkflowQueued
			workflow.WaitingOn = ""
		} else {
			workflow.Status = storage.WorkflowWaiting
			workflow.WaitingOn = name
		}
		workflow.clearLease(now)
		return true, nil
	})
}

// GetWorkflow returns a workflow result.
func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (*storage.WorkflowRecord, error) {
	var result *storage.WorkflowRecord
	err := s.read(ctx, func(state *stateFile) error {
		workflow, ok := state.Workflows[workflowID]
		if !ok {
			return storage.ErrNotFound
		}
		result = &storage.WorkflowRecord{
			Status: workflow.Status,
			Output: cloneBytes(workflow.Output),
			Error:  workflow.Error,
		}
		return nil
	})
	return result, err
}

func (s *Store) read(ctx context.Context, fn func(*stateFile) error) error {
	return s.withLock(ctx, func() error {
		state, err := s.load()
		if err != nil {
			return err
		}
		return fn(state)
	})
}

func (s *Store) update(ctx context.Context, fn func(*stateFile) (bool, error)) error {
	return s.withLock(ctx, func() error {
		state, err := s.load()
		if err != nil {
			return err
		}
		changed, err := fn(state)
		if err != nil || !changed {
			return err
		}
		return s.save(state)
	})
}

func (s *Store) withLock(ctx context.Context, fn func() error) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.gate:
	}
	defer func() { s.gate <- struct{}{} }()

	locked, err := s.lock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return fmt.Errorf("endure: acquire filesystem lock %s: %w", s.lockPath, err)
	}
	if !locked {
		lockErr := ctx.Err()
		if lockErr == nil {
			lockErr = errors.New("lock was not acquired")
		}
		return fmt.Errorf("endure: acquire filesystem lock %s: %w", s.lockPath, lockErr)
	}
	defer func() {
		if unlockErr := s.lock.Unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("endure: release filesystem lock: %w", unlockErr))
		}
	}()
	return fn()
}

func (s *Store) load() (*stateFile, error) {
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		return nil, fmt.Errorf("endure: read filesystem state: %w", err)
	}
	var state stateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("endure: decode filesystem state: %w", err)
	}
	if state.Version != stateVersion {
		return nil, fmt.Errorf("endure: unsupported filesystem state version %d", state.Version)
	}
	if state.Workflows == nil {
		state.Workflows = map[string]*workflowState{}
	}
	for _, workflow := range state.Workflows {
		if workflow.Steps == nil {
			workflow.Steps = map[string]*stepState{}
		}
		if workflow.Signals == nil {
			workflow.Signals = map[string][]byte{}
		}
	}
	return &state, nil
}

func (s *Store) save(state *stateFile) error {
	file, err := os.CreateTemp(s.root, ".endure-*.tmp")
	if err != nil {
		return fmt.Errorf("endure: create filesystem state: %w", err)
	}
	tmpPath := file.Name()
	removeTemp := true
	defer func() {
		_ = file.Close()
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("endure: secure filesystem state: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("endure: encode filesystem state: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("endure: sync filesystem state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("endure: close filesystem state: %w", err)
	}
	if err := os.Rename(tmpPath, s.statePath); err != nil {
		return fmt.Errorf("endure: replace filesystem state: %w", err)
	}
	removeTemp = false
	return nil
}

func (s *stateFile) workflowForLease(lease storage.Lease) (*workflowState, error) {
	workflow, ok := s.Workflows[lease.ID]
	if !ok || workflow.Status != storage.WorkflowRunning || workflow.LeaseToken != lease.Token {
		return nil, storage.ErrLeaseLost
	}
	return workflow, nil
}

func (w *workflowState) clearLease(at time.Time) {
	w.LeaseToken = ""
	w.LeaseExpiresAt = nil
	w.WorkerID = ""
	w.UpdatedAt = at
}

func cloneBytes(input []byte) []byte {
	return append([]byte(nil), input...)
}

func cloneTime(input *time.Time) *time.Time {
	if input == nil {
		return nil
	}
	output := *input
	return &output
}
