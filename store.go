package endure

import (
	"context"
	"time"

	"github.com/appex-uk/go-endure/storage"
)

type store struct {
	backend storage.Storage
}

type lease struct {
	id    string
	token string
}

func (l lease) storageLease() storage.Lease {
	return storage.Lease{ID: l.id, Token: l.token}
}

type claimed struct {
	lease
	name    string
	input   []byte
	attempt int
}

type stepRow struct {
	status    storage.StepStatus
	output    []byte
	errMsg    string
	attempts  int
	notBefore *time.Time
}

const (
	stepRunning   = storage.StepRunning
	stepRetrying  = storage.StepRetrying
	stepCompleted = storage.StepCompleted
	stepFailed    = storage.StepFailed
)

func (s *store) create(ctx context.Context, id, name string, input []byte) error {
	return s.backend.CreateWorkflow(ctx, id, name, input)
}

func (s *store) claim(ctx context.Context, workerID string, leaseFor time.Duration, names []string, limit int) ([]claimed, error) {
	records, err := s.backend.ClaimWorkflows(ctx, storage.ClaimRequest{
		WorkerID: workerID,
		LeaseFor: leaseFor,
		Names:    names,
		Limit:    limit,
	})
	if err != nil {
		return nil, err
	}
	claims := make([]claimed, 0, len(records))
	for _, record := range records {
		claims = append(claims, claimed{
			lease:   lease{id: record.ID, token: record.Token},
			name:    record.Name,
			input:   record.Input,
			attempt: record.Attempt,
		})
	}
	return claims, nil
}

func (s *store) renew(ctx context.Context, l lease, leaseFor time.Duration) error {
	return s.backend.RenewLease(ctx, l.storageLease(), leaseFor)
}

func (s *store) getStep(ctx context.Context, workflowID, name string) (*stepRow, error) {
	record, err := s.backend.GetStep(ctx, workflowID, name)
	if err != nil || record == nil {
		return nil, err
	}
	return &stepRow{
		status:    record.Status,
		output:    record.Output,
		errMsg:    record.Error,
		attempts:  record.Attempts,
		notBefore: record.NotBefore,
	}, nil
}

func (s *store) writeStep(ctx context.Context, l lease, name string, status storage.StepStatus, attempts int, output []byte, errMsg string, notBefore *time.Time) error {
	return s.backend.WriteStep(ctx, l.storageLease(), storage.StepWrite{
		Name:      name,
		Status:    status,
		Attempts:  attempts,
		Output:    output,
		Error:     errMsg,
		NotBefore: notBefore,
	})
}

func (s *store) getSignal(ctx context.Context, workflowID, name string) ([]byte, bool, error) {
	return s.backend.GetSignal(ctx, workflowID, name)
}

func (s *store) signal(ctx context.Context, workflowID, name string, payload []byte) error {
	return s.backend.SignalWorkflow(ctx, workflowID, name, payload)
}

func (s *store) finish(ctx context.Context, l lease, status storage.WorkflowStatus, output []byte, errMsg string) error {
	return s.backend.FinishWorkflow(ctx, l.storageLease(), status, output, errMsg)
}

func (s *store) requeue(ctx context.Context, l lease, runAt time.Time) error {
	return s.backend.RequeueWorkflow(ctx, l.storageLease(), runAt)
}

func (s *store) waitFor(ctx context.Context, l lease, name string) error {
	return s.backend.WaitForSignal(ctx, l.storageLease(), name)
}

type workflowRow struct {
	status storage.WorkflowStatus
	output []byte
	errMsg string
}

func (s *store) getWorkflow(ctx context.Context, id string) (*workflowRow, error) {
	record, err := s.backend.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}
	return &workflowRow{
		status: record.Status,
		output: record.Output,
		errMsg: record.Error,
	}, nil
}
