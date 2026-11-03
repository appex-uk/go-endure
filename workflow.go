package endure

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Workflow is a registered, typed workflow definition.
type Workflow[I, O any] struct {
	e    *Engine
	name string
}

// Register adds a workflow to the engine. It must be called before Run and panics on duplicate names.
func Register[I, O any](e *Engine, name string, fn func(Context, I) (O, error)) *Workflow[I, O] {
	e.register(name, registration{run: func(ctx Context, input []byte) ([]byte, error) {
		var in I
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, Permanent(fmt.Errorf("decode workflow input: %w", err))
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}})
	return &Workflow[I, O]{e: e, name: name}
}

// Start enqueues a workflow under id. Starting an existing id is a no-op that returns its handle,
// which makes Start safe to retry. The id must not be reused with a different workflow name.
func (w *Workflow[I, O]) Start(ctx context.Context, id string, input I) (*Handle[O], error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("endure: encode input: %w", err)
	}
	if err := w.e.st.create(ctx, id, w.name, b); err != nil {
		return nil, err
	}
	return w.Handle(id), nil
}

// Handle returns a handle for an existing workflow id.
func (w *Workflow[I, O]) Handle(id string) *Handle[O] { return &Handle[O]{e: w.e, id: id} }

// Handle observes one workflow.
type Handle[O any] struct {
	e  *Engine
	id string
}

// ID returns the workflow id.
func (h *Handle[O]) ID() string { return h.id }

// Wait polls until the workflow completes and returns its result, or a *WorkflowError if it failed.
func (h *Handle[O]) Wait(ctx context.Context) (O, error) {
	var zero O
	t := time.NewTicker(h.e.opts.PollInterval)
	defer t.Stop()
	for {
		r, err := h.e.st.getWorkflow(ctx, h.id)
		if err != nil {
			return zero, err
		}
		switch r.status {
		case "completed":
			return decode[O](h.id, r.output)
		case "failed":
			return zero, &WorkflowError{ID: h.id, Message: r.errMsg}
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-t.C:
		}
	}
}
