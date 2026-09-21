package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dimen61/runstate/internal/store"
)

type implementation struct {
	validate func(json.RawMessage) error
	execute  func(context.Context, store.StepAttempt, Options) (json.RawMessage, error)
}

var implementations = map[string]implementation{
	"echo":      {validate: validateEcho, execute: executeEcho},
	"sleep":     {validate: validateSleep, execute: executeSleep},
	"flaky":     {validate: validateFlaky, execute: executeFlaky},
	"fake_tool": {validate: validateFakeTool, execute: executeFakeTool},
}

const maxJSONBytes = 1 << 20

type Options struct {
	FakeToolURL string
	HTTPClient  *http.Client
}

type Failure struct {
	Class store.FailureClass
	Cause error
}

type flakyInput struct {
	TemporaryFailures *int            `json:"temporary_failures"`
	Permanent         bool            `json:"permanent"`
	Value             json.RawMessage `json:"value"`
}

func (failure *Failure) Error() string { return failure.Cause.Error() }
func (failure *Failure) Unwrap() error { return failure.Cause }

func Classify(err error) store.FailureClass {
	var failure *Failure
	if errors.As(err, &failure) {
		return failure.Class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return store.FailureTemporary
	}
	return store.FailurePermanent
}

func ValidateDefinition(stepType string, input json.RawMessage) error {
	implementation, ok := implementations[stepType]
	if !ok {
		return fmt.Errorf("unknown step type %q", stepType)
	}
	return implementation.validate(input)
}

func Execute(ctx context.Context, attempt store.StepAttempt, options Options) (json.RawMessage, json.RawMessage, error) {
	implementation, ok := implementations[attempt.StepType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown step type %q", attempt.StepType)
	}
	output, err := implementation.execute(ctx, attempt, options)
	if err != nil {
		return nil, nil, err
	}
	return output, nil, nil
}

func validateFakeTool(input json.RawMessage) error {
	var definition struct {
		Value              json.RawMessage `json:"value"`
		BlockFirstResponse bool            `json:"block_first_response"`
	}
	if err := json.Unmarshal(input, &definition); err != nil || len(definition.Value) == 0 {
		return errors.New("fake_tool input requires value")
	}
	return nil
}

func executeFakeTool(ctx context.Context, attempt store.StepAttempt, options Options) (json.RawMessage, error) {
	if options.FakeToolURL == "" {
		return nil, &Failure{Class: store.FailurePermanent, Cause: errors.New("fake tool URL is not configured")}
	}
	body, err := json.Marshal(map[string]any{"idempotency_key": attempt.IdempotencyKey, "payload": attempt.ResolvedInput})
	if err != nil {
		return nil, &Failure{Class: store.FailurePermanent, Cause: err}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, options.FakeToolURL+"/invoke", bytes.NewReader(body))
	if err != nil {
		return nil, &Failure{Class: store.FailurePermanent, Cause: err}
	}
	request.Header.Set("Content-Type", "application/json")
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, &Failure{Class: store.FailureTemporary, Cause: fmt.Errorf("fake tool request: %w", err)}
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxJSONBytes+1))
	if err != nil {
		return nil, &Failure{Class: store.FailureTemporary, Cause: fmt.Errorf("read fake tool response: %w", err)}
	}
	if response.StatusCode == http.StatusConflict {
		return nil, &Failure{Class: store.FailurePermanent, Cause: errors.New("fake tool idempotency conflict")}
	}
	if response.StatusCode != http.StatusOK {
		class := store.FailurePermanent
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			class = store.FailureTemporary
		}
		return nil, &Failure{Class: class, Cause: fmt.Errorf("fake tool status %d", response.StatusCode)}
	}
	if len(responseBody) > maxJSONBytes || !json.Valid(responseBody) {
		return nil, &Failure{Class: store.FailurePermanent, Cause: errors.New("invalid fake tool result")}
	}
	return responseBody, nil
}

func validateEcho(input json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(input, &object); err != nil {
		return errors.New("echo input must be a JSON object")
	}
	if _, ok := object["value"]; !ok {
		return errors.New("echo input requires value")
	}
	return nil
}

func executeEcho(_ context.Context, attempt store.StepAttempt, _ Options) (json.RawMessage, error) {
	var input struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(attempt.ResolvedInput, &input); err != nil || len(input.Value) == 0 {
		return nil, errors.New("invalid echo input")
	}
	encoded, err := json.Marshal(map[string]json.RawMessage{"value": input.Value})
	if err != nil {
		return nil, fmt.Errorf("encode echo output: %w", err)
	}
	return encoded, nil
}

func validateSleep(input json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(input, &object); err != nil {
		return errors.New("sleep input must be a JSON object")
	}
	var duration string
	if err := json.Unmarshal(object["duration"], &duration); err != nil {
		return errors.New("sleep input requires a duration string")
	}
	parsed, err := time.ParseDuration(duration)
	if err != nil || parsed <= 0 {
		return errors.New("sleep duration must be positive")
	}
	return nil
}

func executeSleep(ctx context.Context, attempt store.StepAttempt, _ Options) (json.RawMessage, error) {
	var input struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(attempt.ResolvedInput, &input); err != nil {
		return nil, errors.New("invalid sleep input")
	}
	duration, err := time.ParseDuration(input.Duration)
	if err != nil || duration <= 0 {
		return nil, errors.New("invalid sleep duration")
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	output, _ := json.Marshal(map[string]string{"slept": input.Duration})
	return output, nil
}

func validateFlaky(input json.RawMessage) error {
	var definition flakyInput
	if err := json.Unmarshal(input, &definition); err != nil {
		return errors.New("flaky input must be a JSON object")
	}
	if definition.TemporaryFailures != nil && *definition.TemporaryFailures < 0 {
		return errors.New("flaky temporary_failures must be non-negative")
	}
	if definition.Permanent && definition.TemporaryFailures != nil && *definition.TemporaryFailures != 0 {
		return errors.New("flaky input cannot combine permanent and temporary failures")
	}
	if len(definition.Value) == 0 {
		return errors.New("flaky input requires value")
	}
	return nil
}

func executeFlaky(_ context.Context, attempt store.StepAttempt, _ Options) (json.RawMessage, error) {
	var input flakyInput
	if err := json.Unmarshal(attempt.ResolvedInput, &input); err != nil || len(input.Value) == 0 {
		return nil, &Failure{Class: store.FailurePermanent, Cause: errors.New("invalid flaky input")}
	}
	if input.Permanent {
		return nil, &Failure{Class: store.FailurePermanent, Cause: errors.New("controlled_permanent_failure")}
	}
	temporaryFailures := 0
	if input.TemporaryFailures != nil {
		temporaryFailures = *input.TemporaryFailures
	}
	if attempt.Attempt <= temporaryFailures {
		return nil, &Failure{Class: store.FailureTemporary, Cause: errors.New("controlled_temporary_failure")}
	}
	output, err := json.Marshal(map[string]json.RawMessage{"value": input.Value})
	if err != nil {
		return nil, &Failure{Class: store.FailurePermanent, Cause: fmt.Errorf("encode flaky output: %w", err)}
	}
	return output, nil
}
