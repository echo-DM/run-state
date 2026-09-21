package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dimen61/runstate/internal/store"
)

type implementation struct {
	validate func(json.RawMessage) error
	execute  func(context.Context, json.RawMessage) (json.RawMessage, error)
}

var implementations = map[string]implementation{
	"echo":  {validate: validateEcho, execute: executeEcho},
	"sleep": {validate: validateSleep, execute: executeSleep},
}

func ValidateDefinition(stepType string, input json.RawMessage) error {
	implementation, ok := implementations[stepType]
	if !ok {
		return fmt.Errorf("unknown step type %q", stepType)
	}
	return implementation.validate(input)
}

func Execute(ctx context.Context, attempt store.StepAttempt) (json.RawMessage, json.RawMessage, error) {
	implementation, ok := implementations[attempt.StepType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown step type %q", attempt.StepType)
	}
	output, err := implementation.execute(ctx, attempt.ResolvedInput)
	if err != nil {
		return nil, nil, err
	}
	return output, nil, nil
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

func executeEcho(_ context.Context, inputJSON json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(inputJSON, &input); err != nil || len(input.Value) == 0 {
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

func executeSleep(ctx context.Context, inputJSON json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(inputJSON, &input); err != nil {
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
