package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const referenceKey = "$ref"

func ResolveInput(configured, previousOutput, checkpoint json.RawMessage) (json.RawMessage, error) {
	configuredValue, err := decodeJSON(configured)
	if err != nil {
		return nil, fmt.Errorf("decode configured input: %w", err)
	}
	previousValue, err := decodeOptionalJSON(previousOutput)
	if err != nil {
		return nil, fmt.Errorf("decode previous output: %w", err)
	}
	checkpointValue, err := decodeOptionalJSON(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	resolved, err := resolveValue(configuredValue, previousValue, checkpointValue)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("encode resolved input: %w", err)
	}
	return encoded, nil
}

func ValidateInputReferences(configured json.RawMessage, allowPrevious bool) error {
	value, err := decodeJSON(configured)
	if err != nil {
		return err
	}
	return validateReferences(value, allowPrevious)
}

func validateReferences(value any, allowPrevious bool) error {
	switch current := value.(type) {
	case []any:
		for _, item := range current {
			if err := validateReferences(item, allowPrevious); err != nil {
				return err
			}
		}
	case map[string]any:
		if reference, exists := current[referenceKey]; exists {
			if len(current) != 1 {
				return errors.New("$ref object cannot contain other fields")
			}
			name, ok := reference.(string)
			if !ok {
				return errors.New("$ref must be a string")
			}
			switch name {
			case "previous_output":
				if !allowPrevious {
					return errors.New("first step cannot reference previous_output")
				}
			case "checkpoint":
			default:
				return fmt.Errorf("unknown input reference %q", name)
			}
			return nil
		}
		for _, item := range current {
			if err := validateReferences(item, allowPrevious); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolveValue(value, previousOutput, checkpoint any) (any, error) {
	switch current := value.(type) {
	case []any:
		resolved := make([]any, len(current))
		for index, item := range current {
			var err error
			resolved[index], err = resolveValue(item, previousOutput, checkpoint)
			if err != nil {
				return nil, err
			}
		}
		return resolved, nil
	case map[string]any:
		if reference, exists := current[referenceKey]; exists {
			if len(current) != 1 {
				return nil, errors.New("$ref object cannot contain other fields")
			}
			name, ok := reference.(string)
			if !ok {
				return nil, errors.New("$ref must be a string")
			}
			switch name {
			case "previous_output":
				if previousOutput == nil {
					return nil, errors.New("previous_output is unavailable")
				}
				return previousOutput, nil
			case "checkpoint":
				if checkpoint == nil {
					return nil, errors.New("checkpoint is unavailable")
				}
				return checkpoint, nil
			default:
				return nil, fmt.Errorf("unknown input reference %q", name)
			}
		}
		resolved := make(map[string]any, len(current))
		for key, item := range current {
			var err error
			resolved[key], err = resolveValue(item, previousOutput, checkpoint)
			if err != nil {
				return nil, err
			}
		}
		return resolved, nil
	default:
		return value, nil
	}
}

func decodeJSON(value json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func decodeOptionalJSON(value json.RawMessage) (any, error) {
	if len(value) == 0 || bytes.Equal(value, []byte("null")) {
		return nil, nil
	}
	return decodeJSON(value)
}
