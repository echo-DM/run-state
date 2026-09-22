package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dimen61/runstate/internal/executor"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

const maxDocumentBytes = 1 << 20

type taskStore interface {
	CreateTask(ctx context.Context, definition task.Definition) (task.Task, error)
	LoadTask(ctx context.Context, taskID string) (task.Task, error)
	LoadEvents(ctx context.Context, taskID string) ([]task.Event, error)
	CancelTask(ctx context.Context, taskID string) (store.CancelResult, error)
	DecideApproval(ctx context.Context, taskID, stepID, decision string) (store.ApprovalResult, error)
}

type Handler struct {
	store taskStore
}

func New(store taskStore) http.Handler {
	handler := &Handler{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", handler.createTask)
	mux.HandleFunc("GET /tasks/{id}", handler.getTask)
	mux.HandleFunc("GET /tasks/{id}/events", handler.getEvents)
	mux.HandleFunc("POST /tasks/{id}/cancel", handler.cancelTask)
	mux.HandleFunc("POST /tasks/{id}/approve", func(writer http.ResponseWriter, request *http.Request) {
		handler.decideApproval(writer, request, "approved")
	})
	mux.HandleFunc("POST /tasks/{id}/reject", func(writer http.ResponseWriter, request *http.Request) {
		handler.decideApproval(writer, request, "rejected")
	})
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (handler *Handler) decideApproval(writer http.ResponseWriter, request *http.Request, decision string) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxDocumentBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input struct {
		StepID string `json:"step_id"`
	}
	if err := decoder.Decode(&input); err != nil {
		writeError(writer, http.StatusBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if err := ensureEOF(decoder); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(input.StepID) == "" || input.StepID != strings.TrimSpace(input.StepID) {
		writeError(writer, http.StatusBadRequest, "step_id is required")
		return
	}
	result, err := handler.store.DecideApproval(request.Context(), request.PathValue("id"), input.StepID, decision)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(writer, http.StatusNotFound, "task or step not found")
	case errors.Is(err, store.ErrConflict):
		writeError(writer, http.StatusConflict, "approval is not current or conflicts with a saved decision")
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "decide approval failed")
	default:
		writeJSON(writer, http.StatusOK, result)
	}
}

func (handler *Handler) cancelTask(writer http.ResponseWriter, request *http.Request) {
	result, err := handler.store.CancelTask(request.Context(), request.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(writer, http.StatusNotFound, "task not found")
	case errors.Is(err, store.ErrConflict):
		writeError(writer, http.StatusConflict, "task is already terminal")
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "cancel task failed")
	case result == store.CancellationRequested:
		writeJSON(writer, http.StatusAccepted, map[string]string{"status": "cancellation_requested"})
	default:
		writeJSON(writer, http.StatusOK, map[string]string{"status": "cancelled"})
	}
}

func (handler *Handler) getEvents(writer http.ResponseWriter, request *http.Request) {
	events, err := handler.store.LoadEvents(request.Context(), request.PathValue("id"))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "load task events failed")
		return
	}
	writeJSON(writer, http.StatusOK, events)
}

type createTaskRequest struct {
	TenantID           *string             `json:"tenant_id"`
	RunAt              json.RawMessage     `json:"run_at"`
	TaskTimeoutSeconds *int                `json:"task_timeout_seconds"`
	Steps              []createStepRequest `json:"steps"`
}

type createStepRequest struct {
	Type           string          `json:"type"`
	Input          json.RawMessage `json:"input"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
	MaxAttempts    *int            `json:"max_attempts"`
}

func (handler *Handler) createTask(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxDocumentBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input createTaskRequest
	if err := decoder.Decode(&input); err != nil {
		writeError(writer, http.StatusBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if err := ensureEOF(decoder); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	definition, err := validateDefinition(input)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	created, err := handler.store.CreateTask(request.Context(), definition)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "create task failed")
		return
	}
	writeJSON(writer, http.StatusCreated, created)
}

func (handler *Handler) getTask(writer http.ResponseWriter, request *http.Request) {
	loaded, err := handler.store.LoadTask(request.Context(), request.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(writer, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "load task failed")
		return
	}
	writeJSON(writer, http.StatusOK, loaded)
}

func validateDefinition(input createTaskRequest) (task.Definition, error) {
	if len(input.RunAt) > 0 {
		return task.Definition{}, errors.New("scheduled tasks are not implemented")
	}
	if len(input.Steps) == 0 || len(input.Steps) > 100 {
		return task.Definition{}, errors.New("steps must contain between 1 and 100 items")
	}
	taskTimeoutSeconds := 3600
	if input.TaskTimeoutSeconds != nil {
		taskTimeoutSeconds = *input.TaskTimeoutSeconds
	}
	if taskTimeoutSeconds <= 0 {
		return task.Definition{}, errors.New("task_timeout_seconds must be positive")
	}
	definition := task.Definition{TenantID: input.TenantID, TaskTimeoutSeconds: taskTimeoutSeconds}
	for index, step := range input.Steps {
		if len(step.Input) == 0 || len(step.Input) > maxDocumentBytes || !json.Valid(step.Input) {
			return task.Definition{}, fmt.Errorf("step %d: input must be valid JSON no larger than 1 MiB", index)
		}
		timeoutSeconds := 30
		if step.TimeoutSeconds != nil {
			timeoutSeconds = *step.TimeoutSeconds
		}
		if timeoutSeconds <= 0 {
			return task.Definition{}, fmt.Errorf("step %d: timeout_seconds must be positive", index)
		}
		maxAttempts := 3
		if step.MaxAttempts != nil {
			maxAttempts = *step.MaxAttempts
		}
		if maxAttempts <= 0 {
			return task.Definition{}, fmt.Errorf("step %d: max_attempts must be positive", index)
		}
		if step.Type != task.StepTypeApproval {
			if err := executor.ValidateDefinition(step.Type, step.Input); err != nil {
				return task.Definition{}, fmt.Errorf("step %d: %w", index, err)
			}
		}
		if err := task.ValidateInputReferences(step.Input, index > 0); err != nil {
			return task.Definition{}, fmt.Errorf("step %d: %w", index, err)
		}
		definition.Steps = append(definition.Steps, task.StepDefinition{
			Type: step.Type, Input: step.Input, TimeoutSeconds: timeoutSeconds, MaxAttempts: maxAttempts,
		})
	}
	return definition, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON document")
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}
