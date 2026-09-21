package faketool

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxPayloadBytes = 1 << 20
	maxBodyBytes    = maxPayloadBytes + 4096
)

type Handler struct{ pool *pgxpool.Pool }

type invocation struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

type payload struct {
	Value              json.RawMessage `json:"value"`
	BlockFirstResponse bool            `json:"block_first_response"`
}

type Effect struct {
	Key          string          `json:"key"`
	Payload      json.RawMessage `json:"payload"`
	Result       json.RawMessage `json:"result"`
	EffectCount  int             `json:"effect_count"`
	RequestCount int             `json:"request_count"`
}

func New(pool *pgxpool.Pool) http.Handler {
	handler := &Handler{pool: pool}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /invoke", handler.invoke)
	mux.HandleFunc("GET /effects/{key}", handler.effect)
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) })
	return mux
}

func (handler *Handler) invoke(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var call invocation
	if err := decoder.Decode(&call); err != nil || call.IdempotencyKey == "" || len(call.Payload) == 0 || !json.Valid(call.Payload) {
		http.Error(writer, "invalid invocation", http.StatusBadRequest)
		return
	}
	if len(call.Payload) > maxPayloadBytes {
		http.Error(writer, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	var input payload
	if err := json.Unmarshal(call.Payload, &input); err != nil || len(input.Value) == 0 {
		http.Error(writer, "payload requires value", http.StatusBadRequest)
		return
	}
	result, _ := json.Marshal(map[string]json.RawMessage{"value": input.Value})
	effect, created, err := handler.record(request.Context(), call.IdempotencyKey, call.Payload, result)
	if errors.Is(err, errPayloadConflict) {
		http.Error(writer, "idempotency key payload conflict", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(writer, "persist effect", http.StatusInternalServerError)
		return
	}
	if created && input.BlockFirstResponse {
		<-request.Context().Done()
		return
	}
	writeJSON(writer, http.StatusOK, effect.Result)
}

var errPayloadConflict = errors.New("idempotency payload conflict")

func (handler *Handler) record(ctx context.Context, key string, requestPayload, result json.RawMessage) (Effect, bool, error) {
	tx, err := handler.pool.Begin(ctx)
	if err != nil {
		return Effect{}, false, err
	}
	defer tx.Rollback(ctx)
	command, err := tx.Exec(ctx, `
		INSERT INTO fake_tool_effects (idempotency_key, payload, result)
		VALUES ($1, $2, $3) ON CONFLICT (idempotency_key) DO NOTHING`, key, requestPayload, result)
	if err != nil {
		return Effect{}, false, err
	}
	created := command.RowsAffected() == 1
	if !created {
		if _, err := tx.Exec(ctx, `UPDATE fake_tool_effects SET request_count = request_count + 1 WHERE idempotency_key = $1`, key); err != nil {
			return Effect{}, false, err
		}
	}
	var effect Effect
	err = tx.QueryRow(ctx, `SELECT idempotency_key, payload, result, effect_count, request_count FROM fake_tool_effects WHERE idempotency_key = $1 FOR UPDATE`, key).
		Scan(&effect.Key, &effect.Payload, &effect.Result, &effect.EffectCount, &effect.RequestCount)
	if err != nil {
		return Effect{}, false, err
	}
	var same bool
	if err := tx.QueryRow(ctx, `SELECT $1::jsonb = $2::jsonb`, effect.Payload, requestPayload).Scan(&same); err != nil {
		return Effect{}, false, err
	}
	if !same {
		return Effect{}, false, errPayloadConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return Effect{}, false, err
	}
	return effect, created, nil
}

func (handler *Handler) effect(writer http.ResponseWriter, request *http.Request) {
	key := strings.TrimSpace(request.PathValue("key"))
	var effect Effect
	err := handler.pool.QueryRow(request.Context(), `SELECT idempotency_key, payload, result, effect_count, request_count FROM fake_tool_effects WHERE idempotency_key = $1`, key).
		Scan(&effect.Key, &effect.Payload, &effect.Result, &effect.EffectCount, &effect.RequestCount)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(writer, "load effect", http.StatusInternalServerError)
		return
	}
	writeJSON(writer, http.StatusOK, effect)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
