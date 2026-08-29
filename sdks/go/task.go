package pgtask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// DefinitionOptions configures every request made from a task definition.
type DefinitionOptions struct {
	QueueName      string
	HandlerVersion int
}

// EnqueueOptions configures one task request.
type EnqueueOptions struct {
	RunAt          *time.Time
	Priority       int16
	MaxAttempts    int
	IdempotencyKey *string
	Headers        map[string]any
}

// EnqueueResult reports the durable task ID and whether this call created it.
type EnqueueResult struct {
	TaskID  string
	Created bool
}

// EnqueueRequest combines one typed payload with its per-task options.
type EnqueueRequest[Payload any] struct {
	Payload Payload
	Options EnqueueOptions
}

type enqueueRequestValue struct {
	TaskName       string          `json:"task_name"`
	Payload        json.RawMessage `json:"payload"`
	QueueName      string          `json:"queue_name"`
	HandlerVersion int             `json:"handler_version"`
	RunAt          *time.Time      `json:"run_at"`
	Priority       int16           `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key"`
	Headers        json.RawMessage `json:"headers"`
}

// TaskDefinition binds a payload and result type to a registered task name.
type TaskDefinition[Payload, Result any] struct {
	name           string
	queueName      string
	handlerVersion int
}

// DefineTask creates a typed task definition.
func DefineTask[Payload, Result any](name string, options DefinitionOptions) (TaskDefinition[Payload, Result], error) {
	queueName := options.QueueName
	if queueName == "" {
		queueName = "default"
	}
	handlerVersion := options.HandlerVersion
	if handlerVersion == 0 {
		handlerVersion = 1
	}
	if err := validateName("task", name, 255); err != nil {
		return TaskDefinition[Payload, Result]{}, err
	}
	if err := validateName("queue", queueName, 128); err != nil {
		return TaskDefinition[Payload, Result]{}, err
	}
	if handlerVersion < 1 {
		return TaskDefinition[Payload, Result]{}, fmt.Errorf("handler version must be positive")
	}
	return TaskDefinition[Payload, Result]{name: name, queueName: queueName, handlerVersion: handlerVersion}, nil
}

// Enqueue adds a task using the definition's typed payload.
func (definition TaskDefinition[Payload, Result]) Enqueue(
	ctx context.Context,
	client *Client,
	payload Payload,
	options EnqueueOptions,
) (TaskHandle[Result], error) {
	if err := client.ensureStorageProtocol(ctx); err != nil {
		return TaskHandle[Result]{}, err
	}
	result, err := definition.EnqueueOn(ctx, client.pool, payload, options)
	if err != nil {
		return TaskHandle[Result]{}, err
	}
	return TaskHandle[Result]{ID: result.TaskID, client: client}, nil
}

// EnqueueMany adds tasks in one PostgreSQL transaction.
func (definition TaskDefinition[Payload, Result]) EnqueueMany(
	ctx context.Context,
	client *Client,
	requests []EnqueueRequest[Payload],
) ([]TaskHandle[Result], error) {
	if err := client.ensureStorageProtocol(ctx); err != nil {
		return nil, err
	}
	results, err := definition.EnqueueManyOn(ctx, client.pool, requests)
	if err != nil {
		return nil, err
	}
	handles := make([]TaskHandle[Result], len(results))
	for index, result := range results {
		handles[index] = TaskHandle[Result]{ID: result.TaskID, client: client}
	}
	return handles, nil
}

// EnqueueOn adds a task through a pool, connection, or transaction.
func (definition TaskDefinition[Payload, Result]) EnqueueOn(
	ctx context.Context,
	executor QueryRowExecutor,
	payload Payload,
	options EnqueueOptions,
) (EnqueueResult, error) {
	request, err := definition.requestValue(ctx, payload, options)
	if err != nil {
		return EnqueueResult{}, err
	}
	var id string
	var created bool
	err = executor.QueryRow(
		ctx,
		`SELECT task_id::text, created FROM pgtask.enqueue($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
		request.TaskName,
		request.Payload,
		request.QueueName,
		request.HandlerVersion,
		request.RunAt,
		request.Priority,
		request.MaxAttempts,
		request.IdempotencyKey,
		request.Headers,
	).Scan(&id, &created)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue task: %w", err)
	}
	return EnqueueResult{TaskID: id, Created: created}, nil
}

// EnqueueManyOn adds tasks through a pool, connection, or transaction.
func (definition TaskDefinition[Payload, Result]) EnqueueManyOn(
	ctx context.Context,
	executor QueryExecutor,
	requests []EnqueueRequest[Payload],
) ([]EnqueueResult, error) {
	values := make([]enqueueRequestValue, len(requests))
	for index, request := range requests {
		value, err := definition.requestValue(ctx, request.Payload, request.Options)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("encode enqueue batch: %w", err)
	}
	rows, err := executor.Query(
		ctx,
		`SELECT request_index, task_id::text, created FROM pgtask.enqueue_many($1::jsonb) ORDER BY request_index`,
		encoded,
	)
	if err != nil {
		return nil, fmt.Errorf("enqueue tasks: %w", err)
	}
	defer rows.Close()
	results := make([]EnqueueResult, 0, len(requests))
	for rows.Next() {
		var requestIndex int
		var result EnqueueResult
		if err := rows.Scan(&requestIndex, &result.TaskID, &result.Created); err != nil {
			return nil, fmt.Errorf("read enqueue result: %w", err)
		}
		if requestIndex != len(results) {
			return nil, errors.New("pgtask.enqueue_many returned results out of order")
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read enqueue results: %w", err)
	}
	if len(results) != len(requests) {
		return nil, errors.New("pgtask.enqueue_many returned an incomplete result set")
	}
	return results, nil
}

func (definition TaskDefinition[Payload, Result]) requestValue(
	ctx context.Context,
	payload Payload,
	options EnqueueOptions,
) (enqueueRequestValue, error) {
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}
	if maxAttempts < 1 {
		return enqueueRequestValue{}, errors.New("max attempts must be positive")
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return enqueueRequestValue{}, fmt.Errorf("encode payload: %w", err)
	}
	headers := make(map[string]any, len(options.Headers))
	for key, value := range options.Headers {
		headers[key] = value
	}
	injectTraceContext(ctx, headers)
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return enqueueRequestValue{}, fmt.Errorf("encode headers: %w", err)
	}
	return enqueueRequestValue{
		TaskName:       definition.name,
		Payload:        payloadJSON,
		QueueName:      definition.queueName,
		HandlerVersion: definition.handlerVersion,
		RunAt:          options.RunAt,
		Priority:       options.Priority,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: options.IdempotencyKey,
		Headers:        headersJSON,
	}, nil
}

// QueryRowExecutor is implemented by pgx pools, connections, and transactions.
type QueryRowExecutor interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// QueryExecutor is implemented by pgx pools, connections, and transactions.
type QueryExecutor interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func validateName(kind, value string, maximum int) error {
	if value == "" || len(value) > maximum || !namePattern.MatchString(value) {
		return fmt.Errorf("invalid %s name", kind)
	}
	return nil
}
