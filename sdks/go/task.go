package pgtask

import (
	"context"
	"encoding/json"
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

// EnqueueOn adds a task through a pool, connection, or transaction.
func (definition TaskDefinition[Payload, Result]) EnqueueOn(
	ctx context.Context,
	executor QueryRowExecutor,
	payload Payload,
	options EnqueueOptions,
) (EnqueueResult, error) {
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}
	if maxAttempts < 1 {
		return EnqueueResult{}, fmt.Errorf("max attempts must be positive")
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("encode payload: %w", err)
	}
	headers := make(map[string]any, len(options.Headers))
	for key, value := range options.Headers {
		headers[key] = value
	}
	injectTraceContext(ctx, headers)
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("encode headers: %w", err)
	}
	var id string
	var created bool
	err = executor.QueryRow(
		ctx,
		`SELECT task_id::text, created FROM pgtask.enqueue($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
		definition.name,
		payloadJSON,
		definition.queueName,
		definition.handlerVersion,
		options.RunAt,
		options.Priority,
		maxAttempts,
		options.IdempotencyKey,
		headersJSON,
	).Scan(&id, &created)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue task: %w", err)
	}
	return EnqueueResult{TaskID: id, Created: created}, nil
}

// QueryRowExecutor is implemented by pgx pools, connections, and transactions.
type QueryRowExecutor interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func validateName(kind, value string, maximum int) error {
	if value == "" || len(value) > maximum || !namePattern.MatchString(value) {
		return fmt.Errorf("invalid %s name", kind)
	}
	return nil
}
