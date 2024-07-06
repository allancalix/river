// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.25.0
// source: river_job.sql

package dbsqlc

import (
	"context"
	"time"

	"github.com/lib/pq"
)

const jobCancel = `-- name: JobCancel :one
WITH locked_job AS (
    SELECT
        id, queue, state, finalized_at
    FROM river_job
    WHERE river_job.id = $1
    FOR UPDATE
),
notification AS (
    SELECT
        id,
        pg_notify(
            concat(current_schema(), '.', $2::text),
            json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text
        )
    FROM
        locked_job
    WHERE
        state NOT IN ('cancelled', 'completed', 'discarded')
        AND finalized_at IS NULL
),
updated_job AS (
    UPDATE river_job
    SET
        -- If the job is actively running, we want to let its current client and
        -- producer handle the cancellation. Otherwise, immediately cancel it.
        state = CASE WHEN state = 'running'::river_job_state THEN state ELSE 'cancelled'::river_job_state END,
        finalized_at = CASE WHEN state = 'running'::river_job_state THEN finalized_at ELSE now() END,
        -- Mark the job as cancelled by query so that the rescuer knows not to
        -- rescue it, even if it gets stuck in the running state:
        metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], $3::jsonb, true)
    FROM notification
    WHERE river_job.id = notification.id
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`

type JobCancelParams struct {
	ID                int64
	ControlTopic      string
	CancelAttemptedAt string
}

func (q *Queries) JobCancel(ctx context.Context, db DBTX, arg *JobCancelParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobCancel, arg.ID, arg.ControlTopic, arg.CancelAttemptedAt)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobCountByState = `-- name: JobCountByState :one
SELECT count(*)
FROM river_job
WHERE state = $1
`

func (q *Queries) JobCountByState(ctx context.Context, db DBTX, state RiverJobState) (int64, error) {
	row := db.QueryRowContext(ctx, jobCountByState, state)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const jobDelete = `-- name: JobDelete :one
WITH job_to_delete AS (
    SELECT id
    FROM river_job
    WHERE river_job.id = $1
    FOR UPDATE
),
deleted_job AS (
    DELETE
    FROM river_job
    USING job_to_delete
    WHERE river_job.id = job_to_delete.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'::river_job_state
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM deleted_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM deleted_job
`

func (q *Queries) JobDelete(ctx context.Context, db DBTX, id int64) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobDelete, id)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobDeleteBefore = `-- name: JobDeleteBefore :one
WITH deleted_jobs AS (
    DELETE FROM river_job
    WHERE id IN (
        SELECT id
        FROM river_job
        WHERE
            (state = 'cancelled' AND finalized_at < $1::timestamptz) OR
            (state = 'completed' AND finalized_at < $2::timestamptz) OR
            (state = 'discarded' AND finalized_at < $3::timestamptz)
        ORDER BY id
        LIMIT $4::bigint
    )
    RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
)
SELECT count(*)
FROM deleted_jobs
`

type JobDeleteBeforeParams struct {
	CancelledFinalizedAtHorizon time.Time
	CompletedFinalizedAtHorizon time.Time
	DiscardedFinalizedAtHorizon time.Time
	Max                         int64
}

func (q *Queries) JobDeleteBefore(ctx context.Context, db DBTX, arg *JobDeleteBeforeParams) (int64, error) {
	row := db.QueryRowContext(ctx, jobDeleteBefore,
		arg.CancelledFinalizedAtHorizon,
		arg.CompletedFinalizedAtHorizon,
		arg.DiscardedFinalizedAtHorizon,
		arg.Max,
	)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const jobGetAvailable = `-- name: JobGetAvailable :many
WITH locked_jobs AS (
    SELECT
        id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
    FROM
        river_job
    WHERE
        state = 'available'::river_job_state
        AND queue = $2::text
        AND scheduled_at <= now()
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT $3::integer
    FOR UPDATE
    SKIP LOCKED
)
UPDATE
    river_job
SET
    state = 'running'::river_job_state,
    attempt = river_job.attempt + 1,
    attempted_at = now(),
    attempted_by = array_append(river_job.attempted_by, $1::text)
FROM
    locked_jobs
WHERE
    river_job.id = locked_jobs.id
RETURNING
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
`

type JobGetAvailableParams struct {
	AttemptedBy string
	Queue       string
	Max         int32
}

func (q *Queries) JobGetAvailable(ctx context.Context, db DBTX, arg *JobGetAvailableParams) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobGetAvailable, arg.AttemptedBy, arg.Queue, arg.Max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetByID = `-- name: JobGetByID :one
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = $1
LIMIT 1
`

func (q *Queries) JobGetByID(ctx context.Context, db DBTX, id int64) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobGetByID, id)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobGetByIDMany = `-- name: JobGetByIDMany :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = any($1::bigint[])
ORDER BY id
`

func (q *Queries) JobGetByIDMany(ctx context.Context, db DBTX, id []int64) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobGetByIDMany, pq.Array(id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetByKindAndUniqueProperties = `-- name: JobGetByKindAndUniqueProperties :one
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE kind = $1
    AND CASE WHEN $2::boolean THEN args = $3 ELSE true END
    AND CASE WHEN $4::boolean THEN tstzrange($5::timestamptz, $6::timestamptz, '[)') @> created_at ELSE true END
    AND CASE WHEN $7::boolean THEN queue = $8 ELSE true END
    AND CASE WHEN $9::boolean THEN state::text = any($10::text[]) ELSE true END
`

type JobGetByKindAndUniquePropertiesParams struct {
	Kind           string
	ByArgs         bool
	Args           string
	ByCreatedAt    bool
	CreatedAtBegin time.Time
	CreatedAtEnd   time.Time
	ByQueue        bool
	Queue          string
	ByState        bool
	State          []string
}

func (q *Queries) JobGetByKindAndUniqueProperties(ctx context.Context, db DBTX, arg *JobGetByKindAndUniquePropertiesParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobGetByKindAndUniqueProperties,
		arg.Kind,
		arg.ByArgs,
		arg.Args,
		arg.ByCreatedAt,
		arg.CreatedAtBegin,
		arg.CreatedAtEnd,
		arg.ByQueue,
		arg.Queue,
		arg.ByState,
		pq.Array(arg.State),
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobGetByKindMany = `-- name: JobGetByKindMany :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE kind = any($1::text[])
ORDER BY id
`

func (q *Queries) JobGetByKindMany(ctx context.Context, db DBTX, kind []string) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobGetByKindMany, pq.Array(kind))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobGetStuck = `-- name: JobGetStuck :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE state = 'running'::river_job_state
    AND attempted_at < $1::timestamptz
ORDER BY id
LIMIT $2
`

type JobGetStuckParams struct {
	StuckHorizon time.Time
	Max          int32
}

func (q *Queries) JobGetStuck(ctx context.Context, db DBTX, arg *JobGetStuckParams) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobGetStuck, arg.StuckHorizon, arg.Max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobInsertFast = `-- name: JobInsertFast :one
INSERT INTO river_job(
    args,
    created_at,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) VALUES (
    $1::jsonb,
    coalesce($2::timestamptz, now()),
    $3,
    $4::text,
    $5::smallint,
    coalesce($6::jsonb, '{}'),
    $7::smallint,
    $8::text,
    coalesce($9::timestamptz, now()),
    $10::river_job_state,
    coalesce($11::varchar(255)[], '{}')
) RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobInsertFastParams struct {
	Args        string
	CreatedAt   *time.Time
	FinalizedAt *time.Time
	Kind        string
	MaxAttempts int16
	Metadata    string
	Priority    int16
	Queue       string
	ScheduledAt *time.Time
	State       RiverJobState
	Tags        []string
}

func (q *Queries) JobInsertFast(ctx context.Context, db DBTX, arg *JobInsertFastParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobInsertFast,
		arg.Args,
		arg.CreatedAt,
		arg.FinalizedAt,
		arg.Kind,
		arg.MaxAttempts,
		arg.Metadata,
		arg.Priority,
		arg.Queue,
		arg.ScheduledAt,
		arg.State,
		pq.Array(arg.Tags),
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobInsertFastMany = `-- name: JobInsertFastMany :execrows
INSERT INTO river_job(
    args,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) SELECT
    unnest($1::jsonb[]),
    unnest($2::text[]),
    unnest($3::smallint[]),
    unnest($4::jsonb[]),
    unnest($5::smallint[]),
    unnest($6::text[]),
    unnest($7::timestamptz[]),
    unnest($8::river_job_state[]),

    -- lib/pq really, REALLY does not play nicely with multi-dimensional arrays,
    -- so instead we pack each set of tags into a string, send them through,
    -- then unpack them here into an array to put in each row. This isn't
    -- necessary in the Pgx driver where copyfrom is used instead.
    string_to_array(unnest($9::text[]), ',')
`

type JobInsertFastManyParams struct {
	Args        []string
	Kind        []string
	MaxAttempts []int16
	Metadata    []string
	Priority    []int16
	Queue       []string
	ScheduledAt []time.Time
	State       []RiverJobState
	Tags        []string
}

func (q *Queries) JobInsertFastMany(ctx context.Context, db DBTX, arg *JobInsertFastManyParams) (int64, error) {
	result, err := db.ExecContext(ctx, jobInsertFastMany,
		pq.Array(arg.Args),
		pq.Array(arg.Kind),
		pq.Array(arg.MaxAttempts),
		pq.Array(arg.Metadata),
		pq.Array(arg.Priority),
		pq.Array(arg.Queue),
		pq.Array(arg.ScheduledAt),
		pq.Array(arg.State),
		pq.Array(arg.Tags),
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

const jobInsertFull = `-- name: JobInsertFull :one
INSERT INTO river_job(
    args,
    attempt,
    attempted_at,
    created_at,
    errors,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) VALUES (
    $1::jsonb,
    coalesce($2::smallint, 0),
    $3,
    coalesce($4::timestamptz, now()),
    $5::jsonb[],
    $6,
    $7::text,
    $8::smallint,
    coalesce($9::jsonb, '{}'),
    $10::smallint,
    $11::text,
    coalesce($12::timestamptz, now()),
    $13::river_job_state,
    coalesce($14::varchar(255)[], '{}')
) RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobInsertFullParams struct {
	Args        string
	Attempt     int16
	AttemptedAt *time.Time
	CreatedAt   *time.Time
	Errors      []string
	FinalizedAt *time.Time
	Kind        string
	MaxAttempts int16
	Metadata    string
	Priority    int16
	Queue       string
	ScheduledAt *time.Time
	State       RiverJobState
	Tags        []string
}

func (q *Queries) JobInsertFull(ctx context.Context, db DBTX, arg *JobInsertFullParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobInsertFull,
		arg.Args,
		arg.Attempt,
		arg.AttemptedAt,
		arg.CreatedAt,
		pq.Array(arg.Errors),
		arg.FinalizedAt,
		arg.Kind,
		arg.MaxAttempts,
		arg.Metadata,
		arg.Priority,
		arg.Queue,
		arg.ScheduledAt,
		arg.State,
		pq.Array(arg.Tags),
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobRescueMany = `-- name: JobRescueMany :exec
UPDATE river_job
SET
    errors = array_append(errors, updated_job.error),
    finalized_at = updated_job.finalized_at,
    scheduled_at = updated_job.scheduled_at,
    state = updated_job.state
FROM (
    SELECT
        unnest($1::bigint[]) AS id,
        unnest($2::jsonb[]) AS error,
        nullif(unnest($3::timestamptz[]), '0001-01-01 00:00:00 +0000') AS finalized_at,
        unnest($4::timestamptz[]) AS scheduled_at,
        unnest($5::text[])::river_job_state AS state
) AS updated_job
WHERE river_job.id = updated_job.id
`

type JobRescueManyParams struct {
	ID          []int64
	Error       []string
	FinalizedAt []time.Time
	ScheduledAt []time.Time
	State       []string
}

// Run by the rescuer to queue for retry or discard depending on job state.
func (q *Queries) JobRescueMany(ctx context.Context, db DBTX, arg *JobRescueManyParams) error {
	_, err := db.ExecContext(ctx, jobRescueMany,
		pq.Array(arg.ID),
		pq.Array(arg.Error),
		pq.Array(arg.FinalizedAt),
		pq.Array(arg.ScheduledAt),
		pq.Array(arg.State),
	)
	return err
}

const jobRetry = `-- name: JobRetry :one
WITH job_to_update AS (
    SELECT id
    FROM river_job
    WHERE river_job.id = $1
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        state = 'available'::river_job_state,
        scheduled_at = now(),
        max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
        finalized_at = NULL
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'::river_job_state
        -- If the job is already available with a prior scheduled_at, leave it alone.
        AND NOT (river_job.state = 'available'::river_job_state AND river_job.scheduled_at < now())
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`

func (q *Queries) JobRetry(ctx context.Context, db DBTX, id int64) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobRetry, id)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobSchedule = `-- name: JobSchedule :many
WITH jobs_to_schedule AS (
    SELECT id
    FROM river_job
    WHERE
        state IN ('retryable', 'scheduled')
        AND queue IS NOT NULL
        AND priority >= 0
        AND river_job.scheduled_at <= $1::timestamptz
    ORDER BY
        priority,
        scheduled_at,
        id
    LIMIT $2::bigint
    FOR UPDATE
),
river_job_scheduled AS (
    UPDATE river_job
    SET state = 'available'::river_job_state
    FROM jobs_to_schedule
    WHERE river_job.id = jobs_to_schedule.id
    RETURNING river_job.id
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id IN (SELECT id FROM river_job_scheduled)
`

type JobScheduleParams struct {
	Now time.Time
	Max int64
}

func (q *Queries) JobSchedule(ctx context.Context, db DBTX, arg *JobScheduleParams) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobSchedule, arg.Now, arg.Max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobSetCompleteIfRunningMany = `-- name: JobSetCompleteIfRunningMany :many
WITH job_to_finalized_at AS (
    SELECT
        unnest($1::bigint[]) AS id,
        unnest($2::timestamptz[]) AS finalized_at
),
job_to_update AS (
    SELECT river_job.id, job_to_finalized_at.finalized_at
    FROM river_job, job_to_finalized_at
    WHERE river_job.id = job_to_finalized_at.id
        AND river_job.state = 'running'::river_job_state
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        finalized_at = job_to_update.finalized_at,
        state = 'completed'
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id IN (SELECT id FROM job_to_finalized_at EXCEPT SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`

type JobSetCompleteIfRunningManyParams struct {
	ID          []int64
	FinalizedAt []time.Time
}

func (q *Queries) JobSetCompleteIfRunningMany(ctx context.Context, db DBTX, arg *JobSetCompleteIfRunningManyParams) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobSetCompleteIfRunningMany, pq.Array(arg.ID), pq.Array(arg.FinalizedAt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*RiverJob
	for rows.Next() {
		var i RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const jobSetStateIfRunning = `-- name: JobSetStateIfRunning :one
WITH job_to_update AS (
    SELECT
        id,
        $1::river_job_state IN ('retryable'::river_job_state, 'scheduled'::river_job_state) AND metadata ? 'cancel_attempted_at' AS should_cancel
    FROM river_job
    WHERE id = $2::bigint
    FOR UPDATE
),
updated_job AS (
    UPDATE river_job
    SET
        state        = CASE WHEN should_cancel                                          THEN 'cancelled'::river_job_state
                            ELSE $1::river_job_state END,
        finalized_at = CASE WHEN should_cancel                                          THEN now()
                            WHEN $3::boolean                       THEN $4
                            ELSE finalized_at END,
        errors       = CASE WHEN $5::boolean                              THEN array_append(errors, $6::jsonb)
                            ELSE errors       END,
        max_attempts = CASE WHEN NOT should_cancel AND $7::boolean    THEN $8
                            ELSE max_attempts END,
        scheduled_at = CASE WHEN NOT should_cancel AND $9::boolean THEN $10::timestamptz
                            ELSE scheduled_at END
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        AND river_job.state = 'running'::river_job_state
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = $2::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`

type JobSetStateIfRunningParams struct {
	State               RiverJobState
	ID                  int64
	FinalizedAtDoUpdate bool
	FinalizedAt         *time.Time
	ErrorDoUpdate       bool
	Error               string
	MaxAttemptsUpdate   bool
	MaxAttempts         int16
	ScheduledAtDoUpdate bool
	ScheduledAt         *time.Time
}

func (q *Queries) JobSetStateIfRunning(ctx context.Context, db DBTX, arg *JobSetStateIfRunningParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobSetStateIfRunning,
		arg.State,
		arg.ID,
		arg.FinalizedAtDoUpdate,
		arg.FinalizedAt,
		arg.ErrorDoUpdate,
		arg.Error,
		arg.MaxAttemptsUpdate,
		arg.MaxAttempts,
		arg.ScheduledAtDoUpdate,
		arg.ScheduledAt,
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}

const jobUpdate = `-- name: JobUpdate :one
UPDATE river_job
SET
    attempt = CASE WHEN $1::boolean THEN $2 ELSE attempt END,
    attempted_at = CASE WHEN $3::boolean THEN $4 ELSE attempted_at END,
    errors = CASE WHEN $5::boolean THEN $6::jsonb[] ELSE errors END,
    finalized_at = CASE WHEN $7::boolean THEN $8 ELSE finalized_at END,
    state = CASE WHEN $9::boolean THEN $10 ELSE state END
WHERE id = $11
RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`

type JobUpdateParams struct {
	AttemptDoUpdate     bool
	Attempt             int16
	AttemptedAtDoUpdate bool
	AttemptedAt         *time.Time
	ErrorsDoUpdate      bool
	Errors              []string
	FinalizedAtDoUpdate bool
	FinalizedAt         *time.Time
	StateDoUpdate       bool
	State               RiverJobState
	ID                  int64
}

// A generalized update for any property on a job. This brings in a large number
// of parameters and therefore may be more suitable for testing than production.
func (q *Queries) JobUpdate(ctx context.Context, db DBTX, arg *JobUpdateParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobUpdate,
		arg.AttemptDoUpdate,
		arg.Attempt,
		arg.AttemptedAtDoUpdate,
		arg.AttemptedAt,
		arg.ErrorsDoUpdate,
		pq.Array(arg.Errors),
		arg.FinalizedAtDoUpdate,
		arg.FinalizedAt,
		arg.StateDoUpdate,
		arg.State,
		arg.ID,
	)
	var i RiverJob
	err := row.Scan(
		&i.ID,
		&i.Args,
		&i.Attempt,
		&i.AttemptedAt,
		pq.Array(&i.AttemptedBy),
		&i.CreatedAt,
		pq.Array(&i.Errors),
		&i.FinalizedAt,
		&i.Kind,
		&i.MaxAttempts,
		&i.Metadata,
		&i.Priority,
		&i.Queue,
		&i.State,
		&i.ScheduledAt,
		pq.Array(&i.Tags),
	)
	return &i, err
}
