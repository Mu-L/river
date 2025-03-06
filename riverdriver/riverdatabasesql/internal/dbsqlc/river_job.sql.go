// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0
// source: river_job.sql

package dbsqlc

import (
	"context"
	"time"

	"github.com/lib/pq"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql/internal/pgtypealias"
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
        state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
        finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE now() END,
        -- Mark the job as cancelled by query so that the rescuer knows not to
        -- rescue it, even if it gets stuck in the running state:
        metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], $3::jsonb, true)
    FROM notification
    WHERE river_job.id = notification.id
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
FROM river_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
		&i.UniqueKey,
		&i.UniqueStates,
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
        AND river_job.state != 'running'
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
FROM river_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM deleted_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
		&i.UniqueKey,
		&i.UniqueStates,
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
    RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
        id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
    FROM
        river_job
    WHERE
        state = 'available'
        AND queue = $2::text
        AND scheduled_at <= coalesce($3::timestamptz, now())
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT $4::integer
    FOR UPDATE
    SKIP LOCKED
)
UPDATE
    river_job
SET
    state = 'running',
    attempt = river_job.attempt + 1,
    attempted_at = now(),
    attempted_by = array_append(river_job.attempted_by, $1::text)
FROM
    locked_jobs
WHERE
    river_job.id = locked_jobs.id
RETURNING
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states
`

type JobGetAvailableParams struct {
	AttemptedBy string
	Queue       string
	Now         *time.Time
	Max         int32
}

func (q *Queries) JobGetAvailable(ctx context.Context, db DBTX, arg *JobGetAvailableParams) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobGetAvailable,
		arg.AttemptedBy,
		arg.Queue,
		arg.Now,
		arg.Max,
	)
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
			&i.UniqueKey,
			&i.UniqueStates,
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
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
		&i.UniqueKey,
		&i.UniqueStates,
	)
	return &i, err
}

const jobGetByIDMany = `-- name: JobGetByIDMany :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
			&i.UniqueKey,
			&i.UniqueStates,
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
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
		&i.UniqueKey,
		&i.UniqueStates,
	)
	return &i, err
}

const jobGetByKindMany = `-- name: JobGetByKindMany :many
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
			&i.UniqueKey,
			&i.UniqueStates,
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
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
FROM river_job
WHERE state = 'running'
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
			&i.UniqueKey,
			&i.UniqueStates,
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

const jobInsertFastMany = `-- name: JobInsertFastMany :many
INSERT INTO river_job(
    args,
    created_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) SELECT
    unnest($1::jsonb[]),
    unnest($2::timestamptz[]),
    unnest($3::text[]),
    unnest($4::smallint[]),
    unnest($5::jsonb[]),
    unnest($6::smallint[]),
    unnest($7::text[]),
    unnest($8::timestamptz[]),
    -- To avoid requiring pgx users to register the OID of the river_job_state[]
    -- type, we cast the array to text[] and then to river_job_state.
    unnest($9::text[])::river_job_state,
    -- Unnest on a multi-dimensional array will fully flatten the array, so we
    -- encode the tag list as a comma-separated string and split it in the
    -- query.
    string_to_array(unnest($10::text[]), ','),

    unnest($11::bytea[]),
    unnest($12::bit(8)[])

ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state)
    -- Something needs to be updated for a row to be returned on a conflict.
    DO UPDATE SET kind = EXCLUDED.kind
RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states, (xmax != 0) AS unique_skipped_as_duplicate
`

type JobInsertFastManyParams struct {
	Args         []string
	CreatedAt    []time.Time
	Kind         []string
	MaxAttempts  []int16
	Metadata     []string
	Priority     []int16
	Queue        []string
	ScheduledAt  []time.Time
	State        []string
	Tags         []string
	UniqueKey    []pgtypealias.NullBytea
	UniqueStates []pgtypealias.Bits
}

type JobInsertFastManyRow struct {
	RiverJob                 RiverJob
	UniqueSkippedAsDuplicate bool
}

func (q *Queries) JobInsertFastMany(ctx context.Context, db DBTX, arg *JobInsertFastManyParams) ([]*JobInsertFastManyRow, error) {
	rows, err := db.QueryContext(ctx, jobInsertFastMany,
		pq.Array(arg.Args),
		pq.Array(arg.CreatedAt),
		pq.Array(arg.Kind),
		pq.Array(arg.MaxAttempts),
		pq.Array(arg.Metadata),
		pq.Array(arg.Priority),
		pq.Array(arg.Queue),
		pq.Array(arg.ScheduledAt),
		pq.Array(arg.State),
		pq.Array(arg.Tags),
		pq.Array(arg.UniqueKey),
		pq.Array(arg.UniqueStates),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*JobInsertFastManyRow
	for rows.Next() {
		var i JobInsertFastManyRow
		if err := rows.Scan(
			&i.RiverJob.ID,
			&i.RiverJob.Args,
			&i.RiverJob.Attempt,
			&i.RiverJob.AttemptedAt,
			pq.Array(&i.RiverJob.AttemptedBy),
			&i.RiverJob.CreatedAt,
			pq.Array(&i.RiverJob.Errors),
			&i.RiverJob.FinalizedAt,
			&i.RiverJob.Kind,
			&i.RiverJob.MaxAttempts,
			&i.RiverJob.Metadata,
			&i.RiverJob.Priority,
			&i.RiverJob.Queue,
			&i.RiverJob.State,
			&i.RiverJob.ScheduledAt,
			pq.Array(&i.RiverJob.Tags),
			&i.RiverJob.UniqueKey,
			&i.RiverJob.UniqueStates,
			&i.UniqueSkippedAsDuplicate,
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

const jobInsertFastManyNoReturning = `-- name: JobInsertFastManyNoReturning :execrows
INSERT INTO river_job(
    args,
    created_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) SELECT
    unnest($1::jsonb[]),
    unnest($2::timestamptz[]),
    unnest($3::text[]),
    unnest($4::smallint[]),
    unnest($5::jsonb[]),
    unnest($6::smallint[]),
    unnest($7::text[]),
    unnest($8::timestamptz[]),
    unnest($9::river_job_state[]),

    -- lib/pq really, REALLY does not play nicely with multi-dimensional arrays,
    -- so instead we pack each set of tags into a string, send them through,
    -- then unpack them here into an array to put in each row. This isn't
    -- necessary in the Pgx driver where copyfrom is used instead.
    string_to_array(unnest($10::text[]), ','),

    unnest($11::bytea[]),
    unnest($12::bit(8)[])

ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state)
DO NOTHING
`

type JobInsertFastManyNoReturningParams struct {
	Args         []string
	CreatedAt    []time.Time
	Kind         []string
	MaxAttempts  []int16
	Metadata     []string
	Priority     []int16
	Queue        []string
	ScheduledAt  []time.Time
	State        []RiverJobState
	Tags         []string
	UniqueKey    []pgtypealias.NullBytea
	UniqueStates []pgtypealias.Bits
}

func (q *Queries) JobInsertFastManyNoReturning(ctx context.Context, db DBTX, arg *JobInsertFastManyNoReturningParams) (int64, error) {
	result, err := db.ExecContext(ctx, jobInsertFastManyNoReturning,
		pq.Array(arg.Args),
		pq.Array(arg.CreatedAt),
		pq.Array(arg.Kind),
		pq.Array(arg.MaxAttempts),
		pq.Array(arg.Metadata),
		pq.Array(arg.Priority),
		pq.Array(arg.Queue),
		pq.Array(arg.ScheduledAt),
		pq.Array(arg.State),
		pq.Array(arg.Tags),
		pq.Array(arg.UniqueKey),
		pq.Array(arg.UniqueStates),
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
    attempted_by,
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
    tags,
    unique_key,
    unique_states
) VALUES (
    $1::jsonb,
    coalesce($2::smallint, 0),
    $3,
    coalesce($4::text[], '{}'),
    coalesce($5::timestamptz, now()),
    $6,
    $7,
    $8,
    $9::smallint,
    coalesce($10::jsonb, '{}'),
    $11,
    $12,
    coalesce($13::timestamptz, now()),
    $14,
    coalesce($15::varchar(255)[], '{}'),
    $16,
    $17
) RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
`

type JobInsertFullParams struct {
	Args         string
	Attempt      int16
	AttemptedAt  *time.Time
	AttemptedBy  []string
	CreatedAt    *time.Time
	Errors       []string
	FinalizedAt  *time.Time
	Kind         string
	MaxAttempts  int16
	Metadata     string
	Priority     int16
	Queue        string
	ScheduledAt  *time.Time
	State        RiverJobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates pgtypealias.Bits
}

func (q *Queries) JobInsertFull(ctx context.Context, db DBTX, arg *JobInsertFullParams) (*RiverJob, error) {
	row := db.QueryRowContext(ctx, jobInsertFull,
		arg.Args,
		arg.Attempt,
		arg.AttemptedAt,
		pq.Array(arg.AttemptedBy),
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
		arg.UniqueKey,
		arg.UniqueStates,
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
		&i.UniqueKey,
		&i.UniqueStates,
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
        state = 'available',
        scheduled_at = now(),
        max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
        finalized_at = NULL
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'
        -- If the job is already available with a prior scheduled_at, leave it alone.
        AND NOT (river_job.state = 'available' AND river_job.scheduled_at < now())
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
FROM river_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
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
		&i.UniqueKey,
		&i.UniqueStates,
	)
	return &i, err
}

const jobSchedule = `-- name: JobSchedule :many
WITH jobs_to_schedule AS (
    SELECT
        id,
        unique_key,
        unique_states,
        priority,
        scheduled_at
    FROM river_job
    WHERE
        state IN ('retryable', 'scheduled')
        AND queue IS NOT NULL
        AND priority >= 0
        AND scheduled_at <= $1::timestamptz
    ORDER BY
        priority,
        scheduled_at,
        id
    LIMIT $2::bigint
    FOR UPDATE
),
jobs_with_rownum AS (
    SELECT
        id, unique_key, unique_states, priority, scheduled_at,
        CASE
            WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL THEN
                ROW_NUMBER() OVER (
                    PARTITION BY unique_key
                    ORDER BY priority, scheduled_at, id
                )
            ELSE NULL
        END AS row_num
    FROM jobs_to_schedule
),
unique_conflicts AS (
    SELECT river_job.unique_key
    FROM river_job
    JOIN jobs_with_rownum
        ON river_job.unique_key = jobs_with_rownum.unique_key
        AND river_job.id != jobs_with_rownum.id
    WHERE
        river_job.unique_key IS NOT NULL
        AND river_job.unique_states IS NOT NULL
        AND river_job_state_in_bitmask(river_job.unique_states, river_job.state)
),
job_updates AS (
    SELECT
        job.id,
        job.unique_key,
        job.unique_states,
        CASE
            WHEN job.row_num IS NULL THEN 'available'::river_job_state
            WHEN uc.unique_key IS NOT NULL THEN 'discarded'::river_job_state
            WHEN job.row_num = 1 THEN 'available'::river_job_state
            ELSE 'discarded'::river_job_state
        END AS new_state,
        (job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1)) AS finalized_at_do_update,
        (job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1)) AS metadata_do_update
    FROM jobs_with_rownum job
    LEFT JOIN unique_conflicts uc ON job.unique_key = uc.unique_key
),
updated_jobs AS (
    UPDATE river_job
    SET
        state        = job_updates.new_state,
        finalized_at = CASE WHEN job_updates.finalized_at_do_update THEN $1::timestamptz
                            ELSE river_job.finalized_at END,
        metadata     = CASE WHEN job_updates.metadata_do_update THEN river_job.metadata || '{"unique_key_conflict": "scheduler_discarded"}'::jsonb
                            ELSE river_job.metadata END
    FROM job_updates
    WHERE river_job.id = job_updates.id
    RETURNING
        river_job.id,
        job_updates.new_state = 'discarded'::river_job_state AS conflict_discarded
)
SELECT
    river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states,
    updated_jobs.conflict_discarded
FROM river_job
JOIN updated_jobs ON river_job.id = updated_jobs.id
`

type JobScheduleParams struct {
	Now time.Time
	Max int64
}

type JobScheduleRow struct {
	RiverJob          RiverJob
	ConflictDiscarded bool
}

func (q *Queries) JobSchedule(ctx context.Context, db DBTX, arg *JobScheduleParams) ([]*JobScheduleRow, error) {
	rows, err := db.QueryContext(ctx, jobSchedule, arg.Now, arg.Max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*JobScheduleRow
	for rows.Next() {
		var i JobScheduleRow
		if err := rows.Scan(
			&i.RiverJob.ID,
			&i.RiverJob.Args,
			&i.RiverJob.Attempt,
			&i.RiverJob.AttemptedAt,
			pq.Array(&i.RiverJob.AttemptedBy),
			&i.RiverJob.CreatedAt,
			pq.Array(&i.RiverJob.Errors),
			&i.RiverJob.FinalizedAt,
			&i.RiverJob.Kind,
			&i.RiverJob.MaxAttempts,
			&i.RiverJob.Metadata,
			&i.RiverJob.Priority,
			&i.RiverJob.Queue,
			&i.RiverJob.State,
			&i.RiverJob.ScheduledAt,
			pq.Array(&i.RiverJob.Tags),
			&i.RiverJob.UniqueKey,
			&i.RiverJob.UniqueStates,
			&i.ConflictDiscarded,
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

const jobSetStateIfRunningMany = `-- name: JobSetStateIfRunningMany :many
WITH job_input AS (
    SELECT
        unnest($1::bigint[]) AS id,
        unnest($2::boolean[]) AS attempt_do_update,
        unnest($3::int[]) AS attempt,
        unnest($4::boolean[]) AS errors_do_update,
        unnest($5::jsonb[]) AS errors,
        unnest($6::boolean[]) AS finalized_at_do_update,
        unnest($7::timestamptz[]) AS finalized_at,
        unnest($8::boolean[]) AS metadata_do_merge,
        unnest($9::jsonb[]) AS metadata_updates,
        unnest($10::boolean[]) AS scheduled_at_do_update,
        unnest($11::timestamptz[]) AS scheduled_at,
        -- To avoid requiring pgx users to register the OID of the river_job_state[]
        -- type, we cast the array to text[] and then to river_job_state.
        unnest($12::text[])::river_job_state AS state
),
job_to_update AS (
    SELECT
        river_job.id,
        job_input.attempt,
        job_input.attempt_do_update,
        job_input.errors,
        job_input.errors_do_update,
        job_input.finalized_at,
        job_input.finalized_at_do_update,
        job_input.metadata_do_merge,
        job_input.metadata_updates,
        job_input.scheduled_at,
        job_input.scheduled_at_do_update,
        (job_input.state IN ('retryable', 'scheduled') AND river_job.metadata ? 'cancel_attempted_at') AS should_cancel,
        job_input.state
    FROM river_job
    JOIN job_input ON river_job.id = job_input.id
    WHERE river_job.state = 'running' OR job_input.metadata_do_merge
    FOR UPDATE
),
updated_running AS (
    UPDATE river_job
    SET
        attempt      = CASE WHEN NOT job_to_update.should_cancel AND job_to_update.attempt_do_update THEN job_to_update.attempt
                            ELSE river_job.attempt END,
        errors       = CASE WHEN job_to_update.errors_do_update THEN array_append(river_job.errors, job_to_update.errors)
                            ELSE river_job.errors END,
        finalized_at = CASE WHEN job_to_update.should_cancel THEN now()
                            WHEN job_to_update.finalized_at_do_update THEN job_to_update.finalized_at
                            ELSE river_job.finalized_at END,
        metadata     = CASE WHEN job_to_update.metadata_do_merge
                            THEN river_job.metadata || job_to_update.metadata_updates
                            ELSE river_job.metadata END,
        scheduled_at = CASE WHEN NOT job_to_update.should_cancel AND job_to_update.scheduled_at_do_update THEN job_to_update.scheduled_at
                            ELSE river_job.scheduled_at END,
        state        = CASE WHEN job_to_update.should_cancel THEN 'cancelled'::river_job_state
                            ELSE job_to_update.state END
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
      AND river_job.state = 'running'
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states
),
updated_metadata_only AS (
    UPDATE river_job
    SET metadata = river_job.metadata || job_to_update.metadata_updates
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        AND river_job.id NOT IN (SELECT id FROM updated_running)
        AND river_job.state != 'running'
        AND job_to_update.metadata_do_merge
    RETURNING river_job.id, river_job.args, river_job.attempt, river_job.attempted_at, river_job.attempted_by, river_job.created_at, river_job.errors, river_job.finalized_at, river_job.kind, river_job.max_attempts, river_job.metadata, river_job.priority, river_job.queue, river_job.state, river_job.scheduled_at, river_job.tags, river_job.unique_key, river_job.unique_states
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
FROM river_job
WHERE id IN (SELECT id FROM job_input)
    AND id NOT IN (SELECT id FROM updated_metadata_only)
    AND id NOT IN (SELECT id FROM updated_running)
UNION SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states FROM updated_metadata_only
UNION SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states FROM updated_running
`

type JobSetStateIfRunningManyParams struct {
	IDs                 []int64
	AttemptDoUpdate     []bool
	Attempt             []int32
	ErrorsDoUpdate      []bool
	Errors              []string
	FinalizedAtDoUpdate []bool
	FinalizedAt         []time.Time
	MetadataDoMerge     []bool
	MetadataUpdates     []string
	ScheduledAtDoUpdate []bool
	ScheduledAt         []time.Time
	State               []string
}

func (q *Queries) JobSetStateIfRunningMany(ctx context.Context, db DBTX, arg *JobSetStateIfRunningManyParams) ([]*RiverJob, error) {
	rows, err := db.QueryContext(ctx, jobSetStateIfRunningMany,
		pq.Array(arg.IDs),
		pq.Array(arg.AttemptDoUpdate),
		pq.Array(arg.Attempt),
		pq.Array(arg.ErrorsDoUpdate),
		pq.Array(arg.Errors),
		pq.Array(arg.FinalizedAtDoUpdate),
		pq.Array(arg.FinalizedAt),
		pq.Array(arg.MetadataDoMerge),
		pq.Array(arg.MetadataUpdates),
		pq.Array(arg.ScheduledAtDoUpdate),
		pq.Array(arg.ScheduledAt),
		pq.Array(arg.State),
	)
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
			&i.UniqueKey,
			&i.UniqueStates,
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

const jobUpdate = `-- name: JobUpdate :one
UPDATE river_job
SET
    attempt = CASE WHEN $1::boolean THEN $2 ELSE attempt END,
    attempted_at = CASE WHEN $3::boolean THEN $4 ELSE attempted_at END,
    attempted_by = CASE WHEN $5::boolean THEN $6 ELSE attempted_by END,
    errors = CASE WHEN $7::boolean THEN $8::jsonb[] ELSE errors END,
    finalized_at = CASE WHEN $9::boolean THEN $10 ELSE finalized_at END,
    state = CASE WHEN $11::boolean THEN $12 ELSE state END
WHERE id = $13
RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states
`

type JobUpdateParams struct {
	AttemptDoUpdate     bool
	Attempt             int16
	AttemptedAtDoUpdate bool
	AttemptedAt         *time.Time
	AttemptedByDoUpdate bool
	AttemptedBy         []string
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
		arg.AttemptedByDoUpdate,
		pq.Array(arg.AttemptedBy),
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
		&i.UniqueKey,
		&i.UniqueStates,
	)
	return &i, err
}
