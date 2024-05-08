// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.26.0
// source: river_leader.sql

package dbsqlc

import (
	"context"
	"time"
)

const leaderAttemptElect = `-- name: LeaderAttemptElect :execrows
INSERT INTO river_leader(leader_id, elected_at, expires_at)
    VALUES ($1::text, now(), now() + $2::interval)
ON CONFLICT (name)
    DO NOTHING
`

type LeaderAttemptElectParams struct {
	LeaderID string
	TTL      time.Duration
}

func (q *Queries) LeaderAttemptElect(ctx context.Context, db DBTX, arg *LeaderAttemptElectParams) (int64, error) {
	result, err := db.Exec(ctx, q.interpolateSchemaName(leaderAttemptElect), arg.LeaderID, arg.TTL)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const leaderAttemptReelect = `-- name: LeaderAttemptReelect :execrows
INSERT INTO river_leader(leader_id, elected_at, expires_at)
    VALUES ($1::text, now(), now() + $2::interval)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = now() + $2::interval
    WHERE
        river_leader.leader_id = $1::text
`

type LeaderAttemptReelectParams struct {
	LeaderID string
	TTL      time.Duration
}

func (q *Queries) LeaderAttemptReelect(ctx context.Context, db DBTX, arg *LeaderAttemptReelectParams) (int64, error) {
	result, err := db.Exec(ctx, q.interpolateSchemaName(leaderAttemptReelect), arg.LeaderID, arg.TTL)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const leaderDeleteExpired = `-- name: LeaderDeleteExpired :execrows
DELETE FROM river_leader
WHERE expires_at < now()
`

func (q *Queries) LeaderDeleteExpired(ctx context.Context, db DBTX) (int64, error) {
	result, err := db.Exec(ctx, q.interpolateSchemaName(leaderDeleteExpired))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const leaderGetElectedLeader = `-- name: LeaderGetElectedLeader :one
SELECT elected_at, expires_at, leader_id, name
FROM river_leader
`

func (q *Queries) LeaderGetElectedLeader(ctx context.Context, db DBTX) (*RiverLeader, error) {
	row := db.QueryRow(ctx, q.interpolateSchemaName(leaderGetElectedLeader))
	var i RiverLeader
	err := row.Scan(
		&i.ElectedAt,
		&i.ExpiresAt,
		&i.LeaderID,
		&i.Name,
	)
	return &i, err
}

const leaderInsert = `-- name: LeaderInsert :one
INSERT INTO river_leader(
    elected_at,
    expires_at,
    leader_id
) VALUES (
    coalesce($1::timestamptz, now()),
    coalesce($2::timestamptz, now() + $3::interval),
    $4
) RETURNING elected_at, expires_at, leader_id, name
`

type LeaderInsertParams struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	TTL       time.Duration
	LeaderID  string
}

func (q *Queries) LeaderInsert(ctx context.Context, db DBTX, arg *LeaderInsertParams) (*RiverLeader, error) {
	row := db.QueryRow(ctx, q.interpolateSchemaName(leaderInsert),
		arg.ElectedAt,
		arg.ExpiresAt,
		arg.TTL,
		arg.LeaderID,
	)
	var i RiverLeader
	err := row.Scan(
		&i.ElectedAt,
		&i.ExpiresAt,
		&i.LeaderID,
		&i.Name,
	)
	return &i, err
}

const leaderResign = `-- name: LeaderResign :execrows
WITH currently_held_leaders AS (
  SELECT elected_at, expires_at, leader_id, name
  FROM river_leader
  WHERE leader_id = $1::text
  FOR UPDATE
),
notified_resignations AS (
    SELECT pg_notify(
        concat(current_schema(), '.', $2::text),
        json_build_object('leader_id', leader_id, 'action', 'resigned')::text
    )
    FROM currently_held_leaders
)
DELETE FROM river_leader USING notified_resignations
`

type LeaderResignParams struct {
	LeaderID        string
	LeadershipTopic string
}

func (q *Queries) LeaderResign(ctx context.Context, db DBTX, arg *LeaderResignParams) (int64, error) {
	result, err := db.Exec(ctx, q.interpolateSchemaName(leaderResign), arg.LeaderID, arg.LeadershipTopic)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
