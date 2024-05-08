// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.26.0

package dbsqlc

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

func New(schema string) *Queries {
	return &Queries{schema: schema}
}

type Queries struct {
	schema string
}

func (q *Queries) interpolateSchemaName(query string) string {
	return strings.ReplaceAll(query, "sqlc_schema_placeholder.", q.schema+".")
}
