// Package testfactory provides low level helpers for inserting records directly
// into the database.
package testfactory

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

type JobOpts struct {
	Attempt      *int
	AttemptedAt  *time.Time
	AttemptedBy  []string
	CreatedAt    *time.Time
	EncodedArgs  []byte
	Errors       [][]byte
	FinalizedAt  *time.Time
	Kind         *string
	MaxAttempts  *int
	Metadata     []byte
	Priority     *int
	Queue        *string
	ScheduledAt  *time.Time
	Schema       string
	State        *rivertype.JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

func Job(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *JobOpts) *rivertype.JobRow {
	tb.Helper()

	job, err := exec.JobInsertFull(ctx, Job_Build(tb, opts))
	require.NoError(tb, err)

	return job
}

func Job_Build(tb testing.TB, opts *JobOpts) *riverdriver.JobInsertFullParams {
	tb.Helper()

	attemptedAt := opts.AttemptedAt
	if attemptedAt == nil && (opts.State != nil && (slices.Contains([]rivertype.JobState{
		rivertype.JobStateCompleted,
		rivertype.JobStateDiscarded,
		rivertype.JobStateRetryable,
		rivertype.JobStateRunning,
	}, *opts.State))) {
		attemptedAt = ptrutil.Ptr(time.Now())
	}

	encodedArgs := opts.EncodedArgs
	if opts.EncodedArgs == nil {
		encodedArgs = []byte("{}")
	}

	finalizedAt := opts.FinalizedAt
	if finalizedAt == nil && (opts.State != nil && (slices.Contains([]rivertype.JobState{
		rivertype.JobStateCompleted,
		rivertype.JobStateCancelled,
		rivertype.JobStateDiscarded,
	}, *opts.State))) {
		finalizedAt = ptrutil.Ptr(time.Now())
	}

	metadata := opts.Metadata
	if opts.Metadata == nil {
		metadata = []byte("{}")
	}

	tags := opts.Tags
	if tags == nil {
		tags = []string{}
	}

	return &riverdriver.JobInsertFullParams{
		Attempt:      ptrutil.ValOrDefault(opts.Attempt, 0),
		AttemptedAt:  attemptedAt,
		AttemptedBy:  opts.AttemptedBy,
		CreatedAt:    opts.CreatedAt,
		EncodedArgs:  encodedArgs,
		Errors:       opts.Errors,
		FinalizedAt:  finalizedAt,
		Kind:         ptrutil.ValOrDefault(opts.Kind, "fake_job"),
		MaxAttempts:  ptrutil.ValOrDefault(opts.MaxAttempts, rivercommon.MaxAttemptsDefault),
		Metadata:     metadata,
		Priority:     ptrutil.ValOrDefault(opts.Priority, rivercommon.PriorityDefault),
		Queue:        ptrutil.ValOrDefault(opts.Queue, rivercommon.QueueDefault),
		ScheduledAt:  opts.ScheduledAt,
		Schema:       opts.Schema,
		State:        ptrutil.ValOrDefault(opts.State, rivertype.JobStateAvailable),
		Tags:         tags,
		UniqueKey:    opts.UniqueKey,
		UniqueStates: opts.UniqueStates,
	}
}

type LeaderOpts struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	LeaderID  *string
	Now       *time.Time
	Schema    string
}

func Leader(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *LeaderOpts) *riverdriver.Leader {
	tb.Helper()

	leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
		ElectedAt: opts.ElectedAt,
		ExpiresAt: opts.ExpiresAt,
		LeaderID:  ptrutil.ValOrDefault(opts.LeaderID, "test-client-id"),
		Now:       opts.Now,
		Schema:    opts.Schema,
		TTL:       10 * time.Second,
	})
	require.NoError(tb, err)
	return leader
}

type MigrationOpts struct {
	Line    *string
	Schema  string
	Version *int
}

func Migration(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *MigrationOpts) *riverdriver.Migration {
	tb.Helper()

	migration, err := exec.MigrationInsertMany(ctx, &riverdriver.MigrationInsertManyParams{
		Line:     ptrutil.ValOrDefault(opts.Line, riverdriver.MigrationLineMain),
		Schema:   opts.Schema,
		Versions: []int{ptrutil.ValOrDefaultFunc(opts.Version, nextSeq)},
	})
	require.NoError(tb, err)
	return migration[0]
}

var seq int64 = 1 //nolint:gochecknoglobals

func nextSeq() int {
	return int(atomic.AddInt64(&seq, 1))
}

type QueueOpts struct {
	Metadata  []byte
	Name      *string
	PausedAt  *time.Time
	Schema    string
	UpdatedAt *time.Time
}

func Queue(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *QueueOpts) *rivertype.Queue {
	tb.Helper()

	if opts == nil {
		opts = &QueueOpts{}
	}

	metadata := opts.Metadata
	if len(opts.Metadata) == 0 {
		metadata = []byte("{}")
	}

	queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
		Metadata:  metadata,
		Name:      ptrutil.ValOrDefaultFunc(opts.Name, func() string { return fmt.Sprintf("queue_%05d", nextSeq()) }),
		PausedAt:  opts.PausedAt,
		Schema:    opts.Schema,
		UpdatedAt: opts.UpdatedAt,
	})
	require.NoError(tb, err)
	return queue
}
