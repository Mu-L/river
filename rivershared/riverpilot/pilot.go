package riverpilot

import (
	"context"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

// A Pilot bridges the gap between the River client and the driver, implementing
// higher level functionality on top of the driver's underlying queries. It
// tracks closely to the underlying driver's API, but may add additional
// functionality or logic wrapping the queries.
//
// This should be considered a River internal API and its stability is not
// guaranteed. DO NOT USE.
type Pilot interface {
	PilotPeriodicJob

	JobGetAvailable(
		ctx context.Context,
		exec riverdriver.Executor,
		state ProducerState,
		params *riverdriver.JobGetAvailableParams,
	) ([]*rivertype.JobRow, error)

	JobInsertMany(
		ctx context.Context,
		exec riverdriver.Executor,
		params *riverdriver.JobInsertFastManyParams,
	) ([]*riverdriver.JobInsertFastResult, error)

	JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error)

	JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)

	PilotInit(archetype *baseservice.Archetype)

	// ProducerInit is called when a producer is started. It should return the ID
	// of the new producer, a new state object that will be used to track the
	// producer's state, and an error if the producer could not be initialized.
	ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error)

	ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error

	ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error

	QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error
}

// PilotPeriodicJob contains pilot functions related to periodic jobs. This is
// extracted as its own interface so there's less surface area to mock in places
// like the periodic job enqueuer where that's needed.
type PilotPeriodicJob interface {
	// PeriodicJobGetAll gets all currently known periodic jobs.
	//
	// API is not stable. DO NOT USE.
	PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error)

	// PeriodicJobTouchMany updates the `updated_at` timestamp on many jobs at
	// once to keep them alive and reaps any jobs that haven't been seen in some
	// time.
	//
	// API is not stable. DO NOT USE.
	PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error)

	// PeriodicJobUpsertMany upserts many periodic jobs.
	//
	// API is not stable. DO NOT USE.
	PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error)
}

type PeriodicJob struct {
	ID        string
	CreatedAt time.Time
	NextRunAt time.Time
	UpdatedAt time.Time
}

type PeriodicJobGetAllParams struct {
	Schema string
}

type PeriodicJobKeepAliveAndReapParams struct {
	ID     []string
	Schema string
}

type PeriodicJobUpsertManyParams struct {
	Jobs   []*PeriodicJobUpsertParams
	Schema string
}

type PeriodicJobUpsertParams struct {
	ID        string
	NextRunAt time.Time
	UpdatedAt time.Time
}

type ProducerState interface {
	JobFinish(job *rivertype.JobRow)
}

type ProducerInitParams struct {
	ClientID   string
	ProducerID int64
	Queue      string
	Schema     string
}

type ProducerShutdownParams struct {
	ProducerID int64
	Queue      string
	Schema     string
}

type QueueMetadataChangedParams struct {
	Queue    string
	Metadata []byte
}
