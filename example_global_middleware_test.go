package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

type JobBothInsertAndWorkMiddleware struct{ river.MiddlewareDefaults }

func (JobBothInsertAndWorkMiddleware) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	fmt.Printf("JobBothInsertAndWorkMiddleware.InsertMany ran\n")
	return doInner(ctx)
}

func (JobBothInsertAndWorkMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	fmt.Printf("JobBothInsertAndWorkMiddleware.Work ran\n")
	return doInner(ctx)
}

type JobInsertMiddleware struct{ river.MiddlewareDefaults }

func (JobInsertMiddleware) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	fmt.Printf("JobInsertMiddleware.InsertMany ran\n")
	return doInner(ctx)
}

type WorkerMiddleware struct{ river.MiddlewareDefaults }

func (WorkerMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	fmt.Printf("WorkerMiddleware.Work ran\n")
	return doInner(ctx)
}

// Verify interface compliance. It's recommended that these are included in your
// test suite to make sure that your middlewares are complying to the specific
// interface middlewares that you expected them to be.
var (
	_ rivertype.JobInsertMiddleware = &JobBothInsertAndWorkMiddleware{}
	_ rivertype.WorkerMiddleware    = &JobBothInsertAndWorkMiddleware{}
	_ rivertype.JobInsertMiddleware = &JobInsertMiddleware{}
	_ rivertype.WorkerMiddleware    = &WorkerMiddleware{}
)

// Example_globalMiddleware demonstrates the use of middleware to modify River
// behavior which are global to a River client.
func Example_globalMiddleware() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &NoOpWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		// Order is significant. See output below.
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Middleware: []rivertype.Middleware{
			&JobBothInsertAndWorkMiddleware{},
			&JobInsertMiddleware{},
			&WorkerMiddleware{},
		},
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Schema:   riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil), // only necessary for the example test
		TestOnly: true,                                                                         // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, NoOpArgs{}, nil)
	if err != nil {
		panic(err)
	}

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// JobBothInsertAndWorkMiddleware.InsertMany ran
	// JobInsertMiddleware.InsertMany ran
	// JobBothInsertAndWorkMiddleware.Work ran
	// WorkerMiddleware.Work ran
	// NoOpWorker.Work ran
}
