package river

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/middlewarelookup"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/notifylimiter"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/valutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	FetchCooldownDefault = 100 * time.Millisecond
	FetchCooldownMin     = 1 * time.Millisecond

	FetchPollIntervalDefault = 1 * time.Second
	FetchPollIntervalMin     = 1 * time.Millisecond

	JobTimeoutDefault  = 1 * time.Minute
	MaxAttemptsDefault = rivercommon.MaxAttemptsDefault
	PriorityDefault    = rivercommon.PriorityDefault
	QueueDefault       = rivercommon.QueueDefault
	QueueNumWorkersMax = 10_000
)

var postgresSchemaNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// TestConfig contains configuration specific to test environments.
type TestConfig struct {
	// DisableUniqueEnforcement disables the application of unique job
	// constraints. This is useful for testing scenarios when testing a worker
	// that typically uses uniqueness, but where enforcing uniqueness would cause
	// conflicts with parallel test execution.
	//
	// The [rivertest.Worker] type automatically disables uniqueness enforcement
	// when creating jobs.
	DisableUniqueEnforcement bool

	// Time is a time generator to make time stubbable in tests.
	Time rivertype.TimeGenerator
}

// Config is the configuration for a Client.
//
// Both Queues and Workers are required for a client to work jobs, but an
// insert-only client can be initialized by omitting Queues, and not calling
// Start for the client. Workers can also be omitted, but it's better to include
// it so River can check that inserted job kinds have a worker that can run
// them.
type Config struct {
	// AdvisoryLockPrefix is a configurable 32-bit prefix that River will use
	// when generating any key to acquire a Postgres advisory lock. All advisory
	// locks share the same 64-bit number space, so this allows a calling
	// application to guarantee that a River advisory lock will never conflict
	// with one of its own by cordoning each type to its own prefix.
	//
	// If this value isn't set, River defaults to generating key hashes across
	// the entire 64-bit advisory lock number space, which is large enough that
	// conflicts are exceedingly unlikely. If callers don't strictly need this
	// option then it's recommended to leave it unset because the prefix leaves
	// only 32 bits of number space for advisory lock hashes, so it makes
	// internally conflicting River-generated keys more likely.
	//
	// Advisory locks are currently only used for the deprecated fallback/slow
	// path of unique job insertion when pending, scheduled, available, or running
	// are omitted from a customized ByState configuration.
	AdvisoryLockPrefix int32

	// CancelledJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of cancelled jobs.
	//
	// Defaults to 24 hours.
	CancelledJobRetentionPeriod time.Duration

	// CompletedJobRetentionPeriod is the amount of time to keep completed jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of completed jobs.
	//
	// Defaults to 24 hours.
	CompletedJobRetentionPeriod time.Duration

	// DiscardedJobRetentionPeriod is the amount of time to keep discarded jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of discarded jobs.
	//
	// Defaults to 7 days.
	DiscardedJobRetentionPeriod time.Duration

	// ErrorHandler can be configured to be invoked in case of an error or panic
	// occurring in a job. This is often useful for logging and exception
	// tracking, but can also be used to customize retry behavior.
	ErrorHandler ErrorHandler

	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then fetches may be delayed as long as
	// FetchPollInterval.
	//
	// Throughput is limited by this value.
	//
	// Individual QueueConfig structs may override this for a specific queue.
	//
	// Defaults to 100 ms.
	FetchCooldown time.Duration

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	//
	// Individual QueueConfig structs may override this for a specific queue.
	//
	// Defaults to 1 second.
	FetchPollInterval time.Duration

	// ID is the unique identifier for this client. If not set, a random
	// identifier will be generated.
	//
	// This is used to identify the client in job attempts and for leader election.
	// This value must be unique across all clients in the same database and
	// schema and there must not be more than one process running with the same
	// ID at the same time.
	//
	// A client ID should differ between different programs and must be unique
	// across all clients in the same database and schema. There must not be
	// more than one process running with the same ID at the same time.
	// Duplicate IDs between processes will lead to facilities like leader
	// election or client statistics to fail in novel ways. However, the client
	// ID is shared by all executors within any given client. (i.e.  different
	// Go processes have different IDs, but IDs are shared within any given
	// process.)
	//
	// If in doubt, leave this property empty.
	ID string

	// JobCleanerTimeout is the timeout of the individual queries within the job
	// cleaner.
	//
	// Defaults to 30 seconds, which should be more than enough time for most
	// deployments.
	JobCleanerTimeout time.Duration

	// JobInsertMiddleware are optional functions that can be called around job
	// insertion.
	//
	// Deprecated: Prefer the use of Middleware instead (which may contain
	// instances of rivertype.JobInsertMiddleware).
	JobInsertMiddleware []rivertype.JobInsertMiddleware

	// JobTimeout is the maximum amount of time a job is allowed to run before its
	// context is cancelled. A timeout of zero means JobTimeoutDefault will be
	// used, whereas a value of -1 means the job's context will not be cancelled
	// unless the Client is shutting down.
	//
	// Defaults to 1 minute.
	JobTimeout time.Duration

	// Hooks are functions that may activate at certain points during a job's
	// lifecycle (see rivertype.Hook), installed globally.
	//
	// The effect of hooks in this list will depend on the specific hook
	// interfaces they implement, so for example implementing
	// rivertype.HookInsertBegin will cause the hook to be invoked before a job
	// is inserted, or implementing rivertype.HookWorkBegin will cause it to be
	// invoked before a job is worked. Hook structs may implement multiple hook
	// interfaces.
	//
	// Order in this list is significant. A hook that appears first will be
	// entered before a hook that appears later. For any particular phase, order
	// is relevant only for hooks that will run for that phase. For example, if
	// two rivertype.HookInsertBegin are separated by a rivertype.HookWorkBegin,
	// during job insertion those two outer hooks will run one after another,
	// and the work hook between them will not run. When a job is worked, the
	// work hook runs and the insertion hooks on either side of it are skipped.
	//
	// Jobs may have their own specific hooks by implementing JobArgsWithHooks.
	Hooks []rivertype.Hook

	// Logger is the structured logger to use for logging purposes. If none is
	// specified, logs will be emitted to STDOUT with messages at warn level
	// or higher.
	Logger *slog.Logger

	// MaxAttempts is the default number of times a job will be retried before
	// being discarded. This value is applied to all jobs by default, and can be
	// overridden on individual job types on the JobArgs or on a per-job basis at
	// insertion time.
	//
	// If not specified, defaults to 25 (MaxAttemptsDefault).
	MaxAttempts int

	// Middleware contains middleware that may activate at certain points during
	// a job's lifecycle (see rivertype.Middleware), installed globally.
	//
	// The effect of middleware in this list will depend on the specific
	// middleware interfaces they implement, so for example implementing
	// rivertype.JobInsertMiddleware will cause the middleware to be invoked
	// when jobs are inserted, and implementing rivertype.WorkerMiddleware will
	// cause it to be invoked when a job is worked. Middleware structs may
	// implement multiple middleware interfaces.
	//
	// Order in this list is significant. Middleware that appears first will be
	// entered before middleware that appears later. For any particular phase,
	// order is relevant only for middlewares that will run for that phase. For
	// example, if two rivertype.JobInsertMiddleware are separated by a
	// rivertype.WorkerMiddleware, during job insertion those two outer
	// middlewares will run one after another, and the work middleware between
	// them will not run. When a job is worked, the work middleware runs and the
	// insertion middlewares on either side of it are skipped.
	Middleware []rivertype.Middleware

	// PeriodicJobs are a set of periodic jobs to run at the specified intervals
	// in the client.
	PeriodicJobs []*PeriodicJob

	// PollOnly starts the client in "poll only" mode, which avoids issuing
	// `LISTEN` statements to wait for events like a leadership resignation or
	// new job available. The program instead polls periodically to look for
	// changes (checking for new jobs on the period in FetchPollInterval).
	//
	// The downside of this mode of operation is that events will usually be
	// noticed less quickly. A new job in the queue may have to wait up to
	// FetchPollInterval to be locked for work. When a leader resigns, it will
	// be up to five seconds before a new one elects itself.
	//
	// The upside is that it makes River compatible with systems where
	// listen/notify isn't available. For example, PgBouncer in transaction
	// pooling mode.
	PollOnly bool

	// Queues is a list of queue names for this client to operate on along with
	// configuration for the queue like the maximum number of workers to run for
	// each queue.
	//
	// This field may be omitted for a program that's only queueing jobs rather
	// than working them. If it's specified, then Workers must also be given.
	Queues map[string]QueueConfig

	// ReindexerSchedule is the schedule for running the reindexer. If nil, the
	// reindexer will run at midnight UTC every day.
	ReindexerSchedule PeriodicSchedule

	// ReindexerTimeout is the amount of time to wait for the reindexer to run a
	// single reindex operation before cancelling it via context. Set to -1 to
	// disable the timeout.
	//
	// Defaults to 1 minute.
	ReindexerTimeout time.Duration

	// RescueStuckJobsAfter is the amount of time a job can be running before it
	// is considered stuck. A stuck job which has not yet reached its max attempts
	// will be scheduled for a retry, while one which has exhausted its attempts
	// will be discarded.  This prevents jobs from being stuck forever if a worker
	// crashes or is killed.
	//
	// Note that this can result in repeat or duplicate execution of a job that is
	// not actually stuck but is still working. The value should be set higher
	// than the maximum duration you expect your jobs to run. Setting a value too
	// low will result in more duplicate executions, whereas too high of a value
	// will result in jobs being stuck for longer than necessary before they are
	// retried.
	//
	// RescueStuckJobsAfter must be greater than JobTimeout. Otherwise, jobs
	// would become eligible for rescue while they're still running.
	//
	// Defaults to 1 hour, or in cases where JobTimeout has been configured and
	// is greater than 1 hour, JobTimeout + 1 hour.
	RescueStuckJobsAfter time.Duration

	// RetryPolicy is a configurable retry policy for the client.
	//
	// Defaults to DefaultRetryPolicy.
	RetryPolicy ClientRetryPolicy

	// Schema is a non-standard Schema where River tables are located. All table
	// references in database queries will use this value as a prefix.
	//
	// Defaults to empty, which causes the client to look for tables using the
	// setting of Postgres `search_path`.
	Schema string

	// SkipJobKindValidation causes the job kind format validation check to be
	// skipped. This is available as an interim stopgap for users that have
	// invalid job kind names, but would rather disable the check rather than
	// fix them immediately.
	//
	// Deprecated: This option will be removed in a future versions so that job
	// kinds will always have to have a valid format.
	SkipJobKindValidation bool

	// SkipUnknownJobCheck is a flag to control whether the client should skip
	// checking to see if a registered worker exists in the client's worker bundle
	// for a job arg prior to insertion.
	//
	// This can be set to true to allow a client to insert jobs which are
	// intended to be worked by a different client which effectively makes
	// the client's insertion behavior mimic that of an insert-only client.
	//
	// Defaults to false.
	SkipUnknownJobCheck bool

	// Test holds configuration specific to test environments.
	Test TestConfig

	// TestOnly can be set to true to disable certain features that are useful
	// in production, but which may be harmful to tests, in ways like having the
	// effect of making them slower. It should not be used outside of test
	// suites.
	//
	// For example, queue maintenance services normally stagger their startup
	// with a random jittered sleep so they don't all try to work at the same
	// time. This is nice in production, but makes starting and stopping the
	// client in a test case slower.
	TestOnly bool

	// Workers is a bundle of registered job workers.
	//
	// This field may be omitted for a program that's only enqueueing jobs
	// rather than working them, but if it is configured the client can validate
	// ahead of time that a worker is properly registered for an inserted job.
	// (i.e.  That it wasn't forgotten by accident.)
	Workers *Workers

	// WorkerMiddleware are optional functions that can be called around
	// all job executions.
	//
	// Deprecated: Prefer the use of Middleware instead (which may contain
	// instances of rivertype.WorkerMiddleware).
	WorkerMiddleware []rivertype.WorkerMiddleware

	// queuePollInterval is the amount of time between periodic checks for queue
	// setting changes. This is only used in poll-only mode (when no notifier is
	// provided).
	//
	// This is internal for the time being as it hasn't had any major demand to
	// be exposed, but it's needed to make sure that our poll-only tests can
	// finish in a timely manner.
	queuePollInterval time.Duration

	// Scheduler run interval. Shared between the scheduler and producer/job
	// executors, but not currently exposed for configuration.
	schedulerInterval time.Duration
}

// WithDefaults returns a copy of the Config with all default values applied.
func (c *Config) WithDefaults() *Config {
	if c == nil {
		c = &Config{}
	}

	// Use the existing logger if set, otherwise create a default one.
	logger := c.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	}

	// Compute the default rescue value. For convenience, if JobTimeout is specified
	// but RescueStuckJobsAfter is not set (or less than 1) and JobTimeout is large, use
	// JobTimeout + maintenance.JobRescuerRescueAfterDefault as the default.
	rescueAfter := maintenance.JobRescuerRescueAfterDefault
	if c.JobTimeout > 0 && c.RescueStuckJobsAfter < 1 && c.JobTimeout > c.RescueStuckJobsAfter {
		rescueAfter = c.JobTimeout + maintenance.JobRescuerRescueAfterDefault
	}

	// Set default retry policy if none is provided.
	retryPolicy := c.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = &DefaultClientRetryPolicy{}
	}

	return &Config{
		AdvisoryLockPrefix:          c.AdvisoryLockPrefix,
		CancelledJobRetentionPeriod: cmp.Or(c.CancelledJobRetentionPeriod, maintenance.CancelledJobRetentionPeriodDefault),
		CompletedJobRetentionPeriod: cmp.Or(c.CompletedJobRetentionPeriod, maintenance.CompletedJobRetentionPeriodDefault),
		DiscardedJobRetentionPeriod: cmp.Or(c.DiscardedJobRetentionPeriod, maintenance.DiscardedJobRetentionPeriodDefault),
		ErrorHandler:                c.ErrorHandler,
		FetchCooldown:               cmp.Or(c.FetchCooldown, FetchCooldownDefault),
		FetchPollInterval:           cmp.Or(c.FetchPollInterval, FetchPollIntervalDefault),
		ID:                          valutil.ValOrDefaultFunc(c.ID, func() string { return defaultClientID(time.Now().UTC()) }),
		Hooks:                       c.Hooks,
		JobInsertMiddleware:         c.JobInsertMiddleware,
		JobTimeout:                  cmp.Or(c.JobTimeout, JobTimeoutDefault),
		Logger:                      logger,
		MaxAttempts:                 cmp.Or(c.MaxAttempts, MaxAttemptsDefault),
		Middleware:                  c.Middleware,
		PeriodicJobs:                c.PeriodicJobs,
		PollOnly:                    c.PollOnly,
		Queues:                      c.Queues,
		ReindexerSchedule:           c.ReindexerSchedule,
		ReindexerTimeout:            cmp.Or(c.ReindexerTimeout, maintenance.ReindexerTimeoutDefault),
		RescueStuckJobsAfter:        cmp.Or(c.RescueStuckJobsAfter, rescueAfter),
		RetryPolicy:                 retryPolicy,
		Schema:                      c.Schema,
		SkipJobKindValidation:       c.SkipJobKindValidation,
		SkipUnknownJobCheck:         c.SkipUnknownJobCheck,
		Test:                        c.Test,
		TestOnly:                    c.TestOnly,
		WorkerMiddleware:            c.WorkerMiddleware,
		Workers:                     c.Workers,
		queuePollInterval:           c.queuePollInterval,
		schedulerInterval:           cmp.Or(c.schedulerInterval, maintenance.JobSchedulerIntervalDefault),
	}
}

func (c *Config) validate() error {
	if c.CancelledJobRetentionPeriod < -1 {
		return errors.New("CancelledJobRetentionPeriod time cannot be less than zero, except for -1 (infinite)")
	}
	if c.CompletedJobRetentionPeriod < -1 {
		return errors.New("CompletedJobRetentionPeriod cannot be less than zero, except for -1 (infinite)")
	}
	if c.DiscardedJobRetentionPeriod < -1 {
		return errors.New("DiscardedJobRetentionPeriod cannot be less than zero, except for -1 (infinite)")
	}
	if c.FetchCooldown < FetchCooldownMin {
		return fmt.Errorf("FetchCooldown must be at least %s", FetchCooldownMin)
	}
	if c.FetchPollInterval < FetchPollIntervalMin {
		return fmt.Errorf("FetchPollInterval must be at least %s", FetchPollIntervalMin)
	}
	if c.FetchPollInterval < c.FetchCooldown {
		return fmt.Errorf("FetchPollInterval cannot be shorter than FetchCooldown (%s)", c.FetchCooldown)
	}
	if len(c.ID) > 100 {
		return errors.New("ID cannot be longer than 100 characters")
	}
	if c.JobTimeout < -1 {
		return errors.New("JobTimeout cannot be negative, except for -1 (infinite)")
	}
	if c.MaxAttempts < 0 {
		return errors.New("MaxAttempts cannot be less than zero")
	}
	if len(c.Middleware) > 0 && (len(c.JobInsertMiddleware) > 0 || len(c.WorkerMiddleware) > 0) {
		return errors.New("only one of the pair JobInsertMiddleware/WorkerMiddleware or Middleware may be provided (Middleware is recommended, and may contain both job insert and worker middleware)")
	}
	if c.ReindexerTimeout < -1 {
		return errors.New("ReindexerTimeout cannot be negative, except for -1 (infinite)")
	}
	if c.RescueStuckJobsAfter < 0 {
		return errors.New("RescueStuckJobsAfter cannot be less than zero")
	}
	if c.RescueStuckJobsAfter < c.JobTimeout {
		return errors.New("RescueStuckJobsAfter cannot be less than JobTimeout")
	}

	// Max Postgres notification topic length is 63 and we prefix schema to
	// notification topic, so whatever schema the user specifies must fit inside
	// this convention.
	maxSchemaLength := 63 - 1 - len(string(notifier.NotificationTopicLongest)) // -1 for the dot in `<schema>.<topic>`
	if len(c.Schema) > maxSchemaLength {
		return fmt.Errorf("Schema length must be less than or equal to %d characters", maxSchemaLength)
	}
	if c.Schema != "" && !postgresSchemaNameRE.MatchString(c.Schema) {
		return errors.New("Schema name can only contain letters, numbers, and underscores, and must start with a letter or underscore")
	}

	for queue, queueConfig := range c.Queues {
		if err := queueConfig.validate(queue, c.FetchCooldown, c.FetchPollInterval); err != nil {
			return err
		}
	}

	if c.Workers == nil && c.Queues != nil {
		return errors.New("Workers must be set if Queues is set")
	}

	if c.Workers != nil {
		for _, workerInfo := range c.Workers.workersMap {
			kind := workerInfo.jobArgs.Kind()
			if !rivercommon.UserSpecifiedIDOrKindRE.MatchString(kind) {
				if c.SkipJobKindValidation {
					c.Logger.Warn("job kind should match regex; this will be an error in future versions", //nolint:noctx
						slog.String("kind", kind),
						slog.String("regex", rivercommon.UserSpecifiedIDOrKindRE.String()),
					)
				} else {
					return fmt.Errorf("job kind %q should match regex %s", kind, rivercommon.UserSpecifiedIDOrKindRE.String())
				}
			}
		}
	}

	return nil
}

// Indicates whether with the given configuration, this client will be expected
// to execute jobs (rather than just being used to enqueue them). Executing jobs
// requires a set of configured queues.
func (c *Config) willExecuteJobs() bool {
	return len(c.Queues) > 0
}

// QueueConfig contains queue-specific configuration.
type QueueConfig struct {
	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then fetches may be delayed as long as
	// FetchPollInterval.
	//
	// Throughput is limited by this value.
	//
	// If non-zero, this overrides the FetchCooldown setting in the Client's
	// Config.
	FetchCooldown time.Duration

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	//
	// If non-zero, this overrides the FetchCooldown setting in the Client's
	// Config.
	FetchPollInterval time.Duration

	// MaxWorkers is the maximum number of workers to run for the queue, or put
	// otherwise, the maximum parallelism to run.
	//
	// This is the maximum number of workers within this particular client
	// instance, but note that it doesn't control the total number of workers
	// across parallel processes. Installations will want to calculate their
	// total number by multiplying this number by the number of parallel nodes
	// running River clients configured to the same database and queue.
	//
	// Requires a minimum of 1, and a maximum of 10,000.
	MaxWorkers int
}

func (c QueueConfig) validate(queueName string, clientFetchCooldown time.Duration, clientFetchPollInterval time.Duration) error {
	if c.FetchCooldown < 0 {
		return fmt.Errorf("FetchCooldown cannot be less than zero")
	}
	if c.FetchPollInterval < 0 {
		return fmt.Errorf("FetchPollInterval cannot be less than zero")
	}

	resolvedFetchCooldown := cmp.Or(c.FetchCooldown, clientFetchCooldown)
	resolvedFetchPollInterval := cmp.Or(c.FetchPollInterval, clientFetchPollInterval)
	if resolvedFetchPollInterval < resolvedFetchCooldown {
		return fmt.Errorf("FetchPollInterval cannot be less than FetchCooldown")
	}

	if c.MaxWorkers < 1 || c.MaxWorkers > QueueNumWorkersMax {
		return fmt.Errorf("invalid number of workers for queue %q: %d", queueName, c.MaxWorkers)
	}
	if err := validateQueueName(queueName); err != nil {
		return err
	}

	return nil
}

// Client is a single isolated instance of River. Your application may use
// multiple instances operating on different databases or Postgres schemas
// within a single database.
type Client[TTx any] struct {
	// BaseService and BaseStartStop can't be embedded like on other services
	// because their properties would leak to the external API.
	baseService   baseservice.BaseService
	baseStartStop startstop.BaseStartStop

	completer              jobcompleter.JobCompleter
	config                 *Config
	driver                 riverdriver.Driver[TTx]
	elector                *leadership.Elector
	hookLookupByJob        *hooklookup.JobHookLookup
	hookLookupGlobal       hooklookup.HookLookupInterface
	insertNotifyLimiter    *notifylimiter.Limiter
	middlewareLookupGlobal middlewarelookup.MiddlewareLookupInterface
	notifier               *notifier.Notifier // may be nil in poll-only mode
	periodicJobs           *PeriodicJobBundle
	pilot                  riverpilot.Pilot
	producersByQueueName   map[string]*producer
	queueMaintainer        *maintenance.QueueMaintainer
	queues                 *QueueBundle
	services               []startstop.Service
	stopped                <-chan struct{}
	subscriptionManager    *subscriptionManager
	testSignals            clientTestSignals

	// workCancel cancels the context used for all work goroutines. Normal Stop
	// does not cancel that context.
	workCancel context.CancelCauseFunc
}

// Test-only signals.
type clientTestSignals struct {
	electedLeader testsignal.TestSignal[struct{}] // notifies when elected leader

	jobCleaner          *maintenance.JobCleanerTestSignals
	jobRescuer          *maintenance.JobRescuerTestSignals
	jobScheduler        *maintenance.JobSchedulerTestSignals
	periodicJobEnqueuer *maintenance.PeriodicJobEnqueuerTestSignals
	queueCleaner        *maintenance.QueueCleanerTestSignals
	reindexer           *maintenance.ReindexerTestSignals
}

func (ts *clientTestSignals) Init(tb testutil.TestingTB) {
	ts.electedLeader.Init(tb)

	if ts.jobCleaner != nil {
		ts.jobCleaner.Init(tb)
	}
	if ts.jobRescuer != nil {
		ts.jobRescuer.Init(tb)
	}
	if ts.jobScheduler != nil {
		ts.jobScheduler.Init(tb)
	}
	if ts.periodicJobEnqueuer != nil {
		ts.periodicJobEnqueuer.Init(tb)
	}
	if ts.queueCleaner != nil {
		ts.queueCleaner.Init(tb)
	}
	if ts.reindexer != nil {
		ts.reindexer.Init(tb)
	}
}

var (
	// ErrNotFound is returned when a query by ID does not match any existing
	// rows. For example, attempting to cancel a job that doesn't exist will
	// return this error.
	ErrNotFound = rivertype.ErrNotFound

	errMissingConfig                 = errors.New("missing config")
	errMissingDatabasePoolWithQueues = errors.New("must have a non-nil database pool to execute jobs (either use a driver with database pool or don't configure Queues)")
	errMissingDriver                 = errors.New("missing database driver (try wrapping a Pgx pool with river/riverdriver/riverpgxv5.New)")
)

// NewClient creates a new Client with the given database driver and
// configuration.
//
// Currently only one driver is supported, which is Pgx v5. See package
// riverpgxv5.
//
// The function takes a generic parameter TTx representing a transaction type,
// but it can be omitted because it'll generally always be inferred from the
// driver. For example:
//
//	import "github.com/riverqueue/river"
//	import "github.com/riverqueue/river/riverdriver/riverpgxv5"
//
//	...
//
//	dbPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//	if err != nil {
//		// handle error
//	}
//	defer dbPool.Close()
//
//	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
//		...
//	})
//	if err != nil {
//		// handle error
//	}
func NewClient[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Client[TTx], error) {
	if driver == nil {
		return nil, errMissingDriver
	}
	if config == nil {
		return nil, errMissingConfig
	}

	config = config.WithDefaults()

	if err := config.validate(); err != nil {
		return nil, err
	}

	archetype := baseservice.NewArchetype(config.Logger)
	if config.Test.Time != nil {
		if withStub, ok := config.Test.Time.(baseservice.TimeGeneratorWithStub); ok {
			archetype.Time = withStub
		} else {
			archetype.Time = &baseservice.TimeGeneratorWithStubWrapper{TimeGenerator: config.Test.Time}
		}
	}

	for _, hook := range config.Hooks {
		if withBaseService, ok := hook.(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}

	client := &Client[TTx]{
		config:               config,
		driver:               driver,
		hookLookupByJob:      hooklookup.NewJobHookLookup(),
		hookLookupGlobal:     hooklookup.NewHookLookup(config.Hooks),
		producersByQueueName: make(map[string]*producer),
		testSignals:          clientTestSignals{},
		workCancel:           func(cause error) {}, // replaced on start, but here in case StopAndCancel is called before start up
	}

	client.queues = &QueueBundle{
		addProducer:             client.addProducer,
		clientFetchCooldown:     config.FetchCooldown,
		clientFetchPollInterval: config.FetchPollInterval,
		clientWillExecuteJobs:   config.willExecuteJobs(),
	}

	baseservice.Init(archetype, &client.baseService)
	client.baseService.Name = "Client" // Have to correct the name because base service isn't embedded like it usually is
	client.insertNotifyLimiter = notifylimiter.NewLimiter(archetype, config.FetchCooldown)

	// Validation ensures that config.JobInsertMiddleware/WorkerMiddleware or
	// the more abstract config.Middleware for middleware are set, but not both,
	// so in practice we never append all three of these to each other.
	{
		middleware := config.Middleware
		for _, jobInsertMiddleware := range config.JobInsertMiddleware {
			middleware = append(middleware, jobInsertMiddleware)
		}
	outerLoop:
		for _, workerMiddleware := range config.WorkerMiddleware {
			// Don't add the middleware if it also implements JobInsertMiddleware
			// and the instance has been added to config.JobInsertMiddleware. This
			// is a hedge to make sure we don't accidentally double add middleware
			// as we've converted over to the unified config.Middleware setting.
			if workerMiddlewareAsJobInsertMiddleware, ok := workerMiddleware.(rivertype.JobInsertMiddleware); ok {
				for _, jobInsertMiddleware := range config.JobInsertMiddleware {
					if workerMiddlewareAsJobInsertMiddleware == jobInsertMiddleware {
						continue outerLoop
					}
				}
			}

			middleware = append(middleware, workerMiddleware)
		}

		for _, middleware := range middleware {
			if withBaseService, ok := middleware.(baseservice.WithBaseService); ok {
				baseservice.Init(archetype, withBaseService)
			}
		}

		client.middlewareLookupGlobal = middlewarelookup.NewMiddlewareLookup(middleware)
	}

	pluginDriver, _ := driver.(driverPlugin[TTx])
	if pluginDriver != nil {
		pluginDriver.PluginInit(archetype)
		client.pilot = pluginDriver.PluginPilot()
	}
	if client.pilot == nil {
		client.pilot = &riverpilot.StandardPilot{}
	}
	client.pilot.PilotInit(archetype)
	pluginPilot, _ := client.pilot.(pilotPlugin)

	if withBaseService, ok := config.RetryPolicy.(baseservice.WithBaseService); ok {
		baseservice.Init(archetype, withBaseService)
	}

	// There are a number of internal components that are only needed/desired if
	// we're actually going to be working jobs (as opposed to just enqueueing
	// them):
	if config.willExecuteJobs() {
		if !driver.PoolIsSet() {
			return nil, errMissingDatabasePoolWithQueues
		}

		client.completer = jobcompleter.NewBatchCompleter(archetype, config.Schema, driver.GetExecutor(), client.pilot, nil)
		client.subscriptionManager = newSubscriptionManager(archetype, nil)
		client.services = append(client.services, client.completer, client.subscriptionManager)

		if driver.SupportsListener() {
			// In poll only mode, we don't try to initialize a notifier that
			// uses listen/notify. Instead, each service polls for changes it's
			// interested in. e.g. Elector polls to see if leader has expired.
			if !config.PollOnly {
				client.notifier = notifier.New(archetype, driver.GetListener(&riverdriver.GetListenenerParams{Schema: config.Schema}))
				client.services = append(client.services, client.notifier)
			}
		} else {
			config.Logger.Info("Driver does not support listener; entering poll only mode") //nolint:noctx
		}

		client.elector = leadership.NewElector(archetype, driver.GetExecutor(), client.notifier, &leadership.Config{
			ClientID: config.ID,
			Schema:   config.Schema,
		})
		client.services = append(client.services, client.elector)

		for queue, queueConfig := range config.Queues {
			if _, err := client.addProducer(queue, queueConfig); err != nil {
				return nil, err
			}
		}

		client.services = append(client.services,
			startstop.StartStopFunc(client.logStatsLoop))

		client.services = append(client.services,
			startstop.StartStopFunc(client.handleLeadershipChangeLoop))

		if pluginPilot != nil {
			client.services = append(client.services, pluginPilot.PluginServices()...)
		}

		//
		// Maintenance services
		//

		maintenanceServices := []startstop.Service{}

		{
			jobCleaner := maintenance.NewJobCleaner(archetype, &maintenance.JobCleanerConfig{
				CancelledJobRetentionPeriod: config.CancelledJobRetentionPeriod,
				CompletedJobRetentionPeriod: config.CompletedJobRetentionPeriod,
				DiscardedJobRetentionPeriod: config.DiscardedJobRetentionPeriod,
				Schema:                      config.Schema,
				Timeout:                     config.JobCleanerTimeout,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobCleaner)
			client.testSignals.jobCleaner = &jobCleaner.TestSignals
		}

		{
			jobRescuer := maintenance.NewRescuer(archetype, &maintenance.JobRescuerConfig{
				ClientRetryPolicy: config.RetryPolicy,
				RescueAfter:       config.RescueStuckJobsAfter,
				Schema:            config.Schema,
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
					if workerInfo, ok := config.Workers.workersMap[kind]; ok {
						return workerInfo.workUnitFactory
					}
					return nil
				},
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobRescuer)
			client.testSignals.jobRescuer = &jobRescuer.TestSignals
		}

		{
			jobScheduler := maintenance.NewJobScheduler(archetype, &maintenance.JobSchedulerConfig{
				Interval:     config.schedulerInterval,
				NotifyInsert: client.maybeNotifyInsertForQueues,
				Schema:       config.Schema,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobScheduler)
			client.testSignals.jobScheduler = &jobScheduler.TestSignals
		}

		{
			periodicJobEnqueuer, err := maintenance.NewPeriodicJobEnqueuer(archetype, &maintenance.PeriodicJobEnqueuerConfig{
				AdvisoryLockPrefix: config.AdvisoryLockPrefix,
				Insert: func(ctx context.Context, execTx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) error {
					_, err := client.insertMany(ctx, execTx, insertParams)
					return err
				},
				Pilot:  client.pilot,
				Schema: config.Schema,
			}, driver.GetExecutor())
			if err != nil {
				return nil, err
			}
			maintenanceServices = append(maintenanceServices, periodicJobEnqueuer)
			client.testSignals.periodicJobEnqueuer = &periodicJobEnqueuer.TestSignals

			client.periodicJobs = newPeriodicJobBundle(client.config, periodicJobEnqueuer)
			client.periodicJobs.AddMany(config.PeriodicJobs)
		}

		{
			queueCleaner := maintenance.NewQueueCleaner(archetype, &maintenance.QueueCleanerConfig{
				RetentionPeriod: maintenance.QueueRetentionPeriodDefault,
				Schema:          config.Schema,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, queueCleaner)
			client.testSignals.queueCleaner = &queueCleaner.TestSignals
		}

		{
			var scheduleFunc func(time.Time) time.Time
			if config.ReindexerSchedule != nil {
				scheduleFunc = config.ReindexerSchedule.Next
			}

			reindexer := maintenance.NewReindexer(archetype, &maintenance.ReindexerConfig{
				ScheduleFunc: scheduleFunc,
				Schema:       config.Schema,
				Timeout:      config.ReindexerTimeout,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, reindexer)
			client.testSignals.reindexer = &reindexer.TestSignals
		}

		if pluginPilot != nil {
			maintenanceServices = append(maintenanceServices, pluginPilot.PluginMaintenanceServices()...)
		}

		// Not added to the main services list because the queue maintainer is
		// started conditionally based on whether the client is the leader.
		client.queueMaintainer = maintenance.NewQueueMaintainer(archetype, maintenanceServices)

		if config.TestOnly {
			client.queueMaintainer.StaggerStartupDisable(true)
		}
	}

	return client, nil
}

// Start starts the client's job fetching and working loops. Once this is called,
// the client will run in a background goroutine until stopped. All jobs are
// run with a context inheriting from the provided context, but with a timeout
// deadline applied based on the job's settings.
//
// A graceful shutdown stops fetching new jobs but allows any previously fetched
// jobs to complete. This can be initiated with the Stop method.
//
// A more abrupt shutdown can be achieved by either cancelling the provided
// context or by calling StopAndCancel. This will not only stop fetching new
// jobs, but will also cancel the context for any currently-running jobs. If
// using StopAndCancel, there's no need to also call Stop.
func (c *Client[TTx]) Start(ctx context.Context) error {
	fetchCtx, shouldStart, started, stopped := c.baseStartStop.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	c.queues.startStopMu.Lock()
	defer c.queues.startStopMu.Unlock()

	// BaseStartStop will set its stopped channel to nil after it stops, so make
	// sure to take a channel reference before finishing stopped.
	c.stopped = c.baseStartStop.StoppedUnsafe()

	producersAsServices := func() []startstop.Service {
		return sliceutil.Map(
			maputil.Values(c.producersByQueueName),
			func(p *producer) startstop.Service { return p },
		)
	}

	// Startup code. Wrapped in a closure so it doesn't have to remember to
	// close the stopped channel if returning with an error.
	if err := func() error {
		if !c.config.willExecuteJobs() {
			return errors.New("client Queues and Workers must be configured for a client to start working")
		}
		if c.config.Workers != nil && len(c.config.Workers.workersMap) < 1 {
			return errors.New("at least one Worker must be added to the Workers bundle")
		}

		// Before doing anything else, make an initial connection to the database to
		// verify that it appears healthy. Many of the subcomponents below start up
		// in a goroutine and in case of initial failure, only produce a log line,
		// so even in the case of a fundamental failure like the database not being
		// available, the client appears to have started even though it's completely
		// non-functional. Here we try to make an initial assessment of health and
		// return quickly in case of an apparent problem.
		if err := c.driver.GetExecutor().Exec(fetchCtx, "SELECT 1"); err != nil {
			return fmt.Errorf("error making initial connection to database: %w", err)
		}

		// Each time we start, we need a fresh completer subscribe channel to
		// send job completion events on, because the completer will close it
		// each time it shuts down.
		completerSubscribeCh := make(chan []jobcompleter.CompleterJobUpdated, 10)
		c.completer.ResetSubscribeChan(completerSubscribeCh)
		c.subscriptionManager.ResetSubscribeChan(completerSubscribeCh)

		// In case of error, stop any services that might have started. This
		// is safe because even services that were never started will still
		// tolerate being stopped.
		stopServicesOnError := func() {
			startstop.StopAllParallel(c.services...)
		}

		// The completer is part of the services list below, but although it can
		// stop gracefully along with all the other services, it needs to be
		// started with a context that's _not_ cancelled if the user-provided
		// context is cancelled.  This ensures that even when fetch is cancelled on
		// shutdown, the completer is still given a separate opportunity to start
		// stopping only after the producers have finished up and returned.
		if err := c.completer.Start(context.WithoutCancel(ctx)); err != nil {
			stopServicesOnError()
			return err
		}

		// We use separate contexts for fetching and working to allow for a graceful
		// stop. Both inherit from the provided context, so if it's cancelled, a
		// more aggressive stop will be initiated.
		workCtx, workCancel := context.WithCancelCause(withClient(ctx, c))

		if err := startstop.StartAll(fetchCtx, c.services...); err != nil {
			workCancel(err)
			stopServicesOnError()
			return err
		}

		for _, producer := range c.producersByQueueName {
			if err := producer.StartWorkContext(fetchCtx, workCtx); err != nil {
				workCancel(err)
				startstop.StopAllParallel(producersAsServices()...)
				stopServicesOnError()
				return err
			}
		}

		c.queues.fetchCtx = fetchCtx
		c.queues.workCtx = workCtx
		c.workCancel = workCancel

		return nil
	}(); err != nil {
		defer stopped()
		if errors.Is(context.Cause(fetchCtx), startstop.ErrStop) {
			return nil
		}
		return err
	}

	// Generate producer services while c.queues.startStopMu.Lock() is still
	// held. This is used for WaitAllStarted below, but don't use it elsewhere
	// because new producers may have been added while the client is running.
	producerServices := producersAsServices()

	go func() {
		// Wait for all subservices to start up before signaling our own start.
		// This isn't strictly needed, but gives tests a way to fully confirm
		// that all goroutines for subservices are spun up before continuing.
		//
		// Stop also cancels the "started" channel, so in case of a context
		// cancellation, this statement will fall through. The client will
		// briefly start, but then immediately stop again.
		startstop.WaitAllStarted(append(
			c.services,
			producerServices..., // see comment on this variable
		)...)

		started()
		defer stopped()

		c.baseService.Logger.InfoContext(ctx, "River client started", slog.String("client_id", c.ID()))
		defer c.baseService.Logger.InfoContext(ctx, "River client stopped", slog.String("client_id", c.ID()))

		// The call to Stop cancels this context. Block here until shutdown.
		<-fetchCtx.Done()

		c.queues.startStopMu.Lock()
		defer c.queues.startStopMu.Unlock()

		// On stop, have the producers stop fetching first of all.
		c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Stopping producers")
		startstop.StopAllParallel(producersAsServices()...)
		c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": All producers stopped")

		c.workCancel(rivercommon.ErrStop)

		// Stop all mainline services where stop order isn't important.
		startstop.StopAllParallel(append(
			// This list of services contains the completer, which should always
			// stop after the producers so that any remaining work that was enqueued
			// will have a chance to have its state completed as it finishes.
			//
			// TODO: there's a risk here that the completer is stuck on a job that
			// won't complete. We probably need a timeout or way to move on in those
			// cases.
			c.services,

			// Will only be started if this client was leader, but can tolerate a
			// stop without having been started.
			c.queueMaintainer,
		)...)
	}()

	return nil
}

// Stop performs a graceful shutdown of the Client. It signals all producers
// to stop fetching new jobs and waits for any fetched or in-progress jobs to
// complete before exiting. If the provided context is done before shutdown has
// completed, Stop will return immediately with the context's error.
//
// There's no need to call this method if a hard stop has already been initiated
// by cancelling the context passed to Start or by calling StopAndCancel.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	shouldStop, stopped, finalizeStop := c.baseStartStop.StopInit()
	if !shouldStop {
		return nil
	}

	select {
	case <-ctx.Done(): // stop context cancelled
		finalizeStop(false) // not stopped; allow Stop to be called again
		return ctx.Err()
	case <-stopped:
		finalizeStop(true)
		return nil
	}
}

// StopAndCancel shuts down the client and cancels all work in progress. It is a
// more aggressive stop than Stop because the contexts for any in-progress jobs
// are cancelled. However, it still waits for jobs to complete before returning,
// even though their contexts are cancelled. If the provided context is done
// before shutdown has completed, Stop will return immediately with the
// context's error.
//
// This can also be initiated by cancelling the context passed to Run. There is
// no need to call this method if the context passed to Run is cancelled
// instead.
func (c *Client[TTx]) StopAndCancel(ctx context.Context) error {
	c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Hard stop started; cancelling all work")
	c.workCancel(rivercommon.ErrStop)

	shouldStop, stopped, finalizeStop := c.baseStartStop.StopInit()
	if !shouldStop {
		return nil
	}

	select {
	case <-ctx.Done(): // stop context cancelled
		finalizeStop(false) // not stopped; allow Stop to be called again
		return ctx.Err()
	case <-stopped:
		finalizeStop(true)
		return nil
	}
}

// Stopped returns a channel that will be closed when the Client has stopped.
// It can be used to wait for a graceful shutdown to complete.
//
// It is not affected by any contexts passed to Stop or StopAndCancel.
func (c *Client[TTx]) Stopped() <-chan struct{} {
	return c.stopped
}

// Subscribe subscribes to the provided kinds of events that occur within the
// client, like EventKindJobCompleted for when a job completes.
//
// Returns a channel over which to receive events along with a cancel function
// that can be used to cancel and tear down resources associated with the
// subscription. It's recommended but not necessary to invoke the cancel
// function. Resources will be freed when the client stops in case it's not.
//
// The event channel is buffered and sends on it are non-blocking. Consumers
// must process events in a timely manner or it's possible for events to be
// dropped. Any slow operations performed in a response to a receipt (e.g.
// persisting to a database) should be made asynchronous to avoid event loss.
//
// Callers must specify the kinds of events they're interested in. This allows
// for forward compatibility in case new kinds of events are added in future
// versions. If new event kinds are added, callers will have to explicitly add
// them to their requested list and ensure they can be handled correctly.
func (c *Client[TTx]) Subscribe(kinds ...EventKind) (<-chan *Event, func()) {
	return c.SubscribeConfig(&SubscribeConfig{Kinds: kinds})
}

// The default maximum size of the subscribe channel. Events that would overflow
// it will be dropped.
const subscribeChanSizeDefault = 1_000

// SubscribeConfig is more thorough subscription configuration used for
// Client.SubscribeConfig.
type SubscribeConfig struct {
	// ChanSize is the size of the buffered channel that will be created for the
	// subscription. Incoming events that overall this number because a listener
	// isn't reading from the channel in a timely manner will be dropped.
	//
	// Defaults to 1000.
	ChanSize int

	// Kinds are the kinds of events that the subscription will receive.
	// Requiring that kinds are specified explicitly allows for forward
	// compatibility in case new kinds of events are added in future versions.
	// If new event kinds are added, callers will have to explicitly add them to
	// their requested list and ensure they can be handled correctly.
	Kinds []EventKind
}

// Special internal variant that lets us inject an overridden size.
func (c *Client[TTx]) SubscribeConfig(config *SubscribeConfig) (<-chan *Event, func()) {
	if c.subscriptionManager == nil {
		panic("created a subscription on a client that will never work jobs (Queues not configured)")
	}

	return c.subscriptionManager.SubscribeConfig(config)
}

// Dump aggregate stats from job completions to logs periodically.  These
// numbers don't mean much in themselves, but can give a rough idea of the
// proportions of each compared to each other, and may help flag outlying values
// indicative of a problem.
func (c *Client[TTx]) logStatsLoop(ctx context.Context, shouldStart bool, started, stopped func()) error {
	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				c.subscriptionManager.logStats(ctx, c.baseService.Name)
			}
		}
	}()

	return nil
}

func (c *Client[TTx]) handleLeadershipChangeLoop(ctx context.Context, shouldStart bool, started, stopped func()) error {
	handleLeadershipChange := func(ctx context.Context, notification *leadership.Notification) {
		c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Election change received",
			slog.String("client_id", c.config.ID), slog.Bool("is_leader", notification.IsLeader))

		switch {
		case notification.IsLeader:
			// Starting the queue maintainer can take a little time so send to
			// this test signal _first_ so tests waiting on it can finish,
			// cancel the queue maintainer start, and overall run much faster.
			c.testSignals.electedLeader.Signal(struct{}{})

			if err := c.queueMaintainer.Start(ctx); err != nil {
				c.baseService.Logger.ErrorContext(ctx, "Error starting queue maintainer", slog.String("err", err.Error()))
			}

		default:
			c.queueMaintainer.Stop()
		}
	}

	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		sub := c.elector.Listen()
		defer sub.Unlisten()

		for {
			select {
			case <-ctx.Done():
				return

			case notification := <-sub.C():
				handleLeadershipChange(ctx, notification)
			}
		}
	}()

	return nil
}

// Driver exposes the underlying driver used by the client.
//
// API is not stable. DO NOT USE.
func (c *Client[TTx]) Driver() riverdriver.Driver[TTx] {
	return c.driver
}

// JobCancel cancels the job with the given ID. If possible, the job is
// cancelled immediately and will not be retried. The provided context is used
// for the underlying Postgres update and can be used to cancel the operation or
// apply a timeout.
//
// If the job is still in the queue (available, scheduled, or retryable), it is
// immediately marked as cancelled and will not be retried.
//
// If the job is already finalized (cancelled, completed, or discarded), no
// changes are made.
//
// If the job is currently running, it is not immediately cancelled, but is
// instead marked for cancellation. The client running the job will also be
// notified (via LISTEN/NOTIFY) to cancel the running job's context. Although
// the job's context will be cancelled, since Go does not provide a mechanism to
// interrupt a running goroutine the job will continue running until it returns.
// As always, it is important for workers to respect context cancellation and
// return promptly when the job context is done.
//
// Once the cancellation signal is received by the client running the job, any
// error returned by that job will result in it being cancelled permanently and
// not retried. However if the job returns no error, it will be completed as
// usual.
//
// In the event the running job finishes executing _before_ the cancellation
// signal is received but _after_ this update was made, the behavior depends on
// which state the job is being transitioned into (based on its return error):
//
//   - If the job completed successfully, was cancelled from within, or was
//     discarded due to exceeding its max attempts, the job will be updated as
//     usual.
//   - If the job was snoozed to run again later or encountered a retryable error,
//     the job will be marked as cancelled and will not be attempted again.
//
// Returns the up-to-date JobRow for the specified jobID if it exists. Returns
// ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobCancel(ctx context.Context, jobID int64) (*rivertype.JobRow, error) {
	job, err := c.jobCancel(ctx, c.driver.GetExecutor(), jobID)
	if err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(job.Queue, &controlEventPayload{
		Action: controlActionCancel,
		JobID:  job.ID,
		Queue:  job.Queue,
	})

	return job, nil
}

// JobCancelTx cancels the job with the given ID within the specified
// transaction. This variant lets a caller cancel a job atomically alongside
// other database changes. A cancelled job doesn't take effect until the
// transaction commits, and if the transaction rolls back, so too is the
// cancelled job.
//
// If possible, the job is cancelled immediately and will not be retried. The
// provided context is used for the underlying Postgres update and can be used
// to cancel the operation or apply a timeout.
//
// If the job is still in the queue (available, scheduled, or retryable), it is
// immediately marked as cancelled and will not be retried.
//
// If the job is already finalized (cancelled, completed, or discarded), no
// changes are made.
//
// If the job is currently running, it is not immediately cancelled, but is
// instead marked for cancellation. The client running the job will also be
// notified (via LISTEN/NOTIFY) to cancel the running job's context. Although
// the job's context will be cancelled, since Go does not provide a mechanism to
// interrupt a running goroutine the job will continue running until it returns.
// As always, it is important for workers to respect context cancellation and
// return promptly when the job context is done.
//
// Once the cancellation signal is received by the client running the job, any
// error returned by that job will result in it being cancelled permanently and
// not retried. However if the job returns no error, it will be completed as
// usual.
//
// In the event the running job finishes executing _before_ the cancellation
// signal is received but _after_ this update was made, the behavior depends on
// which state the job is being transitioned into (based on its return error):
//
//   - If the job completed successfully, was cancelled from within, or was
//     discarded due to exceeding its max attempts, the job will be updated as
//     usual.
//   - If the job was snoozed to run again later or encountered a retryable error,
//     the job will be marked as cancelled and will not be attempted again.
//
// Returns the up-to-date JobRow for the specified jobID if it exists. Returns
// ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobCancelTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobRow, error) {
	return c.jobCancel(ctx, c.driver.UnwrapExecutor(tx), jobID)
}

func (c *Client[TTx]) jobCancel(ctx context.Context, exec riverdriver.Executor, jobID int64) (*rivertype.JobRow, error) {
	return exec.JobCancel(ctx, &riverdriver.JobCancelParams{
		ID:                jobID,
		CancelAttemptedAt: c.baseService.Time.NowUTC(),
		ControlTopic:      string(notifier.NotificationTopicControl),
		Now:               c.baseService.Time.NowUTCOrNil(),
		Schema:            c.config.Schema,
	})
}

// JobDelete deletes the job with the given ID from the database, returning the
// deleted row if it was deleted. Jobs in the running state are not deleted,
// instead returning rivertype.ErrJobRunning.
func (c *Client[TTx]) JobDelete(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.driver.GetExecutor().JobDelete(ctx, &riverdriver.JobDeleteParams{
		ID:     id,
		Schema: c.config.Schema,
	})
}

// JobDelete deletes the job with the given ID from the database, returning the
// deleted row if it was deleted. Jobs in the running state are not deleted,
// instead returning rivertype.ErrJobRunning. This variant lets a caller retry a
// job atomically alongside other database changes. A deleted job isn't deleted
// until the transaction commits, and if the transaction rolls back, so too is
// the deleted job.
func (c *Client[TTx]) JobDeleteTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.driver.UnwrapExecutor(tx).JobDelete(ctx, &riverdriver.JobDeleteParams{
		ID:     id,
		Schema: c.config.Schema,
	})
}

// JobGet fetches a single job by its ID. Returns the up-to-date JobRow for the
// specified jobID if it exists. Returns ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobGet(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.driver.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{
		ID:     id,
		Schema: c.config.Schema,
	})
}

// JobGetTx fetches a single job by its ID, within a transaction. Returns the
// up-to-date JobRow for the specified jobID if it exists. Returns ErrNotFound
// if the job doesn't exist.
func (c *Client[TTx]) JobGetTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.driver.UnwrapExecutor(tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{
		ID:     id,
		Schema: c.config.Schema,
	})
}

// JobRetry updates the job with the given ID to make it immediately available
// to be retried. Jobs in the running state are not touched, while jobs in any
// other state are made available. To prevent jobs already waiting in the queue
// from being set back in line, the job's scheduled_at field is set to the
// current time only if it's not already in the past.
//
// MaxAttempts is also incremented by one if the job has already exhausted its
// max attempts.
func (c *Client[TTx]) JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.jobRetry(ctx, c.driver.GetExecutor(), id)
}

// JobRetryTx updates the job with the given ID to make it immediately available
// to be retried, within the specified transaction. This variant lets a caller
// retry a job atomically alongside other database changes. A retried job isn't
// visible to be worked until the transaction commits, and if the transaction
// rolls back, so too is the retried job.
//
// Jobs in the running state are not touched, while jobs in any other state are
// made available. To prevent jobs already waiting in the queue from being set
// back in line, the job's scheduled_at field is set to the current time only if
// it's not already in the past.
//
// MaxAttempts is also incremented by one if the job has already exhausted its
// max attempts.
func (c *Client[TTx]) JobRetryTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.jobRetry(ctx, c.driver.UnwrapExecutor(tx), id)
}

func (c *Client[TTx]) jobRetry(ctx context.Context, exec riverdriver.Executor, id int64) (*rivertype.JobRow, error) {
	return c.pilot.JobRetry(ctx, exec, &riverdriver.JobRetryParams{
		ID:     id,
		Now:    c.baseService.Time.NowUTCOrNil(),
		Schema: c.config.Schema,
	})
}

// ID returns the unique ID of this client as set in its config or
// auto-generated if not specified.
func (c *Client[TTx]) ID() string {
	return c.config.ID
}

func insertParamsFromConfigArgsAndOptions(archetype *baseservice.Archetype, config *Config, args JobArgs, insertOpts *InsertOpts) (*rivertype.JobInsertParams, error) {
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("error marshaling args to JSON: %w", err)
	}

	if insertOpts == nil {
		insertOpts = &InsertOpts{}
	}

	var jobInsertOpts InsertOpts
	if argsWithOpts, ok := args.(JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}

	// If the time is stubbed (in a test), use that for `created_at`. Otherwise,
	// leave an empty value which will either use the database's `now()` or be defaulted
	// by drivers as necessary.
	createdAt := archetype.Time.NowUTCOrNil()

	maxAttempts := cmp.Or(insertOpts.MaxAttempts, jobInsertOpts.MaxAttempts, config.MaxAttempts)
	priority := cmp.Or(insertOpts.Priority, jobInsertOpts.Priority, rivercommon.PriorityDefault)
	queue := cmp.Or(insertOpts.Queue, jobInsertOpts.Queue, rivercommon.QueueDefault)

	if err := validateQueueName(queue); err != nil {
		return nil, err
	}

	tags := insertOpts.Tags
	if insertOpts.Tags == nil {
		tags = jobInsertOpts.Tags
	}
	if tags == nil {
		tags = []string{}
	} else {
		for _, tag := range tags {
			if len(tag) > 255 {
				return nil, errors.New("tags should be a maximum of 255 characters long")
			}
			if !tagRE.MatchString(tag) {
				return nil, errors.New("tags should match regex " + tagRE.String())
			}
		}
	}

	if priority > 4 {
		return nil, errors.New("priority must be between 1 and 4")
	}

	var uniqueOpts UniqueOpts
	if !config.Test.DisableUniqueEnforcement {
		uniqueOpts = insertOpts.UniqueOpts
		if uniqueOpts.isEmpty() {
			uniqueOpts = jobInsertOpts.UniqueOpts
		}
	}
	if err := uniqueOpts.validate(); err != nil {
		return nil, err
	}

	metadata := insertOpts.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	insertParams := &rivertype.JobInsertParams{
		Args:        args,
		CreatedAt:   createdAt,
		EncodedArgs: encodedArgs,
		Kind:        args.Kind(),
		MaxAttempts: maxAttempts,
		Metadata:    metadata,
		Priority:    priority,
		Queue:       queue,
		State:       rivertype.JobStateAvailable,
		Tags:        tags,
	}
	if !uniqueOpts.isEmpty() {
		internalUniqueOpts := (*dbunique.UniqueOpts)(&uniqueOpts)
		insertParams.UniqueKey, err = dbunique.UniqueKey(archetype.Time, internalUniqueOpts, insertParams)
		if err != nil {
			return nil, err
		}
		insertParams.UniqueStates = internalUniqueOpts.StateBitmask()
	}

	switch {
	case !insertOpts.ScheduledAt.IsZero():
		insertParams.ScheduledAt = &insertOpts.ScheduledAt
		insertParams.State = rivertype.JobStateScheduled
	case !jobInsertOpts.ScheduledAt.IsZero():
		insertParams.ScheduledAt = &jobInsertOpts.ScheduledAt
		insertParams.State = rivertype.JobStateScheduled
	default:
		// Use a stubbed time if there was one, but otherwise prefer the value
		// generated by the database. createdAt is nil unless time is stubbed.
		insertParams.ScheduledAt = createdAt
	}

	if insertOpts.Pending {
		insertParams.State = rivertype.JobStatePending
	}

	return insertParams, nil
}

var errNoDriverDBPool = errors.New("driver must have non-nil database pool to use non-transactional methods like Insert and InsertMany (try InsertTx or InsertManyTx instead")

// Insert inserts a new job with the provided args. Job opts can be used to
// override any defaults that may have been provided by an implementation of
// JobArgsWithInsertOpts.InsertOpts, as well as any global defaults. The
// provided context is used for the underlying Postgres insert and can be used
// to cancel the operation or apply a timeout.
//
//	jobRow, err := client.Insert(insertCtx, MyArgs{}, nil)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	res, err := dbutil.WithTxV(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) (*insertManySharedResult, error) {
		return c.validateParamsAndInsertMany(ctx, execTx, []InsertManyParams{{Args: args, InsertOpts: opts}})
	})
	if err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerJobFetch(res.QueuesDeduped)

	return res.InsertResults[0], nil
}

// InsertTx inserts a new job with the provided args on the given transaction.
// Job opts can be used to override any defaults that may have been provided by
// an implementation of JobArgsWithInsertOpts.InsertOpts, as well as any global
// defaults. The provided context is used for the underlying Postgres insert and
// can be used to cancel the operation or apply a timeout.
//
//	jobRow, err := client.InsertTx(insertCtx, tx, MyArgs{}, nil)
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. It's also possible to insert a job outside a transaction, but this
// usage is recommended to ensure that all data a job needs to run is available
// by the time it starts. Because of snapshot visibility guarantees across
// transactions, the job will not be worked until the transaction has committed,
// and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	res, err := c.validateParamsAndInsertMany(ctx, c.driver.UnwrapExecutor(tx), []InsertManyParams{{Args: args, InsertOpts: opts}})
	if err != nil {
		return nil, err
	}
	return res.InsertResults[0], nil
}

// InsertManyParams encapsulates a single job combined with insert options for
// use with batch insertion.
type InsertManyParams struct {
	// Args are the arguments of the job to insert.
	Args JobArgs

	// InsertOpts are insertion options for this job.
	InsertOpts *InsertOpts
}

// InsertMany inserts many jobs at once. Each job is inserted as an
// InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertMany(ctx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	res, err := dbutil.WithTxV(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) (*insertManySharedResult, error) {
		return c.validateParamsAndInsertMany(ctx, execTx, params)
	})
	if err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerJobFetch(res.QueuesDeduped)

	return res.InsertResults, nil
}

// InsertManyTx inserts many jobs at once. Each job is inserted as an
// InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertManyTx(ctx, tx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	res, err := c.validateParamsAndInsertMany(ctx, c.driver.UnwrapExecutor(tx), params)
	if err != nil {
		return nil, err
	}
	return res.InsertResults, nil
}

// validateParamsAndInsertMany is a helper method that wraps the insertMany
// method to provide param validation and conversion prior to calling the actual
// insertMany method. This allows insertMany to be reused by the
// PeriodicJobEnqueuer which cannot reference top-level river package types.
func (c *Client[TTx]) validateParamsAndInsertMany(ctx context.Context, execTx riverdriver.ExecutorTx, params []InsertManyParams) (*insertManySharedResult, error) {
	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return nil, err
	}

	return c.insertMany(ctx, execTx, insertParams)
}

// insertMany is a shared code path for InsertMany and InsertManyTx, also used
// by the PeriodicJobEnqueuer.
func (c *Client[TTx]) insertMany(ctx context.Context, execTx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) (*insertManySharedResult, error) {
	return c.insertManyShared(ctx, execTx, insertParams, func(ctx context.Context, insertParams []*riverdriver.JobInsertFastParams) ([]*rivertype.JobInsertResult, error) {
		results, err := c.pilot.JobInsertMany(ctx, execTx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: c.config.Schema,
		})
		if err != nil {
			return nil, err
		}

		return sliceutil.Map(results,
			func(result *riverdriver.JobInsertFastResult) *rivertype.JobInsertResult {
				return (*rivertype.JobInsertResult)(result)
			},
		), nil
	})
}

type insertManySharedResult struct {
	InsertResults []*rivertype.JobInsertResult
	QueuesDeduped []string
}

// The shared code path for all Insert and InsertMany methods. It takes a
// function that executes the actual insert operation and allows for different
// implementations of the insert query to be passed in, each mapping their
// results back to a common result type.
func (c *Client[TTx]) insertManyShared(
	ctx context.Context,
	tx riverdriver.ExecutorTx,
	insertParams []*rivertype.JobInsertParams,
	execute func(context.Context, []*riverdriver.JobInsertFastParams) ([]*rivertype.JobInsertResult, error),
) (*insertManySharedResult, error) {
	var queuesDeduped []string

	doInner := func(ctx context.Context) ([]*rivertype.JobInsertResult, error) {
		for _, params := range insertParams {
			for _, hook := range append(
				c.hookLookupGlobal.ByHookKind(hooklookup.HookKindInsertBegin),
				c.hookLookupByJob.ByJobArgs(params.Args).ByHookKind(hooklookup.HookKindInsertBegin)...,
			) {
				if err := hook.(rivertype.HookInsertBegin).InsertBegin(ctx, params); err != nil { //nolint:forcetypeassert
					return nil, err
				}
			}
		}

		finalInsertParams := sliceutil.Map(insertParams, func(params *rivertype.JobInsertParams) *riverdriver.JobInsertFastParams {
			return (*riverdriver.JobInsertFastParams)(params)
		})

		insertResults, err := execute(ctx, finalInsertParams)
		if err != nil {
			return insertResults, err
		}

		queues := make([]string, 0, 10)
		for _, params := range insertParams {
			if params.State == rivertype.JobStateAvailable {
				queues = append(queues, params.Queue)
			}
		}

		queuesDeduped = sliceutil.Uniq(queues)

		if err = c.maybeNotifyInsertForQueues(ctx, tx, queuesDeduped); err != nil {
			return nil, err
		}

		return insertResults, nil
	}

	jobInsertMiddleware := c.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindJobInsert)
	if len(jobInsertMiddleware) > 0 {
		// Wrap middlewares in reverse order so the one defined first is wrapped
		// as the outermost function and is first to receive the operation.
		for i := len(jobInsertMiddleware) - 1; i >= 0; i-- {
			middlewareItem := jobInsertMiddleware[i].(rivertype.JobInsertMiddleware) //nolint:forcetypeassert // capture the current middleware item
			previousDoInner := doInner                                               // Capture the current doInner function
			doInner = func(ctx context.Context) ([]*rivertype.JobInsertResult, error) {
				return middlewareItem.InsertMany(ctx, insertParams, previousDoInner)
			}
		}
	}

	insertResults, err := doInner(ctx)
	if err != nil {
		return nil, err
	}

	return &insertManySharedResult{
		InsertResults: insertResults,
		QueuesDeduped: queuesDeduped,
	}, nil
}

// Validates input parameters for a batch insert operation and generates a set
// of batch insert parameters.
func (c *Client[TTx]) insertManyParams(params []InsertManyParams) ([]*rivertype.JobInsertParams, error) {
	if len(params) < 1 {
		return nil, errors.New("no jobs to insert")
	}

	insertParams := make([]*rivertype.JobInsertParams, len(params))
	for i, param := range params {
		if err := c.validateJobArgs(param.Args); err != nil {
			return nil, err
		}

		insertParamsItem, err := insertParamsFromConfigArgsAndOptions(&c.baseService.Archetype, c.config, param.Args, param.InsertOpts)
		if err != nil {
			return nil, err
		}

		insertParams[i] = insertParamsItem
	}

	return insertParams, nil
}

// Notifies an internal producer of new jobs being queued for work.  Only
// invoked if the client's driver doesn't support a listener. If a listener is
// supported, job notifications go out via listen/notify instead.
//
// Should only ever be invoked *outside* a transaction. If invoked within a
// transaction, the producer wouldn't yet be able to access the new jobs that
// triggered the notification because they're not committed yet.
func (c *Client[TTx]) notifyProducerWithoutListenerJobFetch(queuesDeduped []string) {
	if c.driver.SupportsListener() || len(c.producersByQueueName) < 1 {
		return
	}

	for _, queue := range queuesDeduped {
		if producer, ok := c.producersByQueueName[queue]; ok {
			producer.TriggerJobFetch()
		}
	}
}

// InsertManyFast inserts many jobs at once using Postgres' `COPY FROM` mechanism,
// making the operation quite fast and memory efficient. Each job is inserted as
// an InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertMany(ctx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// Unlike with `InsertMany`, unique conflicts cannot be handled gracefully. If a
// unique constraint is violated, the operation will fail and no jobs will be inserted.
func (c *Client[TTx]) InsertManyFast(ctx context.Context, params []InsertManyParams) (int, error) {
	if !c.driver.PoolIsSet() {
		return 0, errNoDriverDBPool
	}

	// Wrap in a transaction in case we need to notify about inserts.
	res, err := dbutil.WithTxV(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) (*insertManySharedResult, error) {
		return c.insertManyFast(ctx, execTx, params)
	})
	if err != nil {
		return 0, err
	}

	c.notifyProducerWithoutListenerJobFetch(res.QueuesDeduped)

	return len(res.InsertResults), nil
}

// InsertManyTx inserts many jobs at once using Postgres' `COPY FROM` mechanism,
// making the operation quite fast and memory efficient. Each job is inserted as
// an InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertManyTx(ctx, tx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
//
// Unlike with `InsertManyTx`, unique conflicts cannot be handled gracefully. If
// a unique constraint is violated, the operation will fail and no jobs will be
// inserted.
func (c *Client[TTx]) InsertManyFastTx(ctx context.Context, tx TTx, params []InsertManyParams) (int, error) {
	res, err := c.insertManyFast(ctx, c.driver.UnwrapExecutor(tx), params)
	if err != nil {
		return 0, err
	}
	return len(res.InsertResults), nil
}

func (c *Client[TTx]) insertManyFast(ctx context.Context, execTx riverdriver.ExecutorTx, params []InsertManyParams) (*insertManySharedResult, error) {
	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return nil, err
	}

	return c.insertManyShared(ctx, execTx, insertParams, func(ctx context.Context, insertParams []*riverdriver.JobInsertFastParams) ([]*rivertype.JobInsertResult, error) {
		count, err := execTx.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: c.config.Schema,
		})
		if err != nil {
			return nil, err
		}
		return make([]*rivertype.JobInsertResult, count), nil
	})
}

// Notify the given queues that new jobs are available. The queues list will be
// deduplicated and each will be checked to see if it is due for an insert
// notification from this client.
func (c *Client[TTx]) maybeNotifyInsertForQueues(ctx context.Context, tx riverdriver.ExecutorTx, queuesDeduped []string) error {
	if len(queuesDeduped) < 1 {
		return nil
	}

	var (
		payloads        = make([]string, 0, len(queuesDeduped))
		queuesTriggered = make([]string, 0, len(queuesDeduped))
	)

	for _, queue := range queuesDeduped {
		if c.insertNotifyLimiter.ShouldTrigger(queue) {
			payloads = append(payloads, fmt.Sprintf("{\"queue\": %q}", queue))
			queuesTriggered = append(queuesTriggered, queue)
		}
	}

	if len(payloads) < 1 {
		return nil
	}

	if c.driver.SupportsListenNotify() {
		err := tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: payloads,
			Schema:  c.config.Schema,
			Topic:   string(notifier.NotificationTopicInsert),
		})
		if err != nil {
			c.baseService.Logger.ErrorContext(
				ctx,
				c.baseService.Name+": Failed to send job insert notification",
				slog.String("queues", strings.Join(queuesTriggered, ",")),
				slog.String("err", err.Error()),
			)
			return err
		}
	}

	return nil
}

// emit a notification about a queue being paused or resumed.
func (c *Client[TTx]) notifyQueuePauseOrResume(ctx context.Context, tx riverdriver.ExecutorTx, action controlAction, queue string, opts *QueuePauseOpts) (*controlEventPayload, error) {
	c.baseService.Logger.DebugContext(ctx,
		c.baseService.Name+": Notifying about queue state change",
		slog.String("action", string(action)),
		slog.String("queue", queue),
		slog.String("opts", fmt.Sprintf("%+v", opts)),
	)

	controlEvent := &controlEventPayload{Action: action, Queue: queue}

	payload, err := json.Marshal(controlEvent)
	if err != nil {
		return nil, err
	}

	if c.driver.SupportsListenNotify() {
		err = tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: []string{string(payload)},
			Schema:  c.config.Schema,
			Topic:   string(notifier.NotificationTopicControl),
		})
		if err != nil {
			c.baseService.Logger.ErrorContext(
				ctx,
				c.baseService.Name+": Failed to send queue state change notification",
				slog.String("err", err.Error()),
			)
			return nil, err
		}
	}

	return controlEvent, nil
}

// Validates job args prior to insertion. Currently, verifies that a worker to
// handle the kind is registered in the configured workers bundle.
// This validation is skipped if the client is configured as an insert-only (with no workers)
// or if the client is configured to skip unknown job kinds.
func (c *Client[TTx]) validateJobArgs(args JobArgs) error {
	if c.config.Workers == nil || c.config.SkipUnknownJobCheck {
		return nil
	}

	if _, ok := c.config.Workers.workersMap[args.Kind()]; !ok {
		return &UnknownJobKindError{Kind: args.Kind()}
	}

	return nil
}

func (c *Client[TTx]) addProducer(queueName string, queueConfig QueueConfig) (*producer, error) {
	if _, alreadyExists := c.producersByQueueName[queueName]; alreadyExists {
		return nil, &QueueAlreadyAddedError{Name: queueName}
	}

	producer := newProducer(&c.baseService.Archetype, c.driver.GetExecutor(), c.pilot, &producerConfig{
		ClientID:                     c.config.ID,
		Completer:                    c.completer,
		ErrorHandler:                 c.config.ErrorHandler,
		FetchCooldown:                cmp.Or(queueConfig.FetchCooldown, c.config.FetchCooldown),
		FetchPollInterval:            cmp.Or(queueConfig.FetchPollInterval, c.config.FetchPollInterval),
		HookLookupByJob:              c.hookLookupByJob,
		HookLookupGlobal:             c.hookLookupGlobal,
		JobTimeout:                   c.config.JobTimeout,
		MaxWorkers:                   queueConfig.MaxWorkers,
		MiddlewareLookupGlobal:       c.middlewareLookupGlobal,
		Notifier:                     c.notifier,
		Queue:                        queueName,
		QueueEventCallback:           c.subscriptionManager.distributeQueueEvent,
		QueuePollInterval:            c.config.queuePollInterval,
		RetryPolicy:                  c.config.RetryPolicy,
		SchedulerInterval:            c.config.schedulerInterval,
		Schema:                       c.config.Schema,
		StaleProducerRetentionPeriod: 5 * time.Minute,
		Workers:                      c.config.Workers,
	})
	c.producersByQueueName[queueName] = producer
	return producer, nil
}

var nameRegex = regexp.MustCompile(`^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$`)

func validateQueueName(queueName string) error {
	if queueName == "" {
		return errors.New("queue name cannot be empty")
	}
	if len(queueName) > 64 {
		return errors.New("queue name cannot be longer than 64 characters")
	}
	if !nameRegex.MatchString(queueName) {
		return fmt.Errorf("queue name is invalid, expected letters and numbers separated by underscores or hyphens: %q", queueName)
	}
	return nil
}

// JobDeleteManyResult is the result of a job list operation. It contains a list of
// jobs and a cursor for fetching the next page of results.
type JobDeleteManyResult struct {
	// Jobs is a slice of job returned as part of the list operation.
	Jobs []*rivertype.JobRow
}

// JobDeleteMany deletes many jobs at once based on the conditions defined by
// JobDeleteManyParams. Running jobs are always ignored.
//
//	params := river.NewJobDeleteManyParams().First(10).State(rivertype.JobStateCompleted)
//	jobRows, err := client.JobDeleteMany(ctx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobDeleteMany(ctx context.Context, params *JobDeleteManyParams) (*JobDeleteManyResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	if params == nil {
		params = NewJobDeleteManyParams()
	}
	params.schema = c.config.Schema

	listParams, err := dblist.JobMakeDriverParams(ctx, params.toDBParams(), c.driver.SQLFragmentColumnIn)
	if err != nil {
		return nil, err
	}

	jobs, err := c.driver.GetExecutor().JobDeleteMany(ctx, (*riverdriver.JobDeleteManyParams)(listParams))
	if err != nil {
		return nil, err
	}

	return &JobDeleteManyResult{Jobs: jobs}, nil
}

// JobDeleteManyTx deletes many jobs at once based on the conditions defined by
// JobDeleteManyParams. Running jobs are always ignored.
//
//	params := river.NewJobDeleteManyParams().First(10).States(river.JobStateCompleted)
//	jobRows, err := client.JobDeleteManyTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobDeleteManyTx(ctx context.Context, tx TTx, params *JobDeleteManyParams) (*JobDeleteManyResult, error) {
	if params == nil {
		params = NewJobDeleteManyParams()
	}
	params.schema = c.config.Schema

	listParams, err := dblist.JobMakeDriverParams(ctx, params.toDBParams(), c.driver.SQLFragmentColumnIn)
	if err != nil {
		return nil, err
	}

	jobs, err := c.driver.UnwrapExecutor(tx).JobDeleteMany(ctx, (*riverdriver.JobDeleteManyParams)(listParams))
	if err != nil {
		return nil, err
	}

	return &JobDeleteManyResult{Jobs: jobs}, nil
}

// JobListResult is the result of a job list operation. It contains a list of
// jobs and a cursor for fetching the next page of results.
type JobListResult struct {
	// Jobs is a slice of job returned as part of the list operation.
	Jobs []*rivertype.JobRow

	// LastCursor is a cursor that can be used to list the next page of jobs.
	LastCursor *JobListCursor
}

const databaseNameSQLite = "sqlite"

var errJobListParamsMetadataNotSupportedSQLite = errors.New("JobListParams.Metadata is not supported on SQLite")

// JobList returns a paginated list of jobs matching the provided filters. The
// provided context is used for the underlying Postgres query and can be used to
// cancel the operation or apply a timeout.
//
//	params := river.NewJobListParams().First(10).State(rivertype.JobStateCompleted)
//	jobRows, err := client.JobList(ctx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobList(ctx context.Context, params *JobListParams) (*JobListResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	if params == nil {
		params = NewJobListParams()
	}
	params.schema = c.config.Schema

	if c.driver.DatabaseName() == databaseNameSQLite && params.metadataCalled {
		return nil, errJobListParamsMetadataNotSupportedSQLite
	}

	dbParams, err := params.toDBParams()
	if err != nil {
		return nil, err
	}

	listParams, err := dblist.JobMakeDriverParams(ctx, dbParams, c.driver.SQLFragmentColumnIn)
	if err != nil {
		return nil, err
	}

	jobs, err := c.driver.GetExecutor().JobList(ctx, listParams)
	if err != nil {
		return nil, err
	}

	res := &JobListResult{Jobs: jobs}
	if len(jobs) > 0 {
		res.LastCursor = jobListCursorFromJobAndParams(jobs[len(jobs)-1], params)
	}
	return res, nil
}

// JobListTx returns a paginated list of jobs matching the provided filters. The
// provided context is used for the underlying Postgres query and can be used to
// cancel the operation or apply a timeout.
//
//	params := river.NewJobListParams().First(10).States(river.JobStateCompleted)
//	jobRows, err := client.JobListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobListTx(ctx context.Context, tx TTx, params *JobListParams) (*JobListResult, error) {
	if params == nil {
		params = NewJobListParams()
	}
	params.schema = c.config.Schema

	if c.driver.DatabaseName() == databaseNameSQLite && params.metadataCalled {
		return nil, errJobListParamsMetadataNotSupportedSQLite
	}

	dbParams, err := params.toDBParams()
	if err != nil {
		return nil, err
	}

	listParams, err := dblist.JobMakeDriverParams(ctx, dbParams, c.driver.SQLFragmentColumnIn)
	if err != nil {
		return nil, err
	}

	jobs, err := c.driver.UnwrapExecutor(tx).JobList(ctx, listParams)
	if err != nil {
		return nil, err
	}

	res := &JobListResult{Jobs: jobs}
	if len(jobs) > 0 {
		res.LastCursor = jobListCursorFromJobAndParams(jobs[len(jobs)-1], params)
	}
	return res, nil
}

// PeriodicJobs returns the currently configured set of periodic jobs for the
// client, and can be used to add new or remove existing ones.
//
// This function should only be invoked on clients capable of running perioidc
// jobs. Running periodic jobs requires that the client be electable as leader
// to run maintenance services, and being electable as leader requires that a
// client be started. To be startable, a client must have Queues and Workers
// configured. Invoking this function will panic if these conditions aren't met.
func (c *Client[TTx]) PeriodicJobs() *PeriodicJobBundle {
	if !c.config.willExecuteJobs() {
		panic("client Queues and Workers must be configured to modify periodic jobs (otherwise, they'll have no effect because a client not configured to work jobs can't be started)")
	}

	return c.periodicJobs
}

// Driver exposes the underlying pilot used by the client.
//
// API is not stable. DO NOT USE.
func (c *Client[TTx]) Pilot() riverpilot.Pilot {
	return c.pilot
}

// Queues returns the currently configured set of queues for the client, and can
// be used to add new ones.
func (c *Client[TTx]) Queues() *QueueBundle { return c.queues }

// QueueGet returns the queue with the given name. If the queue has not recently
// been active or does not exist, returns ErrNotFound.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
func (c *Client[TTx]) QueueGet(ctx context.Context, name string) (*rivertype.Queue, error) {
	return c.driver.GetExecutor().QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   name,
		Schema: c.config.Schema,
	})
}

// QueueGetTx returns the queue with the given name. If the queue has not recently
// been active or does not exist, returns ErrNotFound.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
func (c *Client[TTx]) QueueGetTx(ctx context.Context, tx TTx, name string) (*rivertype.Queue, error) {
	return c.driver.UnwrapExecutor(tx).QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   name,
		Schema: c.config.Schema,
	})
}

// QueueListResult is the result of a job list operation. It contains a list of
// jobs and leaves room for future cursor functionality.
type QueueListResult struct {
	// Queues is a slice of queues returned as part of the list operation.
	Queues []*rivertype.Queue
}

// QueueList returns a list of all queues that are currently active or were
// recently active. Limit and offset can be used to paginate the results.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
//
//	params := river.NewQueueListParams().First(10)
//	queueRows, err := client.QueueListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) QueueList(ctx context.Context, params *QueueListParams) (*QueueListResult, error) {
	if params == nil {
		params = NewQueueListParams()
	}

	queues, err := c.driver.GetExecutor().QueueList(ctx, &riverdriver.QueueListParams{
		Max:    int(params.paginationCount),
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, err
	}

	return &QueueListResult{Queues: queues}, nil
}

// QueueListTx returns a list of all queues that are currently active or were
// recently active. Limit and offset can be used to paginate the results.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
//
//	params := river.NewQueueListParams().First(10)
//	queueRows, err := client.QueueListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) QueueListTx(ctx context.Context, tx TTx, params *QueueListParams) (*QueueListResult, error) {
	if params == nil {
		params = NewQueueListParams()
	}

	queues, err := c.driver.UnwrapExecutor(tx).QueueList(ctx, &riverdriver.QueueListParams{
		Max:    int(params.paginationCount),
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, err
	}

	return &QueueListResult{Queues: queues}, nil
}

// QueuePause pauses the queue with the given name. When a queue is paused,
// clients will not fetch any more jobs for that particular queue. To pause all
// queues at once, use the special queue name "*".
//
// Clients with a configured notifier should receive a notification about the
// paused queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will pause after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueuePause(ctx context.Context, name string, opts *QueuePauseOpts) error {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.QueuePause(ctx, &riverdriver.QueuePauseParams{
		Name:   name,
		Now:    c.baseService.Time.NowUTCOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	controlEvent, err := c.notifyQueuePauseOrResume(ctx, tx, controlActionPause, name, opts)
	if err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	return nil
}

// QueuePauseTx pauses the queue with the given name. When a queue is paused,
// clients will not fetch any more jobs for that particular queue. To pause all
// queues at once, use the special queue name "*".
//
// Clients with a configured notifier should receive a notification about the
// paused queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will pause after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueuePauseTx(ctx context.Context, tx TTx, name string, opts *QueuePauseOpts) error {
	executorTx := c.driver.UnwrapExecutor(tx)

	if err := executorTx.QueuePause(ctx, &riverdriver.QueuePauseParams{
		Name:   name,
		Now:    c.baseService.Time.NowUTCOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	if _, err := c.notifyQueuePauseOrResume(ctx, executorTx, controlActionPause, name, opts); err != nil {
		return err
	}

	return nil
}

// QueueResume resumes the queue with the given name. If the queue was
// previously paused, any clients configured to work that queue will resume
// fetching additional jobs. To resume all queues at once, use the special queue
// name "*".
//
// Clients with a configured notifier should receive a notification about the
// resumed queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will resume after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueueResume(ctx context.Context, name string, opts *QueuePauseOpts) error {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.QueueResume(ctx, &riverdriver.QueueResumeParams{
		Name:   name,
		Now:    c.baseService.Time.NowUTCOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	controlEvent, err := c.notifyQueuePauseOrResume(ctx, tx, controlActionResume, name, opts)
	if err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	return nil
}

// QueueResume resumes the queue with the given name. If the queue was
// previously paused, any clients configured to work that queue will resume
// fetching additional jobs. To resume all queues at once, use the special queue
// name "*".
//
// Clients with a configured notifier should receive a notification about the
// resumed queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will resume after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueueResumeTx(ctx context.Context, tx TTx, name string, opts *QueuePauseOpts) error {
	executorTx := c.driver.UnwrapExecutor(tx)

	if err := executorTx.QueueResume(ctx, &riverdriver.QueueResumeParams{
		Name:   name,
		Now:    c.baseService.Time.NowUTCOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	if _, err := c.notifyQueuePauseOrResume(ctx, executorTx, controlActionResume, name, opts); err != nil {
		return err
	}

	return nil
}

// QueueUpdateParams are the parameters for a QueueUpdate operation.
type QueueUpdateParams struct {
	// Metadata is the new metadata for the queue. If nil or empty, the metadata
	// will not be changed.
	Metadata []byte
}

// QueueUpdate updates a queue's settings in the database. These settings
// override the settings in the client (if applied).
func (c *Client[TTx]) QueueUpdate(ctx context.Context, name string, params *QueueUpdateParams) (*rivertype.Queue, error) {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	queue, controlEvent, err := c.queueUpdate(ctx, tx, name, params)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	return queue, nil
}

// QueueUpdateTx updates a queue's settings in the database. These settings
// override the settings in the client (if applied).
func (c *Client[TTx]) QueueUpdateTx(ctx context.Context, tx TTx, name string, params *QueueUpdateParams) (*rivertype.Queue, error) {
	queue, _, err := c.queueUpdate(ctx, c.driver.UnwrapExecutor(tx), name, params)
	if err != nil {
		return nil, err
	}
	return queue, nil
}

// Notifies an internal producer of a queue control event like pause/resume.
// Only invoked if the client's driver doesn't support a listener. If a listener
// is supported, control events go out via listen/notify instead.
//
// Should only ever be invoked *outside* a transaction. If invoked within a
// transaction, the producer wouldn't yet be able to access the state that
// triggered the notification because it's not committed yet.
func (c *Client[TTx]) notifyProducerWithoutListenerQueueControlEvent(queue string, controlEvent *controlEventPayload) {
	if c.driver.SupportsListener() || len(c.producersByQueueName) < 1 {
		return
	}

	if producer, ok := c.producersByQueueName[queue]; ok {
		producer.TriggerQueueControlEvent(controlEvent)
	}
}

func (c *Client[TTx]) queueUpdate(ctx context.Context, executorTx riverdriver.ExecutorTx, name string, params *QueueUpdateParams) (*rivertype.Queue, *controlEventPayload, error) {
	updateMetadata := len(params.Metadata) > 0

	queue, err := executorTx.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
		Metadata:         params.Metadata,
		MetadataDoUpdate: updateMetadata,
		Name:             name,
		Schema:           c.config.Schema,
	})
	if err != nil {
		return nil, nil, err
	}

	if !updateMetadata {
		return queue, nil, err
	}

	controlEvent := &controlEventPayload{
		Action:   controlActionMetadataChanged,
		Metadata: params.Metadata,
		Queue:    queue.Name,
	}

	payload, err := json.Marshal(controlEvent)
	if err != nil {
		return nil, nil, err
	}

	if c.driver.SupportsListenNotify() {
		if err := executorTx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: []string{string(payload)},
			Schema:  c.config.Schema,
			Topic:   string(notifier.NotificationTopicControl),
		}); err != nil {
			return nil, nil, err
		}
	}

	return queue, controlEvent, nil
}

// Schema returns the configured schema for the client.
func (c *Client[TTx]) Schema() string { return c.config.Schema }

// QueueBundle is a bundle for adding additional queues. It's made accessible
// through Client.Queues.
type QueueBundle struct {
	// Function that adds a producer to the associated client.
	addProducer func(queueName string, queueConfig QueueConfig) (*producer, error)

	clientFetchCooldown     time.Duration
	clientFetchPollInterval time.Duration

	clientWillExecuteJobs bool

	fetchCtx context.Context //nolint:containedctx

	// Mutex that's acquired when client is starting and stopping and when a
	// queue is being added so that we can be sure that a client is fully
	// stopped or fully started when adding a new queue.
	startStopMu sync.Mutex

	workCtx context.Context //nolint:containedctx
}

// Add adds a new queue to the client. If the client is already started, a
// producer for the queue is started. Context is inherited from the one given to
// Client.Start.
func (b *QueueBundle) Add(queueName string, queueConfig QueueConfig) error {
	if !b.clientWillExecuteJobs {
		return errors.New("client is not configured to execute jobs, cannot add queue")
	}

	if err := queueConfig.validate(queueName, b.clientFetchCooldown, b.clientFetchPollInterval); err != nil {
		return err
	}

	b.startStopMu.Lock()
	defer b.startStopMu.Unlock()

	producer, err := b.addProducer(queueName, queueConfig)
	if err != nil {
		return err
	}

	// Start the queue if the client is already started.
	if b.fetchCtx != nil && b.fetchCtx.Err() == nil {
		if err := producer.StartWorkContext(b.fetchCtx, b.workCtx); err != nil {
			return err
		}
	}

	return nil
}

// Generates a default client ID using the current hostname and time.
func defaultClientID(startedAt time.Time) string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown_host"
	}

	return defaultClientIDWithHost(startedAt, host)
}

// Same as the above, but allows host injection for testability.
func defaultClientIDWithHost(startedAt time.Time, host string) string {
	const maxHostLength = 60

	// Truncate degenerately long host names.
	host = strings.ReplaceAll(host, ".", "_")
	if len(host) > maxHostLength {
		host = host[0:maxHostLength]
	}

	// Dots, hyphens, and colons aren't particularly friendly for double click
	// to select (depends on application and configuration), so avoid them all
	// in favor of underscores.
	//
	// Go's time package is really dumb and can't format subseconds without
	// using a dot. So use the dot, then replace it with an underscore below.
	const rfc3339Compact = "2006_01_02T15_04_05.000000"

	return host + "_" + strings.Replace(startedAt.Format(rfc3339Compact), ".", "_", 1)
}
