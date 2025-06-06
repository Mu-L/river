package testsignal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

func TestTestSignal(t *testing.T) {
	t.Parallel()

	t.Run("Uninitialized", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}

		// Signal can safely be invoked any number of times.
		for range testSignalInternalChanSize + 1 {
			signal.Signal(struct{}{})
		}
	})
	t.Run("Initialized", func(t *testing.T) {
		t.Parallel()

		mockT := testutil.NewMockT(t)

		signal := TestSignal[struct{}]{}
		signal.Init(mockT)

		// Signal can be invoked many times, but not infinitely
		for range testSignalInternalChanSize {
			signal.Signal(struct{}{})
		}
		require.False(t, mockT.Failed)
		require.Empty(t, mockT.LogOutput())

		// Another signal will panic because the internal channel is full.
		signal.Signal(struct{}{})
		require.True(t, mockT.Failed)
		require.Equal(t, "test only signal channel is full\n", mockT.LogOutput())

		// And we can now wait on all the emitted signals.
		for range testSignalInternalChanSize {
			signal.WaitOrTimeout()
		}
	})

	t.Run("RequireEmpty", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}

		require.PanicsWithValue(t, "test only signal is not initialized; called outside of tests?", func() {
			signal.RequireEmpty()
		})

		mockT := testutil.NewMockT(t)
		signal.Init(mockT)

		signal.RequireEmpty() // succeeds

		signal.Signal(struct{}{})

		signal.RequireEmpty()
		require.True(t, mockT.Failed)
		require.Equal(t, "test signal should be empty, but wasn't\ngot value: {}\n\n", mockT.LogOutput())
	})

	t.Run("WaitC", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}
		signal.Init(t)

		select {
		case <-signal.WaitC():
			require.FailNow(t, "Test signal should not have fired")
		default:
		}

		signal.Signal(struct{}{})

		select {
		case <-signal.WaitC():
		default:
			require.FailNow(t, "Test signal should have fired")
		}
	})

	t.Run("WaitOrTimeout", func(t *testing.T) {
		t.Parallel()

		signal := TestSignal[struct{}]{}

		require.PanicsWithValue(t, "test only signal is not initialized; called outside of tests?", func() {
			signal.WaitOrTimeout()
		})

		signal.Init(t)

		signal.Signal(struct{}{})

		signal.WaitOrTimeout()
	})
}

// Marked as non-parallel because `t.Setenv` is not compatible with `t.Parallel`.
func TestWaitTimeout(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	require.Equal(t, 3*time.Second, riversharedtest.WaitTimeout())

	t.Setenv("GITHUB_ACTIONS", "true")
	require.Equal(t, 10*time.Second, riversharedtest.WaitTimeout())
}
