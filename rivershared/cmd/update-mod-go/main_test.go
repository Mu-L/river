package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
)

const sampleGoMod = `module github.com/riverqueue/river

go 1.22

toolchain go1.23.1

require (
	github.com/riverqueue/river/riverdriver v0.0.0-00010101000000-000000000000
	github.com/riverqueue/river/riverdriver/riverdatabasesql v0.0.0-00010101000000-000000000000
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.0.12
)`

func TestParseAndUpdateGoModFile(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (string, *testBundle) { //nolint:unparam
		t.Helper()

		file, err := os.CreateTemp(t.TempDir(), "go.mod")
		require.NoError(t, err)
		t.Cleanup(func() { os.Remove(file.Name()) })

		_, err = file.WriteString(sampleGoMod)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		return file.Name(), &testBundle{}
	}

	requireDirectives := func(t *testing.T, filename, goVersion, toolchainName string) {
		t.Helper()

		fileData, err := os.ReadFile(filename)
		require.NoError(t, err)

		modFile, err := modfile.Parse(filename, fileData, nil)
		require.NoError(t, err)

		require.Equal(t, goVersion, modFile.Go.Version)
		require.Equal(t, toolchainName, modFile.Toolchain.Name)
	}

	t.Run("WritesChanges", func(t *testing.T) {
		t.Parallel()

		filename, _ := setup(t)

		anyMismatch, err := parseAndUpdateGoModFile(false, filename, "go.work", "1.23", "go1.23.2")
		require.NoError(t, err)
		require.True(t, anyMismatch)

		// Reread the file that the command above just wrote and make sure the right
		// changes were made.
		requireDirectives(t, filename, "1.23", "go1.23.2")

		// Running again is allowed and should be idempotent. This time it'll
		// return that no changes were made.
		anyMismatch, err = parseAndUpdateGoModFile(false, filename, "go.work", "1.23", "go1.23.2")
		require.NoError(t, err)
		require.False(t, anyMismatch)
	})

	t.Run("NoChanges", func(t *testing.T) {
		t.Parallel()

		filename, _ := setup(t)

		anyMismatch, err := parseAndUpdateGoModFile(false, filename, "go.work", "1.22", "go1.23.1")
		require.NoError(t, err)
		require.False(t, anyMismatch)

		// Expect no changes made in file.
		requireDirectives(t, filename, "1.22", "go1.23.1")
	})

	t.Run("CheckOnlyGoMismatch", func(t *testing.T) {
		t.Parallel()

		filename, _ := setup(t)

		_, err := parseAndUpdateGoModFile(true, filename, "go.work", "1.23", "go1.23.1")
		require.EqualError(t, err, fmt.Sprintf("go directive of %q (%s) doesn't match %q (%s)", filename, "1.22", "go.work", "1.23"))
	})

	t.Run("CheckOnlyToolchainMismatch", func(t *testing.T) {
		t.Parallel()

		filename, _ := setup(t)

		_, err := parseAndUpdateGoModFile(true, filename, "go.work", "1.22", "go1.23.2")
		require.EqualError(t, err, fmt.Sprintf("toolchain directive of %q (%s) doesn't match %q (%s)", filename, "go1.23.1", "go.work", "go1.23.2"))
	})

	t.Run("CheckOnlyNoChanges", func(t *testing.T) {
		t.Parallel()

		filename, _ := setup(t)

		anyMismatch, err := parseAndUpdateGoModFile(true, filename, "go.work", "1.22", "go1.23.1")
		require.NoError(t, err)
		require.False(t, anyMismatch)

		requireDirectives(t, filename, "1.22", "go1.23.1")
	})
}
