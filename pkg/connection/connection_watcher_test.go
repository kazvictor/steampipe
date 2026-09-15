package connection

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	perror_helpers "github.com/turbot/pipe-fittings/v2/error_helpers"
	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
	pplugin "github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/steampipe/v2/pkg/steampipeconfig"
)

// spyPluginManager records the calls handleFileWatcherEvent makes, so the test can assert on them.
// The channels are buffered so nothing blocks if an assertion is not waiting.
type spyPluginManager struct {
	mockPluginManager
	notifications chan perror_helpers.ErrorAndWarnings
	configChanges chan ConnectionConfigMap
}

func newSpyPluginManager() *spyPluginManager {
	return &spyPluginManager{
		notifications: make(chan perror_helpers.ErrorAndWarnings, 4),
		configChanges: make(chan ConnectionConfigMap, 4),
	}
}

func (m *spyPluginManager) SendPostgresErrorsAndWarningsNotification(_ context.Context, ew perror_helpers.ErrorAndWarnings) {
	select {
	case m.notifications <- ew:
	default:
	}
}

func (m *spyPluginManager) OnConnectionConfigChanged(_ context.Context, configMap ConnectionConfigMap, _ map[string]*pplugin.Plugin) {
	select {
	case m.configChanges <- configMap:
	default:
	}
}

// newTestWatcher builds a ConnectionWatcher directly, bypassing NewConnectionWatcher (which starts
// a real fsnotify watcher on the real config dir) and stubbing out the refresh (the real
// RefreshConnections takes package level locks and dereferences a nil connection pool in a test).
// It returns the watcher, the spy plugin manager and a channel which receives one value per
// refresh call.
func newTestWatcher(t *testing.T) (*ConnectionWatcher, *spyPluginManager, chan struct{}) {
	t.Helper()
	spy := newSpyPluginManager()
	refreshed := make(chan struct{}, 4)
	w := &ConnectionWatcher{
		pluginManager: spy,
		refreshConnectionsFunc: func(context.Context, pluginManager, ...string) *steampipeconfig.RefreshConnectionResult {
			select {
			case refreshed <- struct{}{}:
			default:
			}
			return nil
		},
	}
	return w, spy, refreshed
}

// setupConfigDir points the app at a scratch install dir and returns its config dir. It also saves
// and restores the process globals which handleFileWatcherEvent mutates.
func setupConfigDir(t *testing.T) string {
	t.Helper()
	prevInstallDir := app_specific.InstallDir
	prevGlobalConfig := steampipeconfig.GlobalConfig
	app_specific.InstallDir = t.TempDir()
	t.Cleanup(func() {
		app_specific.InstallDir = prevInstallDir
		steampipeconfig.GlobalConfig = prevGlobalConfig
	})
	return pfilepaths.EnsureConfigDir()
}

func writeSpcFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
		t.Fatalf("failed to write %s: %v", name, err)
	}
}

// TestHandleFileWatcherEvent_RefreshesWithPartialConfig is the regression test for #5039: a config
// folder containing one unparseable file must NOT stop the watcher refreshing connections. Before
// the fix, LoadConnectionConfig failed the whole load for the one bad file and the watcher returned
// early, so no connection was ever synced again for as long as that file existed.
func TestHandleFileWatcherEvent_RefreshesWithPartialConfig(t *testing.T) {
	configDir := setupConfigDir(t)
	writeSpcFile(t, configDir, "good.spc", `
connection "conn_good" {
  plugin = "chaos"
}
`)
	// unterminated block - this file cannot be parsed
	writeSpcFile(t, configDir, "bad.spc", `connection "conn_bad" {`)

	w, spy, refreshed := newTestWatcher(t)
	w.handleFileWatcherEvent(nil)

	// the refresh must have been called - this is the #5039 assertion
	select {
	case <-refreshed:
	case <-time.After(5 * time.Second):
		t.Fatal("a single unparseable config file must not stop connections being refreshed")
	}

	// the user must be told about the bad file, but this must not be reported as an error
	select {
	case ew := <-spy.notifications:
		if ew.GetError() != nil {
			t.Errorf("a single unparseable config file must not produce an error, got: %v", ew.GetError())
		}
		if !warningsMention(ew.Warnings, "bad.spc") {
			t.Errorf("expected a warning naming bad.spc, got %v", ew.Warnings)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected a notification describing the unparseable file")
	}

	// the connections which DID parse must be live
	// NOTE: assert on GlobalConfig rather than the ConnectionConfigMap passed to
	// OnConnectionConfigChanged - NewConnectionConfigMap excludes connections which are in error,
	// and in a unit test no plugin is installed so every connection carries an error
	if steampipeconfig.GlobalConfig == nil {
		t.Fatal("GlobalConfig should have been set from the config which loaded successfully")
	}
	if _, ok := steampipeconfig.GlobalConfig.Connections["conn_good"]; !ok {
		t.Error("the connection from the valid file should have loaded")
	}
	if _, ok := steampipeconfig.GlobalConfig.Connections["conn_bad"]; ok {
		t.Error("the connection from the unparseable file must not have loaded")
	}
}

// TestHandleFileWatcherEvent_SkipsRefreshWhenConfigCannotBeLoaded asserts the other half of the
// contract: when NOTHING could be loaded there is no config to refresh with, so the watcher must
// skip the refresh and leave the previously loaded config in effect rather than tearing down every
// connection.
//
// NOTE: this drives the error through a failure to bootstrap the config folder rather than through
// "no config file parses". LoadConnectionConfig runs ensureDefaultConfigFile first, which
// guarantees a parseable default.spc.sample in the real config dir, so a config folder in which
// nothing parses is not reachable from here - that case is covered directly against loadConfig by
// TestLoadConfig_AllFilesUnparseableReturnsError. Both reach this same branch of the watcher.
func TestHandleFileWatcherEvent_SkipsRefreshWhenConfigCannotBeLoaded(t *testing.T) {
	configDir := setupConfigDir(t)
	writeSpcFile(t, configDir, "good.spc", `
connection "conn_good" {
  plugin = "chaos"
}
`)
	// make writing the default config file impossible, so no config can be loaded at all
	if err := os.Mkdir(filepath.Join(configDir, "default.spc.sample"), 0755); err != nil {
		t.Fatalf("failed to set up the config dir: %v", err)
	}

	previousConfig := steampipeconfig.NewSteampipeConfig("")
	steampipeconfig.GlobalConfig = previousConfig

	w, spy, refreshed := newTestWatcher(t)
	w.handleFileWatcherEvent(nil)

	// the notification must report a real error
	select {
	case ew := <-spy.notifications:
		if ew.GetError() == nil {
			t.Errorf("a config which cannot be loaded at all must be reported as an error, got warnings %v", ew.Warnings)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected a notification describing the failure to load any config")
	}

	// the previously loaded config must still be in effect, and no refresh must have been queued
	if steampipeconfig.GlobalConfig != previousConfig {
		t.Error("the previously loaded config must remain in effect when no config can be loaded")
	}
	select {
	case <-refreshed:
		t.Error("connections must not be refreshed when no config can be loaded")
	case <-time.After(250 * time.Millisecond):
	}
}

func warningsMention(warnings []string, substr string) bool {
	for _, w := range warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}
