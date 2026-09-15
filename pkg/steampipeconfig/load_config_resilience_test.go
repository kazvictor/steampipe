package steampipeconfig

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/versionfile"
)

// setInstallDir points the app at a scratch install dir; several filepath
// helpers panic if it is unset.
func setInstallDir(t *testing.T) {
	t.Helper()
	prev := app_specific.InstallDir
	app_specific.InstallDir = t.TempDir()
	t.Cleanup(func() { app_specific.InstallDir = prev })
}

// writeConfigFile writes a .spc file into dir.
func writeConfigFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
		t.Fatalf("failed to write %s: %v", name, err)
	}
}

func connectionConfig(name string) string {
	return `
connection "` + name + `" {
  plugin = "chaos"
}
`
}

// TestLoadConfig_DuplicateConnectionDoesNotFailWholeLoad asserts that a single
// duplicate connection name is reported as a warning and skipped, while every
// other connection still loads.
//
// This matters because loadConfig re-parses the WHOLE config folder and the
// connection watcher aborts the refresh when it returns an error: failing the
// entire load for one bad block stops all connections from being synced (no
// schemas created, no connection config updates applied) for as long as the
// offending block exists, while the file watcher keeps running.
func TestLoadConfig_DuplicateConnectionDoesNotFailWholeLoad(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "a.spc", connectionConfig("conn_a"))
	writeConfigFile(t, dir, "b.spc", connectionConfig("conn_b"))
	// duplicates conn_a, declared in a second file
	writeConfigFile(t, dir, "c.spc", connectionConfig("conn_a"))

	config := NewSteampipeConfig("")
	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{include: []string{"*.spc"}})

	if err := res.GetError(); err != nil {
		t.Fatalf("a duplicate connection must not fail the whole config load, got error: %v", err)
	}
	if len(res.Warnings) == 0 {
		t.Error("expected a warning describing the duplicate connection")
	}
	for _, name := range []string{"conn_a", "conn_b"} {
		if _, ok := config.Connections[name]; !ok {
			t.Errorf("connection %q should still have loaded despite the duplicate", name)
		}
	}
}

// TestLoadConfig_InvalidConnectionNameDoesNotFailWholeLoad asserts the same
// contract for a connection whose name is not a valid schema name.
func TestLoadConfig_InvalidConnectionNameDoesNotFailWholeLoad(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "good.spc", connectionConfig("conn_good"))
	// "pg_catalog" is reserved, so this connection name is invalid
	writeConfigFile(t, dir, "bad.spc", connectionConfig("pg_catalog"))

	config := NewSteampipeConfig("")
	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{include: []string{"*.spc"}})

	if err := res.GetError(); err != nil {
		t.Fatalf("an invalid connection name must not fail the whole config load, got error: %v", err)
	}
	if _, ok := config.Connections["conn_good"]; !ok {
		t.Error("the valid connection should still have loaded")
	}
	if _, ok := config.Connections["pg_catalog"]; ok {
		t.Error("the invalid connection must be skipped")
	}
}

// TestLoadConfig_MalformedFileDoesNotFailWholeLoad asserts that a file which
// cannot be parsed at all disables only the contents of that file - every other
// file in the config folder still loads.
//
// loadConfig re-parses the WHOLE config folder on every file watcher event, so
// failing the load for one malformed file stops ALL connections from being
// synced for as long as that file exists. See #5039.
func TestLoadConfig_MalformedFileDoesNotFailWholeLoad(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "good.spc", connectionConfig("conn_good"))
	// unterminated block - this file cannot be parsed
	writeConfigFile(t, dir, "bad.spc", `connection "conn_bad" {`)

	config := NewSteampipeConfig("")
	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{include: []string{"*.spc"}})

	if err := res.GetError(); err != nil {
		t.Fatalf("a malformed config file must not fail the whole config load, got error: %v", err)
	}
	if len(res.Warnings) == 0 {
		t.Error("expected a warning describing the malformed file")
	}
	if _, ok := config.Connections["conn_good"]; !ok {
		t.Error("the connection in the valid file should still have loaded")
	}
	if _, ok := config.Connections["conn_bad"]; ok {
		t.Error("the connection in the malformed file must be skipped")
	}
}

// TestLoadConfig_DuplicatePluginInstanceDoesNotFailWholeLoad asserts that a
// duplicate plugin instance label is reported as a warning and skipped, while
// the rest of the config still loads.
func TestLoadConfig_DuplicatePluginInstanceDoesNotFailWholeLoad(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "plugins.spc", `
plugin "chaos" {
  memory_max_mb = 2048
}

plugin "chaos" {
  memory_max_mb = 4096
}
`)
	writeConfigFile(t, dir, "conn.spc", connectionConfig("conn_good"))

	config := NewSteampipeConfig("")
	// NOTE: addPlugin returns early (without detecting the duplicate) for a plugin which is not
	// installed, so the plugin must be present in the version map for this test to be meaningful
	config.PluginVersions = map[string]*versionfile.InstalledVersion{
		plugin.ResolvePluginImageRef("chaos"): {
			Name:    "chaos",
			Version: "1.0.0",
		},
	}

	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{include: []string{"*.spc"}})

	if err := res.GetError(); err != nil {
		t.Fatalf("a duplicate plugin instance must not fail the whole config load, got error: %v", err)
	}
	if !hasWarningContaining(res.Warnings, "duplicate plugin instance") {
		t.Errorf("expected a warning describing the duplicate plugin instance, got %v", res.Warnings)
	}
	if _, ok := config.Connections["conn_good"]; !ok {
		t.Error("the connection should still have loaded despite the duplicate plugin instance")
	}
	// the first instance is kept
	instance, ok := config.PluginsInstances["chaos"]
	if !ok {
		t.Fatal("the first plugin instance should have been kept")
	}
	if instance.MemoryMaxMb == nil || *instance.MemoryMaxMb != 2048 {
		t.Errorf("expected the FIRST plugin instance to be kept (memory_max_mb 2048), got %v", instance.MemoryMaxMb)
	}
}

// TestLoadConfig_DuplicateOptionsBlockDoesNotFailWholeLoad asserts that a
// duplicated options block is reported as a warning and skipped, while the rest
// of the config still loads.
func TestLoadConfig_DuplicateOptionsBlockDoesNotFailWholeLoad(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "options.spc", `
options "database" {
  port = 9193
}

options "database" {
  port = 9194
}
`)
	writeConfigFile(t, dir, "conn.spc", connectionConfig("conn_good"))

	config := NewSteampipeConfig("")
	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{include: []string{"*.spc"}})

	if err := res.GetError(); err != nil {
		t.Fatalf("a duplicate options block must not fail the whole config load, got error: %v", err)
	}
	if !hasWarningContaining(res.Warnings, "multiple instances of 'database' options block") {
		t.Errorf("expected a warning describing the duplicate options block, got %v", res.Warnings)
	}
	if _, ok := config.Connections["conn_good"]; !ok {
		t.Error("the connection should still have loaded despite the duplicate options block")
	}
}

// TestLoadConfig_DisallowedOptionsBlockDoesNotFailWholeLoad asserts that an
// options block which is not permitted in this folder is reported as a warning
// and skipped, while the rest of the config still loads.
func TestLoadConfig_DisallowedOptionsBlockDoesNotFailWholeLoad(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "options.spc", `
options "general" {
  update_check = "true"
}
`)
	writeConfigFile(t, dir, "conn.spc", connectionConfig("conn_good"))

	config := NewSteampipeConfig("")
	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{
		include:        []string{"*.spc"},
		allowedOptions: []string{"database"},
	})

	if err := res.GetError(); err != nil {
		t.Fatalf("a disallowed options block must not fail the whole config load, got error: %v", err)
	}
	if !hasWarningContaining(res.Warnings, "'general' options block is not permitted") {
		t.Errorf("expected a warning describing the disallowed options block, got %v", res.Warnings)
	}
	if _, ok := config.Connections["conn_good"]; !ok {
		t.Error("the connection should still have loaded despite the disallowed options block")
	}
}

// TestLoadConfig_AllFilesUnparseableReturnsError asserts that a config folder in
// which NOTHING parses is still a hard error. Callers must be able to tell this
// apart from a partial load: there is no config to refresh with, so the
// connection watcher leaves the previously loaded config in effect rather than
// tearing down every connection.
func TestLoadConfig_AllFilesUnparseableReturnsError(t *testing.T) {
	setInstallDir(t)
	dir := t.TempDir()
	writeConfigFile(t, dir, "a.spc", `connection "conn_a" {`)
	writeConfigFile(t, dir, "b.spc", `this is not valid hcl !!`)

	config := NewSteampipeConfig("")
	res := loadConfig(context.TODO(), dir, config, &loadConfigOptions{include: []string{"*.spc"}})

	if res.GetError() == nil {
		t.Fatal("a config folder in which no file parses must return an error")
	}
	if len(config.Connections) != 0 {
		t.Errorf("expected no connections to be loaded, got %v", config.Connections)
	}
	// the warnings must still describe each file, so the user can tell what to fix
	for _, name := range []string{"a.spc", "b.spc"} {
		if !hasWarningContaining(res.Warnings, name) {
			t.Errorf("expected a warning naming %q, got %v", name, res.Warnings)
		}
	}
}

func hasWarningContaining(warnings []string, substr string) bool {
	for _, w := range warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}
