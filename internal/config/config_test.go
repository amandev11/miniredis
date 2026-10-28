package config_test

import (
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/config"
)

// env builds a getenv function from a map, so tests never touch the real
// process environment.
func env(pairs map[string]string) func(string) string {
	return func(key string) string { return pairs[key] }
}

func TestDefaults(t *testing.T) {
	cfg, err := config.Parse(nil, env(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Addr != "127.0.0.1:6379" {
		t.Errorf("Addr = %q, want the loopback default", cfg.Addr)
	}
	if !config.IsLoopbackAddr(cfg.Addr) {
		t.Error("the default address must be a loopback address")
	}
	if !cfg.AppendOnly {
		t.Error("the journal should be enabled by default")
	}
	if cfg.AppendFsync != "everysec" {
		t.Errorf("AppendFsync = %q, want everysec", cfg.AppendFsync)
	}
	if cfg.RequirePass != "" {
		t.Error("no password should be configured by default")
	}
	if cfg.LogValues {
		t.Error("stored values must not be logged by default")
	}
	if cfg.MaxClients <= 0 || cfg.MaxRequestBytes <= 0 || cfg.MaxValueBytes <= 0 {
		t.Error("limits must have sensible defaults")
	}
	if !cfg.PersistenceEnabled() {
		t.Error("persistence should be enabled by default")
	}
}

func TestEnvironmentOverridesDefaults(t *testing.T) {
	cfg, err := config.Parse(nil, env(map[string]string{
		"MINIREDIS_ADDR":         "127.0.0.1:7777",
		"MINIREDIS_DATA_DIR":     "/tmp/miniredis-test",
		"MINIREDIS_APPENDONLY":   "false",
		"MINIREDIS_APPENDFSYNC":  "always",
		"MINIREDIS_MAX_CLIENTS":  "7",
		"MINIREDIS_IDLE_TIMEOUT": "45",
		"MINIREDIS_LOG_LEVEL":    "debug",
		"MINIREDIS_PASSWORD":     "",
		"MINIREDIS_REQUIREPASS":  "from-env",
	}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Addr != "127.0.0.1:7777" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.DataDir != "/tmp/miniredis-test" {
		t.Errorf("DataDir = %q", cfg.DataDir)
	}
	if cfg.AppendOnly {
		t.Error("appendonly was not disabled by the environment")
	}
	if cfg.AppendFsync != "always" {
		t.Errorf("AppendFsync = %q", cfg.AppendFsync)
	}
	if cfg.MaxClients != 7 {
		t.Errorf("MaxClients = %d", cfg.MaxClients)
	}
	// A bare number of seconds is accepted for durations.
	if cfg.IdleTimeout != 45*time.Second {
		t.Errorf("IdleTimeout = %v, want 45s", cfg.IdleTimeout)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
	if cfg.RequirePass != "from-env" {
		t.Errorf("RequirePass = %q", cfg.RequirePass)
	}
}

// TestFlagsCanDisableAStringSetting documents that an empty environment value
// leaves the default in place, so clearing a setting needs the flag form.
func TestFlagsCanDisableAStringSetting(t *testing.T) {
	cfg, err := config.Parse([]string{"--http-addr", "", "--data-dir", "/tmp/x"}, env(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.HTTPAddr != "" {
		t.Fatalf("HTTPAddr = %q, want the flag to disable it", cfg.HTTPAddr)
	}
}

func TestFlagsOverrideEnvironment(t *testing.T) {
	cfg, err := config.Parse([]string{
		"--addr", "127.0.0.1:8888",
		"--max-clients", "99",
		"--appendfsync", "no",
		"--snapshot-interval", "2m",
	}, env(map[string]string{
		"MINIREDIS_ADDR":        "127.0.0.1:7777",
		"MINIREDIS_MAX_CLIENTS": "7",
	}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Addr != "127.0.0.1:8888" {
		t.Errorf("Addr = %q: a flag must beat the environment", cfg.Addr)
	}
	if cfg.MaxClients != 99 {
		t.Errorf("MaxClients = %d: a flag must beat the environment", cfg.MaxClients)
	}
	if cfg.AppendFsync != "no" {
		t.Errorf("AppendFsync = %q", cfg.AppendFsync)
	}
	if cfg.SnapshotInterval != 2*time.Minute {
		t.Errorf("SnapshotInterval = %v", cfg.SnapshotInterval)
	}
}

func TestHelpIsReportedWithoutError(t *testing.T) {
	_, err := config.Parse([]string{"--help"}, env(nil))
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("got %v, want flag.ErrHelp", err)
	}

	usage := config.Default().Usage()
	for _, want := range []string{"--addr", "--data-dir", "--appendfsync", "--requirepass", "MINIREDIS_"} {
		if !strings.Contains(usage, want) {
			t.Errorf("usage text does not mention %q", want)
		}
	}
	if strings.Contains(usage, "password for") {
		t.Error("usage text should not imply a password value is printed")
	}
}

func TestUnexpectedArgumentIsRejected(t *testing.T) {
	if _, err := config.Parse([]string{"extra"}, env(nil)); err == nil {
		t.Fatal("a positional argument was accepted")
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"bad addr", []string{"--addr", "not-an-address"}, nil},
		{"missing port", []string{"--addr", "127.0.0.1"}, nil},
		{"port out of range", []string{"--addr", "127.0.0.1:70000"}, nil},
		{"port not numeric", []string{"--addr", "127.0.0.1:http"}, nil},
		{"unknown fsync", []string{"--appendfsync", "sometimes"}, nil},
		{"unknown repair policy", []string{"--journal-repair", "maybe"}, nil},
		{"unknown log level", []string{"--log-level", "loud"}, nil},
		{"zero max-clients", []string{"--max-clients", "0"}, nil},
		{"negative max-clients", []string{"--max-clients", "-1"}, nil},
		{"absurd max-clients", []string{"--max-clients", "99999999"}, nil},
		{"zero max-value-bytes", []string{"--max-value-bytes", "0"}, nil},
		{"negative snapshot interval", []string{"--snapshot-interval", "-1s"}, nil},
		{"negative idle timeout", []string{"--idle-timeout", "-1s"}, nil},
		{"bad http addr", []string{"--http-addr", "127.0.0.1:99999"}, nil},
		{"no data dir with persistence", []string{"--data-dir", ""}, nil},
		{"bad boolean in env", nil, map[string]string{"MINIREDIS_APPENDONLY": "perhaps"}},
		{"bad int in env", nil, map[string]string{"MINIREDIS_MAX_CLIENTS": "many"}},
		{"bad duration in env", nil, map[string]string{"MINIREDIS_IDLE_TIMEOUT": "soon"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := config.Parse(tc.args, env(tc.env)); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

// TestNonLoopbackRequiresAuthentication is the main security guard in the
// configuration layer.
func TestNonLoopbackRequiresAuthentication(t *testing.T) {
	base := []string{"--data-dir", "/tmp/miniredis-test"}

	for _, addr := range []string{"0.0.0.0:6379", "[::]:6379", ":6379", "10.0.0.5:6379"} {
		t.Run(addr, func(t *testing.T) {
			args := append(append([]string{}, base...), "--addr", addr)

			if _, err := config.Parse(args, env(nil)); err == nil {
				t.Fatal("binding a public address without a password was allowed")
			}

			withPassword := append(append([]string{}, args...), "--requirepass", "letmein")
			if _, err := config.Parse(withPassword, env(nil)); err != nil {
				t.Fatalf("a password should make it valid: %v", err)
			}

			insecure := append(append([]string{}, args...), "--allow-insecure")
			if _, err := config.Parse(insecure, env(nil)); err != nil {
				t.Fatalf("--allow-insecure should make it valid: %v", err)
			}
		})
	}
}

func TestLoopbackAddressesAreAccepted(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:6379", "localhost:6379", "[::1]:6379"} {
		args := []string{"--addr", addr, "--data-dir", "/tmp/miniredis-test"}
		if _, err := config.Parse(args, env(nil)); err != nil {
			t.Errorf("loopback address %q was rejected: %v", addr, err)
		}
	}
}

func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:6379": true,
		"localhost:6379": true,
		"[::1]:6379":     true,
		"0.0.0.0:6379":   false,
		"[::]:6379":      false,
		":6379":          false,
		"garbage":        false,
	}
	for addr, want := range cases {
		if got := config.IsLoopbackAddr(addr); got != want {
			t.Errorf("IsLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestRedactedHidesPassword(t *testing.T) {
	cfg, err := config.Parse([]string{
		"--data-dir", "/tmp/x",
		"--requirepass", "hunter2",
	}, env(nil))
	if err != nil {
		t.Fatal(err)
	}

	red := cfg.Redacted()
	if red.RequirePass == "hunter2" {
		t.Fatal("Redacted left the password in place")
	}
	if cfg.RequirePass != "hunter2" {
		t.Error("Redacted must not modify the original configuration")
	}

	// LogAttrs must never render the password value.
	rendered := cfg.LogAttrs().String()
	if strings.Contains(rendered, "hunter2") {
		t.Fatalf("LogAttrs exposed the password: %s", rendered)
	}
	if !strings.Contains(rendered, "auth_required=true") {
		t.Errorf("LogAttrs should report that authentication is required: %s", rendered)
	}
}

func TestPersistenceEnabled(t *testing.T) {
	cases := []struct {
		name string
		cfg  config.Config
		want bool
	}{
		{"journal only", config.Config{AppendOnly: true}, true},
		{"snapshot interval", config.Config{SnapshotInterval: time.Minute}, true},
		{"snapshot on shutdown", config.Config{SnapshotOnShutdown: true}, true},
		{"everything off", config.Config{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.PersistenceEnabled(); got != tc.want {
				t.Fatalf("PersistenceEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBulkLimitIsRaisedToTheRequestLimit(t *testing.T) {
	// A bulk string cannot be larger than the request that carries it, so the
	// request limit is raised instead of the bulk limit being silently
	// violated.
	cfg, err := config.Parse([]string{
		"--data-dir", "/tmp/x",
		"--max-request-bytes", "1024",
		"--max-bulk-bytes", "4096",
	}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRequestBytes < cfg.MaxBulkBytes {
		t.Fatalf("MaxRequestBytes = %d, MaxBulkBytes = %d", cfg.MaxRequestBytes, cfg.MaxBulkBytes)
	}
}
