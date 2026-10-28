// Package config parses and validates MiniRedis's settings.
//
// Precedence is: built-in defaults, then environment variables, then
// command-line flags. Every setting can be reached from either route, which is
// what makes the Docker image configurable without changing the image.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix is prepended to every environment variable name.
const EnvPrefix = "MINIREDIS_"

// Version is the reported server version.
const Version = "0.1.0"

// Config is the fully resolved server configuration.
type Config struct {
	// Addr is the TCP listen address, host:port.
	Addr string
	// DataDir holds the journal and snapshots.
	DataDir string
	// AppendOnly enables the append-only journal.
	AppendOnly bool
	// AppendFsync selects the journal durability mode.
	AppendFsync string
	// JournalRepair selects the damaged-tail policy.
	JournalRepair string
	// SnapshotInterval is the automatic snapshot period; zero disables it.
	SnapshotInterval time.Duration
	// SnapshotOnShutdown writes a snapshot when the server shuts down.
	SnapshotOnShutdown bool

	// ExpireSweepInterval is the active expiration cycle period; zero disables
	// active expiration, leaving only lazy expiration.
	ExpireSweepInterval time.Duration
	// ExpireSampleSize bounds how many entries one cycle examines.
	ExpireSampleSize int

	// MaxClients caps concurrent connections.
	MaxClients int
	// MaxRequestBytes bounds one request's total argument bytes.
	MaxRequestBytes int
	// MaxBulkBytes bounds one bulk string argument.
	MaxBulkBytes int
	// MaxArrayLen bounds the number of arguments in one request.
	MaxArrayLen int
	// MaxLineBytes bounds one protocol line.
	MaxLineBytes int
	// MaxValueBytes bounds a stored string value.
	MaxValueBytes int
	// MaxKeyBytes bounds a key name.
	MaxKeyBytes int
	// MaxHashFields bounds the fields of one hash.
	MaxHashFields int
	// MaxFieldBytes bounds a hash field name or value.
	MaxFieldBytes int
	// MaxJournalRecordBytes bounds one journal record and one snapshot field.
	MaxJournalRecordBytes int

	// IdleTimeout closes a connection with no traffic for this long. Zero
	// disables the idle timeout.
	IdleTimeout time.Duration
	// WriteTimeout bounds how long a single response may take to reach a
	// client, so a client that stops reading cannot pin a goroutine forever.
	WriteTimeout time.Duration

	// RequirePass enables authentication when non-empty. It is never logged
	// and never returned by INFO.
	RequirePass string
	// AllowInsecure permits binding a non-loopback address without a password.
	// It has to be asked for explicitly.
	AllowInsecure bool

	// HTTPAddr enables the HTTP diagnostics endpoint when non-empty.
	HTTPAddr string
	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogValues would include stored values in logs. It exists so the setting
	// is explicit, and it defaults to false because stored data must not leak.
	LogValues bool

	// Version requests the version string instead of starting a server.
	Version bool
}

// Default returns the built-in configuration.
//
// The defaults are deliberately conservative: loopback binding, bounded
// request sizes, and authentication required if the operator binds elsewhere.
func Default() Config {
	return Config{
		Addr:    "127.0.0.1:6379",
		DataDir: "./data",

		AppendOnly:         true,
		AppendFsync:        "everysec",
		JournalRepair:      "truncate",
		SnapshotInterval:   0,
		SnapshotOnShutdown: true,

		ExpireSweepInterval: 100 * time.Millisecond,
		ExpireSampleSize:    20,

		MaxClients:            512,
		MaxRequestBytes:       64 << 20,
		MaxBulkBytes:          64 << 20,
		MaxArrayLen:           1 << 20,
		MaxLineBytes:          64 << 10,
		MaxValueBytes:         64 << 20,
		MaxKeyBytes:           64 << 10,
		MaxHashFields:         1 << 20,
		MaxFieldBytes:         64 << 20,
		MaxJournalRecordBytes: 64 << 20,

		IdleTimeout:  0,
		WriteTimeout: 30 * time.Second,

		HTTPAddr:  "127.0.0.1:7379",
		LogLevel:  "info",
		LogValues: false,
	}
}

// PersistenceEnabled reports whether any durability feature is active.
func (c Config) PersistenceEnabled() bool {
	return c.AppendOnly || c.SnapshotInterval > 0 || c.SnapshotOnShutdown
}

// Parse resolves the configuration from args and the environment.
//
// getenv is injected so that parsing is testable; pass os.Getenv in
// production. A --help request is reported through flag.ErrHelp.
func Parse(args []string, getenv func(string) string) (Config, error) {
	cfg := Default()

	if err := cfg.applyEnv(getenv); err != nil {
		return cfg, err
	}

	fs := flag.NewFlagSet("miniredis", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // usage is printed by the caller

	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "TCP listen address, for example 127.0.0.1:6379")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "directory for the journal and snapshots")
	fs.BoolVar(&cfg.AppendOnly, "appendonly", cfg.AppendOnly, "record mutations in an append-only journal")
	fs.StringVar(&cfg.AppendFsync, "appendfsync", cfg.AppendFsync, "journal durability: always, everysec or no")
	fs.StringVar(&cfg.JournalRepair, "journal-repair", cfg.JournalRepair, "damaged journal tail policy: truncate or fail")
	fs.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "automatic snapshot period, 0 to disable")
	fs.BoolVar(&cfg.SnapshotOnShutdown, "snapshot-on-shutdown", cfg.SnapshotOnShutdown, "write a snapshot during a clean shutdown")

	fs.DurationVar(&cfg.ExpireSweepInterval, "expire-sweep-interval", cfg.ExpireSweepInterval, "active expiration cycle period, 0 to disable")
	fs.IntVar(&cfg.ExpireSampleSize, "expire-sample-size", cfg.ExpireSampleSize, "entries examined per active expiration cycle")

	fs.IntVar(&cfg.MaxClients, "max-clients", cfg.MaxClients, "maximum concurrent client connections")
	fs.IntVar(&cfg.MaxRequestBytes, "max-request-bytes", cfg.MaxRequestBytes, "maximum bytes in one request")
	fs.IntVar(&cfg.MaxBulkBytes, "max-bulk-bytes", cfg.MaxBulkBytes, "maximum bytes in one bulk string argument")
	fs.IntVar(&cfg.MaxArrayLen, "max-array-len", cfg.MaxArrayLen, "maximum number of arguments in one request")
	fs.IntVar(&cfg.MaxLineBytes, "max-line-bytes", cfg.MaxLineBytes, "maximum length of one protocol line")
	fs.IntVar(&cfg.MaxValueBytes, "max-value-bytes", cfg.MaxValueBytes, "maximum size of a stored string value")
	fs.IntVar(&cfg.MaxKeyBytes, "max-key-bytes", cfg.MaxKeyBytes, "maximum size of a key name")
	fs.IntVar(&cfg.MaxHashFields, "max-hash-fields", cfg.MaxHashFields, "maximum fields in one hash")
	fs.IntVar(&cfg.MaxFieldBytes, "max-field-bytes", cfg.MaxFieldBytes, "maximum size of a hash field or value")
	fs.IntVar(&cfg.MaxJournalRecordBytes, "max-journal-record-bytes", cfg.MaxJournalRecordBytes, "maximum size of one journal record")

	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "close idle connections after this long, 0 to disable")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "maximum time to deliver one response")

	fs.StringVar(&cfg.RequirePass, "requirepass", cfg.RequirePass, "require AUTH with this password (never logged)")
	fs.BoolVar(&cfg.AllowInsecure, "allow-insecure", cfg.AllowInsecure, "allow binding a non-loopback address without a password")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP diagnostics listen address, empty to disable")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level: debug, info, warn or error")
	fs.BoolVar(&cfg.LogValues, "log-values", cfg.LogValues, "include stored values in logs (unsafe, off by default)")
	fs.BoolVar(&cfg.Version, "version", cfg.Version, "print the version and exit")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return cfg, cfg.Validate()
}

// applyEnv reads MINIREDIS_* variables over the defaults.
//
// An empty environment variable is indistinguishable from an unset one, so it
// leaves the default in place instead of clearing the setting. Use the matching
// flag, for example --http-addr="", to clear a string setting.
func (c *Config) applyEnv(getenv func(string) string) error {
	stringVar := func(name string, dst *string) {
		if v := getenv(EnvPrefix + name); v != "" {
			*dst = v
		}
	}
	boolVar := func(name string, dst *bool) error {
		raw := getenv(EnvPrefix + name)
		if raw == "" {
			return nil
		}
		v, err := parseBool(raw)
		if err != nil {
			return fmt.Errorf("%s%s: %w", EnvPrefix, name, err)
		}
		*dst = v
		return nil
	}
	intVar := func(name string, dst *int) error {
		raw := getenv(EnvPrefix + name)
		if raw == "" {
			return nil
		}
		v, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("%s%s: %w", EnvPrefix, name, err)
		}
		*dst = v
		return nil
	}
	durationVar := func(name string, dst *time.Duration) error {
		raw := getenv(EnvPrefix + name)
		if raw == "" {
			return nil
		}
		d, err := parseDuration(raw)
		if err != nil {
			return fmt.Errorf("%s%s: %w", EnvPrefix, name, err)
		}
		*dst = d
		return nil
	}

	stringVar("ADDR", &c.Addr)
	stringVar("DATA_DIR", &c.DataDir)
	stringVar("APPENDFSYNC", &c.AppendFsync)
	stringVar("JOURNAL_REPAIR", &c.JournalRepair)
	stringVar("REQUIREPASS", &c.RequirePass)
	stringVar("HTTP_ADDR", &c.HTTPAddr)
	stringVar("LOG_LEVEL", &c.LogLevel)

	for _, err := range []error{
		boolVar("APPENDONLY", &c.AppendOnly),
		boolVar("SNAPSHOT_ON_SHUTDOWN", &c.SnapshotOnShutdown),
		boolVar("ALLOW_INSECURE", &c.AllowInsecure),
		boolVar("LOG_VALUES", &c.LogValues),
		intVar("EXPIRE_SAMPLE_SIZE", &c.ExpireSampleSize),
		intVar("MAX_CLIENTS", &c.MaxClients),
		intVar("MAX_REQUEST_BYTES", &c.MaxRequestBytes),
		intVar("MAX_BULK_BYTES", &c.MaxBulkBytes),
		intVar("MAX_ARRAY_LEN", &c.MaxArrayLen),
		intVar("MAX_LINE_BYTES", &c.MaxLineBytes),
		intVar("MAX_VALUE_BYTES", &c.MaxValueBytes),
		intVar("MAX_KEY_BYTES", &c.MaxKeyBytes),
		intVar("MAX_HASH_FIELDS", &c.MaxHashFields),
		intVar("MAX_FIELD_BYTES", &c.MaxFieldBytes),
		intVar("MAX_JOURNAL_RECORD_BYTES", &c.MaxJournalRecordBytes),
		durationVar("SNAPSHOT_INTERVAL", &c.SnapshotInterval),
		durationVar("EXPIRE_SWEEP_INTERVAL", &c.ExpireSweepInterval),
		durationVar("IDLE_TIMEOUT", &c.IdleTimeout),
		durationVar("WRITE_TIMEOUT", &c.WriteTimeout),
	} {
		if err != nil {
			return err
		}
	}
	return nil
}

// parseBool accepts the usual spellings plus 1/0.
func parseBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean %q", raw)
	}
}

// parseDuration accepts a Go duration or a bare number of seconds, which is
// what operators usually write in an environment variable.
func parseDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("empty duration")
	}
	if n, err := strconv.Atoi(raw); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	return time.ParseDuration(raw)
}

// Validate checks the configuration and normalises derived values.
func (c *Config) Validate() error {
	host, port, err := net.SplitHostPort(c.Addr)
	if err != nil {
		return fmt.Errorf("invalid addr %q: %w", c.Addr, err)
	}
	// Port 0 asks the operating system for an ephemeral port, which is what
	// tests use; any other port must be in range.
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("invalid port %q in addr %q", port, c.Addr)
	}
	if !isLoopbackHost(host) && c.RequirePass == "" && !c.AllowInsecure {
		return fmt.Errorf("addr %q is not a loopback address: set --requirepass, or pass --allow-insecure to accept an unauthenticated public listener", c.Addr)
	}

	if c.DataDir == "" && c.PersistenceEnabled() {
		return errors.New("persistence requires a data directory; set --data-dir or disable persistence")
	}

	switch c.AppendFsync {
	case "always", "everysec", "no":
	default:
		return fmt.Errorf("invalid appendfsync %q: want always, everysec or no", c.AppendFsync)
	}
	switch c.JournalRepair {
	case "truncate", "fail":
	default:
		return fmt.Errorf("invalid journal-repair %q: want truncate or fail", c.JournalRepair)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid log-level %q: want debug, info, warn or error", c.LogLevel)
	}

	limits := []struct {
		name  string
		value int
		max   int
	}{
		{"max-clients", c.MaxClients, 1 << 20},
		{"max-request-bytes", c.MaxRequestBytes, 1 << 30},
		{"max-bulk-bytes", c.MaxBulkBytes, 512 << 20},
		{"max-array-len", c.MaxArrayLen, 1 << 22},
		{"max-line-bytes", c.MaxLineBytes, 1 << 20},
		{"max-value-bytes", c.MaxValueBytes, 1 << 30},
		{"max-key-bytes", c.MaxKeyBytes, 1 << 20},
		{"max-hash-fields", c.MaxHashFields, 1 << 24},
		{"max-field-bytes", c.MaxFieldBytes, 1 << 30},
		{"max-journal-record-bytes", c.MaxJournalRecordBytes, 1 << 30},
		{"expire-sample-size", c.ExpireSampleSize, 1 << 22},
	}
	for _, l := range limits {
		if l.value <= 0 {
			return fmt.Errorf("invalid %s: must be positive, got %d", l.name, l.value)
		}
		if l.value > l.max {
			return fmt.Errorf("invalid %s: %d exceeds the supported maximum of %d", l.name, l.value, l.max)
		}
	}

	if c.SnapshotInterval < 0 {
		return errors.New("invalid snapshot-interval: must not be negative")
	}
	if c.ExpireSweepInterval < 0 {
		return errors.New("invalid expire-sweep-interval: must not be negative")
	}
	if c.IdleTimeout < 0 {
		return errors.New("invalid idle-timeout: must not be negative")
	}
	if c.WriteTimeout < 0 {
		return errors.New("invalid write-timeout: must not be negative")
	}

	if c.HTTPAddr != "" {
		h, p, err := net.SplitHostPort(c.HTTPAddr)
		if err != nil {
			return fmt.Errorf("invalid http-addr %q: %w", c.HTTPAddr, err)
		}
		port, err := strconv.Atoi(p)
		if err != nil || port < 0 || port > 65535 {
			return fmt.Errorf("invalid port %q in http-addr %q", p, c.HTTPAddr)
		}
		if !isLoopbackHost(h) && c.RequirePass == "" && !c.AllowInsecure {
			return fmt.Errorf("http-addr %q is not a loopback address: set --requirepass or pass --allow-insecure", c.HTTPAddr)
		}
	}

	// A request cannot contain a bulk string larger than the request itself.
	if c.MaxBulkBytes > c.MaxRequestBytes {
		c.MaxRequestBytes = c.MaxBulkBytes
	}
	return nil
}

// isLoopbackHost reports whether a host resolves to the loopback interface.
// An empty host or a wildcard address is not loopback: it is every interface.
func isLoopbackHost(host string) bool {
	switch host {
	case "":
		return false
	case "localhost":
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

// IsLoopbackAddr reports whether addr binds only the loopback interface.
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return isLoopbackHost(host)
}

// Usage returns the human-readable help text.
func (c Config) Usage() string {
	var b strings.Builder
	b.WriteString("MiniRedis is a Redis-inspired in-memory database server written in Go.\n\n")
	b.WriteString("Usage:\n  miniredis [flags]\n\n")
	b.WriteString("Every flag can also be set through an environment variable with the\n")
	b.WriteString(EnvPrefix + " prefix and the flag name upper-cased, with dashes replaced\nby underscores (for example " + EnvPrefix + "ADDR, " + EnvPrefix + "MAX_CLIENTS).\n\n")
	b.WriteString("Flags:\n")
	b.WriteString("  --addr string                    TCP listen address (default 127.0.0.1:6379)\n")
	b.WriteString("  --data-dir string                directory for the journal and snapshots (default ./data)\n")
	b.WriteString("  --appendonly                     record every mutation in an append-only journal (default true)\n")
	b.WriteString("  --appendfsync string             journal durability: always, everysec or no (default everysec)\n")
	b.WriteString("  --journal-repair string          damaged journal tail policy: truncate or fail (default truncate)\n")
	b.WriteString("  --snapshot-interval duration     write a snapshot every interval, 0 to disable (default 0)\n")
	b.WriteString("  --snapshot-on-shutdown           write a snapshot during a clean shutdown (default true)\n")
	b.WriteString("  --expire-sweep-interval duration active expiration cycle period, 0 to disable (default 100ms)\n")
	b.WriteString("  --expire-sample-size int         entries examined per expiration cycle (default 20)\n")
	b.WriteString("  --max-clients int                maximum concurrent connections (default 512)\n")
	b.WriteString("  --max-request-bytes int          maximum bytes in one request (default 67108864)\n")
	b.WriteString("  --max-bulk-bytes int             maximum bytes in one bulk string (default 67108864)\n")
	b.WriteString("  --max-array-len int              maximum arguments per request (default 1048576)\n")
	b.WriteString("  --max-line-bytes int             maximum protocol line length (default 65536)\n")
	b.WriteString("  --max-value-bytes int            maximum stored string value size (default 67108864)\n")
	b.WriteString("  --max-key-bytes int              maximum key name size (default 65536)\n")
	b.WriteString("  --max-hash-fields int            maximum fields in one hash (default 1048576)\n")
	b.WriteString("  --max-field-bytes int            maximum hash field or value size (default 67108864)\n")
	b.WriteString("  --max-journal-record-bytes int   maximum journal record size (default 67108864)\n")
	b.WriteString("  --idle-timeout duration          close idle connections after this long, 0 disables (default 0)\n")
	b.WriteString("  --write-timeout duration         maximum time to deliver one response (default 30s)\n")
	b.WriteString("  --requirepass string             require AUTH with this password (default empty)\n")
	b.WriteString("  --allow-insecure                 allow a non-loopback listener without a password (default false)\n")
	b.WriteString("  --http-addr string               HTTP diagnostics address, empty disables (default 127.0.0.1:7379)\n")
	b.WriteString("  --log-level string               debug, info, warn or error (default info)\n")
	b.WriteString("  --log-values                     include stored values in logs (default false, unsafe)\n")
	b.WriteString("  --version                        print the version and exit\n")
	b.WriteString("\nAuthentication is a plain password over an unencrypted TCP connection; it is\n")
	b.WriteString("not a substitute for TLS or a private network. See docs/SECURITY.md.\n")
	return b.String()
}

// SlogLevel maps LogLevel to its numeric form for the logger setup.
func (c Config) SlogLevel() int {
	switch c.LogLevel {
	case "debug":
		return -4
	case "info":
		return 0
	case "warn":
		return 4
	case "error":
		return 8
	default:
		return 0
	}
}

// Redacted returns a copy safe to log: the password is replaced.
func (c Config) Redacted() Config {
	out := c
	if out.RequirePass != "" {
		out.RequirePass = "[redacted]"
	}
	return out
}
