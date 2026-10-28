package config

import "log/slog"

// LogAttrs renders the configuration for logging.
//
// It exists so that a server can log its effective settings without ever
// printing the password: RequirePass is reduced to a boolean. It is a separate
// file so that the config package's core stays free of logging concerns.
func (c Config) LogAttrs() slog.Value {
	return slog.GroupValue(
		slog.String("addr", c.Addr),
		slog.String("data_dir", c.DataDir),
		slog.Bool("appendonly", c.AppendOnly),
		slog.String("appendfsync", c.AppendFsync),
		slog.String("journal_repair", c.JournalRepair),
		slog.String("snapshot_interval", c.SnapshotInterval.String()),
		slog.Bool("snapshot_on_shutdown", c.SnapshotOnShutdown),
		slog.String("expire_sweep_interval", c.ExpireSweepInterval.String()),
		slog.Int("expire_sample_size", c.ExpireSampleSize),
		slog.Int("max_clients", c.MaxClients),
		slog.Int("max_request_bytes", c.MaxRequestBytes),
		slog.Int("max_value_bytes", c.MaxValueBytes),
		slog.Duration("idle_timeout", c.IdleTimeout),
		slog.Duration("write_timeout", c.WriteTimeout),
		// The password itself is never logged, only whether one is set.
		slog.Bool("auth_required", c.RequirePass != ""),
		slog.Bool("allow_insecure", c.AllowInsecure),
		slog.String("http_addr", c.HTTPAddr),
		slog.String("log_level", c.LogLevel),
		slog.Bool("log_values", c.LogValues),
	)
}
