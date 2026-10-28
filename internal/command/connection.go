package command

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/amandev11/miniredis/internal/protocol"
)

// registerConnectionCommands installs the connection and diagnostic commands.
func registerConnectionCommands(r *Registry) {
	r.Register(&Spec{
		Name: "PING", Arity: -1, Flags: FlagConnection | FlagNoAuth | FlagReadOnly,
		Hint: "[message]", Summary: "Reply with PONG, or echo the message.",
		Handler: cmdPing,
	})
	r.Register(&Spec{
		Name: "ECHO", Arity: 2, Flags: FlagConnection | FlagNoAuth | FlagReadOnly,
		Hint: "message", Summary: "Reply with the message.",
		Handler: cmdEcho,
	})
	r.Register(&Spec{
		Name: "QUIT", Arity: 1, Flags: FlagConnection | FlagNoAuth,
		Hint: "", Summary: "Reply OK and close the connection.",
		Handler: cmdQuit,
	})
	r.Register(&Spec{
		Name: "AUTH", Arity: -2, Flags: FlagConnection | FlagNoAuth,
		Hint: "[username] password", Summary: "Authenticate this connection when a password is configured.",
		Handler: cmdAuth,
	})
	r.Register(&Spec{
		Name: "SELECT", Arity: 2, Flags: FlagConnection | FlagNoAuth | FlagReadOnly,
		Hint: "index", Summary: "Select the database. MiniRedis exposes only database 0.",
		Handler: cmdSelect,
	})
	r.Register(&Spec{
		Name: "INFO", Arity: -1, Flags: FlagServer | FlagReadOnly,
		Hint: "[section]", Summary: "Report measured server, keyspace, persistence and command statistics.",
		Handler: cmdInfo,
	})
	r.Register(&Spec{
		Name: "SHUTDOWN", Arity: -1, Flags: FlagServer | FlagAdmin,
		Hint: "[NOSAVE|SAVE]", Summary: "Reply OK and stop the server.",
		Handler: cmdShutdown,
	})
}

func cmdPing(c *Context) (protocol.Value, error) {
	switch c.NumArgs() {
	case 0:
		return protocol.Simple("PONG"), nil
	case 1:
		return protocol.Bulk(c.Arg(0)), nil
	default:
		return protocol.Value{}, errorf("ERR wrong number of arguments for 'ping' command")
	}
}

func cmdEcho(c *Context) (protocol.Value, error) {
	return protocol.Bulk(c.Arg(0)), nil
}

func cmdQuit(c *Context) (protocol.Value, error) {
	c.CloseAfterReply = true
	return protocol.Simple("OK"), nil
}

// cmdAuth authenticates the connection.
//
// Two forms are accepted, matching Redis 6+: "AUTH password" and
// "AUTH username password". MiniRedis has a single implicit user, so the
// username is accepted but only "default" is recognised; anything else is a
// failure rather than a silently ignored field.
func cmdAuth(c *Context) (protocol.Value, error) {
	if !c.AuthRequired {
		return protocol.Value{}, &Error{
			Message: "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
		}
	}
	if c.Runtime == nil {
		return protocol.Value{}, &Error{Message: "ERR authentication is unavailable on this server"}
	}

	var password []byte
	switch c.NumArgs() {
	case 1:
		password = c.Arg(0)
	case 2:
		if string(c.Arg(0)) != "default" {
			return protocol.Value{}, ErrWrongPass
		}
		password = c.Arg(1)
	default:
		return protocol.Value{}, errorf("ERR wrong number of arguments for 'auth' command")
	}

	if !c.Runtime.CheckPassword(password) {
		return protocol.Value{}, ErrWrongPass
	}
	c.Authenticated = true
	return protocol.Simple("OK"), nil
}

func cmdSelect(c *Context) (protocol.Value, error) {
	n, err := argInt("index", c.Arg(0))
	if err != nil {
		return protocol.Value{}, errorf("ERR invalid DB index")
	}
	if n != 0 {
		return protocol.Value{}, errorf("ERR DB index is out of range: MiniRedis exposes only database 0")
	}
	return protocol.Simple("OK"), nil
}

func cmdShutdown(c *Context) (protocol.Value, error) {
	if c.NumArgs() > 1 {
		return protocol.Value{}, errorf("ERR syntax error")
	}
	if c.NumArgs() == 1 {
		switch upperName(c.Arg(0)) {
		case "NOSAVE", "SAVE":
			// Accepted for compatibility. Persistence behaviour is owned by
			// the server configuration, not by a client command.
		default:
			return protocol.Value{}, errorf("ERR syntax error")
		}
	}
	c.CloseAfterReply = true
	if c.Runtime != nil {
		c.Runtime.RequestShutdown()
	}
	return protocol.Simple("OK"), nil
}

// cmdInfo renders INFO. Every number comes from the running server.
func cmdInfo(c *Context) (protocol.Value, error) {
	section := "default"
	if c.NumArgs() >= 1 && len(c.Arg(0)) > 0 {
		section = strings.ToLower(string(c.Arg(0)))
	}

	info := fallbackInfo()
	if c.Runtime != nil {
		info = c.Runtime.ServerInfo()
	}

	var b strings.Builder
	write := func(name string, fn func(*strings.Builder)) {
		switch section {
		case "all":
			fn(&b)
		case "default":
			if name != "commandstats" {
				fn(&b)
			}
		default:
			if section == name {
				fn(&b)
			}
		}
	}

	write("server", func(b *strings.Builder) {
		sectionHeader(b, "Server")
		kv(b, "miniredis_version", info.Version)
		kv(b, "miniredis_mode", info.Mode)
		kv(b, "go_version", info.GoVersion)
		kv(b, "os", info.OS)
		kv(b, "arch", info.Arch)
		kv(b, "process_id", strconv.Itoa(info.ProcessID))
		kv(b, "tcp_bind", info.Bind)
		kv(b, "tcp_port", strconv.Itoa(info.Port))
		kv(b, "uptime_in_seconds", strconv.FormatInt(info.UptimeSeconds, 10))
		kv(b, "data_dir", info.DataDir)
		kv(b, "command_count", strconv.Itoa(registryOf(c).Len()))
	})

	write("clients", func(b *strings.Builder) {
		sectionHeader(b, "Clients")
		kv(b, "connected_clients", strconv.Itoa(info.Clients.ConnectedClients))
		kv(b, "max_clients", strconv.Itoa(info.Clients.MaxClients))
		kv(b, "total_connections_received", strconv.FormatUint(info.Clients.TotalConnections, 10))
		kv(b, "rejected_connections", strconv.FormatUint(info.Clients.RejectedConnections, 10))
	})

	write("memory", func(b *strings.Builder) {
		sectionHeader(b, "Memory")
		kv(b, "keyspace_payload_bytes_approx", strconv.FormatInt(info.Store.ApproxBytes, 10))
		kv(b, "go_heap_alloc_bytes", strconv.FormatUint(info.Store.HeapAllocBytes, 10))
		kv(b, "go_heap_sys_bytes", strconv.FormatUint(info.Store.HeapSysBytes, 10))
		b.WriteString("# Memory figures are process measurements; keyspace_payload_bytes_approx\n")
		b.WriteString("# excludes Go map overhead and is therefore a lower bound.\n")
	})

	write("persistence", func(b *strings.Builder) {
		p := info.Persistence
		sectionHeader(b, "Persistence")
		kv(b, "persistence_enabled", boolInt(p.Enabled))
		kv(b, "journal_enabled", boolInt(p.JournalEnabled))
		kv(b, "appendfsync", p.FsyncMode)
		kv(b, "journal_path", p.JournalPath)
		kv(b, "journal_size_bytes", strconv.FormatInt(p.JournalSizeBytes, 10))
		kv(b, "journal_records_appended", strconv.FormatUint(p.JournalAppends, 10))
		kv(b, "journal_sequence", strconv.FormatUint(p.JournalSeq, 10))
		kv(b, "journal_fsyncs", strconv.FormatUint(p.JournalFsyncs, 10))
		kv(b, "journal_last_fsync_ms", strconv.FormatInt(p.JournalLastFsyncMS, 10))
		kv(b, "journal_write_errors", strconv.FormatUint(p.JournalWriteErrors, 10))
		kv(b, "journal_poisoned", boolInt(p.JournalPoisoned))
		if p.JournalPoisonReason != "" {
			kv(b, "journal_poison_reason", p.JournalPoisonReason)
		}
		kv(b, "snapshot_path", p.SnapshotPath)
		kv(b, "snapshot_exists", boolInt(p.SnapshotExists))
		kv(b, "snapshot_count", strconv.FormatUint(p.SnapshotCount, 10))
		kv(b, "snapshot_last_ms", strconv.FormatInt(p.SnapshotLastMS, 10))
		kv(b, "snapshot_last_sequence", strconv.FormatUint(p.SnapshotLastSeq, 10))
		kv(b, "snapshot_last_bytes", strconv.FormatInt(p.SnapshotLastBytes, 10))
		if p.LastError != "" {
			kv(b, "persistence_last_error", p.LastError)
		} else {
			kv(b, "persistence_last_error", "")
		}
		if r := p.Recovery; r != nil {
			kv(b, "recovery_status", r.Status)
			kv(b, "recovery_duration_ms", strconv.FormatInt(r.DurationMS, 10))
			kv(b, "recovery_completed_at_ms", strconv.FormatInt(r.CompletedAtMS, 10))
			kv(b, "recovery_snapshot_loaded", boolInt(r.SnapshotLoaded))
			kv(b, "recovery_snapshot_sequence", strconv.FormatUint(r.SnapshotSeq, 10))
			kv(b, "recovery_snapshot_keys", strconv.Itoa(r.SnapshotKeys))
			kv(b, "recovery_journal_records", strconv.Itoa(r.JournalRecords))
			kv(b, "recovery_journal_applied", strconv.Itoa(r.JournalApplied))
			kv(b, "recovery_journal_skipped", strconv.Itoa(r.JournalSkipped))
			kv(b, "recovery_orphan_records", strconv.Itoa(r.JournalOrphans))
			kv(b, "recovery_discarded_bytes", strconv.FormatInt(r.DiscardedBytes, 10))
			kv(b, "recovery_tail_repaired", boolInt(r.Repaired))
			for i, w := range r.Warnings {
				kv(b, "recovery_warning_"+strconv.Itoa(i), w)
			}
		} else {
			kv(b, "recovery_status", "none")
		}
	})

	write("stats", func(b *strings.Builder) {
		sectionHeader(b, "Stats")
		kv(b, "total_commands_processed", strconv.FormatUint(info.Exec.CommandsProcessed, 10))
		kv(b, "total_command_errors", strconv.FormatUint(info.Exec.CommandErrors, 10))
		kv(b, "protocol_errors", strconv.FormatUint(info.Exec.ProtocolErrors, 10))
		kv(b, "auth_failures", strconv.FormatUint(info.Exec.AuthFailures, 10))
		kv(b, "keys_expired_total", strconv.FormatUint(info.Exec.KeysExpired, 10))
		kv(b, "avg_command_usec_lifetime", fmt.Sprintf("%.2f", info.Exec.AvgCommandMicros))
		kv(b, "commands_per_second_lifetime", fmt.Sprintf("%.2f", info.Exec.CommandsPerSecond))
	})

	write("keyspace", func(b *strings.Builder) {
		sectionHeader(b, "Keyspace")
		kv(b, "db0", fmt.Sprintf("keys=%d,expires=%d,approx_bytes=%d",
			info.Store.Keys, info.Store.Expires, info.Store.ApproxBytes))
	})

	write("commandstats", func(b *strings.Builder) {
		sectionHeader(b, "Commandstats")
		for _, cs := range info.CommandStat {
			perCall := 0.0
			if cs.Calls > 0 {
				perCall = float64(cs.Micros) / float64(cs.Calls)
			}
			b.WriteString("cmdstat_")
			b.WriteString(strings.ToLower(cs.Name))
			fmt.Fprintf(b, ":calls=%d,usec=%d,usec_per_call=%.2f,errors=%d\n",
				cs.Calls, cs.Micros, perCall, cs.Errors)
		}
	})

	return protocol.BulkString(b.String()), nil
}

func sectionHeader(b *strings.Builder, name string) {
	b.WriteString("# ")
	b.WriteString(name)
	b.WriteString("\r\n")
}

func kv(b *strings.Builder, key, value string) {
	b.WriteString(key)
	b.WriteString(":")
	b.WriteString(value)
	b.WriteString("\r\n")
}

func boolInt(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

// fallbackInfo fills the fields INFO can derive without a server, which is
// what unit tests and the command layer's own tests use.
func fallbackInfo() Info {
	return Info{
		Version:   "unknown",
		Mode:      "standalone",
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
}

// uptimeSeconds is a helper shared with the server's Info assembly.
func uptimeSeconds(start time.Time) int64 {
	if start.IsZero() {
		return 0
	}
	return int64(time.Since(start).Seconds())
}
