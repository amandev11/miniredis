// Command miniredis runs the MiniRedis server.
//
// It is a thin wrapper: configuration parsing lives in internal/config, the
// network and lifecycle in internal/server, and the signal handling here.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/amandev11/miniredis/internal/config"
	"github.com/amandev11/miniredis/internal/server"
)

// shutdownGrace is how long a shutdown may take before it is forced.
const shutdownGrace = 15 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "miniredis: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Parse(args, os.Getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Print(config.Default().Usage())
			return flag.ErrHelp
		}
		return err
	}
	if cfg.Version {
		fmt.Printf("miniredis %s\n", config.Version)
		return nil
	}

	logger := newLogger(cfg.LogLevel)
	logger.Info("starting", "version", config.Version, "config", cfg.Redacted().LogAttrs())

	srv, err := server.New(cfg, logger)
	if err != nil {
		return err
	}
	if err := srv.Start(); err != nil {
		return err
	}

	// Block until the operator signals the process or a client issues
	// SHUTDOWN.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	stop() // restore default behaviour for a second signal

	logger.Info("signal received, shutting down", "grace", shutdownGrace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// newLogger builds a text logger at the configured level. Logs go to stderr so
// that a caller can still pipe stdout.
func newLogger(level string) *slog.Logger {
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slogLevel})
	return slog.New(handler)
}
