package server

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amandev11/miniredis/internal/command"
	"github.com/amandev11/miniredis/internal/protocol"
)

// Limits on a single connection.
const (
	// maxAuthAttempts is how many failed AUTHs a connection may make before
	// the server drops it. It slows down password guessing over one socket.
	maxAuthAttempts = 5
	// pipelineFlushReplies is how many replies may accumulate before the
	// buffer is flushed, so a client that sends a partial pipeline still gets
	// its earlier answers.
	pipelineFlushReplies = 64
	// pipelineFlushBytes flushes early when encoded replies reach this size.
	pipelineFlushBytes = 16 << 10
)

// connection is one client's session. Each connection owns its parser, its
// writer and its state, so a slow client can never block another one.
type connection struct {
	id  uint64
	srv *Server
	nc  net.Conn
	log *slog.Logger

	parser *protocol.Parser
	writer *protocol.Writer

	// authed and closeAfterReply are only touched by this connection's
	// goroutine, so they need no synchronisation.
	authed          bool
	closeAfterReply bool
	authFailures    int

	busy      atomic.Bool
	closeOnce sync.Once
}

func newConnection(srv *Server, id uint64, nc net.Conn) *connection {
	w := protocol.NewWriter(nc)
	// A response may never be larger than the configured maximum value size.
	w.MaxBulkBytes = srv.cfg.MaxValueBytes
	return &connection{
		id:     id,
		srv:    srv,
		nc:     nc,
		log:    srv.log.With("client_id", id, "remote", nc.RemoteAddr().String()),
		parser: srv.newProtocolParser(nc),
		writer: w,
	}
}

// isBusy reports whether a command is currently executing.
func (c *connection) isBusy() bool { return c.busy.Load() }

// close shuts the socket down exactly once, which unblocks any pending read.
func (c *connection) close() {
	c.closeOnce.Do(func() { _ = c.nc.Close() })
}

// serve runs the read-execute-reply loop for this connection.
func (c *connection) serve() {
	defer c.close()
	c.log.Debug("client connected")

	repliesSinceFlush := 0
	for {
		if idle := c.srv.cfg.IdleTimeout; idle > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(idle))
		}

		args, err := c.parser.ReadCommand()
		if err != nil {
			c.handleReadError(err)
			return
		}

		c.busy.Store(true)
		reply := c.execute(args)
		c.busy.Store(false)

		if err := c.writeReply(reply); err != nil {
			c.log.Debug("write failed", "err", err)
			return
		}
		repliesSinceFlush++

		// Batch a pipelined burst into one flush, but never leave a reply
		// unflushed for long.
		if c.parser.Buffered() > 0 &&
			repliesSinceFlush < pipelineFlushReplies &&
			c.writer.Buffered() < pipelineFlushBytes {
			continue
		}
		if err := c.flush(); err != nil {
			c.log.Debug("flush failed", "err", err)
			return
		}
		repliesSinceFlush = 0

		if c.closeAfterReply {
			c.log.Debug("closing after reply")
			return
		}
	}
}

// execute dispatches one command and converts failures into reply frames.
func (c *connection) execute(args [][]byte) protocol.Value {
	ctx := &command.Context{
		Args:         args,
		Store:        c.srv.store,
		Registry:     c.srv.reg,
		Metrics:      c.srv.met,
		Runtime:      c.srv,
		Authed:       c.authed || c.srv.cfg.RequirePass == "",
		AuthRequired: c.srv.cfg.RequirePass != "",
		ClientID:     c.id,
		RemoteAddr:   c.nc.RemoteAddr().String(),
	}

	start := time.Now()
	reply, err := c.srv.reg.Dispatch(ctx)
	c.srv.met.RecordCommand(ctx.Name(), time.Since(start), err != nil)

	if err != nil {
		var cmdErr *command.Error
		if errors.As(err, &cmdErr) {
			if cmdErr.AuthFailure {
				c.authFailures++
				c.srv.met.AuthFailures.Add(1)
				if c.authFailures >= maxAuthAttempts {
					// Drop the socket so a single connection cannot be used to
					// keep guessing.
					c.closeAfterReply = true
					c.log.Warn("closing connection after repeated authentication failures",
						"attempts", c.authFailures)
				}
			}
			// The message is already a RESP error string; note that no stored
			// value is ever included in one.
			return protocol.Errorf("%s", cmdErr.Message)
		}
		// An internal failure must not leak internals to the client, but it
		// must be visible in the log.
		c.log.Error("command failed", "command", ctx.Name(), "err", err)
		return protocol.Errorf("ERR internal error")
	}

	if ctx.Authenticated {
		c.authed = true
	}
	if ctx.CloseAfterReply {
		c.closeAfterReply = true
	}
	return reply
}

// writeReply applies the write deadline before encoding a reply.
func (c *connection) writeReply(v protocol.Value) error {
	if timeout := c.srv.cfg.WriteTimeout; timeout > 0 {
		_ = c.nc.SetWriteDeadline(time.Now().Add(timeout))
	}
	return c.writer.Write(v)
}

func (c *connection) flush() error {
	if timeout := c.srv.cfg.WriteTimeout; timeout > 0 {
		_ = c.nc.SetWriteDeadline(time.Now().Add(timeout))
	}
	return c.writer.Flush()
}

// handleReadError classifies why the read loop ended.
//
// A protocol error means the byte stream is desynchronised, so the only safe
// recovery is to report the error and close the connection. A clean EOF is
// normal, and everything else is treated as a transport problem.
func (c *connection) handleReadError(err error) {
	switch {
	case errors.Is(err, io.EOF):
		c.log.Debug("client disconnected")

	case protocol.IsProtocolError(err):
		c.srv.met.ProtocolErrors.Add(1)
		// The message is built from sanitised fragments only: no raw client
		// payload and no stored value is echoed back.
		if writeErr := c.writeReply(protocol.Errorf("ERR %s", err.Error())); writeErr == nil {
			_ = c.flush()
		}
		c.log.Warn("protocol error", "err", err)

	case errors.Is(err, io.ErrUnexpectedEOF):
		c.log.Debug("client disconnected mid-frame")

	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			c.log.Debug("connection closed after idle timeout", "timeout", c.srv.cfg.IdleTimeout.String())
			return
		}
		if c.srv.isClosing() {
			c.log.Debug("connection closed during shutdown")
			return
		}
		c.log.Debug("connection read failed", "err", err)
	}
}
