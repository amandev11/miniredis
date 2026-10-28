// Command mini-cli is MiniRedis's standalone client.
//
// It is an ordinary TCP client: it opens a socket, serialises commands as RESP
// arrays of bulk strings, and prints the replies. It never imports the
// database engine, so it exercises exactly the same protocol path any other
// client would.
//
// Usage:
//
//	mini-cli                                  # interactive session
//	mini-cli SET greeting hello               # one command, then exit
//	mini-cli -h 10.0.0.5 -p 6379 PING
//	mini-cli -a secret GET key                # authenticate first
//	mini-cli help                             # command table from the server
//	echo "SET k v" | mini-cli                 # read commands from stdin
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amandev11/miniredis/internal/protocol"
)

// Exit codes.
const (
	exitOK         = 0
	exitCommandErr = 1
	exitUsage      = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

type options struct {
	host     string
	port     int
	addr     string
	password string
	timeout  time.Duration
	raw      bool
	help     bool
	version  bool
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	opts, rest, err := parseFlags(args, stderr)
	if err != nil {
		return exitUsage
	}
	if opts.help {
		printUsage(stdout)
		return exitOK
	}
	if opts.version {
		fmt.Fprintf(stdout, "mini-cli %s\n", version)
		return exitOK
	}

	// The client speaks to whatever address it was given; nothing else about
	// the server is assumed.
	addr := opts.addr
	if addr == "" {
		addr = net.JoinHostPort(opts.host, strconv.Itoa(opts.port))
	}

	client, err := dial(addr, opts.timeout)
	if err != nil {
		fmt.Fprintf(stderr, "mini-cli: cannot connect to %s: %v\n", addr, err)
		return exitCommandErr
	}
	defer client.close()

	if opts.password != "" {
		reply, err := client.do("AUTH", opts.password)
		if err != nil {
			fmt.Fprintf(stderr, "mini-cli: AUTH failed: %v\n", err)
			return exitCommandErr
		}
		if reply.IsError() {
			fmt.Fprintf(stderr, "mini-cli: %s\n", reply.Render())
			return exitCommandErr
		}
	}

	if len(rest) > 0 && strings.EqualFold(rest[0], "help") {
		return printCommandTable(client, stdout, stderr)
	}

	if len(rest) > 0 {
		return runOnce(client, rest, opts, stdout, stderr)
	}
	return runInteractive(client, addr, opts, stdin, stdout, stderr)
}

func parseFlags(args []string, stderr io.Writer) (options, []string, error) {
	var opts options
	fs := flag.NewFlagSet("mini-cli", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&opts.host, "h", "127.0.0.1", "server host")
	fs.StringVar(&opts.host, "host", "127.0.0.1", "server host")
	fs.IntVar(&opts.port, "p", 6379, "server port")
	fs.IntVar(&opts.port, "port", 6379, "server port")
	fs.StringVar(&opts.addr, "addr", "", "server address as host:port, overrides -h and -p")
	fs.StringVar(&opts.password, "a", "", "password for AUTH (never logged)")
	fs.StringVar(&opts.password, "password", "", "password for AUTH (never logged)")
	fs.DurationVar(&opts.timeout, "timeout", 5*time.Second, "connection and read timeout")
	fs.BoolVar(&opts.raw, "raw", false, "print bulk strings without quoting or escaping")
	fs.BoolVar(&opts.version, "version", false, "print the client version and exit")
	fs.BoolVar(&opts.help, "help", false, "print this help and exit")

	fs.Usage = func() { printUsage(stderr) }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			// Treated as a usage request: the caller prints the help text.
			return options{help: true}, nil, nil
		}
		return opts, nil, err
	}
	return opts, fs.Args(), nil
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `mini-cli is the command-line client for MiniRedis.

Usage:
  mini-cli [flags] [command argument ...]

With no command, mini-cli opens an interactive session. With a command, it
sends that one command and exits with status 1 if the server replied with an
error. Without a command and with input on stdin, it reads one command per
line, which makes shell pipelines work.

Flags:
  -h, --host string     server host (default "127.0.0.1")
  -p, --port int        server port (default 6379)
      --addr string     server address as host:port, overrides -h and -p
  -a, --password string password for AUTH (never logged)
      --timeout duration connection and read timeout (default 5s)
      --raw             print bulk strings without quoting or escaping
      --version         print the client version and exit
      --help            print this help and exit

Examples:
  mini-cli PING
  mini-cli SET greeting hello
  mini-cli HSET user:1 name aman role engineer
  mini-cli -a "$MINIREDIS_PASSWORD" INFO server
  mini-cli help          # command table, fetched from the server
`)
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// client is a minimal RESP client over one TCP connection.
type client struct {
	conn    net.Conn
	parser  *protocol.Parser
	timeout time.Duration
}

func dial(addr string, timeout time.Duration) (*client, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &client{
		conn:    c,
		parser:  protocol.NewParser(c, protocol.DefaultLimits()),
		timeout: timeout,
	}, nil
}

func (c *client) close() { _ = c.conn.Close() }

// do sends one command and returns the reply. A protocol error frame is
// returned as a value, not as an error: only transport failures are errors.
func (c *client) do(args ...string) (protocol.Value, error) {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	if err := c.write(protocol.EncodeCommand(raw...)); err != nil {
		return protocol.Value{}, err
	}
	return c.read()
}

func (c *client) write(b []byte) error {
	if c.timeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	_, err := c.conn.Write(b)
	return err
}

func (c *client) read() (protocol.Value, error) {
	if c.timeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	return c.parser.ReadValue()
}

// ---------------------------------------------------------------------------
// Modes
// ---------------------------------------------------------------------------

// runOnce sends a single command and exits.
func runOnce(c *client, args []string, opts options, stdout, stderr io.Writer) int {
	reply, err := c.do(args...)
	if err != nil {
		fmt.Fprintf(stderr, "mini-cli: %v\n", err)
		return exitCommandErr
	}
	printReply(stdout, reply, opts.raw)
	if reply.IsError() {
		return exitCommandErr
	}
	return exitOK
}

// runInteractive reads commands line by line.
//
// The prompt is only printed when stdin is a terminal, so piping a script in
// produces clean output.
func runInteractive(c *client, addr string, opts options, stdin io.Reader, stdout, stderr io.Writer) int {
	prompt := stdinIsTerminal(stdin)
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)

	for {
		if prompt {
			fmt.Fprintf(stdout, "%s> ", addr)
		}
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args, err := tokenize(line)
		if err != nil {
			fmt.Fprintf(stderr, "mini-cli: %v\n", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToLower(args[0]) {
		case "exit", "quit":
			return exitOK
		}

		reply, err := c.do(args...)
		if err != nil {
			fmt.Fprintf(stderr, "mini-cli: %v\n", err)
			return exitCommandErr
		}
		printReply(stdout, reply, opts.raw)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(stderr, "mini-cli: reading input: %v\n", err)
		return exitCommandErr
	}
	return exitOK
}

// printReply renders a reply for a human.
//
// Binary payloads are escaped rather than written raw, so a value containing
// control bytes cannot corrupt the terminal.
func printReply(w io.Writer, v protocol.Value, raw bool) {
	if raw && (v.Type == protocol.TypeBulkString || v.Type == protocol.TypeSimpleString) && !v.IsNull {
		_, _ = w.Write(v.Str)
		fmt.Fprintln(w)
		return
	}
	fmt.Fprintln(w, v.Render())
}

// printCommandTable fetches the command table from the server.
//
// The table comes from the running server's own introspection, so it can never
// disagree with the commands the server actually implements.
func printCommandTable(c *client, stdout, stderr io.Writer) int {
	namesReply, err := c.do("COMMAND", "LIST")
	if err != nil {
		fmt.Fprintf(stderr, "mini-cli: %v\n", err)
		return exitCommandErr
	}
	if namesReply.IsError() {
		fmt.Fprintf(stderr, "mini-cli: %s\n", namesReply.Render())
		return exitCommandErr
	}

	names := make([]string, 0, len(namesReply.Array))
	for _, item := range namesReply.Array {
		names = append(names, string(item.Str))
	}
	sort.Strings(names)

	args := append([]string{"COMMAND", "INFO"}, names...)
	infoReply, err := c.do(args...)
	if err != nil {
		fmt.Fprintf(stderr, "mini-cli: %v\n", err)
		return exitCommandErr
	}

	fmt.Fprintf(stdout, "%d commands\n\n", len(names))
	for _, entry := range infoReply.Array {
		if entry.IsNull || len(entry.Array) < 5 {
			continue
		}
		fmt.Fprintf(stdout, "%-9s %s\n            %s\n",
			string(entry.Array[0].Str), string(entry.Array[3].Str), string(entry.Array[4].Str))
	}
	return exitOK
}

// ---------------------------------------------------------------------------
// Input handling
// ---------------------------------------------------------------------------

// stdinIsTerminal reports whether input is an interactive terminal.
func stdinIsTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// tokenize splits a command line into arguments, supporting the quoting rules
// documented in docs/PROTOCOL.md:
//
//   - bare words run to the next space;
//   - 'single quotes' are literal and have no escape sequences;
//   - "double quotes" support \n, \r, \t, \b, \a, \\, \" and \xHH;
//   - a backslash outside quotes escapes the next character.
//
// It returns an error for an unterminated quote rather than silently accepting
// a truncated command.
func tokenize(line string) ([]string, error) {
	var (
		args    []string
		current []byte
		haveArg bool
	)
	runes := []rune(line)

	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		switch ch {
		case ' ', '\t':
			if haveArg {
				args = append(args, string(current))
				current = current[:0]
				haveArg = false
			}

		case '\'':
			haveArg = true
			i++
			closed := false
			for ; i < len(runes); i++ {
				if runes[i] == '\'' {
					closed = true
					break
				}
				current = append(current, string(runes[i])...)
			}
			if !closed {
				return nil, errors.New("unterminated single quote")
			}

		case '"':
			haveArg = true
			i++
			closed := false
			for ; i < len(runes); i++ {
				if runes[i] == '"' {
					closed = true
					break
				}
				if runes[i] != '\\' {
					current = append(current, string(runes[i])...)
					continue
				}
				// Escape sequence inside double quotes.
				i++
				if i >= len(runes) {
					return nil, errors.New("trailing backslash inside double quotes")
				}
				switch runes[i] {
				case 'n':
					current = append(current, '\n')
				case 'r':
					current = append(current, '\r')
				case 't':
					current = append(current, '\t')
				case 'b':
					current = append(current, '\b')
				case 'a':
					current = append(current, '\a')
				case '\\':
					current = append(current, '\\')
				case '"':
					current = append(current, '"')
				case 'x':
					if i+2 >= len(runes) {
						return nil, errors.New("truncated \\x escape")
					}
					hi, ok1 := hexDigit(runes[i+1])
					lo, ok2 := hexDigit(runes[i+2])
					if !ok1 || !ok2 {
						return nil, fmt.Errorf("invalid \\x escape %q", string(runes[i+1:i+3]))
					}
					current = append(current, hi<<4|lo)
					i += 2
				default:
					// Unknown escapes keep the escaped character, which is
					// what redis-cli does.
					current = append(current, string(runes[i])...)
				}
			}
			if !closed {
				return nil, errors.New("unterminated double quote")
			}

		case '\\':
			haveArg = true
			i++
			if i >= len(runes) {
				return nil, errors.New("trailing backslash")
			}
			current = append(current, string(runes[i])...)

		default:
			haveArg = true
			current = append(current, string(ch)...)
		}
	}

	if haveArg {
		args = append(args, string(current))
	}
	return args, nil
}

func hexDigit(r rune) (byte, bool) {
	switch {
	case r >= '0' && r <= '9':
		return byte(r - '0'), true
	case r >= 'a' && r <= 'f':
		return byte(r-'a') + 10, true
	case r >= 'A' && r <= 'F':
		return byte(r-'A') + 10, true
	default:
		return 0, false
	}
}

// version is the client version, kept in step with the server's.
const version = "0.1.0"
