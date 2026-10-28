# Security

MiniRedis is an educational database engine. It is designed to be safe by
default on a developer machine, and it is honest about what it does not
provide. This document states the actual behaviour, not an aspiration.

## 1. Network exposure

* **Default binding is loopback**: `--addr 127.0.0.1:6379`, and the HTTP
  diagnostics endpoint defaults to `127.0.0.1:7379`. Neither is reachable from
  another machine out of the box.
* **A non-loopback listener requires a password.** If the address is not a
  loopback address and `--requirepass` is empty, the server refuses to start:

  ```
  miniredis: addr "0.0.0.0:6379" is not a loopback address: set --requirepass,
  or pass --allow-insecure to accept an unauthenticated public listener
  ```

  The `--allow-insecure` flag exists so that an operator can make an informed
  decision (a private network, a container behind a proxy). It is never implied
  and never default.
* **No TLS.** Traffic is plain TCP. Passwords and data travel in clear text.
  Anyone who needs confidentiality must terminate TLS in front of MiniRedis
  (a proxy, a service mesh, an SSH tunnel, or a private network). MiniRedis does
  not pretend to provide transport encryption.

## 2. Authentication

* `--requirepass <password>` enables a single shared password. Without it, any
  client that can reach the port has full access.
* `AUTH <password>` authenticates the connection. `AUTH <username> <password>` is
  accepted for client compatibility; there is exactly one user.
* The comparison is constant-time (`crypto/subtle`) so that a timing
  measurement cannot recover the password byte by byte.
* Repeated failures close the connection, which bounds online guessing from a
  single connection. There is no account lockout and no rate limiting beyond
  that: **a password remains the only barrier for a reachable port.**
* The password is never logged, never returned by `INFO`, never included in the
  HTTP diagnostics payload, and never written to the data directory. The startup
  log line prints the configuration through `Config.Redacted()`, which replaces
  it with `[redacted]`.
* Prefer the environment over the command line: `MINIREDIS_REQUIREPASS=...`
  keeps the value out of the process list (`ps`), whereas `--requirepass=...`
  does not. `scripts/demo.sh` uses the environment variable for exactly this
  reason.
* There are **no ACLs and no per-key permissions**: authentication is all or
  nothing for the whole keyspace.

## 3. Handling of stored data

* Stored values never appear in logs by default. The log records command
  *names*, timings, counts and errors; never keys or values.
  `--log-values` exists to make the opposite choice explicit and is off by
  default. Do not enable it with real data.
* The HTTP diagnostics endpoint exposes measurements only: counters, sizes and
  status. It cannot return a key or a value, and it does not include the
  password. The API is three fixed routes — `/healthz`, `/ready`, `/metrics` —
  with no query parameters, so there is no request that can be shaped to leak
  stored content.
* `INFO` reports configuration values and counters. Keyspace sections report
  counts, never names.
* Error replies describe the failure and the key that caused it only when the
  client already supplied that key. They never reveal the contents of another
  key.
* The CLI escapes control bytes when printing values, so a stored value cannot
  inject terminal escape sequences into a user's session.
* `MiniRedis does not encrypt data at rest.` Journal and snapshot files contain
  the keys and values in plain, documented binary form. Filesystem permissions
  are the only protection.

## 4. Filesystem protections

* The data directory is created with mode `0700`. Files are created `0600`.
* Symlinks are refused: if the data directory or a persistence file is a
  symbolic link, or is not a regular file, the server refuses to use it. That
  removes a class of "write the journal somewhere unexpected" mistakes.
* Snapshots are written to a temporary file and renamed into place, so an
  interrupted write cannot destroy the previous snapshot.
* File names are fixed (`miniredis.mrj`, `miniredis.mrs`) and are never derived
  from client input, so no command can cause a write outside the data
  directory.
* The container image runs as a dedicated unprivileged user (`miniredis`,
  uid 10001) and owns only `/data`.

## 5. Resource limits

Every client-controlled length is bounded before it is allocated:

| Limit | Default | Purpose |
| --- | --- | --- |
| `--max-clients` | 512 | Concurrent connections |
| `--max-bulk-bytes` | 64 MiB | One bulk string argument |
| `--max-request-bytes` | 64 MiB | Total argument bytes in one request |
| `--max-array-len` | 1 048 576 | Arguments per request |
| `--max-line-bytes` | 64 KiB | One protocol line |
| `--max-value-bytes` | 64 MiB | Stored string value |
| `--max-key-bytes` | 64 KiB | Key name |
| `--max-hash-fields` | 1 048 576 | Fields in one hash |
| `--max-field-bytes` | 64 MiB | Hash field name or value |
| `--max-journal-record-bytes` | 64 MiB | One journal record, and one field inside a snapshot |

A bulk string is read in bounded chunks rather than in one allocation of the
declared size, so a client that declares 60 MiB and sends nothing costs one
chunk of memory, not 60 MiB. Exceeding a limit produces an error reply and, for
protocol-level violations, closes that connection; the server itself keeps
running.

`--idle-timeout` and `--write-timeout` bound how long a peer may hold a
connection or stop reading.

## 6. Reporting a problem

If you find a security-relevant bug, please open an issue describing the
behaviour and how to reproduce it. Do not include real credentials or real data
in a report; a minimal reproduction is more useful anyway.

## 7. Known limitations

MiniRedis does **not** provide, and does not claim to provide:

* transport encryption (no TLS);
* multiple users, roles or per-key permissions;
* a protected-mode peer check, firewall rules or connection rate limiting;
* auditing of who did what;
* encrypted storage or secure deletion (removing a key does not wipe bytes from
  the journal, and a snapshot may still contain a value deleted after it was
  taken — the journal keeps the deletion record, and recovery applies it);
* resistance to a well-resourced denial-of-service attack. Limits bound a single
  request, but a client with the password can still fill memory with legitimate
  commands and fill the disk with legitimate writes.

Statements like "you can expose MiniRedis to the internet" are not supported by
this project and must not be inferred from the fact that it runs.
