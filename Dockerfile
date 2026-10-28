# MiniRedis container image.
#
# The build stage compiles static binaries; the runtime stage contains only
# those binaries, so the image has no toolchain and no package manager in it.
# Keep GO_VERSION in step with the go directive in go.mod.

ARG GO_VERSION=1.24

# ---- build stage ---------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# Copy the module file and the source. MiniRedis has no third-party
# dependencies, so there is no dependency download step to cache.
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/miniredis ./cmd/miniredis \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mini-cli ./cmd/mini-cli \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mini-bench ./cmd/mini-bench

# ---- runtime stage -------------------------------------------------------
FROM alpine:3.20

# A dedicated unprivileged account owns the data directory. The server refuses
# to use a data directory it cannot write, so the ownership matters.
RUN addgroup -S -g 10001 miniredis \
 && adduser -S -D -H -u 10001 -G miniredis miniredis \
 && mkdir -p /data \
 && chown miniredis:miniredis /data \
 && chmod 0700 /data

COPY --from=build /out/miniredis /usr/local/bin/miniredis
COPY --from=build /out/mini-cli /usr/local/bin/mini-cli
COPY --from=build /out/mini-bench /usr/local/bin/mini-bench

USER miniredis
WORKDIR /home/miniredis

# The journal and snapshots live here; mount a volume to keep them.
VOLUME ["/data"]

# 6379 is the RESP endpoint, 7379 the HTTP diagnostics endpoint.
EXPOSE 6379 7379

# The listener is bound to every interface inside the container, so the server
# requires a password: pass MINIREDIS_REQUIREPASS (see docker-compose.yml).
# Without it the server exits with an explanatory error rather than starting an
# open database.
#
# A liveness check that only reads the diagnostics endpoint, so it never
# touches stored data.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:7379/healthz || exit 1

# SIGTERM is handled: the server stops accepting, drains connections, writes a
# snapshot and exits.
STOPSIGNAL SIGTERM

ENTRYPOINT ["miniredis"]
CMD ["--addr=0.0.0.0:6379", "--data-dir=/data", "--http-addr=0.0.0.0:7379"]
