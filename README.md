# moq-streaming

A small testbed for measuring per-object delivery timeouts over Media over
QUIC (MoQ). The pipeline is a publisher, a relay, and one or more subscribers,
connected by QUIC and using `RESET_STREAM_AT` on timeout to partially deliver
subgroups.

The relay is the object under test: each subscriber can request a delivery
timeout and a reset strategy, and the relay drops or partially delivers group
data accordingly.

## Layout

- `cmd/publisher`, `cmd/relay`, `cmd/subscriber` — CLI entry points.
- `publisher/` — ffmpeg demuxer, annex-B NAL parser, MoQ publisher mapping one
  GoP per MoQ group.
- `relay/` — track cache, per-subscriber forward loops, timeout strategies.
- `subscriber/` — QUIC client, optional MPEG-TS muxer, mpv sink with IPC for
  frame-level display logs.
- `testdata/` — video samples (a few small clips are checked in; supply your
  own `.mp4` for anything larger).

## Build

```
GOPRIVATE=git.kom.e-technik.tu-darmstadt.de go build ./...
```

Requires Go 1.24. The `replace` in `go.mod` points to a moqtransport fork
hosted on the TU Darmstadt KOM GitLab; `GOPRIVATE` (or a global git
`insteadOf` rule) is needed to fetch it over SSH.

## Quick run

```
./run-test.sh testdata/test_10s.mp4 12
```

This builds the three binaries and starts publisher, relay, and two
subscribers for 12 seconds, writing CSV logs and process logs under
`results/<timestamp>/`.

## Timeout strategies

Selected on the relay via `--strategy`:

- `none` — deliver everything reliably.
- `reset-stream` — reset the whole subgroup on timeout.
- `reset-at-object` — `RESET_STREAM_AT` after each completed object.
- `reset-at-keyframe` — `RESET_STREAM_AT` after the I-frame only.

`--default-timeout` sets the per-object deadline when the subscriber does not
request one via MoQ `DELIVERY_TIMEOUT`.

## Tests

```
go test ./...
```

Most tests spin up an in-process publisher/relay/subscriber over loopback
QUIC. The `publisher/demux*` tests and the `Test*Strategy*` tests require
`ffmpeg` on `PATH`.
