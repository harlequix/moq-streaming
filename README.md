# moq-streaming

Publisher, relay, and subscriber for measuring per-object delivery timeouts
over Media over QUIC (MoQ). When an object misses its delivery timeout, the
relay applies a reset strategy (`RESET_STREAM` or `RESET_STREAM_AT`) to drop or
partially deliver the rest of its group.

## Layout

- `cmd/publisher`, `cmd/relay`, `cmd/subscriber`: CLI entry points.
- `publisher/`: ffmpeg demux, Annex-B NAL parsing, one GoP per MoQ group.
- `relay/`: track cache, per-subscriber forward loop, timeout strategies.
- `subscriber/`: QUIC client and mpv sink with per-frame display logging.

## Build

    go build ./...

Requires Go 1.24 or newer. The `replace` in `go.mod` points to a moqtransport
fork (`github.com/harlequix/moqtransport`, branch `moq-timeout`) that adds
`RESET_STREAM_AT`; it is fetched automatically.

## Run

The publisher reads an H.264 file through ffmpeg; the subscriber renders with
mpv. A minimal publisher to relay to subscriber chain:

    ./moq-publisher --video path/to/clip.mp4 --namespace live --track video --listen :4443
    ./moq-relay --upstream 127.0.0.1:4443 --listen :4444 --namespace live --track video --strategy reset-at-keyframe --default-timeout 200ms
    ./moq-subscriber --relay 127.0.0.1:4444 --namespace live --track video

`run-test.sh <video> <seconds>` runs the same pipeline and writes CSV logs.

## Reset strategies

Selected on the relay via `--strategy`:

- `none`: deliver everything reliably.
- `reset-stream`: reset the whole subgroup on timeout.
- `reset-at-object`: `RESET_STREAM_AT` after each completed object.
- `reset-at-keyframe`: `RESET_STREAM_AT` after the keyframe only.

## Tests

    go test ./...

The demux and strategy tests require `ffmpeg` on `PATH`.
