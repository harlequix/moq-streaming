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

    go build -o moq-publisher ./cmd/publisher
    go build -o moq-relay ./cmd/relay
    go build -o moq-subscriber ./cmd/subscriber

Requires Go 1.24 or newer. The `replace` in `go.mod` points to a moqtransport
fork (`github.com/harlequix/moqtransport`, branch `moq-timeout`) that adds
`RESET_STREAM_AT`; it is fetched automatically.

Running needs `ffmpeg` and `ffprobe` (publisher, `run-test.sh`), `mpv`
(subscriber), and `column` (`run-test.sh` summary). On Ubuntu:

    sudo apt install golang-go ffmpeg mpv bsdextrautils

## Run

The publisher reads an H.264 file through ffmpeg; the subscriber renders with
mpv. `testdata/testsrc_30s.mp4` is a 30 s test clip (640x360, 30 fps, keyframe
every 2 s, no B-frames). A minimal publisher to relay to subscriber chain:

    ./moq-publisher --video testdata/testsrc_30s.mp4 --namespace live --track video --listen :4443
    ./moq-relay --upstream 127.0.0.1:4443 --listen :4444 --namespace live --track video --strategy reset-at-keyframe --default-timeout 200ms
    ./moq-subscriber --relay 127.0.0.1:4444 --namespace live --track video --fps 30

Set `--fps` to the clip's framerate! Without it mpv assumes 25 fps for raw
H.264 and the display log timestamps are off. Encode without B-frames, with
`-bf 0` or `-profile:v baseline`. With B-frames the publisher's PTS are wrong
and the results are invalid.

`run-test.sh [video] [seconds]` (defaults: the test clip, 30 s) starts a
publisher, a relay with `--strategy none`, and two subscribers with `--fps`
read from the clip via ffprobe, then writes CSV logs to `results/`.

## Reset strategies

Selected on the relay via `--strategy`:

- `none`: deliver everything reliably.
- `reset-stream`: reset the whole subgroup on timeout.
- `reset-at-object`: `RESET_STREAM_AT` after each completed object.
- `reset-at-keyframe`: `RESET_STREAM_AT` after the keyframe only.

## Tests

    go test ./...

The demux and strategy tests require `ffmpeg` on `PATH`.
