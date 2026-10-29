FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/moq-publisher ./cmd/publisher \
    && CGO_ENABLED=0 go build -o /out/moq-relay ./cmd/relay \
    && CGO_ENABLED=0 go build -o /out/moq-subscriber ./cmd/subscriber

FROM ubuntu:26.10 AS publisher
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/moq-publisher /usr/local/bin/
COPY testdata/testsrc_30s.mp4 /data/
ENTRYPOINT ["moq-publisher"]

FROM ubuntu:26.10 AS relay
COPY --from=build /out/moq-relay /usr/local/bin/
ENTRYPOINT ["moq-relay"]

FROM ubuntu:26.10 AS subscriber
RUN apt-get update \
    && apt-get install -y --no-install-recommends mpv \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/moq-subscriber /usr/local/bin/
COPY subscriber/framelog.lua /usr/local/bin/
ENTRYPOINT ["moq-subscriber"]
