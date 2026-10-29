BINARIES := moq-publisher moq-relay moq-subscriber

.PHONY: build publisher relay subscriber test fmt run-test docker clean

build: publisher relay subscriber

publisher:
	go build -o moq-publisher ./cmd/publisher

relay:
	go build -o moq-relay ./cmd/relay

subscriber:
	go build -o moq-subscriber ./cmd/subscriber

test:
	go test ./...

fmt:
	go fmt ./...
	go mod tidy

run-test:
	./run-test.sh

docker:
	docker compose build

clean:
	rm -f $(BINARIES)
