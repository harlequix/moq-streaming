module moq-relay

go 1.24

require (
	github.com/mengelbart/moqtransport v0.0.0
	github.com/quic-go/quic-go v0.59.0
)

require (
	github.com/mengelbart/qlog v0.1.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)

replace github.com/mengelbart/moqtransport => github.com/harlequix/moqtransport v0.0.0-20260411070518-4b4a10dad4c7
