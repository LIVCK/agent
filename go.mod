module github.com/LIVCK/agent

go 1.24

require (
	github.com/LIVCK/agent/pkg/wire v0.0.0
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.0
	google.golang.org/protobuf v1.36.11
)

require github.com/gorilla/websocket v1.5.3

// pkg/wire is a separate, unpublished module living in this repo; resolve it
// from the sibling directory instead of the module proxy (dev-only).
replace github.com/LIVCK/agent/pkg/wire => ./pkg/wire
