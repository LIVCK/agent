# livck-agent

A lightweight Linux monitoring agent for [LIVCK Cloud](https://livck.cloud). It
samples system metrics — CPU, memory, load, disk, network, pressure, and
optional GPU/SMART/reachability probes — and reports lifecycle events like
reboots, OOM kills, clean shutdowns, and disk-full or read-only mounts. The
agent collects, buffers, and sends; every decision (what's an incident, who to
alert, how long to keep data) is made by the backend, not here.

It is outbound-only. There is no command channel and no inbound port: the config
it pulls describes only what to measure, never what to run. It runs as a
non-root systemd service under a hardened unit, and it needs no privileges beyond
reading `/proc`, `/sys`, and mount stats.

## Install

The agent is distributed as a `.deb`/`.rpm` and enrolled with a short-lived
token from your LIVCK dashboard:

```sh
curl -fsSL https://get.livck.cloud/install.sh | sudo sh -s -- --token <ENROLLMENT_TOKEN> --name web-01
```

The dashboard shows the exact one-liner (with the token written to a `0600`
temp file so it never lands in your shell history or `/proc/cmdline`).

## Commands

```
livck-agent run        collect and report (the systemd service runs this)
livck-agent enroll     register with the backend and persist the identity
livck-agent doctor     check platform support, clock skew, and connectivity
livck-agent version    print the build version
```

## Wire contract

The agent talks to the backend over a single frozen wire protocol in
[`pkg/wire`](pkg/wire). It's a separate Go module
(`github.com/LIVCK/agent/pkg/wire`) so the contract is versioned independently of
the agent binary and the backend can import just the protocol. See
[`pkg/wire/CONTRACT.md`](pkg/wire/CONTRACT.md) for the wire format, the metric
catalog, the error codes, and the response bodies.

## Layout

```
cmd/livck-agent      CLI and run-loop wiring
cmd/livck-loadgen    fake-fleet load generator (testing)
pkg/wire             the frozen protobuf contract + metric catalog
internal/platform    injectable Clock / filesystem / host abstractions
internal/config      config pull, validation, atomic swap
internal/collector   metric sources (cpu, mem, disk, net, gpu, smart, probes…)
internal/buffer      bounded report ring + shutdown spool
internal/sender      batch build, compression, retry, response handling
internal/event       lifecycle event queue
internal/enroll      identity + token persistence
internal/lifecycle   reboot / shutdown / OOM detection
internal/runner      the collect loop and graceful shutdown
internal/doctor      the doctor command
```

## Build and test

```sh
make build       # static, stripped binary (CGO disabled)
make test-race   # tests with the race detector
make lint        # golangci-lint
make wire        # build and test the wire module
```

`pkg/wire` is a nested module. A standalone checkout resolves it through the
`replace` directive in `go.mod`; inside the LIVCK monorepo a `go.work` binds it
from source.

## License

MIT — see [LICENSE](LICENSE).
