# Wire Contract: livck-agent <-> pulse (v1)

> STOP - read before touching `wire.proto` or `catalog.json`.
>
> `pkg/wire` is a frozen, cross-repo contract. The agent speaks it, pulse
> (the ingest backend) reads it. There is no shared build to catch drift: a
> change on one side alone silently breaks production - rows stop being
> ingested, or liveness stops refreshing. The field numbers, enum values,
> catalog keys, error codes and response bodies below are an API, not
> implementation detail.
>
> Only additive changes are allowed after the first public commit: new fields
> with new numbers, new enum values, new catalog keys with a `catalog_version`
> bump. Never renumber or retype a field, never reuse or lift a reserved range,
> never remove or rename a catalog key without the lockstep in the last
> section.
>
> This file is the public excerpt. It carries only ID-free contracts: the wire
> proto, the `sys.*` catalog, the error codes and the response bodies. Internal
> identifiers (the numeric service and organization ids that pulse stamps into
> its output) never appear here or on this wire.

The contract has four parts:

1. The protobuf batch the agent sends to pulse.
2. The `sys.*` metric catalog (`catalog.json`).
3. The error code and status catalog pulse returns.
4. The JSON response bodies the agent parses.

---

## 1. Wire proto (agent -> pulse)

Source of truth: [`wire.proto`](wire.proto), package `livck.wire.v1`, generated
into [`wire.pb.go`](wire.pb.go).

Transport: `POST /v1/ingest`, `Content-Type: application/x-protobuf`,
`Content-Encoding: zstd`, `Authorization: Bearer lvk_...`.

### MetricBatch

One ingest request body. It may carry only reports, only events, or both.

| Field | # | Type | Notes |
|---|---|---|---|
| `schema_version` | 1 | uint32 | Must be 1 in v1. `0` => 400 `UNSUPPORTED_SCHEMA_VERSION`. Deprecation band in the last section. |
| `idempotency_key` | 2 | string | UUIDv4. Over 64 chars => 400 `IDEMPOTENCY_KEY_TOO_LONG`; not UUIDv4 form => 400 `MALFORMED_BODY`. |
| `agent_version` | 3 | string | Agent build version. |
| `agent_instance_id` | 4 | string | Instance UUID. Also the clone signal: pulse compares it to the token's bound instance; mismatch => 409 `INSTANCE_CONFLICT`. |
| `sent_at_unix_ms` | 5 | int64 | Send time, ms. Skew only. `abs(skew) > 5min` => 422 `CLOCK_SKEW` (final, does not refresh liveness). |
| `applied_config_version` | 6 | uint32 | Config version the agent has applied (echo). |
| `fingerprint` | 7 | map<string,string> | machine_id_hash, boot_id, cloud_instance_id?, hostname. First batch and on change. |
| `reports` | 8 | repeated Report | 1..200. More than one is backfill. Server enforces `caps.max_reports`. |
| `events` | 9 | repeated Event | 1..20, server enforced (`caps.max_events`). Overflow drops item-wise, reason `event_budget`. |
| `service_public_id` | 10 | string | Empty for managed tokens (else 400 `SERVICE_ADDRESSING_FORBIDDEN`). Required for user tokens; empty => 400 `SERVICE_ADDRESSING_REQUIRED`. |

Reserved: `20 to 29` (future label support; do not assign).

### Report

One aggregation window of metrics.

| Field | # | Type | Notes |
|---|---|---|---|
| `sampled_at_unix_ms` | 1 | int64 | Wall clock at the END of the window. Sample time, never send time. |
| `metrics` | 2 | map<string,double> | `sys.*` keys from the catalog (part 2). May be empty (event-carrier batch). |
| `meta` | 3 | map<string,string> | Flat host meta. First report and on change. Never `ips_*`. Optional `window_samples` marks a partial window. |

Reserved: `10 to 14` (future per-series labels; lifting changes series identity).

### Event

A discrete lifecycle or health signal. Dedupe anchor is `event_id`.

| Field | # | Type | Notes |
|---|---|---|---|
| `event_id` | 1 | string | UUIDv4. |
| `occurred_at_unix_ms` | 2 | int64 | When it happened, ms. |
| `type` | 3 | EventType | Unknown enum value is dropped and counted. |
| `meta` | 4 | map<string,string> | Type-validated. `reason`, if present, is capped at 200 chars. |

Reserved: `10 to 14`.

### EventType

Closed enum. `EVENT_TYPE_UNSPECIFIED = 0` is the proto3 default: an event whose
type the server does not recognize maps to it and is dropped.

| Value | # | meta |
|---|---|---|
| `EVENT_TYPE_UNSPECIFIED` | 0 | (unknown / drop) |
| `EVENT_TYPE_INSTALL` | 1 | version |
| `EVENT_TYPE_ENROLL` | 2 | version |
| `EVENT_TYPE_BOOT` | 3 | boot_id, downtime_seconds? |
| `EVENT_TYPE_UNEXPECTED_REBOOT` | 4 | downtime_seconds |
| `EVENT_TYPE_REBOOT_SCHEDULED` | 5 | type(reboot/poweroff), at, reason |
| `EVENT_TYPE_CLEAN_SHUTDOWN` | 6 | type, reason? |
| `EVENT_TYPE_OOM_KILL` | 7 | count |
| `EVENT_TYPE_FS_READONLY` | 8 | mount |
| `EVENT_TYPE_DISK_FULL` | 9 | mount |
| `EVENT_TYPE_CLOCK_SKEW_DETECTED` | 10 | skew_ms |
| `EVENT_TYPE_AGENT_UPDATE_STARTED` | 11 | from, to |
| `EVENT_TYPE_AGENT_UPDATE_APPLIED` | 12 | from, to |
| `EVENT_TYPE_AGENT_UPDATE_FAILED` | 13 | from, to, error? |
| `EVENT_TYPE_CONFIG_APPLIED` | 14 | version |
| `EVENT_TYPE_CONFIG_ERROR` | 15 | version, error? |
| `EVENT_TYPE_BUFFER_OVERFLOW` | 16 | dropped_count |
| `EVENT_TYPE_UNINSTALL` | 17 | - |
| `EVENT_TYPE_UNIT_FAILED` | 18 | unit, exec_status?, n_restarts |
| `EVENT_TYPE_UNIT_RESTARTED` | 19 | unit, n_restarts |
| `EVENT_TYPE_UNIT_FLAPPING` | 20 | unit, n_restarts |

---

## 2. sys.* metric catalog

Source of truth: [`catalog.json`](catalog.json), read in Go via `Catalog()`,
`Lookup(key)`, `CatalogVersion()`; validated by `Validate()`.

The catalog is a fixed list. pulse imports this package and drops any `sys.*`
key not in it (counted as `unknown_key`, never a batch reject) - that is the
hard bound on ClickHouse cardinality. Because pulse imports the catalog there is
no second copy to drift.

Each entry has:

- `key` - the metric key. Wildcard entries hold the placeholder literally, e.g.
  `sys.disk.{mount}.used_pct`.
- `unit` - a normalized token: `percent`, `bytes`, `seconds`, `count`, `float`,
  `bytes_per_second`, `per_second`, `celsius`, `watt`, `bool`, `hours`.
- `source` - where the agent reads it, e.g. `/proc/stat`, `statfs`, `self`,
  `sysfs|nvidia-smi`, `smartctl`.
- `agg` - how per-sample values collapse into one report value:
  - `avg` - mean of the window.
  - `avg+max` - emit the mean plus a `.max` companion key.
  - `max` - window peak (a transient spike is not averaged away).
  - `last` - most recent sample (cumulative counters, static gauges).
  - `delta` - reserved for future use.
- `wildcard` (optional) - the placeholder segment name: `mount`, `dev`, `iface`,
  or `gpu`. Per-report cardinality caps: 20 mounts, 10 disk devices, 10 network
  interfaces, 8 GPUs, 16 SMART devices. The `dev` wildcard names both the
  `sys.diskio.*` and `sys.smart.*` segments (a `/dev` device); the cap is
  per-family. The `gpu` segment is the GPU's PCI bus address (stable across
  reboots), not an enumeration index.

The `sys.gpu.*` and `sys.smart.*` families are opt-in hardware telemetry: the
agent emits them only when the operator enables the `gpu` / `smart` feature AND
the hardware/tool is present, so a report from a general-purpose host carries
none of them.

`catalog_version` is 2 and moves with this module's version. Adding a key with a
`catalog_version` bump is additive; removing, renaming, or changing the `agg` of
a key is breaking. Version 2 added the `sys.gpu.*` (6 keys) and `sys.smart.*`
(6 keys) families over version 1.

---

## 3. Error and status catalog (pulse -> agent)

Rule: 4xx is final, 429 and 5xx are retryable, never the reverse. A final 4xx
means the agent drops the batch (poison protection). Invalid items inside an
otherwise-valid batch are not error codes - they are dropped item-wise and
counted (see part 4).

| Status | Codes |
|---|---|
| 202 | ok / duplicate |
| 400 | `MALFORMED_BODY`, `UNSUPPORTED_SCHEMA_VERSION` (incl. 0), `SERVICE_ADDRESSING_FORBIDDEN`, `SERVICE_ADDRESSING_REQUIRED`, `IDEMPOTENCY_KEY_TOO_LONG` |
| 401 | `TOKEN_INVALID` |
| 403 | `AGENT_PAUSED_LIMIT`, `ORG_SUSPENDED`, `PERMISSION_MISSING`, `ORG_INGEST_DISABLED` |
| 404 | unknown route only |
| 409 | `INSTANCE_CONFLICT` (clone gate) |
| 413 | body cap |
| 422 | `CLOCK_SKEW` (batch level, `abs(sent_at - now) > 5min`; final, does not refresh liveness) |
| 429 | `RATE_LIMITED`, `BUFFER_FULL`, `QUOTA_EXCEEDED` (with `Retry-After`) |
| 500 | `INTERNAL` |
| 503 | `RETRY_LATER` (with `Retry-After`) |

`QUOTA_EXCEEDED` carries `Retry-After = min(seconds to UTC midnight, 3600)`.

---

## 4. Response bodies

Two JSON bodies the agent parses. These are the byte-exact surfaces; the golden
fixtures (`response_202.golden.json`, `response_error.golden.json`) are the
authoritative form, this section is the reference.

### 202 accepted

```json
{
  "status": "ok",
  "config_version": 7,
  "server_time_unix_ms": 1752840001213,
  "dropped": { "reports": 0, "keys": 0, "events": 0 },
  "drop_reasons": { "unknown_key": 2 }
}
```

- `config_version` is the pull trigger: if it is ahead of the agent's applied
  version, the agent fetches config.
- `server_time_unix_ms` drives skew logging on the agent.
- `dropped` counts items dropped in this batch. `drop_reasons` maps a reason
  slug to a count and is present only when non-empty. Slugs: `time_window`,
  `unknown_key`, `cardinality`, `ip_meta`, `sanity`, `event_schema`,
  `event_type`, `series_cap`, `feature_disabled`, `grammar`, `org_disabled`,
  `event_budget`, `register_unavailable`.

### Error envelope

```json
{
  "error": {
    "code": "AGENT_LIMIT_REACHED",
    "message": "...",
    "retryable": false,
    "retry_after_seconds": null,
    "request_id": "..."
  }
}
```

`error.code` drives the retry matrix in part 3. `retryable` mirrors the 4xx /
429 / 5xx rule; `retry_after_seconds` is set for 429 and 503.

---

## When you must change this contract

1. Make the change in both repos in lockstep (agent `pkg/wire` + pulse reader),
   plus the Laravel consumer where a downstream field is involved.
2. Prefer additive changes: a new field with a new number, a new enum value, a
   new catalog key with a `catalog_version` bump. Old senders and readers keep
   working.
3. Update the golden fixture and its sha256 in every repo that checks it, in one
   coordinated change. Additive changes still bump fixture bytes and hashes.
4. Never ship a renumber, a retype, a reused reserved range, or a removed
   catalog key to one repo alone. Deploy server first (pulse before the agent
   rollout).
