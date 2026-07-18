package sender

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// outcome is how the sender treats an ingest response. It collapses the full
// status catalog into the four actions the retry matrix needs.
type outcome int

const (
	// outcomeOK is a 202: the batch is accepted, remove its reports and events.
	outcomeOK outcome = iota
	// outcomeRetry is a 429, 404 or 5xx (or an unexpected status): keep the data
	// and back off. Retry-After is honoured.
	outcomeRetry
	// outcomeDrop is a terminal 4xx (400, 413, 422): the batch is poison,
	// discard it so it cannot wedge the pipeline.
	outcomeDrop
	// outcomeQuarantine is a 401, 403 or 409: back off long, keep the data,
	// never wipe (revoked/suspended/paused/clone all clear without a rebuild).
	outcomeQuarantine
)

// response is the parsed ingest reply.
type response struct {
	outcome       outcome
	status        int
	configVersion uint32
	serverTimeMs  int64
	retryAfter    time.Duration
	errorCode     string
}

type okBody struct {
	Status           string `json:"status"`
	ConfigVersion    uint32 `json:"config_version"`
	ServerTimeUnixMs int64  `json:"server_time_unix_ms"`
}

type errBody struct {
	Error struct {
		Code              string `json:"code"`
		Message           string `json:"message"`
		Retryable         bool   `json:"retryable"`
		RetryAfterSeconds *int   `json:"retry_after_seconds"`
	} `json:"error"`
}

// classify maps a status code to an outcome. Only the three genuinely terminal
// 4xx (400 malformed, 413 body-cap, 422 clock-skew) discard the batch; every
// other 4xx keeps the data, because dropping on a recoverable state loses
// metrics. 401/403/409 quarantine (revoked/suspended/paused/clone - all clear on
// their own once re-enrolled, unsuspended or unpaused), and 404 retries (a route
// gone during a pulse deploy must not cost data).
func classify(status int) outcome {
	switch {
	case status == http.StatusAccepted:
		return outcomeOK
	case status == http.StatusUnauthorized:
		// 401 TOKEN_INVALID.
		return outcomeQuarantine
	case status == http.StatusForbidden:
		// 403 ORG_SUSPENDED | AGENT_PAUSED_LIMIT | PERMISSION_MISSING |
		// ORG_INGEST_DISABLED: recoverable authz/policy, never wipe.
		return outcomeQuarantine
	case status == http.StatusConflict:
		// 409 INSTANCE_CONFLICT (clone-gate): recoverable via re-enroll, never wipe.
		return outcomeQuarantine
	case status == http.StatusTooManyRequests:
		return outcomeRetry
	case status == http.StatusNotFound:
		// 404 unknown route: transient during a pulse deploy, keep the data.
		return outcomeRetry
	case status >= 500:
		return outcomeRetry
	case status >= 400:
		// Terminal 4xx (400, 413, 422): poison, discard so it cannot wedge.
		return outcomeDrop
	default:
		// Unexpected 2xx/3xx: treat as transient and retry.
		return outcomeRetry
	}
}

// parseResponse builds a response from the status, headers and body.
func parseResponse(status int, header http.Header, body []byte) response {
	r := response{outcome: classify(status), status: status}
	switch r.outcome {
	case outcomeOK:
		var ok okBody
		if json.Unmarshal(body, &ok) == nil {
			r.configVersion = ok.ConfigVersion
			r.serverTimeMs = ok.ServerTimeUnixMs
		}
	default:
		var e errBody
		if json.Unmarshal(body, &e) == nil {
			r.errorCode = e.Error.Code
			if e.Error.RetryAfterSeconds != nil {
				r.retryAfter = time.Duration(*e.Error.RetryAfterSeconds) * time.Second
			}
		}
		if h := parseRetryAfter(header.Get("Retry-After")); h > 0 {
			r.retryAfter = h
		}
	}
	return r
}

// parseRetryAfter parses a Retry-After header: an integer seconds value. HTTP
// dates are not honoured (pulse sends seconds); an unparseable value yields 0.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}
