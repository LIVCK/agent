package sender

import (
	"net/http"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		status int
		want   outcome
	}{
		{202, outcomeOK},
		{401, outcomeQuarantine},
		{429, outcomeRetry},
		{500, outcomeRetry},
		{503, outcomeRetry},
		{400, outcomeDrop},
		{403, outcomeQuarantine}, // ORG_SUSPENDED/AGENT_PAUSED_LIMIT/... - recoverable, never wipe
		{404, outcomeRetry},      // unknown route, transient during a pulse deploy
		{409, outcomeQuarantine}, // INSTANCE_CONFLICT clone-gate - recoverable via re-enroll
		{413, outcomeDrop},
		{422, outcomeDrop},
		{301, outcomeRetry},
	}
	for _, c := range cases {
		if got := classify(c.status); got != c.want {
			t.Fatalf("classify(%d) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestParseResponseOK(t *testing.T) {
	body := []byte(`{"status":"ok","config_version":9,"server_time_unix_ms":1700000000000}`)
	r := parseResponse(202, http.Header{}, body)
	if r.outcome != outcomeOK {
		t.Fatalf("want ok, got %v", r.outcome)
	}
	if r.configVersion != 9 || r.serverTimeMs != 1700000000000 {
		t.Fatalf("parsed wrong 202 body: %+v", r)
	}
	if r.dropped.any() {
		t.Fatalf("no dropped block must parse as zero drops: %+v", r.dropped)
	}
}

func TestParseResponseOKDropped(t *testing.T) {
	body := []byte(`{"status":"ok","config_version":9,"server_time_unix_ms":1700000000000,` +
		`"dropped":{"reports":0,"keys":12,"events":1},"drop_reasons":{"cardinality":12,"sanity":1}}`)
	r := parseResponse(202, http.Header{}, body)
	if !r.dropped.any() || r.dropped.Keys != 12 || r.dropped.Events != 1 {
		t.Fatalf("dropped block not parsed: %+v", r.dropped)
	}
	if r.dropReasons["cardinality"] != 12 || r.dropReasons["sanity"] != 1 {
		t.Fatalf("drop_reasons not parsed: %+v", r.dropReasons)
	}
}

func TestParseResponseRetryAfterHeaderWins(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "42")
	body := []byte(`{"error":{"code":"RATE_LIMITED","retryable":true,"retry_after_seconds":5}}`)
	r := parseResponse(429, h, body)
	if r.retryAfter != 42*time.Second {
		t.Fatalf("Retry-After header should win, got %v", r.retryAfter)
	}
	if r.errorCode != "RATE_LIMITED" {
		t.Fatalf("error code = %q", r.errorCode)
	}
}

func TestParseResponseRetryAfterBodyFallback(t *testing.T) {
	body := []byte(`{"error":{"code":"QUOTA_EXCEEDED","retryable":true,"retry_after_seconds":7}}`)
	r := parseResponse(429, http.Header{}, body)
	if r.retryAfter != 7*time.Second {
		t.Fatalf("body retry_after_seconds should apply, got %v", r.retryAfter)
	}
}
