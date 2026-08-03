package sender

import (
	"context"
	"strconv"
	"testing"
)

func gib(n uint64) string { return strconv.FormatUint(n<<30, 10) }

func TestMetaDigestIgnoresBootTime(t *testing.T) {
	// btime drifts a second or two with every NTP correction on a host that has not
	// rebooted in months. If that counted as a change, every refresh would re-send
	// the whole profile and the "on change" rule would mean nothing.
	a := map[string]string{"distro": "ubuntu", "boot_time": "1755500000"}
	b := map[string]string{"distro": "ubuntu", "boot_time": "1755500002"}

	if MetaDigest(a) != MetaDigest(b) {
		t.Fatal("a shifted boot_time must not read as a profile change")
	}
}

func TestMetaDigestIgnoresBalloonRAMDrift(t *testing.T) {
	sixteen := map[string]string{"ram_total_bytes": gib(16)}
	ballooned := map[string]string{"ram_total_bytes": strconv.FormatUint((16<<30)-(200<<20), 10)}

	if MetaDigest(sixteen) != MetaDigest(ballooned) {
		t.Fatal("balloon drift below a GiB must not read as a profile change")
	}
	if MetaDigest(sixteen) == MetaDigest(map[string]string{"ram_total_bytes": gib(24)}) {
		t.Fatal("a real resize upward must read as a profile change")
	}
	if MetaDigest(sixteen) == MetaDigest(map[string]string{"ram_total_bytes": gib(8)}) {
		t.Fatal("a real resize downward must read as a profile change")
	}

	// An unexpected format is compared verbatim rather than swallowed.
	if MetaDigest(map[string]string{"ram_total_bytes": "n/a"}) == MetaDigest(map[string]string{"ram_total_bytes": "n/a?"}) {
		t.Fatal("unparseable values must still compare exactly")
	}
}

func TestMetaDigestDetectsRealChanges(t *testing.T) {
	base := map[string]string{"distro": "ubuntu", "distro_version": "24.04", "kernel": "6.8.0-31"}

	cases := map[string]map[string]string{
		"distro upgrade": {"distro": "ubuntu", "distro_version": "26.04", "kernel": "6.8.0-31"},
		"kernel swap":    {"distro": "ubuntu", "distro_version": "24.04", "kernel": "6.11.0-9"},
		"added key":      {"distro": "ubuntu", "distro_version": "24.04", "kernel": "6.8.0-31", "virtualization": "kvm"},
		"removed key":    {"distro": "ubuntu", "distro_version": "24.04"},
	}
	for name, changed := range cases {
		if MetaDigest(base) == MetaDigest(changed) {
			t.Fatalf("%s must read as a profile change", name)
		}
	}

	// Map order is not part of the identity.
	reordered := map[string]string{"kernel": "6.8.0-31", "distro_version": "24.04", "distro": "ubuntu"}
	if MetaDigest(base) != MetaDigest(reordered) {
		t.Fatal("digest must not depend on map iteration order")
	}
}

func TestMetaDigestSeparatesKeyAndValue(t *testing.T) {
	if MetaDigest(map[string]string{"ab": "c"}) == MetaDigest(map[string]string{"a": "bc"}) {
		t.Fatal("key and value must not run together in the digest")
	}
}

// TestSenderResendsMetaOnlyWhenItChanged is the regression for the frozen system
// tags: an agent that ran through a distro upgrade kept reporting the profile it
// read at process start, so os/distro/arch stayed on the pre-upgrade values until
// something happened to restart the process.
func TestSenderResendsMetaOnlyWhenItChanged(t *testing.T) {
	f := newFixture(t, nil, nil,
		func(o *Options) { o.Meta = map[string]string{"distro": "ubuntu", "distro_version": "24.04"} })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	// 1) The first report of the process life carries the profile.
	addReport(f.ring, f.clock.Now().UnixMilli())
	f.sender.Wake()
	waitFor(t, func() bool { return f.server.count() >= 1 }, "bootstrap batch delivered")

	// 2) A refresh that read the same facts must change nothing at all.
	f.sender.SetMeta(map[string]string{"distro": "ubuntu", "distro_version": "24.04"})
	addReport(f.ring, f.clock.Now().UnixMilli()+1000)
	f.sender.Wake()
	waitFor(t, func() bool { return f.server.count() >= 2 }, "steady-state batch delivered")

	// 3) A genuine distro upgrade re-arms delivery exactly once.
	f.sender.SetMeta(map[string]string{"distro": "ubuntu", "distro_version": "26.04"})
	addReport(f.ring, f.clock.Now().UnixMilli()+2000)
	f.sender.Wake()
	waitFor(t, func() bool { return f.server.count() >= 3 }, "post-upgrade batch delivered")

	addReport(f.ring, f.clock.Now().UnixMilli()+3000)
	f.sender.Wake()
	waitFor(t, func() bool { return f.server.count() >= 4 }, "settled batch delivered")
	cancel()

	batches := f.server.allBatches()
	if len(batches) < 4 {
		t.Fatalf("want 4 batches, got %d", len(batches))
	}
	if got := batches[0].Reports[0].Meta["distro_version"]; got != "24.04" {
		t.Fatalf("first report must carry the startup profile, got %q", got)
	}
	if len(batches[1].Reports[0].Meta) != 0 {
		t.Fatalf("an unchanged profile must not ride again, got %+v", batches[1].Reports[0].Meta)
	}
	if got := batches[2].Reports[0].Meta["distro_version"]; got != "26.04" {
		t.Fatalf("a changed profile must ride the next batch, got %q", got)
	}
	if len(batches[3].Reports[0].Meta) != 0 {
		t.Fatalf("the changed profile must settle after its 202, got %+v", batches[3].Reports[0].Meta)
	}
}

// TestSenderResendsMetaAfterRestart pins the other half of the contract: a fresh
// process sends its profile even when nothing about the host changed, because the
// new process has no idea what the control plane already knows.
func TestSenderResendsMetaAfterRestart(t *testing.T) {
	meta := map[string]string{"distro": "ubuntu", "distro_version": "24.04"}

	for _, run := range []string{"first process", "after restart"} {
		f := newFixture(t, nil, nil, func(o *Options) { o.Meta = meta })

		ctx, cancel := context.WithCancel(context.Background())
		go f.sender.Run(ctx)

		addReport(f.ring, f.clock.Now().UnixMilli())
		f.sender.Wake()
		waitFor(t, func() bool { return f.server.count() >= 1 }, run+": batch delivered")
		cancel()

		batches := f.server.allBatches()
		if got := batches[0].Reports[0].Meta["distro_version"]; got != "24.04" {
			t.Fatalf("%s: the first report after start must carry the profile, got %q", run, got)
		}
	}
}

// TestSenderMetaRefreshedMidFlightIsNotSwallowed guards the ack bookkeeping: the
// 202 for a batch built before a refresh must not mark the NEWER profile delivered.
func TestSenderMetaRefreshedMidFlightIsNotSwallowed(t *testing.T) {
	f := newFixture(t, nil, nil,
		func(o *Options) { o.Meta = map[string]string{"distro": "ubuntu", "distro_version": "24.04"} })

	addReport(f.ring, f.clock.Now().UnixMilli())
	p := f.sender.buildPending()
	if p == nil || !p.carriedMeta {
		t.Fatal("expected a pending batch carrying the startup profile")
	}

	f.sender.SetMeta(map[string]string{"distro": "ubuntu", "distro_version": "26.04"})
	f.sender.onAccepted(p, response{})

	f.sender.mu.Lock()
	sent := f.sender.metaSent
	f.sender.mu.Unlock()

	if sent {
		t.Fatal("a 202 for the previous profile must not mark the refreshed one delivered")
	}
}
