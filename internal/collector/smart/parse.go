package smart

import "github.com/LIVCK/agent/internal/collector"

// scanResult is the subset of `smartctl --json --scan` the collector reads.
type scanResult struct {
	Devices []scanDevice `json:"devices"`
}

type scanDevice struct {
	Name string `json:"name"`
}

// deviceInfo is the subset of `smartctl --json -a <dev>` the collector reads.
// Every field is a pointer or a slice so its absence is distinguishable from a
// zero value: an unreported attribute yields no key rather than a fake zero.
// Both ATA and NVMe drives populate smart_status, temperature.current and
// power_on_time.hours; the ATA attribute table carries reallocated/pending
// sector counts; NVMe carries wear as percentage_used.
type deviceInfo struct {
	ModelName   string `json:"model_name"`
	SmartStatus *struct {
		Passed bool `json:"passed"`
	} `json:"smart_status"`
	Temperature *struct {
		Current *float64 `json:"current"`
	} `json:"temperature"`
	PowerOnTime *struct {
		Hours *float64 `json:"hours"`
	} `json:"power_on_time"`
	// power_cycle_count is top-level and populated by both ATA and NVMe.
	PowerCycleCount *float64 `json:"power_cycle_count"`
	AtaAttrs        *struct {
		Table []struct {
			ID  int `json:"id"`
			Raw struct {
				Value float64 `json:"value"`
			} `json:"raw"`
		} `json:"table"`
	} `json:"ata_smart_attributes"`
	NvmeLog *struct {
		PercentageUsed   *float64 `json:"percentage_used"`
		AvailableSpare   *float64 `json:"available_spare"`
		MediaErrors      *float64 `json:"media_errors"`
		UnsafeShutdowns  *float64 `json:"unsafe_shutdowns"`
		CriticalWarning  *float64 `json:"critical_warning"`
		DataUnitsWritten *float64 `json:"data_units_written"`
		DataUnitsRead    *float64 `json:"data_units_read"`
	} `json:"nvme_smart_health_information_log"`
}

// nvmeDataUnitBytes converts an NVMe SMART "data units" count to bytes: one unit is 1000 × 512-byte
// blocks = 512,000 bytes (NVMe spec §5.14.1.2).
const nvmeDataUnitBytes = 512000

// SMART attribute IDs read from the ATA table.
const (
	attrReallocatedSectorCt  = 5
	attrCurrentPendingSector = 197
)

// samples assembles the present keys for one device.
func (d deviceInfo) samples(seg string) []collector.Sample {
	base := "sys.smart." + seg + "."
	var out []collector.Sample
	if d.SmartStatus != nil {
		v := 0.0
		if d.SmartStatus.Passed {
			v = 1.0
		}
		out = append(out, collector.Sample{Key: base + "health_ok", Value: v})
	}
	if d.Temperature != nil && d.Temperature.Current != nil {
		out = append(out, collector.Sample{Key: base + "temp_c", Value: *d.Temperature.Current})
	}
	if v, ok := d.ataRaw(attrReallocatedSectorCt); ok {
		out = append(out, collector.Sample{Key: base + "reallocated", Value: v})
	}
	if v, ok := d.ataRaw(attrCurrentPendingSector); ok {
		out = append(out, collector.Sample{Key: base + "pending", Value: v})
	}
	if d.PowerOnTime != nil && d.PowerOnTime.Hours != nil {
		out = append(out, collector.Sample{Key: base + "power_on_hours", Value: *d.PowerOnTime.Hours})
	}
	if d.PowerCycleCount != nil {
		out = append(out, collector.Sample{Key: base + "power_cycles", Value: *d.PowerCycleCount})
	}
	if d.NvmeLog != nil {
		nl := d.NvmeLog
		if nl.PercentageUsed != nil {
			// percentage_used can exceed 100 once a drive passes its rated life; clamp so the percent
			// stays ingestable (pulse drops out-of-range percents), pinning a worn-out drive at 100.
			out = append(out, collector.Sample{Key: base + "wear_pct", Value: collector.ClampPercent(*nl.PercentageUsed)})
		}
		if nl.AvailableSpare != nil {
			out = append(out, collector.Sample{Key: base + "available_spare_pct", Value: collector.ClampPercent(*nl.AvailableSpare)})
		}
		if nl.MediaErrors != nil {
			out = append(out, collector.Sample{Key: base + "media_errors", Value: *nl.MediaErrors})
		}
		if nl.UnsafeShutdowns != nil {
			out = append(out, collector.Sample{Key: base + "unsafe_shutdowns", Value: *nl.UnsafeShutdowns})
		}
		if nl.CriticalWarning != nil {
			out = append(out, collector.Sample{Key: base + "critical_warning", Value: *nl.CriticalWarning})
		}
		if nl.DataUnitsWritten != nil {
			out = append(out, collector.Sample{Key: base + "data_written_bytes", Value: *nl.DataUnitsWritten * nvmeDataUnitBytes})
		}
		if nl.DataUnitsRead != nil {
			out = append(out, collector.Sample{Key: base + "data_read_bytes", Value: *nl.DataUnitsRead * nvmeDataUnitBytes})
		}
	}
	return out
}

// ataRaw returns the raw value of an ATA attribute by id, or ok=false when the
// device has no ATA attribute table (an NVMe drive) or lacks that attribute.
func (d deviceInfo) ataRaw(id int) (float64, bool) {
	if d.AtaAttrs == nil {
		return 0, false
	}
	for _, a := range d.AtaAttrs.Table {
		if a.ID == id {
			return a.Raw.Value, true
		}
	}
	return 0, false
}
