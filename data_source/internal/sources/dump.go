package sources

import (
	"math"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
)

// DumpOptions configures a one-off bulk dataset.
//
// Unlike the live sources, the dump is *deterministic*: every point is a pure
// function of (Seed, global index). That reproducibility is what makes
// cursor-based pagination coherent — fetching page 2 of a dataset returns the
// same rows whether or not you fetched page 1, and any point can be produced in
// O(1) without replaying the ones before it.
type DumpOptions struct {
	Count       int   // total points to emit
	DeviceCount int   // devices to spread across
	StartTS     int64 // inclusive window start (epoch ms)
	EndTS       int64 // inclusive window end (epoch ms)
	Seed        int64 // dataset seed; same seed + params => identical dataset
}

const dumpDayMS = 24 * 60 * 60 * 1000

// seriesCount is DeviceCount × number of metrics — the size of one timestamp's
// worth of points.
func (o DumpOptions) seriesCount() int {
	return o.DeviceCount * len(model.MetricNames)
}

// steps is how many distinct timestamps the dump spans.
func (o DumpOptions) steps() int {
	sc := o.seriesCount()
	if sc == 0 || o.Count <= 0 {
		return 0
	}
	s := (o.Count + sc - 1) / sc // ceil
	if s < 1 {
		s = 1
	}
	return s
}

// pointAt returns the point at global index i (0 <= i < Count). Points are laid
// out timestamp-major then device-major then metric-minor, matching the live
// catalog ordering, so a dump reads like a real time-ordered export.
func (o DumpOptions) pointAt(i int) model.DataPoint {
	sc := o.seriesCount()
	nMetrics := len(model.MetricNames)
	step := i / sc
	cat := i % sc
	deviceIdx := cat / nMetrics
	metricIdx := cat % nMetrics
	metric := model.MetricNames[metricIdx]
	spec := model.Metrics[metric]

	span := o.EndTS - o.StartTS
	if span < 0 {
		span = 0
	}
	var interval float64
	if steps := o.steps(); steps > 1 {
		interval = float64(span) / float64(steps-1)
	}
	ts := o.StartTS + int64(float64(step)*interval)

	return model.DataPoint{
		TS:       ts,
		DeviceID: model.DeviceID(deviceIdx),
		Metric:   metric,
		SeriesID: model.DeviceID(deviceIdx) + ":" + metric,
		Value:    dumpValue(uint64(o.Seed), deviceIdx, metricIdx, spec, ts, step),
		Unit:     spec.Unit,
	}
}

// GenerateDumpRange emits points [offset, offset+limit) clamped to [0, Count),
// in order, and returns the next offset (the end of the emitted range). A limit
// <= 0 means "to the end". If emit returns an error, generation stops and the
// index reached is returned alongside it.
func GenerateDumpRange(o DumpOptions, offset, limit int, emit func(model.DataPoint) error) (int, error) {
	if offset < 0 {
		offset = 0
	}
	if offset > o.Count {
		offset = o.Count
	}
	end := o.Count
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	for i := offset; i < end; i++ {
		if err := emit(o.pointAt(i)); err != nil {
			return i, err
		}
	}
	return end, nil
}

// GenerateDump emits the entire dataset (the non-paginated streaming path).
func GenerateDump(o DumpOptions, emit func(model.DataPoint) error) error {
	_, err := GenerateDumpRange(o, 0, 0, emit)
	return err
}

// ---- deterministic value synthesis ---------------------------------------

// dumpValue reproduces the look of the live generator (daily seasonality +
// drifting walk + noise) but as a stateless, seekable function of step. The
// "walk" is fractal value-noise so it stays continuous without accumulating
// state, which is what allows O(1) random access for pagination.
func dumpValue(seed uint64, deviceIdx, metricIdx int, spec model.MetricSpec, ts int64, step int) float64 {
	base := hashU(seed, uint64(deviceIdx), uint64(metricIdx))
	phase := u01(base) * 2 * math.Pi

	tod := float64(((ts%dumpDayMS)+dumpDayMS)%dumpDayMS) / float64(dumpDayMS)
	seasonal := spec.Amplitude * math.Sin(2*math.Pi*tod+phase)

	walk := fractalWalk(base, step) * spec.Amplitude * 0.6
	noise := gaussianHash(base, step) * spec.Noise

	v := spec.Base + seasonal + walk + noise
	return math.Round(clampF(v, spec.Min, spec.Max)*100) / 100
}

// fractalWalk sums a few sine octaves with seed-derived phases to produce a
// smooth, slowly drifting signal in roughly [-1.7, 1.7].
func fractalWalk(seed uint64, step int) float64 {
	s := float64(step)
	sum, amp, freq := 0.0, 1.0, 0.01
	for o := 0; o < 4; o++ {
		ph := u01(hashU(seed, uint64(o), 0x5715)) * 2 * math.Pi
		sum += amp * math.Sin(freq*s+ph)
		amp *= 0.5
		freq *= 2.17
	}
	return sum
}

// gaussianHash returns a deterministic standard-normal sample via Box–Muller,
// keyed by (seed, step), so per-point noise is reproducible.
func gaussianHash(seed uint64, step int) float64 {
	u1 := u01(hashU(seed, uint64(step), 0xA1))
	u2 := u01(hashU(seed, uint64(step), 0xB2))
	if u1 < 1e-12 {
		u1 = 1e-12
	}
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// splitmix64-style integer mix; fast and well-distributed.
func mix(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

func hashU(vals ...uint64) uint64 {
	h := uint64(1469598103934665603)
	for _, v := range vals {
		h = mix(h ^ v)
	}
	return h
}

// u01 maps a hash to a float in [0, 1).
func u01(h uint64) float64 {
	return float64(h>>11) / float64(uint64(1)<<53)
}

func clampF(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}
