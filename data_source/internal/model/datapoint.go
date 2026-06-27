// Package model defines the shared data shape and the per-series value
// generator used by every source (time series, stream, dump). Keeping a single
// "universe" of series means a consumer sees consistent seriesIds across all
// three sources.
package model

import (
	"math"
	"math/rand"
)

// DataPoint is the flat record every source emits. It serialises cleanly to
// JSON, NDJSON and CSV and is cheap to render in bulk.
type DataPoint struct {
	TS       int64   `json:"ts"`       // epoch milliseconds
	DeviceID string  `json:"deviceId"` // e.g. "device-001"
	Metric   string  `json:"metric"`   // one of MetricNames
	SeriesID string  `json:"seriesId"` // "<deviceId>:<metric>" — the subscribe/query key
	Value    float64 `json:"value"`
	Unit     string  `json:"unit"`
}

// MetricSpec describes how a single metric behaves over time.
type MetricSpec struct {
	Unit      string
	Base      float64 // mean the series oscillates around
	Amplitude float64 // peak-to-mean swing of the daily seasonal cycle
	Drift     float64 // std-dev of the random-walk step per tick
	Noise     float64 // std-dev of independent per-tick observation noise
	Min       float64
	Max       float64
}

// Metrics is the catalog of metric behaviours. The map is read-only after init.
var Metrics = map[string]MetricSpec{
	"temperature": {Unit: "°C", Base: 21, Amplitude: 6, Drift: 0.05, Noise: 0.2, Min: -10, Max: 45},
	"humidity":    {Unit: "%", Base: 55, Amplitude: 15, Drift: 0.2, Noise: 0.5, Min: 0, Max: 100},
	"voltage":     {Unit: "V", Base: 230, Amplitude: 4, Drift: 0.1, Noise: 0.5, Min: 200, Max: 260},
	"cpu":         {Unit: "%", Base: 35, Amplitude: 25, Drift: 1.5, Noise: 4, Min: 0, Max: 100},
	"memory":      {Unit: "%", Base: 60, Amplitude: 12, Drift: 0.4, Noise: 1, Min: 0, Max: 100},
	"network":     {Unit: "Mbps", Base: 120, Amplitude: 80, Drift: 5, Noise: 12, Min: 0, Max: 1000},
}

// MetricNames lists metrics in a stable order (maps don't iterate
// deterministically, and we want reproducible partition/series layout).
var MetricNames = []string{"temperature", "humidity", "voltage", "cpu", "memory", "network"}

const dayMS = 24 * 60 * 60 * 1000

// SeriesGenerator produces values for one series. Call At with non-decreasing
// timestamps; the random-walk component evolves on each call. It is not safe
// for concurrent use — give each goroutine its own generator (or guard it).
type SeriesGenerator struct {
	DeviceID string
	Metric   string
	SeriesID string
	spec     MetricSpec
	phase    float64
	walk     float64
	rng      *rand.Rand
}

// NewSeriesGenerator builds a generator with a randomised phase/seed so series
// don't all peak at the same instant.
func NewSeriesGenerator(deviceID, metric string) *SeriesGenerator {
	spec := Metrics[metric]
	rng := rand.New(rand.NewSource(rand.Int63()))
	return &SeriesGenerator{
		DeviceID: deviceID,
		Metric:   metric,
		SeriesID: deviceID + ":" + metric,
		spec:     spec,
		phase:    rng.Float64() * 2 * math.Pi,
		walk:     rng.NormFloat64() * spec.Amplitude * 0.25,
		rng:      rng,
	}
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

// At returns the data point for this series at time ts (epoch ms).
func (g *SeriesGenerator) At(ts int64) DataPoint {
	timeOfDay := float64(ts%dayMS) / float64(dayMS) // 0..1
	seasonal := g.spec.Amplitude * math.Sin(2*math.Pi*timeOfDay+g.phase)

	// Evolve and gently decay the walk so it stays bounded over long runs.
	g.walk = g.walk*0.999 + g.rng.NormFloat64()*g.spec.Drift

	raw := g.spec.Base + seasonal + g.walk + g.rng.NormFloat64()*g.spec.Noise
	value := math.Round(clamp(raw, g.spec.Min, g.spec.Max)*100) / 100

	return DataPoint{
		TS:       ts,
		DeviceID: g.DeviceID,
		Metric:   g.Metric,
		SeriesID: g.SeriesID,
		Value:    value,
		Unit:     g.spec.Unit,
	}
}
