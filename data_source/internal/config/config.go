// Package config holds tunables for the mock data source. Defaults can be
// overridden by environment variables so several instances with different
// shapes/rates can run side by side while you build the consumer app.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr string // listen address, e.g. ":4000"

	// DeviceCount sizes the series catalog: DeviceCount * 6 metrics = total series.
	DeviceCount int

	// Time series source.
	TSResolution  time.Duration // how often each series gets a new point while running
	TSMaxPoints   int           // per-series ring-buffer capacity (caps memory)
	TSBackfillHrs int           // hours of history synthesised on start

	// Stream (Kafka-style) source.
	StreamPPS        int // default firehose rate, points/sec across all partitions
	StreamPartitions int // partitions per topic
	StreamRetention  int // records retained per partition (the replayable log)

	// Dump source.
	DumpCount     int // default number of points for the bulk dump
	DumpWindowHrs int // time window (hours) the dump is spread across, ending now
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envStr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// Default builds a Config from environment variables, falling back to sensible
// defaults tuned for ~500k-row rendering experiments.
func Default() Config {
	return Config{
		Addr:             envStr("ADDR", ":4000"),
		DeviceCount:      envInt("DEVICE_COUNT", 50),
		TSResolution:     time.Duration(envInt("TS_RESOLUTION_MS", 1000)) * time.Millisecond,
		TSMaxPoints:      envInt("TS_MAX_POINTS", 50_000),
		TSBackfillHrs:    envInt("TS_BACKFILL_HOURS", 6),
		StreamPPS:        envInt("STREAM_PPS", 200),
		StreamPartitions: envInt("STREAM_PARTITIONS", 4),
		StreamRetention:  envInt("STREAM_RETENTION", 100_000),
		DumpCount:        envInt("DUMP_COUNT", 500_000),
		DumpWindowHrs:    envInt("DUMP_WINDOW_HOURS", 24),
	}
}
