/**
 * Central configuration for the mock data source.
 *
 * Every value can be overridden with an environment variable so you can run
 * several instances with different shapes/rates while building the consumer app.
 */
function num(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export const config = {
  /** HTTP/WebSocket port the standalone app listens on. */
  port: num('PORT', 4000),

  /**
   * Size of the series catalog. Every device emits one series per metric, so the
   * total number of distinct series is `deviceCount * METRIC_COUNT` (6 metrics).
   */
  deviceCount: num('DEVICE_COUNT', 50),

  timeSeries: {
    /** How often a new point is generated for every series while running. */
    resolutionMs: num('TS_RESOLUTION_MS', 1000),
    /** Per-series ring-buffer capacity (caps memory regardless of uptime). */
    maxPointsPerSeries: num('TS_MAX_POINTS', 50_000),
    /** On start, synthesise this many hours of history so queries return data immediately. */
    backfillHours: num('TS_BACKFILL_HOURS', 6),
  },

  stream: {
    /** Default firehose rate in points per second across all series. */
    pointsPerSecond: num('STREAM_PPS', 200),
    /** How often batches are flushed to subscribers (lower = smoother, more overhead). */
    batchIntervalMs: num('STREAM_BATCH_MS', 100),
  },

  dump: {
    /** Default number of points produced by the bulk dump endpoint/script. */
    defaultCount: num('DUMP_COUNT', 500_000),
    /** Time window the dump is spread across, in hours, ending "now". */
    windowHours: num('DUMP_WINDOW_HOURS', 24),
  },
} as const;

export type Config = typeof config;
