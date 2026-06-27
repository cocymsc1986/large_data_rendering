/**
 * Core data model + per-series value generator.
 *
 * The shape is deliberately small and flat so it serialises cleanly to JSON,
 * NDJSON and CSV, and is cheap to render in bulk:
 *
 *   { ts, deviceId, metric, seriesId, value, unit }
 *
 * Values are produced with a mix of daily seasonality (a sine wave), a bounded
 * random walk, and per-step noise, then clamped to a sensible range. This gives
 * charts something that looks alive rather than pure noise.
 */

export type MetricName =
  | 'temperature'
  | 'humidity'
  | 'voltage'
  | 'cpu'
  | 'memory'
  | 'network';

export interface DataPoint {
  /** Epoch milliseconds. */
  ts: number;
  deviceId: string;
  metric: MetricName;
  /** `${deviceId}:${metric}` — the unique key a consumer subscribes to / queries. */
  seriesId: string;
  value: number;
  unit: string;
}

interface MetricSpec {
  unit: string;
  /** Mean the series oscillates around. */
  base: number;
  /** Peak-to-mean swing of the daily seasonal cycle. */
  amplitude: number;
  /** Std-dev of the random-walk step applied each tick. */
  drift: number;
  /** Std-dev of the independent per-tick observation noise. */
  noise: number;
  min: number;
  max: number;
}

export const METRICS: Record<MetricName, MetricSpec> = {
  temperature: { unit: '°C', base: 21, amplitude: 6, drift: 0.05, noise: 0.2, min: -10, max: 45 },
  humidity: { unit: '%', base: 55, amplitude: 15, drift: 0.2, noise: 0.5, min: 0, max: 100 },
  voltage: { unit: 'V', base: 230, amplitude: 4, drift: 0.1, noise: 0.5, min: 200, max: 260 },
  cpu: { unit: '%', base: 35, amplitude: 25, drift: 1.5, noise: 4, min: 0, max: 100 },
  memory: { unit: '%', base: 60, amplitude: 12, drift: 0.4, noise: 1, min: 0, max: 100 },
  network: { unit: 'Mbps', base: 120, amplitude: 80, drift: 5, noise: 12, min: 0, max: 1000 },
};

export const METRIC_NAMES = Object.keys(METRICS) as MetricName[];

const DAY_MS = 24 * 60 * 60 * 1000;

/** Standard-normal sample via Box–Muller. */
function gaussian(): number {
  let u = 0;
  let v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function clamp(x: number, lo: number, hi: number): number {
  return x < lo ? lo : x > hi ? hi : x;
}

/**
 * Stateful generator for a single series. Call {@link at} repeatedly with
 * non-decreasing timestamps; the random-walk component evolves with each call.
 */
export class SeriesGenerator {
  readonly seriesId: string;
  private readonly spec: MetricSpec;
  private readonly phase: number;
  /** Random-walk offset, kept loosely centred on zero. */
  private walk = 0;

  constructor(readonly deviceId: string, readonly metric: MetricName) {
    this.seriesId = `${deviceId}:${metric}`;
    this.spec = METRICS[metric];
    // Stagger each series so they don't all peak at the same instant.
    this.phase = Math.random() * Math.PI * 2;
    this.walk = gaussian() * this.spec.amplitude * 0.25;
  }

  at(ts: number): DataPoint {
    const { spec } = this;
    const timeOfDay = (ts % DAY_MS) / DAY_MS; // 0..1
    const seasonal = spec.amplitude * Math.sin(2 * Math.PI * timeOfDay + this.phase);

    // Evolve and gently decay the walk so it stays bounded over long runs.
    this.walk = this.walk * 0.999 + gaussian() * spec.drift;

    const raw = spec.base + seasonal + this.walk + gaussian() * spec.noise;
    const value = Math.round(clamp(raw, spec.min, spec.max) * 100) / 100;

    return {
      ts,
      deviceId: this.deviceId,
      metric: this.metric,
      seriesId: this.seriesId,
      value,
      unit: spec.unit,
    };
  }
}
