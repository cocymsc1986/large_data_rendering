import { METRIC_NAMES, SeriesGenerator } from './datapoint';

/** Stable device id, zero-padded so it sorts/aligns nicely: device-001, device-002, … */
export function deviceId(index: number): string {
  return `device-${String(index + 1).padStart(3, '0')}`;
}

/**
 * Build the full catalog of series generators: one generator per
 * (device × metric) pair. With the default 50 devices and 6 metrics that is
 * 300 distinct series.
 *
 * The catalog is the shared "universe" that every source (time series, stream,
 * dump) draws from, so a consumer sees a consistent set of seriesIds across all
 * three.
 */
export function buildCatalog(deviceCount: number): SeriesGenerator[] {
  const generators: SeriesGenerator[] = [];
  for (let d = 0; d < deviceCount; d++) {
    const id = deviceId(d);
    for (const metric of METRIC_NAMES) {
      generators.push(new SeriesGenerator(id, metric));
    }
  }
  return generators;
}
