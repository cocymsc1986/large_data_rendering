import { buildCatalog } from '../generator/catalog';
import type { DataPoint } from '../generator/datapoint';

export interface DumpOptions {
  /** Total number of points to emit. */
  count: number;
  /** Number of devices to spread points across. */
  deviceCount: number;
  /** Inclusive start of the time window (epoch ms). */
  startTs: number;
  /** Inclusive end of the time window (epoch ms). */
  endTs: number;
}

/**
 * Lazily generate a bulk "dumped" dataset as a stream of points.
 *
 * Points are produced timestamp-by-timestamp across the whole series catalog so
 * the output is naturally ordered by time, then by series — exactly what you get
 * from a real export. Because this is a generator it has O(1) memory regardless
 * of `count`, which is what lets the endpoint stream 500k+ rows safely.
 */
export function* generateDump(opts: DumpOptions): Generator<DataPoint> {
  const generators = buildCatalog(opts.deviceCount);
  const seriesCount = generators.length;
  if (seriesCount === 0 || opts.count <= 0) return;

  const steps = Math.max(1, Math.ceil(opts.count / seriesCount));
  const span = Math.max(0, opts.endTs - opts.startTs);
  const interval = steps > 1 ? span / (steps - 1) : 0;

  let emitted = 0;
  for (let step = 0; step < steps && emitted < opts.count; step++) {
    const ts = Math.round(opts.startTs + step * interval);
    for (const gen of generators) {
      if (emitted >= opts.count) break;
      yield gen.at(ts);
      emitted++;
    }
  }
}
