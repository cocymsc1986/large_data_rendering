import type { RangePoint } from './ringBuffer';

export interface Bucket {
  /** Representative timestamp for the bucket (bucket start). */
  ts: number;
  /** Mean value in the bucket. */
  value: number;
  min: number;
  max: number;
  count: number;
}

/**
 * Bucket-aggregate downsampling: split [from, to] into at most `maxPoints`
 * equal-width time buckets and reduce each to {avg, min, max, count}.
 *
 * This is the simplest server-side reduction that keeps a chart honest at any
 * zoom level — the consumer asks for "give me ~2000 points for this range" and
 * never has to transfer the raw firehose. (Swap in LTTB later if you want
 * shape-preserving downsampling; the wire format here already carries min/max.)
 */
export function downsample(points: RangePoint[], from: number, to: number, maxPoints: number): Bucket[] {
  if (points.length === 0) return [];
  if (points.length <= maxPoints) {
    return points.map((p) => ({ ts: p.ts, value: p.value, min: p.value, max: p.value, count: 1 }));
  }

  const span = Math.max(1, to - from);
  const bucketCount = Math.max(1, maxPoints);
  const width = span / bucketCount;

  const buckets: (Bucket | undefined)[] = new Array(bucketCount);
  for (const p of points) {
    let b = Math.floor((p.ts - from) / width);
    if (b < 0) b = 0;
    if (b >= bucketCount) b = bucketCount - 1;

    const existing = buckets[b];
    if (!existing) {
      buckets[b] = { ts: from + b * width, value: p.value, min: p.value, max: p.value, count: 1 };
    } else {
      existing.value += p.value;
      existing.min = Math.min(existing.min, p.value);
      existing.max = Math.max(existing.max, p.value);
      existing.count++;
    }
  }

  const out: Bucket[] = [];
  for (const b of buckets) {
    if (!b) continue;
    b.value = Math.round((b.value / b.count) * 100) / 100;
    out.push(b);
  }
  return out;
}
