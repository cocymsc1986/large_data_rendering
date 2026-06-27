/**
 * Fixed-capacity ring buffer for a single time series, backed by two
 * Float64Arrays (timestamps + values). Using typed arrays keeps memory flat and
 * predictable even with hundreds of series × tens of thousands of points each —
 * a small taste of the kind of storage layout that matters when you scale up.
 *
 * Points are assumed to be pushed in non-decreasing timestamp order, so range
 * queries can rely on the buffer being time-ordered.
 */
export interface RangePoint {
  ts: number;
  value: number;
}

export class RingBuffer {
  private readonly ts: Float64Array;
  private readonly val: Float64Array;
  private start = 0; // index of the oldest element
  private count = 0; // number of valid elements

  constructor(readonly capacity: number) {
    this.ts = new Float64Array(capacity);
    this.val = new Float64Array(capacity);
  }

  get size(): number {
    return this.count;
  }

  push(ts: number, value: number): void {
    const idx = (this.start + this.count) % this.capacity;
    this.ts[idx] = ts;
    this.val[idx] = value;
    if (this.count < this.capacity) {
      this.count++;
    } else {
      // Buffer full: overwrite oldest, advance start.
      this.start = (this.start + 1) % this.capacity;
    }
  }

  /** Oldest / newest timestamps currently held, or undefined when empty. */
  bounds(): { from: number; to: number } | undefined {
    if (this.count === 0) return undefined;
    const oldest = this.ts[this.start];
    const newest = this.ts[(this.start + this.count - 1) % this.capacity];
    return { from: oldest, to: newest };
  }

  /** Collect points with `from <= ts <= to`, in time order. */
  range(from: number, to: number): RangePoint[] {
    const out: RangePoint[] = [];
    for (let i = 0; i < this.count; i++) {
      const idx = (this.start + i) % this.capacity;
      const t = this.ts[idx];
      if (t < from) continue;
      if (t > to) break; // ordered, so we can stop early
      out.push({ ts: t, value: this.val[idx] });
    }
    return out;
  }
}
