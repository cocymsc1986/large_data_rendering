package sources

// RangePoint is a single (timestamp, value) sample returned from a query.
type RangePoint struct {
	TS    int64   `json:"ts"`
	Value float64 `json:"value"`
}

// ringBuffer is a fixed-capacity, time-ordered store for one series, backed by
// parallel slices of timestamps and values. Using primitive slices keeps memory
// flat and predictable across hundreds of series — a small taste of the storage
// layout that matters when you scale up. Points are assumed pushed in
// non-decreasing timestamp order. Not safe for concurrent use; callers guard it.
type ringBuffer struct {
	ts    []int64
	val   []float64
	start int // index of the oldest element
	count int
	cap   int
}

func newRingBuffer(capacity int) *ringBuffer {
	if capacity < 1 {
		capacity = 1
	}
	return &ringBuffer{
		ts:  make([]int64, capacity),
		val: make([]float64, capacity),
		cap: capacity,
	}
}

func (r *ringBuffer) push(ts int64, value float64) {
	idx := (r.start + r.count) % r.cap
	r.ts[idx] = ts
	r.val[idx] = value
	if r.count < r.cap {
		r.count++
	} else {
		r.start = (r.start + 1) % r.cap // full: overwrite oldest
	}
}

func (r *ringBuffer) size() int { return r.count }

// reset empties the buffer, keeping the underlying allocation. Stale slots are
// left in place but become unreachable.
func (r *ringBuffer) reset() {
	r.start = 0
	r.count = 0
}

// bounds returns the oldest/newest timestamps held, and ok=false when empty.
func (r *ringBuffer) bounds() (from, to int64, ok bool) {
	if r.count == 0 {
		return 0, 0, false
	}
	oldest := r.ts[r.start]
	newest := r.ts[(r.start+r.count-1)%r.cap]
	return oldest, newest, true
}

// rangeQuery returns points with from <= ts <= to, in time order.
func (r *ringBuffer) rangeQuery(from, to int64) []RangePoint {
	out := make([]RangePoint, 0)
	for i := 0; i < r.count; i++ {
		idx := (r.start + i) % r.cap
		t := r.ts[idx]
		if t < from {
			continue
		}
		if t > to {
			break // ordered: nothing later qualifies
		}
		out = append(out, RangePoint{TS: t, Value: r.val[idx]})
	}
	return out
}
