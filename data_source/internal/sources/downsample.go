package sources

import "math"

// Bucket is one aggregated point produced by downsampling.
type Bucket struct {
	TS    int64   `json:"ts"`    // bucket start timestamp
	Value float64 `json:"value"` // mean value in the bucket
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Count int     `json:"count"`
}

// Downsample splits [from, to] into at most maxPoints equal-width time buckets
// and reduces each to {avg, min, max, count}. This is the simplest server-side
// reduction that keeps a chart honest at any zoom level: the consumer asks for
// "~N points for this range" instead of transferring the raw firehose. The wire
// format carries min/max so you can render an envelope; swap in LTTB later if
// you want shape-preserving downsampling.
func Downsample(points []RangePoint, from, to int64, maxPoints int) []Bucket {
	if len(points) == 0 {
		return []Bucket{}
	}
	if maxPoints < 1 {
		maxPoints = 1
	}
	if len(points) <= maxPoints {
		out := make([]Bucket, len(points))
		for i, p := range points {
			out[i] = Bucket{TS: p.TS, Value: p.Value, Min: p.Value, Max: p.Value, Count: 1}
		}
		return out
	}

	span := to - from
	if span < 1 {
		span = 1
	}
	width := float64(span) / float64(maxPoints)

	type acc struct {
		sum, min, max float64
		count         int
		ts            int64
	}
	buckets := make([]*acc, maxPoints)

	for _, p := range points {
		b := int(float64(p.TS-from) / width)
		if b < 0 {
			b = 0
		}
		if b >= maxPoints {
			b = maxPoints - 1
		}
		if buckets[b] == nil {
			buckets[b] = &acc{sum: p.Value, min: p.Value, max: p.Value, count: 1, ts: from + int64(float64(b)*width)}
		} else {
			a := buckets[b]
			a.sum += p.Value
			a.min = math.Min(a.min, p.Value)
			a.max = math.Max(a.max, p.Value)
			a.count++
		}
	}

	out := make([]Bucket, 0, maxPoints)
	for _, a := range buckets {
		if a == nil {
			continue
		}
		out = append(out, Bucket{
			TS:    a.ts,
			Value: math.Round(a.sum/float64(a.count)*100) / 100,
			Min:   a.min,
			Max:   a.max,
			Count: a.count,
		})
	}
	return out
}
