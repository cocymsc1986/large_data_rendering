package sources

import (
	"sort"
	"sync"
	"time"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
)

// TimeSeriesSource is a toggleable source that, while running, continuously
// appends a new point per series at a fixed resolution into per-series ring
// buffers. Consumers query historical ranges (with optional downsampling) — the
// classic "fetch a window to draw a chart" access pattern.
type TimeSeriesSource struct {
	resolution time.Duration
	maxPoints  int
	gens       []*model.SeriesGenerator

	mu       sync.RWMutex
	store    map[string]*ringBuffer // seriesId -> buffer
	running  bool
	stopCh   chan struct{}
	produced int64
}

func NewTimeSeriesSource(deviceCount int, resolution time.Duration, maxPoints int) *TimeSeriesSource {
	gens := model.BuildCatalog(deviceCount)
	store := make(map[string]*ringBuffer, len(gens))
	for _, g := range gens {
		store[g.SeriesID] = newRingBuffer(maxPoints)
	}
	return &TimeSeriesSource{
		resolution: resolution,
		maxPoints:  maxPoints,
		gens:       gens,
		store:      store,
	}
}

// Start begins generation. If backfillHours > 0 it first synthesises history so
// queries return data immediately. Calling Start while running is a no-op.
func (s *TimeSeriesSource) Start(backfillHours int) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	stopCh := s.stopCh
	s.mu.Unlock()

	if backfillHours > 0 {
		s.backfill(backfillHours)
	}

	go s.loop(stopCh)
}

// Stop halts generation. Stored data is retained so it can still be queried.
func (s *TimeSeriesSource) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.running = false
	close(s.stopCh)
	s.stopCh = nil
}

func (s *TimeSeriesSource) backfill(hours int) {
	now := time.Now().UnixMilli()
	step := s.resolution.Milliseconds()
	if step < 1 {
		step = 1000
	}
	start := now - int64(hours)*3600_000
	s.mu.Lock()
	defer s.mu.Unlock()
	for ts := start; ts <= now; ts += step {
		for _, g := range s.gens {
			p := g.At(ts)
			s.store[p.SeriesID].push(p.TS, p.Value)
			s.produced++
		}
	}
}

func (s *TimeSeriesSource) loop(stopCh chan struct{}) {
	ticker := time.NewTicker(s.resolution)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			ts := time.Now().UnixMilli()
			s.mu.Lock()
			for _, g := range s.gens {
				p := g.At(ts)
				s.store[p.SeriesID].push(p.TS, p.Value)
				s.produced++
			}
			s.mu.Unlock()
		}
	}
}

// Status describes the source for the control API.
type Status struct {
	Running     bool   `json:"running"`
	SeriesCount int    `json:"seriesCount"`
	Produced    int64  `json:"produced"`
	Resolution  string `json:"resolution"`
}

func (s *TimeSeriesSource) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Status{
		Running:     s.running,
		SeriesCount: len(s.gens),
		Produced:    s.produced,
		Resolution:  s.resolution.String(),
	}
}

// SeriesInfo summarises one stored series.
type SeriesInfo struct {
	SeriesID string `json:"seriesId"`
	DeviceID string `json:"deviceId"`
	Metric   string `json:"metric"`
	Points   int    `json:"points"`
	From     int64  `json:"from"`
	To       int64  `json:"to"`
}

// ListSeries returns metadata for every series, sorted by id.
func (s *TimeSeriesSource) ListSeries() []SeriesInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SeriesInfo, 0, len(s.gens))
	for _, g := range s.gens {
		buf := s.store[g.SeriesID]
		from, to, _ := buf.bounds()
		out = append(out, SeriesInfo{
			SeriesID: g.SeriesID,
			DeviceID: g.DeviceID,
			Metric:   g.Metric,
			Points:   buf.size(),
			From:     from,
			To:       to,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SeriesID < out[j].SeriesID })
	return out
}

// QueryResult is the payload returned by Query.
type QueryResult struct {
	SeriesID    string   `json:"seriesId"`
	From        int64    `json:"from"`
	To          int64    `json:"to"`
	Raw         int      `json:"raw"`      // raw points in range before downsampling
	Returned    int      `json:"returned"` // points actually returned
	Downsampled bool     `json:"downsampled"`
	Points      []Bucket `json:"points"`
}

// Query returns points for seriesId within [from, to], downsampled to at most
// maxPoints buckets. ok=false means the seriesId is unknown.
func (s *TimeSeriesSource) Query(seriesID string, from, to int64, maxPoints int) (QueryResult, bool) {
	s.mu.RLock()
	buf, exists := s.store[seriesID]
	var raw []RangePoint
	if exists {
		raw = buf.rangeQuery(from, to)
	}
	s.mu.RUnlock()

	if !exists {
		return QueryResult{}, false
	}
	buckets := Downsample(raw, from, to, maxPoints)
	return QueryResult{
		SeriesID:    seriesID,
		From:        from,
		To:          to,
		Raw:         len(raw),
		Returned:    len(buckets),
		Downsampled: len(buckets) < len(raw),
		Points:      buckets,
	}, true
}
