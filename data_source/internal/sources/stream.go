package sources

import (
	"sync"
	"time"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
)

// Record is one message on a Kafka-style topic/partition log. It mirrors the
// fields of a Kafka record you'd care about when rendering: a monotonic offset,
// a partition, a key, a timestamp and the payload.
type Record struct {
	Topic     string  `json:"topic"`
	Partition int     `json:"partition"`
	Offset    int64   `json:"offset"`
	TS        int64   `json:"ts"`
	Key       string  `json:"key"` // deviceId — Kafka partitions by key
	SeriesID  string  `json:"seriesId"`
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit"`
}

// partitionLog is a retention-capped, append-only log for one (topic, partition).
// Offsets are monotonic and never reused; once the log is full the oldest record
// is dropped and the "earliest" offset advances — exactly how a Kafka partition
// behaves under retention. A lagging consumer that asks for a dropped offset is
// fast-forwarded to the earliest retained record.
type partitionLog struct {
	mu         sync.RWMutex
	buf        []Record
	cap        int
	start      int // ring index of the oldest record
	count      int
	baseOffset int64 // offset of the oldest retained record
	nextOffset int64 // offset that will be assigned to the next append
}

func newPartitionLog(capacity int) *partitionLog {
	if capacity < 1 {
		capacity = 1
	}
	return &partitionLog{buf: make([]Record, capacity), cap: capacity}
}

func (l *partitionLog) append(r Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.Offset = l.nextOffset
	idx := (l.start + l.count) % l.cap
	l.buf[idx] = r
	if l.count < l.cap {
		l.count++
	} else {
		l.start = (l.start + 1) % l.cap
		l.baseOffset++
	}
	l.nextOffset++
}

// reset empties the log and rewinds offsets to zero, keeping the allocation.
func (l *partitionLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.start = 0
	l.count = 0
	l.baseOffset = 0
	l.nextOffset = 0
}

// offsets returns the earliest retained and the high-water (next) offset.
func (l *partitionLog) offsets() (earliest, next int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseOffset, l.nextOffset
}

// fetch returns up to max records starting at fromOffset, plus the offset a
// consumer should request next. fromOffset below the earliest retained offset is
// clamped up (lag past retention).
func (l *partitionLog) fetch(fromOffset int64, max int) ([]Record, int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if fromOffset < l.baseOffset {
		fromOffset = l.baseOffset
	}
	if fromOffset >= l.nextOffset {
		return nil, l.nextOffset
	}
	available := int(l.nextOffset - fromOffset)
	n := available
	if max > 0 && n > max {
		n = max
	}
	out := make([]Record, 0, n)
	startIdx := l.start + int(fromOffset-l.baseOffset)
	for i := 0; i < n; i++ {
		out = append(out, l.buf[(startIdx+i)%l.cap])
	}
	return out, fromOffset + int64(n)
}

// StreamSource is a toggleable Kafka-style producer. Topics correspond to
// metrics; each topic has a fixed number of partitions, keyed by device. While
// running it appends records to the logs at a configurable rate; consumers read
// via Fetch (used by both the SSE push endpoint and the HTTP poll endpoint).
type StreamSource struct {
	partitions int
	pps        int // points per second across all partitions
	batchEvery time.Duration
	gens       []*model.SeriesGenerator
	// deviceIndex maps deviceId -> catalog device index, for partition assignment.
	logs map[string][]*partitionLog // topic -> partition logs

	mu       sync.RWMutex
	running  bool
	stopCh   chan struct{}
	produced int64
	nextGen  int // round-robin cursor over gens for production
}

// NewStreamSource builds the topic/partition log structure up front so consumers
// can connect and replay even before production starts.
func NewStreamSource(deviceCount, partitions, retention int, defaultPPS int, batchEvery time.Duration) *StreamSource {
	if partitions < 1 {
		partitions = 1
	}
	gens := model.BuildCatalog(deviceCount)
	logs := make(map[string][]*partitionLog, len(model.MetricNames))
	for _, topic := range model.MetricNames {
		parts := make([]*partitionLog, partitions)
		for p := range parts {
			parts[p] = newPartitionLog(retention)
		}
		logs[topic] = parts
	}
	return &StreamSource{
		partitions: partitions,
		pps:        defaultPPS,
		batchEvery: batchEvery,
		gens:       gens,
		logs:       logs,
	}
}

// partitionFor maps a series to a partition by hashing its device index, mirroring
// Kafka's key-based partitioning so a given device always lands on one partition.
func (s *StreamSource) partitionFor(g *model.SeriesGenerator) int {
	h := 0
	for _, c := range g.DeviceID {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h % s.partitions
}

// Start begins production at pps points/sec. pps<=0 keeps the current rate.
func (s *StreamSource) Start(pps int) {
	s.mu.Lock()
	if s.running {
		if pps > 0 {
			s.pps = pps
		}
		s.mu.Unlock()
		return
	}
	if pps > 0 {
		s.pps = pps
	}
	s.running = true
	s.stopCh = make(chan struct{})
	stopCh := s.stopCh
	s.mu.Unlock()

	go s.loop(stopCh)
}

func (s *StreamSource) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.running = false
	close(s.stopCh)
	s.stopCh = nil
}

func (s *StreamSource) loop(stopCh chan struct{}) {
	ticker := time.NewTicker(s.batchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.produceBatch()
		}
	}
}

// produceBatch appends roughly pps*batchEvery records, round-robining across the
// series catalog so every device/metric gets coverage over time.
func (s *StreamSource) produceBatch() {
	s.mu.Lock()
	n := int(float64(s.pps) * s.batchEvery.Seconds())
	if n < 1 {
		n = 1
	}
	ts := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		g := s.gens[s.nextGen%len(s.gens)]
		s.nextGen++
		p := g.At(ts)
		part := s.partitionFor(g)
		s.logs[p.Metric][part].append(Record{
			Topic:     p.Metric,
			Partition: part,
			TS:        p.TS,
			Key:       p.DeviceID,
			SeriesID:  p.SeriesID,
			Metric:    p.Metric,
			Value:     p.Value,
			Unit:      p.Unit,
		})
		s.produced++
	}
	s.mu.Unlock()
}

// Reset clears every partition log, rewinds all offsets to zero and zeroes the
// produced counter. The running state is left untouched. Note: connected
// consumers holding a cursor ahead of the rewound offsets will see no records
// until production catches back up past their position.
func (s *StreamSource) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, parts := range s.logs {
		for _, lg := range parts {
			lg.reset()
		}
	}
	s.produced = 0
	s.nextGen = 0
}

// StreamStatus describes the producer for the control API.
type StreamStatus struct {
	Running    bool  `json:"running"`
	PPS        int   `json:"pps"`
	Topics     int   `json:"topics"`
	Partitions int   `json:"partitions"`
	Produced   int64 `json:"produced"`
}

func (s *StreamSource) Status() StreamStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return StreamStatus{
		Running:    s.running,
		PPS:        s.pps,
		Topics:     len(s.logs),
		Partitions: s.partitions,
		Produced:   s.produced,
	}
}

// PartitionInfo reports earliest/latest offsets for one partition.
type PartitionInfo struct {
	Partition int   `json:"partition"`
	Earliest  int64 `json:"earliest"`
	Latest    int64 `json:"latest"`
}

// TopicInfo is the metadata for one topic.
type TopicInfo struct {
	Topic      string          `json:"topic"`
	Partitions []PartitionInfo `json:"partitions"`
}

// Topics returns metadata (offsets) for all topics, for consumer bootstrapping.
func (s *StreamSource) Topics() []TopicInfo {
	out := make([]TopicInfo, 0, len(s.logs))
	for _, topic := range model.MetricNames {
		parts := s.logs[topic]
		pis := make([]PartitionInfo, len(parts))
		for p, lg := range parts {
			earliest, next := lg.offsets()
			pis[p] = PartitionInfo{Partition: p, Earliest: earliest, Latest: next}
		}
		out = append(out, TopicInfo{Topic: topic, Partitions: pis})
	}
	return out
}

// PartitionCount returns the number of partitions per topic.
func (s *StreamSource) PartitionCount() int { return s.partitions }

// HasTopic reports whether topic exists.
func (s *StreamSource) HasTopic(topic string) bool {
	_, ok := s.logs[topic]
	return ok
}

// Fetch reads up to maxPerPartition records from each requested partition of a
// topic, starting at the per-partition offsets in cursor. It returns the records
// and the updated cursor (partition -> next offset). Partitions absent from the
// cursor start at their requested default already baked into cursor by the caller.
func (s *StreamSource) Fetch(topic string, partitions []int, cursor map[int]int64, maxPerPartition int) ([]Record, map[int]int64) {
	logs := s.logs[topic]
	if logs == nil {
		return nil, cursor
	}
	out := make([]Record, 0)
	newCursor := make(map[int]int64, len(cursor))
	for k, v := range cursor {
		newCursor[k] = v
	}
	for _, p := range partitions {
		if p < 0 || p >= len(logs) {
			continue
		}
		recs, next := logs[p].fetch(cursor[p], maxPerPartition)
		out = append(out, recs...)
		newCursor[p] = next
	}
	return out, newCursor
}
