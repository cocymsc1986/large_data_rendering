package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/sources"
)

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"partitions": s.stream.PartitionCount(),
		"topics":     s.stream.Topics(),
	})
}

// ---- cursor helpers -------------------------------------------------------

// A cursor is a compact "partition:offset" map, e.g. "0:105,1:98,2:110". It is
// what a consumer carries between fetches (HTTP poll) and what we put in the SSE
// `id:` field so a reconnect (Last-Event-ID) resumes exactly where it left off —
// the moral equivalent of a Kafka committed offset.
func formatCursor(c map[int]int64) string {
	parts := make([]int, 0, len(c))
	for p := range c {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d:%d", p, c[p])
	}
	return b.String()
}

func parseCursor(s string) map[int]int64 {
	out := map[int]int64{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			continue
		}
		p, err1 := strconv.Atoi(strings.TrimSpace(kv[0]))
		o, err2 := strconv.ParseInt(strings.TrimSpace(kv[1]), 10, 64)
		if err1 == nil && err2 == nil {
			out[p] = o
		}
	}
	return out
}

// parsePartitions reads a "0,1,2" list, defaulting to all partitions of topic.
func (s *Server) parsePartitions(raw string) []int {
	n := s.stream.PartitionCount()
	if raw == "" {
		all := make([]int, n)
		for i := range all {
			all[i] = i
		}
		return all
	}
	var out []int
	for _, tok := range strings.Split(raw, ",") {
		if p, err := strconv.Atoi(strings.TrimSpace(tok)); err == nil && p >= 0 && p < n {
			out = append(out, p)
		}
	}
	return out
}

// resolveCursor turns the `offset` parameter (or a Last-Event-ID cursor) into a
// concrete per-partition start map. Accepts "earliest", "latest" (default), or a
// raw cursor string. Partitions absent from a raw cursor default to latest, so a
// consumer only replays what it explicitly asked for.
func (s *Server) resolveCursor(topic string, partitions []int, spec string) map[int]int64 {
	offsetsByPart := map[int][2]int64{} // partition -> [earliest, latest]
	for _, ti := range s.stream.Topics() {
		if ti.Topic != topic {
			continue
		}
		for _, pi := range ti.Partitions {
			offsetsByPart[pi.Partition] = [2]int64{pi.Earliest, pi.Latest}
		}
	}

	cursor := map[int]int64{}
	switch spec {
	case "", "latest":
		for _, p := range partitions {
			cursor[p] = offsetsByPart[p][1]
		}
	case "earliest":
		for _, p := range partitions {
			cursor[p] = offsetsByPart[p][0]
		}
	default:
		parsed := parseCursor(spec)
		for _, p := range partitions {
			if o, ok := parsed[p]; ok {
				cursor[p] = o
			} else {
				cursor[p] = offsetsByPart[p][1] // default new partitions to latest
			}
		}
	}
	return cursor
}

// ---- HTTP poll (Kafka fetch style) ---------------------------------------

// handlePoll serves GET /api/stream/poll?topic=&partitions=&offset=&max=
// It returns the next batch of records and an updated cursor; the consumer polls
// again with that cursor. This is the pull-based alternative to SSE.
func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	if !s.stream.HasTopic(topic) {
		writeErr(w, http.StatusBadRequest, "unknown topic (see /api/stream/topics)")
		return
	}
	partitions := s.parsePartitions(q.Get("partitions"))
	cursor := s.resolveCursor(topic, partitions, q.Get("offset"))
	max := int(parseInt64(q.Get("max"), 1000))

	records, next := s.stream.Fetch(topic, partitions, cursor, max)
	writeJSON(w, http.StatusOK, map[string]any{
		"topic":   topic,
		"count":   len(records),
		"cursor":  formatCursor(next),
		"records": records,
	})
}

// ---- SSE push -------------------------------------------------------------

// handleSSE serves GET /stream/sse?topic=&partitions=&offset= as a Server-Sent
// Events stream. Each event's `id:` carries the cursor so an automatic reconnect
// (browsers resend it as Last-Event-ID) resumes without gaps or dupes.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	if !s.stream.HasTopic(topic) {
		writeErr(w, http.StatusBadRequest, "unknown topic (see /api/stream/topics)")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	partitions := s.parsePartitions(q.Get("partitions"))

	// Last-Event-ID (set by the browser on reconnect) wins over the query param.
	startSpec := q.Get("offset")
	if lei := r.Header.Get("Last-Event-ID"); lei != "" {
		startSpec = lei
	}
	cursor := s.resolveCursor(topic, partitions, startSpec)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "retry: 3000\n: connected to %s\n\n", topic)
	flusher.Flush()

	ctx := r.Context()
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": keep-alive\n\n")
			flusher.Flush()
		case <-poll.C:
			records, next := s.stream.Fetch(topic, partitions, cursor, 5000)
			if len(records) == 0 {
				continue
			}
			cursor = next
			if !writeSSEEvent(w, topic, records, cursor) {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, topic string, records []sources.Record, cursor map[int]int64) bool {
	payload, err := json.Marshal(map[string]any{
		"topic":   topic,
		"count":   len(records),
		"records": records,
	})
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "id: %s\nevent: records\ndata: %s\n\n", formatCursor(cursor), payload); err != nil {
		return false
	}
	return true
}
