// Package httpapi wires the sources to HTTP endpoints: a control API to toggle
// sources, a downsampling time-series query API, a streaming bulk-dump endpoint,
// and Kafka-style stream consumption over SSE and HTTP poll.
package httpapi

import (
	"encoding/json"
	"io/fs"
	"net/http"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/config"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/sources"
)

// Server holds the sources and config behind the HTTP handlers.
type Server struct {
	cfg    config.Config
	ts     *sources.TimeSeriesSource
	stream *sources.StreamSource
	web    fs.FS
}

func NewServer(cfg config.Config, ts *sources.TimeSeriesSource, stream *sources.StreamSource, web fs.FS) *Server {
	return &Server{cfg: cfg, ts: ts, stream: stream, web: web}
}

// Handler builds the router. Uses Go 1.22+ method+pattern routing.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Control API — toggle sources on/off and read status.
	mux.HandleFunc("GET /api/sources", s.handleStatus)
	mux.HandleFunc("POST /api/sources/reset", s.handleResetAll)
	mux.HandleFunc("POST /api/sources/{name}/start", s.handleStart)
	mux.HandleFunc("POST /api/sources/{name}/stop", s.handleStop)
	mux.HandleFunc("POST /api/sources/{name}/reset", s.handleReset)

	// Time-series query API.
	mux.HandleFunc("GET /api/timeseries/series", s.handleSeriesList)
	mux.HandleFunc("GET /api/timeseries", s.handleTimeSeriesQuery)

	// Bulk dump (static dataset).
	mux.HandleFunc("GET /api/dump", s.handleDump)

	// Kafka-style stream.
	mux.HandleFunc("GET /api/stream/topics", s.handleTopics)
	mux.HandleFunc("GET /api/stream/poll", s.handlePoll)
	mux.HandleFunc("GET /stream/sse", s.handleSSE)

	// Control dashboard (embedded static assets).
	mux.Handle("GET /", http.FileServer(http.FS(s.web)))

	return withCORS(mux)
}

// withCORS allows the consumer app (likely on a different origin/port) to call
// these endpoints from the browser.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
