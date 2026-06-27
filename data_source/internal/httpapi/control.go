package httpapi

import (
	"encoding/json"
	"net/http"
)

// statusResponse is the combined view of every source for the dashboard.
type statusResponse struct {
	TimeSeries any `json:"timeSeries"`
	Stream     any `json:"stream"`
	Dump       any `json:"dump"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{
		TimeSeries: s.ts.Status(),
		Stream:     s.stream.Status(),
		Dump: map[string]any{
			"available":    true,
			"defaultCount": s.cfg.DumpCount,
			"formats":      []string{"json", "ndjson", "csv"},
		},
	})
}

// startBody is the optional JSON body for a start request.
type startBody struct {
	PPS           int  `json:"pps"`           // stream: points/sec
	BackfillHours *int `json:"backfillHours"` // timeseries: override backfill
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body startBody
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body) // body is optional
	}

	switch name {
	case "timeseries":
		backfill := s.cfg.TSBackfillHrs
		if body.BackfillHours != nil {
			backfill = *body.BackfillHours
		}
		s.ts.Start(backfill)
		writeJSON(w, http.StatusOK, s.ts.Status())
	case "stream":
		s.stream.Start(body.PPS)
		writeJSON(w, http.StatusOK, s.stream.Status())
	default:
		writeErr(w, http.StatusNotFound, "unknown source: "+name+" (use 'timeseries' or 'stream')")
	}
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	switch name {
	case "timeseries":
		s.ts.Stop()
		writeJSON(w, http.StatusOK, s.ts.Status())
	case "stream":
		s.stream.Stop()
		writeJSON(w, http.StatusOK, s.stream.Status())
	default:
		writeErr(w, http.StatusNotFound, "unknown source: "+name+" (use 'timeseries' or 'stream')")
	}
}
