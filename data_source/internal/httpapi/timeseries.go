package httpapi

import (
	"net/http"
	"strconv"
	"time"
)

func (s *Server) handleSeriesList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.ts.ListSeries())
}

// handleTimeSeriesQuery serves GET /api/timeseries?seriesId=&from=&to=&maxPoints=
// from/to are epoch ms; omitted from defaults to "last 1h", omitted to defaults
// to "now". maxPoints defaults to 2000 — a sane target for a chart width.
func (s *Server) handleTimeSeriesQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	seriesID := q.Get("seriesId")
	if seriesID == "" {
		writeErr(w, http.StatusBadRequest, "seriesId is required (see /api/timeseries/series)")
		return
	}

	now := time.Now().UnixMilli()
	to := parseInt64(q.Get("to"), now)
	from := parseInt64(q.Get("from"), to-3600_000)
	maxPoints := int(parseInt64(q.Get("maxPoints"), 2000))
	if maxPoints < 1 {
		maxPoints = 1
	}
	if from > to {
		writeErr(w, http.StatusBadRequest, "from must be <= to")
		return
	}

	result, ok := s.ts.Query(seriesID, from, to, maxPoints)
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown seriesId: "+seriesID)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func parseInt64(s string, def int64) int64 {
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return n
}
