package httpapi

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/serialize"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/sources"
)

const (
	defaultPageLimit = 10_000
	maxPageLimit     = 200_000
	defaultDumpSeed  = 1
)

// handleDump serves GET /api/dump.
//
// Two modes:
//   - Full stream (default): streams the whole dataset point-by-point in
//     json/ndjson/csv with O(1) memory. Params: count, format, devices, window,
//     seed, download.
//   - Cursor pagination (opt-in, when `cursor` or `limit` is present): returns
//     one page and an opaque `nextCursor` to fetch the next. The dataset is
//     deterministic for a given seed, so paging is coherent and stateless.
func (s *Server) handleDump(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	format := serialize.Format(q.Get("format"))
	if q.Get("format") == "" {
		format = serialize.JSON
	} else if !serialize.IsFormat(q.Get("format")) {
		writeErr(w, http.StatusBadRequest, "format must be json, ndjson or csv")
		return
	}

	if q.Has("cursor") || q.Has("limit") {
		s.serveDumpPage(w, r, q, format)
		return
	}
	s.serveDumpStream(w, r, q, format)
}

// serveDumpStream streams the entire dataset (no pagination).
func (s *Server) serveDumpStream(w http.ResponseWriter, r *http.Request, q url.Values, format serialize.Format) {
	opts := s.dumpOptsFromQuery(q)

	w.Header().Set("Content-Type", serialize.ContentType(format))
	if q.Get("download") != "" {
		w.Header().Set("Content-Disposition", `attachment; filename="dump-`+strconv.Itoa(opts.Count)+"."+string(format)+`"`)
	}
	w.WriteHeader(http.StatusOK)

	bw := bufio.NewWriterSize(w, 64*1024)
	sw := serialize.NewWriter(bw, format)
	if err := sw.Begin(); err != nil {
		return
	}
	ctx := r.Context()
	_, err := sources.GenerateDumpRange(opts, 0, 0, func(p model.DataPoint) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return sw.Row(p)
	})
	if err != nil {
		return
	}
	if err := sw.End(); err != nil {
		return
	}
	_ = bw.Flush()
}

// dumpPage is the JSON envelope for a paginated response.
type dumpPage struct {
	Offset     int               `json:"offset"`
	Limit      int               `json:"limit"`
	Count      int               `json:"count"`
	Returned   int               `json:"returned"`
	HasMore    bool              `json:"hasMore"`
	NextCursor string            `json:"nextCursor,omitempty"`
	Records    []model.DataPoint `json:"records"`
}

// serveDumpPage returns a single page plus a cursor to the next one. JSON wraps
// the rows in an envelope; ndjson/csv stream the rows and return cursor metadata
// in response headers (X-Next-Cursor, X-Has-More, X-Total-Count, X-Offset).
func (s *Server) serveDumpPage(w http.ResponseWriter, r *http.Request, q url.Values, format serialize.Format) {
	var opts sources.DumpOptions
	var offset, limit int

	if cur := q.Get("cursor"); cur != "" {
		c, err := decodeCursor(cur)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		opts = sources.DumpOptions{Count: c.Count, DeviceCount: c.Devices, StartTS: c.StartTS, EndTS: c.EndTS, Seed: c.Seed}
		offset = c.Offset
		limit = c.Limit
	} else {
		opts = s.dumpOptsFromQuery(q)
		offset = 0
	}

	// An explicit ?limit= always wins over the cursor's remembered page size.
	if q.Has("limit") {
		limit = int(parseInt64(q.Get("limit"), int64(defaultPageLimit)))
	}
	if limit <= 0 {
		limit = defaultPageLimit
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	if offset < 0 {
		offset = 0
	}
	if offset > opts.Count {
		offset = opts.Count
	}
	end := opts.Count
	if offset+limit < end {
		end = offset + limit
	}
	hasMore := end < opts.Count

	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(dumpCursor{
			Seed: opts.Seed, Count: opts.Count, Devices: opts.DeviceCount,
			StartTS: opts.StartTS, EndTS: opts.EndTS, Offset: end, Limit: limit,
		})
	}

	if format == serialize.JSON {
		records := make([]model.DataPoint, 0, end-offset)
		_, _ = sources.GenerateDumpRange(opts, offset, limit, func(p model.DataPoint) error {
			records = append(records, p)
			return nil
		})
		writeJSON(w, http.StatusOK, dumpPage{
			Offset: offset, Limit: limit, Count: opts.Count,
			Returned: len(records), HasMore: hasMore, NextCursor: nextCursor, Records: records,
		})
		return
	}

	// ndjson / csv: stream rows, cursor in headers.
	w.Header().Set("Content-Type", serialize.ContentType(format))
	w.Header().Set("X-Total-Count", strconv.Itoa(opts.Count))
	w.Header().Set("X-Offset", strconv.Itoa(offset))
	w.Header().Set("X-Has-More", strconv.FormatBool(hasMore))
	if nextCursor != "" {
		w.Header().Set("X-Next-Cursor", nextCursor)
	}
	w.WriteHeader(http.StatusOK)

	bw := bufio.NewWriterSize(w, 64*1024)
	sw := serialize.NewWriter(bw, format)
	if err := sw.Begin(); err != nil {
		return
	}
	ctx := r.Context()
	_, _ = sources.GenerateDumpRange(opts, offset, limit, func(p model.DataPoint) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return sw.Row(p)
	})
	_ = sw.End()
	_ = bw.Flush()
}

// dumpOptsFromQuery builds dump options from request params, applying defaults.
func (s *Server) dumpOptsFromQuery(q url.Values) sources.DumpOptions {
	count := int(parseInt64(q.Get("count"), int64(s.cfg.DumpCount)))
	if count < 0 {
		count = 0
	}
	devices := int(parseInt64(q.Get("devices"), int64(s.cfg.DeviceCount)))
	if devices < 1 {
		devices = 1
	}
	windowHrs := int(parseInt64(q.Get("window"), int64(s.cfg.DumpWindowHrs)))
	seed := parseInt64(q.Get("seed"), defaultDumpSeed)

	// The window defaults to [now-window, now]. Pin it explicitly with start/end
	// (epoch ms) for a fully reproducible, byte-identical static dataset.
	now := time.Now().UnixMilli()
	end := parseInt64(q.Get("end"), now)
	start := parseInt64(q.Get("start"), end-int64(windowHrs)*3600_000)

	return sources.DumpOptions{
		Count:       count,
		DeviceCount: devices,
		StartTS:     start,
		EndTS:       end,
		Seed:        seed,
	}
}

// ---- opaque cursor --------------------------------------------------------

// dumpCursor captures everything needed to resume a paginated scan: the dataset
// definition (so pages stay coherent) plus the next offset and page size. It is
// base64url-encoded JSON — opaque to clients, who should just echo nextCursor.
type dumpCursor struct {
	Seed    int64 `json:"s"`
	Count   int   `json:"c"`
	Devices int   `json:"d"`
	StartTS int64 `json:"st"`
	EndTS   int64 `json:"et"`
	Offset  int   `json:"o"`
	Limit   int   `json:"l"`
}

func encodeCursor(c dumpCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (dumpCursor, error) {
	var c dumpCursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	return c, nil
}
