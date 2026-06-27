package httpapi

import (
	"bufio"
	"net/http"
	"strconv"
	"time"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/serialize"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/sources"
)

// handleDump serves GET /api/dump?count=&format=&devices=&window=
// It streams a freshly generated bulk dataset. `download=1` sets a filename so a
// browser saves it; otherwise it renders inline. Memory stays O(1) because the
// generator and serializer both work point-by-point straight to the socket.
func (s *Server) handleDump(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	count := int(parseInt64(q.Get("count"), int64(s.cfg.DumpCount)))
	if count < 0 {
		count = 0
	}
	devices := int(parseInt64(q.Get("devices"), int64(s.cfg.DeviceCount)))
	if devices < 1 {
		devices = 1
	}
	windowHrs := int(parseInt64(q.Get("window"), int64(s.cfg.DumpWindowHrs)))

	format := serialize.Format(q.Get("format"))
	if q.Get("format") == "" {
		format = serialize.JSON
	} else if !serialize.IsFormat(q.Get("format")) {
		writeErr(w, http.StatusBadRequest, "format must be json, ndjson or csv")
		return
	}

	now := time.Now().UnixMilli()
	opts := sources.DumpOptions{
		Count:       count,
		DeviceCount: devices,
		StartTS:     now - int64(windowHrs)*3600_000,
		EndTS:       now,
	}

	w.Header().Set("Content-Type", serialize.ContentType(format))
	if q.Get("download") != "" {
		w.Header().Set("Content-Disposition", `attachment; filename="dump-`+strconv.Itoa(count)+"."+string(format)+`"`)
	}
	w.WriteHeader(http.StatusOK)

	bw := bufio.NewWriterSize(w, 64*1024)
	sw := serialize.NewWriter(bw, format)
	if err := sw.Begin(); err != nil {
		return // client likely gone
	}

	ctx := r.Context()
	err := sources.GenerateDump(opts, func(p model.DataPoint) error {
		// Bail out promptly if the client disconnects mid-stream.
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
