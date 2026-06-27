// Package serialize provides streaming writers for the bulk dump in JSON,
// NDJSON and CSV. Each writer takes points one at a time so the HTTP handler can
// stream hundreds of thousands of rows without holding them in memory.
package serialize

import (
	"bufio"
	"strconv"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
)

type Format string

const (
	JSON   Format = "json"
	NDJSON Format = "ndjson"
	CSV    Format = "csv"
)

func IsFormat(s string) bool {
	switch Format(s) {
	case JSON, NDJSON, CSV:
		return true
	}
	return false
}

func ContentType(f Format) string {
	switch f {
	case CSV:
		return "text/csv"
	case NDJSON:
		return "application/x-ndjson"
	default:
		return "application/json"
	}
}

// Writer incrementally serialises points to a buffered writer in the chosen
// format. Call Begin once, Row per point, then End once.
type Writer struct {
	w       *bufio.Writer
	format  Format
	started bool
}

func NewWriter(w *bufio.Writer, format Format) *Writer {
	return &Writer{w: w, format: format}
}

func (s *Writer) Begin() error {
	switch s.format {
	case CSV:
		_, err := s.w.WriteString("ts,deviceId,metric,seriesId,value,unit\n")
		return err
	case JSON:
		return s.w.WriteByte('[')
	default:
		return nil
	}
}

func (s *Writer) Row(p model.DataPoint) error {
	switch s.format {
	case CSV:
		return s.writeCSV(p)
	case NDJSON:
		if err := s.writeJSONObject(p); err != nil {
			return err
		}
		return s.w.WriteByte('\n')
	default: // JSON array
		if s.started {
			if err := s.w.WriteByte(','); err != nil {
				return err
			}
		}
		s.started = true
		return s.writeJSONObject(p)
	}
}

func (s *Writer) End() error {
	if s.format == JSON {
		return s.w.WriteByte(']')
	}
	return nil
}

// writeJSONObject hand-rolls the object to avoid per-point reflection/allocation
// from encoding/json — meaningful when emitting 500k rows.
func (s *Writer) writeJSONObject(p model.DataPoint) error {
	s.w.WriteString(`{"ts":`)
	s.w.WriteString(strconv.FormatInt(p.TS, 10))
	s.w.WriteString(`,"deviceId":"`)
	s.w.WriteString(p.DeviceID)
	s.w.WriteString(`","metric":"`)
	s.w.WriteString(p.Metric)
	s.w.WriteString(`","seriesId":"`)
	s.w.WriteString(p.SeriesID)
	s.w.WriteString(`","value":`)
	s.w.WriteString(strconv.FormatFloat(p.Value, 'f', -1, 64))
	s.w.WriteString(`,"unit":"`)
	s.w.WriteString(p.Unit)
	_, err := s.w.WriteString(`"}`)
	return err
}

func (s *Writer) writeCSV(p model.DataPoint) error {
	s.w.WriteString(strconv.FormatInt(p.TS, 10))
	s.w.WriteByte(',')
	s.w.WriteString(p.DeviceID)
	s.w.WriteByte(',')
	s.w.WriteString(p.Metric)
	s.w.WriteByte(',')
	s.w.WriteString(p.SeriesID)
	s.w.WriteByte(',')
	s.w.WriteString(strconv.FormatFloat(p.Value, 'f', -1, 64))
	s.w.WriteByte(',')
	s.w.WriteString(p.Unit)
	return s.w.WriteByte('\n')
}
