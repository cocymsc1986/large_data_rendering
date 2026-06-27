package sources

import "github.com/cocymsc1986/large_data_rendering/data_source/internal/model"

// DumpOptions configures a one-off bulk dataset.
type DumpOptions struct {
	Count       int   // total points to emit
	DeviceCount int   // devices to spread across
	StartTS     int64 // inclusive window start (epoch ms)
	EndTS       int64 // inclusive window end (epoch ms)
}

// GenerateDump produces a bulk "dumped" dataset and hands each point to emit.
// Points are generated timestamp-by-timestamp across the whole series catalog,
// so output is ordered by time then by series — just like a real export.
//
// Using a callback keeps memory at O(1) regardless of Count, which is what lets
// the HTTP handler stream 500k+ rows without buffering. If emit returns an
// error (e.g. the client disconnected) generation stops and the error is
// returned.
func GenerateDump(opts DumpOptions, emit func(model.DataPoint) error) error {
	gens := model.BuildCatalog(opts.DeviceCount)
	seriesCount := len(gens)
	if seriesCount == 0 || opts.Count <= 0 {
		return nil
	}

	steps := (opts.Count + seriesCount - 1) / seriesCount // ceil
	if steps < 1 {
		steps = 1
	}
	span := opts.EndTS - opts.StartTS
	if span < 0 {
		span = 0
	}
	var interval float64
	if steps > 1 {
		interval = float64(span) / float64(steps-1)
	}

	emitted := 0
	for step := 0; step < steps && emitted < opts.Count; step++ {
		ts := opts.StartTS + int64(float64(step)*interval)
		for _, g := range gens {
			if emitted >= opts.Count {
				break
			}
			if err := emit(g.At(ts)); err != nil {
				return err
			}
			emitted++
		}
	}
	return nil
}
