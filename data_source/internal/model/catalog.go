package model

import "fmt"

// DeviceID returns a stable, zero-padded device id: device-001, device-002, …
func DeviceID(index int) string {
	return fmt.Sprintf("device-%03d", index+1)
}

// BuildCatalog returns one generator per (device × metric) pair. With the
// default 50 devices and 6 metrics that is 300 distinct series. This is the
// shared universe every source draws from.
func BuildCatalog(deviceCount int) []*SeriesGenerator {
	gens := make([]*SeriesGenerator, 0, deviceCount*len(MetricNames))
	for d := 0; d < deviceCount; d++ {
		id := DeviceID(d)
		for _, metric := range MetricNames {
			gens = append(gens, NewSeriesGenerator(id, metric))
		}
	}
	return gens
}
