// Command datasource runs the standalone mock data source: a control dashboard
// plus HTTP/SSE endpoints for time-series, Kafka-style streaming, and bulk dump
// data. Build the consumer app alongside it and point it at this server.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cocymsc1986/large_data_rendering/data_source/internal/config"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/httpapi"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/model"
	"github.com/cocymsc1986/large_data_rendering/data_source/internal/sources"
	"github.com/cocymsc1986/large_data_rendering/data_source/web"
)

func main() {
	cfg := config.Default()

	// Flags override env defaults; either works.
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	flag.IntVar(&cfg.DeviceCount, "devices", cfg.DeviceCount, "number of devices (×6 metrics = total series)")
	autostart := flag.Bool("autostart", false, "start the time-series and stream sources immediately")
	flag.Parse()

	ts := sources.NewTimeSeriesSource(cfg.DeviceCount, cfg.TSResolution, cfg.TSMaxPoints)
	stream := sources.NewStreamSource(cfg.DeviceCount, cfg.StreamPartitions, cfg.StreamRetention, cfg.StreamPPS, time.Duration(100)*time.Millisecond)

	if *autostart {
		ts.Start(cfg.TSBackfillHrs)
		stream.Start(cfg.StreamPPS)
		log.Println("autostart: time-series and stream sources running")
	}

	srv := httpapi.NewServer(cfg, ts, stream, web.FS())
	httpServer := &http.Server{
		Addr:    cfg.Addr,
		Handler: srv.Handler(),
	}

	go func() {
		log.Printf("data_source listening on %s", cfg.Addr)
		log.Printf("dashboard:  http://localhost%s/", cfg.Addr)
		log.Printf("series:     %d devices × %d metrics = %d series", cfg.DeviceCount, len(model.MetricNames), cfg.DeviceCount*len(model.MetricNames))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown on Ctrl-C / SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down…")
	ts.Stop()
	stream.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}
