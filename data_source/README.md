# data_source

A **standalone mock data source** for experimenting with retrieving, transforming
and rendering large datasets (~500k rows). It serves three kinds of data that a
consumer app — the backend/frontend you build alongside it — can subscribe to:

| Source | Pattern | Toggle | How you consume it |
| --- | --- | --- | --- |
| **Time series** | Continuously generated, queryable history | start / stop | `GET /api/timeseries` (server-side downsampled) |
| **Stream** | Kafka-style partitioned log, real-time | start / stop | SSE `GET /stream/sse` or poll `GET /api/stream/poll` |
| **Bulk dump** | One-shot static export | always on | `GET /api/dump` (json / ndjson / csv) |

It's a single Go binary (standard library only — no external dependencies) with
an embedded control dashboard for turning the time-series and stream sources on
and off.

## Why mock Kafka over SSE instead of running a broker?

You asked whether Kafka itself could be mocked. Running or embedding a real
broker (Kafka with KRaft/ZooKeeper, or even Redpanda) is the opposite of
lightweight: it needs a separate process/container and a Kafka client library in
your consumer. So this app **mocks Kafka's semantics** rather than its wire
protocol:

- **Topics** — one per metric (`temperature`, `humidity`, `voltage`, `cpu`, `memory`, `network`).
- **Partitions** — fixed count per topic; a device's key always maps to the same partition.
- **Offsets** — monotonic, never reused; retention-capped logs drop the oldest record and advance the "earliest" offset, just like Kafka.
- **Consumer cursors** — you read from `earliest`, `latest`, or an explicit per-partition offset, and the server hands back the cursor to use next. Over SSE the cursor rides in the event `id:`, so a browser reconnect (`Last-Event-ID`) resumes with no gaps or dupes — the moral equivalent of a committed offset.

The code is structured so a real broker can be dropped in later (replace the
producer in `internal/sources/stream.go` with a Kafka client such as `franz-go`
and have the consumer talk to the broker directly).

## Quick start

```bash
cd data_source
go run ./cmd/datasource            # dashboard at http://localhost:4000
# or start with both generators already running:
go run ./cmd/datasource -autostart
```

Build a self-contained binary (web assets are embedded):

```bash
make build && ./bin/datasource
```

Open **http://localhost:4000/** for the control dashboard, or drive everything
over HTTP from your consumer app.

## Data model

Every source emits the same flat record, so one consumer can read all three:

```jsonc
{ "ts": 1700000000000, "deviceId": "device-001", "metric": "temperature",
  "seriesId": "device-001:temperature", "value": 21.34, "unit": "°C" }
```

`seriesId` (`<deviceId>:<metric>`) is the key you subscribe to / query. With the
default 50 devices × 6 metrics there are **300 distinct series**. Values are
generated with daily seasonality + a bounded random walk + noise, so charts look
alive rather than random.

## Endpoints

### Control
- `GET /api/sources` — status of all three sources.
- `POST /api/sources/timeseries/start` — body `{ "backfillHours": 6 }` (optional).
- `POST /api/sources/timeseries/stop`
- `POST /api/sources/stream/start` — body `{ "pps": 200 }` (optional rate).
- `POST /api/sources/stream/stop`

### Time series (query a window to draw a chart)
- `GET /api/timeseries/series` — list every series with point counts and bounds.
- `GET /api/timeseries?seriesId=device-001:temperature&from=<ms>&to=<ms>&maxPoints=2000`
  - `from`/`to` are epoch ms (default: last hour → now).
  - The server **downsamples** to at most `maxPoints` buckets, each carrying
    `{ts, value(avg), min, max, count}` so you can render an envelope. Swap in
    LTTB later if you want shape-preserving reduction.

### Stream (Kafka-style)
- `GET /api/stream/topics` — topics, partitions, and earliest/latest offsets.
- `GET /stream/sse?topic=cpu&partitions=0,1&offset=latest` — Server-Sent Events.
  - `offset` = `earliest` | `latest` (default) | a cursor like `0:120,1:98`.
  - Each event: `event: records`, `id: <cursor>`, `data: {topic,count,records}`.
- `GET /api/stream/poll?topic=cpu&offset=<cursor>&max=1000` — pull-based fetch.
  Returns `{count, cursor, records}`; poll again with the returned `cursor`.

### Bulk dump (static dataset)
- `GET /api/dump?count=500000&format=ndjson&devices=50&window=24&download=1`
  - `format` = `json` | `ndjson` | `csv`.
  - Streamed point-by-point, so memory stays flat regardless of `count`
    (500k rows generate in well under a second).

## Consuming examples

```js
// Streaming via SSE — auto-reconnect + offset resume are built in.
const es = new EventSource('http://localhost:4000/stream/sse?topic=cpu&offset=latest');
es.addEventListener('records', (e) => {
  const { records } = JSON.parse(e.data);
  // render records…
});
```

```js
// Pull-based (Kafka fetch style).
let cursor = 'earliest';
async function poll() {
  const r = await fetch(`http://localhost:4000/api/stream/poll?topic=cpu&offset=${cursor}&max=2000`);
  const { records, cursor: next } = await r.json();
  cursor = next;            // commit your offset
  // render records…
}
```

```bash
# Bulk dump straight to a file to test static loading/parsing.
curl 'http://localhost:4000/api/dump?count=500000&format=ndjson' -o dump.ndjson
```

## Configuration

Flags or environment variables (flags win):

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `-addr` | `ADDR` | `:4000` | Listen address |
| `-devices` | `DEVICE_COUNT` | `50` | Devices (× 6 metrics = total series) |
| `-autostart` | – | `false` | Start both generators on launch |
| | `TS_RESOLUTION_MS` | `1000` | Time-series point interval |
| | `TS_MAX_POINTS` | `50000` | Per-series ring-buffer capacity |
| | `TS_BACKFILL_HOURS` | `6` | History synthesised on start |
| | `STREAM_PPS` | `200` | Default stream rate (points/sec) |
| | `STREAM_PARTITIONS` | `4` | Partitions per topic |
| | `STREAM_RETENTION` | `100000` | Records retained per partition |
| | `DUMP_COUNT` | `500000` | Default dump size |
| | `DUMP_WINDOW_HOURS` | `24` | Time window a dump spans |

## Layout

```
data_source/
├── cmd/datasource/main.go     # entrypoint: flags, wiring, graceful shutdown
├── internal/
│   ├── config/                # env/flag configuration
│   ├── model/                 # DataPoint + per-series value generator + catalog
│   ├── sources/
│   │   ├── timeseries.go      # toggleable generator + ring-buffer store + query
│   │   ├── ringbuffer.go      # fixed-capacity time-ordered store
│   │   ├── downsample.go      # bucket-aggregate reduction
│   │   ├── stream.go          # Kafka-style partitioned logs + producer
│   │   └── dump.go            # streaming bulk generator
│   ├── serialize/             # streaming json/ndjson/csv writers
│   └── httpapi/               # router + handlers (control, ts, dump, stream/SSE)
└── web/                       # embedded control dashboard (no build step)
```
