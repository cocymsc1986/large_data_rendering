.PHONY: run dev build fmt vet tidy

# Run the server (Ctrl-C to stop).
run:
	go run ./cmd/datasource

# Run with both generators already producing.
dev:
	go run ./cmd/datasource -autostart

# Build a single self-contained binary (web assets are embedded).
build:
	go build -o bin/datasource ./cmd/datasource

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy
