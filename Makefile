build:
	mkdir -p ./bin
	go build -o bin/turnstone cmd/turnstone/main.go
	go build -o bin/turnstone-cli cmd/turnstone-cli/main.go
	go build -o bin/turnstone-generate-config cmd/turnstone-generate-config/main.go
	go build -o bin/turnstone-backup cmd/turnstone-backup/main.go
	go build -o bin/turnstone-restore cmd/turnstone-restore/main.go
	go build -o bin/turnstone-bench cmd/turnstone-bench/main.go
	go build -o bin/turnstone-load cmd/turnstone-load/main.go
	go build -o bin/turnstone-load2 cmd/turnstone-load2/main.go
	cd cmd/turnstone-duck && go mod tidy && go build -o ../../bin/turnstone-duck
	cd cmd/turnstone-clickhouse && go mod tidy && go build -o ../../bin/turnstone-clickhouse

clean:
	rm -rf ./bin

test:
	go test -v ./... | tee test.log

# test-race runs the full suite across ALL packages in one process, with the
# race detector enabled. This must be run as a single `./...` invocation
# (not per-package) so that races between goroutines in different packages
# (e.g. server <-> store <-> stonedb) are still observed by one race
# detector instance.
test-race:
	go test -race -count=1 ./... | tee test-race.log


