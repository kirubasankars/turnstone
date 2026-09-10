# Copyright (c) 2026 Kiruba Sankar Swaminathan
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root of this source tree.

build:
	mkdir -p ./bin
	go build -o bin/turnstone cmd/turnstone/main.go
	go build -o bin/turnstone-cli cmd/turnstone-cli/main.go
	go build -o bin/turnstone-generate-config cmd/turnstone-generate-config/main.go
	go build -o bin/turnstone-bench cmd/turnstone-bench/main.go

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

# bench runs Go microbenchmarks in stonedb. -run=^$ skips unit tests.
bench:
	go test -run=^$ -bench=. -benchmem -count=1 ./stonedb/ | tee bench.log

# bench-all runs microbenchmarks in every package.
bench-all:
	go test -run=^$ -bench=. -benchmem -count=1 ./... | tee bench-all.log
