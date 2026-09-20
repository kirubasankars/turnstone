# Copyright (c) 2026 Kiruba Sankar Swaminathan
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root of this source tree.

build:
	mkdir -p ./bin
	go build -o bin/turnstone ./cmd/turnstone

build-rust:
	cd rust && cargo build --release -p turnstone -p turnstone-rs
	mkdir -p ./bin
	cp rust/target/release/turnstone ./bin/turnstone-rust
	cp rust/target/release/turnstone-rs ./bin/turnstone-rs

test-rust:
	cd rust && cargo test

bench-rust:
	cd rust && cargo bench -p turnstone-engine --bench engine_bench -- --sample-size 20
	cd rust && cargo bench -p turnstone-hashindex --bench hashindex_bench -- --sample-size 20

test-rust-coverage:
	bash scripts/rust-coverage.sh

clean:
	rm -rf ./bin

test:
	go test -v ./... | tee test.log

# test-race runs the full suite across ALL packages in one process, with the
# race detector enabled. This must be run as a single `./...` invocation
# (not per-package) so that races between goroutines in different packages
# (e.g. server <-> database <-> engine) are still observed by one race
# detector instance.
test-race:
	go test -race -count=1 ./... | tee test-race.log

# bench runs Go microbenchmarks in engine and hashindex. -run=^$ skips unit tests.
bench:
	go test -run='^$$' -bench=. -benchmem -count=1 ./engine/ ./engine/hashindex/ | tee bench.log

# bench-all runs microbenchmarks in every package.
bench-all:
	go test -run='^$$' -bench=. -benchmem -count=1 ./... | tee bench-all.log

# bench-vs-postgres runs engine parallel KV benches and, when psql can
# connect, the same one-row-per-transaction shape against PostgreSQL.
bench-vs-postgres:
	scripts/bench-vs-postgres.sh

ui-test:
	cd console/e2e && npm ci && npx playwright install chromium --with-deps && npm test
