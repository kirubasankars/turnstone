build:
	mkdir -p ./bin
	go build -o ./bin/turnstone
	go build -o ./bin/turnstone_cli    ./tools/cli.go

build-tools:
	mkdir -p ./bin
	go build -o ./bin/turnstone_debugger    ./tools/debugger.go
	go build -o ./bin/turnstone_benchmark   ./tools/benchmark.go

clean:
	rm -rf ./bin

test:
	go test -v
	go test ./client -v


