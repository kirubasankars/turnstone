build:
	mkdir -p ./bin
	go build -o ./bin/turnstone
	go build -o ./bin/turnstone_cli    ./tools/cli.go

build-tools:
	mkdir -p ./bin
	go build -o ./bin/debug_reader      ./tools/debug_reader.go
	go build -o ./bin/benchmark         ./tools/benchmark.go
	go build -o ./bin/data_loader       ./tools/data_loader.go
	go build -o ./bin/cdc               ./tools/cdc.go

clean:
	rm -rf ./bin

test:
	go test -v


