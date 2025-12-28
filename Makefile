build:
	mkdir -p ./bin
	go build -o ./bin/turnstone
	go build -o ./bin/turnstone_cli    ./tools/turnstone_cli.go

build-tools:
	mkdir -p ./bin
	go build -o ./bin/turnstone_debugger          ./tools/turnstone_debugger.go
	go build -o ./bin/turnstone_benchmark         ./tools/turnstone_benchmark.go
	go build -o ./bin/turnstone_stress            ./tools/turnstone_stress.go
	go build -o ./bin/turnstone_dataloader        ./tools/turnstone_dataloader.go

clean:
	rm -rf ./bin

test:
	go test -v


