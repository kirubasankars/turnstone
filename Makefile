build:
	mkdir -p ./bin
	go build -o ./bin/turnstone
	go build -o ./bin/turnstone_cli         ./tools/cli.go
	go build -o ./bin/turnstone_stress      ./tools/stress.go
	go build -o ./bin/turnstone_data_loader ./tools/data_loader.go
	go build -o ./bin/turnstone_cdc         ./tools/cdc.go

clean:
	rm -rf ./bin

test:
	go test -v
