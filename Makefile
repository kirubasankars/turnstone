build:
	mkdir -p ./bin
	go build -o ./bin/turnstone
	go build -o ./bin/turnstone_cli         ./tools/cli.go
	go build -o ./bin/raw_reader            ./tools/raw_reader.go

clean:
	rm -rf ./bin

test:
	go test -v
