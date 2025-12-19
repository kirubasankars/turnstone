build:
	mkdir -p ./bin
	go build -o ./bin/turnstone
	go build -o ./bin/turnstone_cli         ./tools/cli.go

clean:
	rm -rf ./bin

test:
	go test -v
