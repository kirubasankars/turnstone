build:
	mkdir -p ./bin
	go build -o bin/turnstone       cmd/turnstone/main.go
	go build -o bin/turnstone_cli   cmd/turnstone_cli/main.go
	go build -o bin/turnstone_bench cmd/turnstone_bench/main.go

clean:
	rm -rf ./bin

test:
	go test -v ./...


