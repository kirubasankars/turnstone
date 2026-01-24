build:
	mkdir -p ./bin
	go build -o bin/turnstone cmd/turnstone/main.go
	go build -o bin/turnstone-cli cmd/turnstone-cli/main.go
	go build -o bin/turnstone-generate-config cmd/turnstone-generate-config/main.go
	go build -o bin/turnstone-bench cmd/turnstone-bench/main.go
	go build -o bin/turnstone-load cmd/turnstone-load/main.go
	go build -o bin/turnstone-load2 cmd/turnstone-load2/main.go
	cd cmd/turnstone-duck && go mod tidy && go build -o ../../bin/turnstone-duck
	cd cmd/turnstone-clickhouse && go mod tidy && go build -o ../../bin/turnstone-clickhouse

clean:
	rm -rf ./bin

test:
	go test -v ./... | tee test.log


