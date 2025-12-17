build:
	rm -rf ./bin
	mkdir ./bin
	go build -o turnstone
	go build -o turnstone_cli ./cli/client.go
	mv turnstone ./bin/turnstone
	mv turnstone_cli ./bin/turnstone_cli
