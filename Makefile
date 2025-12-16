build:
	go build -o turnstone
	go build -o turnstone_cli  ./cli/client.go
	go build -o turnstone_view ./view/view.go
	mkdir -p bin
	rm -rf ./bin/turnstone
	rm -rf ./bin/turnstone_cli
	rm -rf ./bin/turnstone_view 
	mv ./turnstone ./bin/turnstone
	mv ./turnstone_cli ./bin/turnstone_cli
	mv ./turnstone_view ./bin/turnstone_view

clean: 
	rm -rf ./data
	rm -rf ./bin
