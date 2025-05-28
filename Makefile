build:
	go build -mod=vendor
run:
	./extract -appcfg=./config/config.json -extractcfg=./config/extraction/RetailCif.json
