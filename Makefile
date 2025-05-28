build:
	go build -mod=vendor
run:
	./extract -main-config=./config/config.json -extraction-config=./config/extraction/RetailCif.json
