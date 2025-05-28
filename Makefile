build:
	go build -mod=vendor
run:
	./extract -main-config=config.json -extraction-config=RetailCif.json
