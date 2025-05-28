build:
	go build -mod=vendor
run:
	./extract -appCfg=./config/config.json -runCfg=./config/extraction/RetailCif.json -mode="I"
