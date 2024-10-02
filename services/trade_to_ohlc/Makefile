
run-dev:
	cp dev.env .env
	PYTHONPATH=$(shell pwd) poetry run python src/main.py

build:
	docker build -t trade_to_ohlc .

run: build
	docker run \
		--network=redpanda_network \
		--env-file prod.env \
		trade_to_ohlc