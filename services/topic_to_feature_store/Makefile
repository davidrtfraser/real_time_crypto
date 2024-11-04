run-live-dev:
	cp live.dev.env .env
	PYTHONPATH=$(shell pwd) poetry run python src/main.py

run-historical-dev:
	cp historical.dev.env .env
	PYTHONPATH=$(shell pwd) poetry run python src/main.py

build:
	docker build --no-cache -t topic-to-feature-store .

run-live: build
	docker run \
		--network=redpanda_network \
		--env-file=live.prod.env \
		--env-file=credentials.env \
		topic-to-feature-store