training-dev:
	PYTHONPATH=$(shell pwd) poetry run python src/training.py

prediction-dev:
	PYTHONPATH=$(shell pwd) poetry run python src/prediction.py

build:
	docker build -t price_predictor_training .

run: build
	docker run \
		--env-file=.env \
		price_predictor_training