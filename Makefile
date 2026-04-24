.PHONY: install run docker-build docker-run docker-run-minio-etl docker-run-worker docker-up minio-up minio-down run-worker clean

PYTHON ?= python3.12
MINIO_ENDPOINT ?= localhost:9000
MINIO_SOURCE_BUCKET ?= source
MINIO_SINK_BUCKET ?= sink

install:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip
	. .venv/bin/activate && python -m pip install -e .

run:
	. .venv/bin/activate && minio-etl --source-bucket $(MINIO_SOURCE_BUCKET) --endpoint $(MINIO_ENDPOINT) --bucket $(MINIO_SINK_BUCKET)

docker-build:
	docker compose build minio-etl minio-worker prefect-etl

docker-up:
	docker compose up --build

docker-run:
	docker compose run --rm minio-etl

docker-run-minio-etl:
	docker compose run --rm minio-etl

minio-up:
	docker compose up -d minio minio-init

minio-down:
	docker compose down

run-worker:
	. .venv/bin/activate && minio-etl --worker --source-bucket $(MINIO_SOURCE_BUCKET) --endpoint $(MINIO_ENDPOINT) --bucket $(MINIO_SINK_BUCKET)

docker-run-worker:
	docker compose --profile legacy up minio-worker

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
