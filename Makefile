.PHONY: install run docker-build docker-run docker-upload-minio docker-run-minio-etl docker-run-worker minio-up minio-down upload-minio run-worker clean

PYTHON ?= python3.12
SOURCE ?= data/source
SINK ?= data/sink

install:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip
	. .venv/bin/activate && python -m pip install -e .

run:
	. .venv/bin/activate && document-etl --source $(SOURCE) --sink $(SINK)

docker-build:
	docker compose build etl upload-minio minio-etl minio-worker

docker-run:
	docker compose run --rm etl

docker-upload-minio:
	docker compose run --rm upload-minio

docker-run-minio-etl:
	docker compose run --rm minio-etl

minio-up:
	docker compose up -d minio

minio-down:
	docker compose down

upload-minio:
	. .venv/bin/activate && sink-to-minio --sink $(SINK) --endpoint localhost:9000 --bucket document-etl --bucket-per-document

run-worker:
	. .venv/bin/activate && minio-etl-worker --source-bucket source --endpoint localhost:9000 --bucket document-etl

docker-run-worker:
	docker compose run --rm minio-worker

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
