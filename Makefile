.PHONY: install run docker-build docker-run docker-upload-minio minio-up minio-down upload-minio clean

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
	docker compose build etl upload-minio

docker-run:
	docker compose run --rm etl

docker-upload-minio:
	docker compose run --rm upload-minio

minio-up:
	docker compose up -d minio

minio-down:
	docker compose down

upload-minio:
	. .venv/bin/activate && sink-to-minio --sink $(SINK) --endpoint localhost:9000 --bucket document-etl --bucket-per-document

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
