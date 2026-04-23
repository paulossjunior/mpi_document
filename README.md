# MPI Document ETL

Pipeline Python para extrair texto, tabelas e imagens de PDFs/imagens usando Docling.

O fluxo executa:

```text
data/source -> Docling transform -> data/sink
```

## Requisitos

- Python 3.10+
- Docling

As dependencias Python estao declaradas em `pyproject.toml`.

## Instalar

Crie um ambiente virtual com Python 3.10+:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

## Executar

Coloque PDFs ou imagens em `data/source/`.

Execute:

```bash
document-etl --source data/source --sink data/sink
```

Ou, sem instalar o script:

```bash
PYTHONPATH=src python -m document_etl.flow --source data/source --sink data/sink
```

Na primeira execucao, o Docling pode baixar modelos de OCR/layout/tabela e demorar mais. Depois disso, ele tende a usar o cache local.

## Docker

Construa a imagem do projeto:

```bash
docker compose build etl upload-minio
```

Execute o ETL em Docker:

```bash
docker compose run --rm etl
```

Ou via Makefile:

```bash
make docker-build
make docker-run
```

O container monta:

```text
${SOURCE_DIR:-./data/source} -> /app/data/source
${SINK_DIR:-./data/sink}     -> /app/data/sink
```

Assim os PDFs/imagens continuam entrando por `data/source/` e a saida continua aparecendo em `data/sink/<document_id>/`.
Se quiser usar outra pasta como source, passe `SOURCE_DIR` no comando:

```bash
SOURCE_DIR=/caminho/para/pdfs docker compose run --rm etl
```

Tambem da para trocar a pasta de saida:

```bash
SOURCE_DIR=/caminho/para/pdfs SINK_DIR=/caminho/para/sink docker compose run --rm etl
```

O Docker Compose tambem usa o volume `model-cache` para cachear modelos baixados pelo Docling/Hugging Face entre execucoes.

Para subir MinIO e enviar o sink usando Docker:

```bash
docker compose up -d minio
docker compose run --rm upload-minio
```

Ou:

```bash
make minio-up
make docker-upload-minio
```

## MinIO

Suba um MinIO local:

```bash
docker compose up -d minio
```

Console web:

```text
http://localhost:9001
```

Credenciais locais:

```text
usuario: minioadmin
senha: minioadmin
```

Depois de executar o ETL e gerar `data/sink/<document_id>/`, envie tudo para o MinIO criando um bucket por documento:

```bash
sink-to-minio --sink data/sink --endpoint localhost:9000 --bucket document-etl --bucket-per-document
```

Ou via Makefile:

```bash
make minio-up
make upload-minio
```

Cada pasta de documento vira um bucket separado. O nome do bucket usa `document-etl-<document_id>` sanitizado para as regras do MinIO/S3. Dentro dele, a estrutura do documento e preservada:

```text
document-etl-protocolo-ti-comboios-4aa8919b2f7e/
  metadata.json
  text/content.md
  images/page_001.png
  docling/document.json
```

## Saida

Para cada documento, o sink cria:

```text
data/sink/<document_id>/
```

Na pratica, a pasta `data/sink` fica separada por documento. Dentro de cada documento ficam os artefatos separados por tipo:

Regra do projeto: o sink sempre gera uma pasta por documento. Nenhum artefato de documento deve ser salvo diretamente na raiz de `data/sink`.

```text
data/sink/
  <document_id>/
    metadata.json
    text/
      content.txt
      content.md
      blocks.jsonl
    tables/
      tables.jsonl
      table_001.csv
      table_001.html
      table_001.md
    images/
      images.jsonl
      page_001.png
      picture_001.png
      source_image.png
    docling/
      document.json
    errors/
      errors.jsonl
```
