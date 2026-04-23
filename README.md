# Extract PDF Document

[![CI Docker](https://github.com/paulossjunior/extract_pdf_document/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/paulossjunior/extract_pdf_document/actions/workflows/docker-publish.yml)
[![Release](https://img.shields.io/github/v/release/paulossjunior/extract_pdf_document)](https://github.com/paulossjunior/extract_pdf_document/releases)
[![Repository](https://img.shields.io/badge/GitHub-extract__pdf__document-black)](https://github.com/paulossjunior/extract_pdf_document)

Pipeline ETL em Python para extrair dados de PDFs e imagens com Docling, separando a saida em texto, tabelas e imagens.

Fluxo principal:

```text
source folder -> Docling transform -> sink folder
```

Fluxo adicional:

```text
sink folder -> MinIO
```

## O que o projeto faz

- le documentos a partir de uma pasta source
- usa Docling para interpretar PDF/imagem
- separa texto, tabelas e imagens
- cria uma pasta de sink por documento
- envia o sink para MinIO
- publica imagem Docker no GitHub Container Registry

## Arquitetura

O projeto segue o modelo:

- `Source`: pasta local com arquivos de entrada
- `Transform`: Docling, separando texto, imagens e tabelas
- `Sink`: pasta local estruturada por documento

Componentes principais:

- `src/document_etl/sources/local_folder.py`
- `src/document_etl/transforms/docling_transform.py`
- `src/document_etl/sinks/folder_sink.py`
- `src/document_etl/sinks/minio_sink.py`
- `src/document_etl/flow.py`
- `src/document_etl/minio_flow.py`

## Requisitos

- Python 3.10+
- Docker e Docker Compose, se quiser rodar via container

Dependencias Python:

- `docling`
- `minio`
- `pandas`
- `pydantic`

## Instalacao local

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

## Como executar

Coloque seus arquivos em `data/source/`.

Execute localmente:

```bash
document-etl --source data/source --sink data/sink
```

Ou:

```bash
PYTHONPATH=src python -m document_etl.flow --source data/source --sink data/sink
```

Via Makefile:

```bash
make run
```

Na primeira execucao o Docling pode baixar modelos e demorar mais.

## Estrutura de entrada e saida

Entrada:

```text
data/source/
  arquivo1.pdf
  arquivo2.png
```

Saida:

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

Regra do projeto:

- sempre gerar uma pasta por documento em `data/sink/<document_id>/`
- nunca gravar artefatos do documento diretamente na raiz de `data/sink/`

## Metadata gerada

Cada documento recebe um `metadata.json` com:

- `document_id`
- origem do arquivo
- nome e extensao
- tamanho em bytes
- hash SHA-256
- status da conversao
- contagem de blocos de texto, tabelas, imagens e erros

## Docker

Build das imagens:

```bash
docker compose build etl upload-minio minio-etl minio-worker
```

Executar ETL:

```bash
docker compose run --rm etl
```

Via Makefile:

```bash
make docker-build
make docker-run
```

Volumes montados:

```text
${SOURCE_DIR:-./data/source} -> /app/data/source
${SINK_DIR:-./data/sink}     -> /app/data/sink
```

Usando outra pasta local como source:

```bash
SOURCE_DIR=/caminho/para/pdfs docker compose run --rm etl
```

Usando outra pasta local para source e sink:

```bash
SOURCE_DIR=/caminho/para/pdfs SINK_DIR=/caminho/para/sink docker compose run --rm etl
```

O projeto usa o volume `model-cache` para reaproveitar modelos baixados pelo Docling.

Fluxos Docker disponiveis:

- `etl`: pasta local `source -> sink`
- `upload-minio`: `sink local -> MinIO`
- `minio-etl`: `bucket source -> transform -> bucket destino`
- `minio-worker`: worker continuo `bucket source -> transform -> bucket destino`

## MinIO

Subir MinIO local:

```bash
docker compose up -d minio
```

Portas locais:

- API/S3: `http://localhost:9000`
- Console web: `http://localhost:9001`

Credenciais locais:

- usuario: `minioadmin`
- senha: `minioadmin`

Prefixes usados no bucket de entrada do worker:

- `source/`: arquivos novos aguardando processamento
- `processing/`: arquivos ja reservados por um worker
- `failed/`: arquivos que falharam no processamento

Upload do sink para MinIO:

```bash
sink-to-minio --sink data/sink --endpoint localhost:9000 --bucket document-etl --bucket-per-document
```

Via Makefile:

```bash
make minio-up
make upload-minio
```

Via Docker:

```bash
docker compose run --rm upload-minio
```

ETL direto entre buckets no MinIO via Docker:

```bash
docker compose run --rm minio-etl
```

Worker continuo via Docker:

```bash
docker compose run --rm minio-worker
```

Via Makefile:

```bash
make docker-run-minio-etl
make docker-run-worker
```

Fluxo do worker:

```text
source/<arquivo> -> processing/<arquivo> -> transform -> bucket destino -> delete
```

Se houver falha:

```text
source/<arquivo> -> processing/<arquivo> -> failed/<arquivo>
```

Com `--bucket-per-document`, cada pasta em `data/sink/` vira um bucket separado. Exemplo:

```text
document-etl-protocolo-ti-comboios-4aa8919b2f7e/
  metadata.json
  source/PROTOCOLO TI COMBOIOS .pdf
  text/content.md
  images/page_001.png
  docling/document.json
```

## Comandos uteis

Instalar ambiente:

```bash
make install
```

Executar ETL local:

```bash
make run
```

Subir MinIO:

```bash
make minio-up
```

Parar stack Docker:

```bash
make minio-down
```

Upload para MinIO:

```bash
make upload-minio
```

Rodar ETL bucket -> bucket em Docker:

```bash
make docker-run-minio-etl
```

Rodar worker em Docker:

```bash
make docker-run-worker
```

Limpar `__pycache__`:

```bash
make clean
```

## GitHub e imagem Docker

Repositorio:

```text
https://github.com/paulossjunior/extract_pdf_document
```

Imagem Docker publicada:

```text
ghcr.io/paulossjunior/extract_pdf_document:latest
```

Pull da imagem:

```bash
docker pull ghcr.io/paulossjunior/extract_pdf_document:latest
```

Release atual:

```text
v0.1.0
```

## Exemplo de fluxo completo

1. colocar PDFs/imagens em `data/source/`
2. rodar o ETL
3. validar a estrutura em `data/sink/`
4. subir MinIO
5. enviar o sink para os buckets no MinIO

Exemplo:

```bash
make run
make minio-up
make upload-minio
```

Exemplo 100% Docker:

```bash
make docker-build
make minio-up
make docker-run-minio-etl
```

Exemplo 100% Docker com worker:

```bash
make docker-build
make minio-up
make docker-run-worker
```

## Observacoes

- o Docling pode usar aceleracao local quando disponivel
- a imagem Docker do ETL e relativamente grande por causa do stack de modelos
- o bucket no MinIO e criado por documento quando `--bucket-per-document` esta ativo
- no fluxo bucket -> bucket, o arquivo original tambem e salvo em `source/<arquivo_original>` no bucket de destino
- no worker, o arquivo e removido do bucket `source` somente apos processamento e upload com sucesso
- no worker, o arquivo e primeiro movido para `processing/`, evitando que outro worker pegue o mesmo objeto no mesmo ciclo
- em caso de erro, o arquivo vai para `failed/`
