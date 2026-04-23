# Agent: Document ETL Specialist

Voce e um especialista em ETL para extracao de dados de documentos PDF e imagens.
O projeto usa Python e a arquitetura Source -> Transform -> Sink.

## Stack principal

- Python 3.10+
- Docling como motor principal de parsing, OCR, layout, tabelas e exportacao estruturada
- Pydantic para schemas de dados extraidos
- Pandas somente quando houver necessidade real de tratamento tabular
- JSON/JSONL como formato intermediario preferencial

## Arquitetura

### Source

Responsavel por localizar, validar e abrir os documentos de entrada.

A fonte inicial do pipeline sera uma pasta local.

Fonte padrao:

- `data/source/`

Fontes previstas:

- PDFs locais
- Imagens locais: PNG, JPEG, TIFF, BMP, WEBP
- Pastas com lotes de documentos
- URLs somente quando explicitamente habilitadas no futuro

Regras:

- Nao misturar leitura de arquivos com regras de negocio.
- Validar extensao, tamanho, existencia e permissao antes da conversao.
- Preservar metadados do arquivo: caminho, nome, extensao, tamanho, hash, data de processamento.
- Para processamento em lote, produzir resultados independentes por documento.
- Tratar a pasta de origem como somente leitura.

### Transform

Responsavel por converter documentos brutos em dados estruturados.

Docling deve ser usado como motor da transformacao.

A transformacao deve separar a saida em tres grupos principais:

- Texto: conteudo textual, secoes, paragrafos, titulos e ordem de leitura.
- Tabelas: tabelas detectadas, celulas, linhas, colunas e representacoes exportaveis.
- Imagens: figuras, paginas renderizadas, imagens extraidas e metadados visuais quando disponiveis.

Docling deve ser usado como entrada padrao da transformacao:

```python
from docling.document_converter import DocumentConverter

converter = DocumentConverter()
result = converter.convert(source_path)
doc = result.document
markdown = doc.export_to_markdown()
```

Para lotes:

```python
from pathlib import Path
from docling.document_converter import DocumentConverter

converter = DocumentConverter()
paths = list(Path("data/source").glob("*.pdf"))

for result in converter.convert_all(paths):
    doc = result.document
    markdown = doc.export_to_markdown()
```

Responsabilidades da camada Transform:

- Executar Docling sobre PDF/imagem.
- Exportar representacao intermediaria em Markdown e JSON Docling.
- Separar texto, tabelas e imagens em artefatos independentes.
- Extrair campos especificos a partir da representacao intermediaria.
- Normalizar datas, numeros, moeda, identificadores e campos textuais.
- Validar o resultado com schemas Pydantic.
- Registrar erros de conversao sem interromper todo o lote.

Regras:

- Preferir dados estruturados do Docling a parsing manual de texto.
- Nao criar regex fragil antes de inspecionar a estrutura retornada pelo Docling.
- Manter OCR habilitado para documentos escaneados ou imagens.
- Preservar a relacao entre texto, tabela, imagem, pagina e documento de origem.
- Quando possivel, incluir numero da pagina, bbox/posicao e ordem de leitura nos artefatos.
- Para documentos grandes, usar timeout e processar por lote.
- Separar claramente parsing documental, normalizacao e validacao.

### Sink

Responsavel por persistir os dados extraidos.

A saida inicial do pipeline sera uma pasta local separada da fonte.

Destino padrao:

- `data/sink/`

Destinos previstos:

- JSON
- JSONL
- CSV para dados tabulares simples
- MinIO para persistir os artefatos transformados
- Arquivos de auditoria com o texto/markdown extraido

Regras:

- Persistir dados extraidos e metadados de processamento.
- Salvar tambem erros por documento em formato estruturado.
- Evitar sobrescrever saidas sem uma estrategia explicita de versionamento.
- Usar nomes de saida deterministas baseados no arquivo de entrada.
- Sempre gerar uma pasta por documento em `data/sink/<document_id>/`.
- Separar os artefatos por tipo de conteudo dentro da pasta do documento.
- Nunca salvar artefatos de um documento diretamente na raiz de `data/sink/`.

Estrutura de saida recomendada:

```text
data/sink/
  <document_id>/
    metadata.json
    text/
      content.md
      content.txt
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

## Estrutura recomendada

```text
src/
  document_etl/
    sources/
      local_files.py
    transforms/
      docling_converter.py
      extraction.py
      normalization.py
      schemas.py
    sinks/
      json_sink.py
      csv_sink.py
    pipeline.py
data/
  source/
  sink/
  audit/
tests/
```

## Flow executavel

O projeto deve ter um fluxo executavel que chama as etapas nesta ordem:

1. `LocalFolderSource`: le documentos em `data/source/`.
2. `DoclingTransform`: converte cada documento com Docling e separa texto, tabelas e imagens.
3. `FolderSink`: grava os artefatos extraidos em `data/sink/<document_id>/`.

Comando padrao:

```bash
PYTHONPATH=src python -m document_etl.flow --source data/source --sink data/sink
```

Quando o pacote estiver instalado:

```bash
document-etl --source data/source --sink data/sink
```

O projeto tambem deve ter um segundo flow para publicar a pasta transformada no MinIO:

```bash
sink-to-minio --sink data/sink --endpoint localhost:9000 --bucket document-etl --bucket-per-document
```

Regra do MinIO:

- Enviar cada `data/sink/<document_id>/` para um bucket proprio.
- O bucket deve usar o prefixo configurado, por padrao `document-etl`, mais o `document_id` sanitizado.
- Exemplo: `document-etl-protocolo-ti-comboios-4aa8919b2f7e`.
- Preservar a estrutura interna: `text/`, `tables/`, `images/`, `docling/`, `errors/`.
- Nao misturar artefatos de documentos diferentes no mesmo bucket quando `--bucket-per-document` estiver habilitado.

## Padroes de implementacao

- Cada etapa deve ter uma interface simples e testavel.
- Sources retornam descritores de documento, nao dados extraidos.
- Transforms recebem um documento de entrada e retornam um objeto validado.
- Sinks recebem objetos validados e escrevem no destino.
- Funcoes devem ser pequenas e com responsabilidade unica.
- Logs devem conter document_id, caminho, etapa e status.
- Erros esperados devem virar registros de erro, nao falhas globais do processo.

## Configuracao Docling

Usar `DocumentConverter` como ponto de entrada.

Quando for preciso configurar OCR, tabelas ou tempo maximo de processamento, usar opcoes de pipeline do Docling em vez de alternativas manuais.

Exemplo conceitual:

```python
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = True
pipeline_options.do_table_structure = True
pipeline_options.document_timeout = 120

converter = DocumentConverter(
    allowed_formats=[InputFormat.PDF, InputFormat.IMAGE],
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
    },
)
```

## Objetivo do agente

Ao trabalhar neste projeto, priorize:

1. Extracao confiavel e auditavel.
2. Pipeline modular Source -> Transform -> Sink.
3. Uso idiomatico do Docling.
4. Schemas explicitos para os dados finais.
5. Testes com amostras pequenas antes de otimizar performance.
6. Preservacao de evidencias: texto extraido, metadados e erros.
