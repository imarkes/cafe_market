# Cafe Data Pipeline

Pipeline de dados para ingestão, padronização e análise integrada de informações públicas sobre **preços de café**, **indicadores econômicos** e **clima**.

**Criador:** Ivan Marques

---

## Objetivo

O projeto simula uma arquitetura de dados aplicada ao agronegócio, consolidando fontes heterogêneas para apoiar a investigação da seguinte pergunta:

> Como os preços do café arábica e robusta se comportam em relação à SELIC, ao IPCA e às condições climáticas observadas em Patrocínio/MG e Franca/SP?

O pipeline implementa quatro etapas:

- **Raw:** preparação dos arquivos recebidos e extração dos metadados do INMET.
- **Bronze:** ingestão das fontes e persistência em Parquet.
- **Silver:** limpeza, tipagem, tratamento de nulos, deduplicação e aplicação de schemas.
- **Gold:** criação de dimensões, fatos e tabelas analíticas no DuckDB.

---

## Fontes de dados

| Fonte | Conteúdo | Ingestão | Formato |
|---|---|---|---|
| CEPEA/ESALQ | Preços do café arábica e robusta | Arquivo | Excel |
| Banco Central do Brasil — SGS | SELIC e IPCA | API REST | JSON |
| INMET/BDMEP | Clima de Patrocínio/MG e Franca/SP | Arquivo | CSV |

### Séries econômicas

- **SELIC:** código SGS `11`, periodicidade diária.
- **IPCA:** código SGS `433`, periodicidade mensal.

---

## Arquitetura

```text
Fontes públicas
      |
      v
+-------------+
|     Raw     |
| CSV/XLS/API |
+-------------+
      |
      v
+-------------+
|   Bronze    |
|   Parquet   |
+-------------+
      |
      v
+-------------+
|   Silver    |
|   Parquet   |
| Schema / DQ |
+-------------+
      |
      v
+-------------+
|    Gold     |
|   DuckDB    |
| Dim / Fact  |
+-------------+
      |
      v
BI / SQL / Analytics
```

---

## Modelo analítico

A camada Gold contém as seguintes tabelas:

| Tabela | Grão |
|---|---|
| `dim_calendario` | Uma linha por dia |
| `dim_estacao` | Uma linha por estação meteorológica |
| `fact_preco_cafe` | Uma linha por tipo de café e dia de cotação |
| `fact_clima` | Uma linha por estação e dia |
| `fact_indicadores` | Uma linha por dia |
| `cafe_market_analysis_daily` | Uma linha por dia |
| `cafe_market_analysis_monthly` | Uma linha por mês |

A tabela diária é voltada à análise de eventos climáticos e movimentos de curto prazo. A tabela mensal é mais adequada para análises envolvendo o IPCA.

---

## Tecnologias e bibliotecas

### Tecnologias

- Python
- PySpark
- Apache Spark
- DuckDB
- Parquet
- SQL
- Docker
- Docker Compose
- uv

### Bibliotecas principais

- `pyspark`: leitura, transformação e escrita das camadas Bronze e Silver.
- `duckdb`: modelagem relacional e consultas analíticas da camada Gold.
- `requests`: consumo da API pública do Banco Central.
- `spark-excel`: leitura das planilhas Excel do CEPEA.
- `pathlib`: resolução e manipulação de caminhos.
- `logging`: logs e rastreabilidade da execução.
- `contextlib`: gerenciamento da conexão com o DuckDB.

As versões exatas estão registradas em `pyproject.toml` e `uv.lock`.

---

## Estrutura do projeto

```text
Culltivo/
├── app/
│   ├── __init__.py
│   ├── models/
│   │   ├── dim_calendario.sql
│   │   ├── dim_estacao.sql
│   │   ├── fact_clima.sql
│   │   ├── fact_indicadores.sql
│   │   ├── fact_preco_cafe.sql
│   │   ├── cafe_market_analysis_daily.sql
│   │   └── cafe_market_analysis_monthly.sql
│   │
│   └── src/
│       ├── __init__.py
│       ├── run.py
│       ├── database/
│       ├── jobs/
│       ├── pipeline/
│       ├── schemas/
│       └── utils/
│
├── storage/
│   ├── raw/
│   ├── meta/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── main.py
├── pyproject.toml
├── uv.lock
├── Dockerfile
├── compose.yaml
└── README.md
```

---

## Pré-requisitos

### Execução local

- Python 3.12
- Java 17
- uv
- Git

### Execução em container

- Docker
- Docker Compose

---

# Execução local

## 1. Clonar o repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd Culltivo
```

## 2. Instalar o uv

Linux/macOS:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Valide a instalação:

```bash
uv --version
```

## 3. Instalar as dependências

```bash
uv sync
```

## 4. Verificar o Java

```bash
java -version
```

A aplicação utiliza Java 17 para executar o Spark localmente.

## 5. Preparar os arquivos de entrada

Adicione os arquivos nas pastas configuradas no payload.

Exemplo:

```text
storage/raw/
├── ARABICA_CEPEA_<timestamp>.xls
├── ROBUSTA_CEPEA_<timestamp>.xls
├── dados_A523_D_<periodo>.csv
├── dados_A708_D_<periodo>.csv
├── serie_11.json
└── serie_433.json
```

Confira também:

- o nome da aba de cada arquivo Excel;
- a posição inicial da tabela, por exemplo `'Plan 1'!A4`;
- os caminhos `path_raw`, `path_raw_cleaned`, `path_bronze`, `path_silver` e `path_gold`.

## 6. Executar a aplicação

Pela raiz do projeto:

```bash
uv run python main.py
```

Também é possível executar o módulo diretamente:

```bash
uv run python -m app.src.run
```

Sem argumentos, o valor padrão é:

```text
PIPELINE_LAYER=all
```

O fluxo completo segue esta ordem:

```text
Download BCB opcional
        ↓
Raw
        ↓
Bronze
        ↓
Silver
        ↓
Gold
```

---

# Controle de execução

A aplicação aceita argumentos de linha de comando e variáveis de ambiente. Argumentos informados no terminal têm precedência sobre as variáveis de ambiente.

## Camadas disponíveis

| Camada | Função |
|---|---|
| `all` | Executa Raw, Bronze, Silver e Gold; o download do BCB é opcional |
| `download` | Baixa SELIC, IPCA ou ambas |
| `raw` | Prepara os CSVs do INMET |
| `bronze` | Ingere as fontes e grava Parquet |
| `silver` | Limpa, tipa, deduplica e aplica os schemas |
| `gold` | Cria o Data Warehouse analítico no DuckDB |

## Executar o pipeline completo

```bash
uv run python main.py --layer all
```

## Executar o pipeline completo baixando SELIC e IPCA

```bash
uv run python main.py \
  --layer all \
  --download-bcb \
  --bcb-series all
```

## Executar somente Raw

Todas as fontes INMET:

```bash
uv run python main.py --layer raw
```

Uma fonte específica:

```bash
uv run python main.py \
  --layer raw \
  --source inmet_franca
```

A camada Raw atual aceita apenas fontes cujo nome começa com `inmet_`.

## Executar somente Bronze

Todas as fontes:

```bash
uv run python main.py --layer bronze
```

Uma fonte específica:

```bash
uv run python main.py \
  --layer bronze \
  --source robusta
```

Outros exemplos:

```bash
uv run python main.py --layer bronze --source arabica
uv run python main.py --layer bronze --source selic
uv run python main.py --layer bronze --source inmet_patrocinio
```

## Executar somente Silver

Todas as fontes:

```bash
uv run python main.py --layer silver
```

Uma fonte específica:

```bash
uv run python main.py \
  --layer silver \
  --source inmet_patrocinio
```

Outros exemplos:

```bash
uv run python main.py --layer silver --source ipca
uv run python main.py --layer silver --source robusta
uv run python main.py --layer silver --source inmet_franca
```

## Executar somente Gold

```bash
uv run python main.py --layer gold
```

A Gold depende do conjunto das fontes Silver e não aceita `--source`.

## Executar somente o download do Banco Central

Apenas SELIC:

```bash
uv run python main.py \
  --layer download \
  --source selic
```

Apenas IPCA:

```bash
uv run python main.py \
  --layer download \
  --source ipca
```

Todas as séries configuradas:

```bash
uv run python main.py \
  --layer download \
  --bcb-series all
```

A API SGS limita consultas extensas por período. O downloader divide séries longas em janelas de até dez anos e consolida os resultados.

---

## Variáveis de ambiente

```env
PIPELINE_LAYER=all
PIPELINE_SOURCE=robusta
DOWNLOAD_BCB=false
BCB_SERIES=all
LOG_LEVEL=INFO
```

| Variável | Valores | Padrão |
|---|---|---|
| `PIPELINE_LAYER` | `all`, `download`, `raw`, `bronze`, `silver`, `gold` | `all` |
| `PIPELINE_SOURCE` | Nome de uma fonte existente no payload | vazio |
| `DOWNLOAD_BCB` | `true` ou `false` | `false` |
| `BCB_SERIES` | `all`, `selic`, `ipca` | `all` |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` |

Exemplo:

```bash
PIPELINE_LAYER=silver \
PIPELINE_SOURCE=inmet_franca \
uv run python main.py
```

### Regras de validação

- `PIPELINE_SOURCE` não pode ser usado com `PIPELINE_LAYER=all`.
- A camada `gold` não aceita uma fonte isolada.
- A camada `download` aceita somente `selic` ou `ipca` como fonte.
- A camada `raw` atual aceita somente fontes INMET.
- Sem uma fonte específica, a camada selecionada processa todas as fontes aplicáveis.

---

# Execução com Docker

## 1. Construir a imagem

```bash
docker compose build
```

Ou:

```bash
docker build -t culttivo-pipeline:latest .
```

## 2. Executar o pipeline completo

```bash
docker compose run --rm culttivo-pipeline
```

Também é possível utilizar:

```bash
docker compose up --build
```

A aplicação executa jobs batch. O container encerra após a conclusão do fluxo.

## 3. Executar o fluxo completo baixando SELIC e IPCA

```bash
DOWNLOAD_BCB=true \
BCB_SERIES=all \
docker compose run --rm culttivo-pipeline
```

## 4. Executar uma camada específica

Raw:

```bash
PIPELINE_LAYER=raw \
docker compose run --rm culttivo-pipeline
```

Bronze:

```bash
PIPELINE_LAYER=bronze \
docker compose run --rm culttivo-pipeline
```

Silver:

```bash
PIPELINE_LAYER=silver \
docker compose run --rm culttivo-pipeline
```

Gold:

```bash
PIPELINE_LAYER=gold \
docker compose run --rm culttivo-pipeline
```

## 5. Executar uma única fonte

Bronze do café robusta:

```bash
PIPELINE_LAYER=bronze \
PIPELINE_SOURCE=robusta \
docker compose run --rm culttivo-pipeline
```

Silver do INMET Franca:

```bash
PIPELINE_LAYER=silver \
PIPELINE_SOURCE=inmet_franca \
docker compose run --rm culttivo-pipeline
```

Raw do INMET Patrocínio:

```bash
PIPELINE_LAYER=raw \
PIPELINE_SOURCE=inmet_patrocinio \
docker compose run --rm culttivo-pipeline
```

## 6. Executar somente o download do BCB

SELIC:

```bash
PIPELINE_LAYER=download \
PIPELINE_SOURCE=selic \
docker compose run --rm culttivo-pipeline
```

IPCA:

```bash
PIPELINE_LAYER=download \
PIPELINE_SOURCE=ipca \
docker compose run --rm culttivo-pipeline
```

Todas as séries:

```bash
PIPELINE_LAYER=download \
BCB_SERIES=all \
docker compose run --rm culttivo-pipeline
```

## 7. Acompanhar os logs

Quando iniciado com `docker compose up`:

```bash
docker compose logs -f culttivo-pipeline
```

## 8. Variáveis no `compose.yaml`

O Compose deve repassar as opções de execução:

```yaml
services:
  culttivo-pipeline:
    environment:
      PIPELINE_LAYER: ${PIPELINE_LAYER:-all}
      PIPELINE_SOURCE: ${PIPELINE_SOURCE:-}
      DOWNLOAD_BCB: ${DOWNLOAD_BCB:-false}
      BCB_SERIES: ${BCB_SERIES:-all}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
```

## Persistência dos dados

Monte o diretório local no container:

```yaml
volumes:
  - ./storage:/app/storage
```

Assim, arquivos Raw, Bronze, Silver, Gold e o banco DuckDB permanecem disponíveis no host após o encerramento do container.

---

# Consultando a camada Gold

O banco analítico é criado, por padrão, em:

```text
storage/gold/culttivo.duckdb
```

Exemplo em Python:

```python
import duckdb

connection = duckdb.connect(
    "storage/gold/culttivo.duckdb"
)

print(connection.sql("SHOW TABLES").fetchall())

result = connection.sql(
    """
    SELECT *
    FROM cafe_market_analysis_monthly
    ORDER BY mes_referencia
    LIMIT 10
    """
).df()

print(result)
connection.close()
```

Também é possível utilizar a CLI do DuckDB:

```bash
duckdb storage/gold/culttivo.duckdb
```

Consultas iniciais:

```sql
SHOW TABLES;

SELECT COUNT(*)
FROM cafe_market_analysis_daily;
```

---

# Qualidade e governança

O projeto considera:

- schemas explícitos por fonte;
- padronização de nomes em `snake_case`;
- parsing tolerante de datas;
- conversão de números com vírgula decimal;
- tratamento de representações textuais de nulo;
- deduplicação;
- validação de colunas obrigatórias;
- metadados de estação;
- logs de execução;
- separação entre dados brutos, padronizados e analíticos;
- indicadores de qualidade nas tabelas Gold.

Valores climáticos ausentes não são convertidos automaticamente para zero:

- `NULL`: medição ausente ou inválida;
- `0`: medição realizada, sem ocorrência do fenômeno.

---

# Decisões de arquitetura

## PySpark nas camadas Bronze e Silver

PySpark foi utilizado para demonstrar:

- leitura de fontes heterogêneas;
- processamento distribuído;
- aplicação de schemas;
- padronização e transformação;
- persistência em formato colunar.

## DuckDB na camada Gold

DuckDB foi escolhido por ser:

- local;
- gratuito;
- open-source;
- orientado a analytics;
- compatível com Parquet;
- adequado para joins, agregações, dimensões e fatos;
- simples para consumo via SQL e ferramentas de BI.

## Parquet como formato de armazenamento

Parquet oferece:

- armazenamento colunar;
- compressão;
- tipagem;
- leitura seletiva de colunas;
- integração com Spark e DuckDB.

---

# Limitações

- Os dados meteorológicos do INMET são brutos e podem conter falhas de sensor.
- Relações observadas entre clima, SELIC, IPCA e preços não representam causalidade.
- O IPCA possui granularidade mensal e deve ser analisado preferencialmente na tabela mensal.
- Anomalias climáticas calculadas com uma série curta representam anomalias móveis, não uma normal climatológica histórica.
- A ingestão do CEPEA depende da estrutura das planilhas fornecidas.

---

# Melhorias futuras

- Testes unitários e de integração.
- Validação de qualidade com Great Expectations ou Soda.
- Orquestração com Apache Airflow.
- Catálogo, auditoria e lineage.
- Escrita em Delta Lake ou Apache Iceberg.
- Dashboard em Power BI, Metabase ou Streamlit.
- Análise de correlação com defasagens temporais.
- CI/CD para testes e build da imagem Docker.
- Parametrização por ambiente.
- Monitoramento de duração, volume e falhas dos jobs.

---

## Autor

**Ivan Marques**

Projeto desenvolvido como demonstração de arquitetura, engenharia e modelagem analítica de dados aplicada ao mercado de café.
