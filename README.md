# Agro Data Pipeline

Pipeline de dados para ingestão, padronização e análise integrada de informações públicas sobre **preços de café**, **indicadores econômicos** e **clima**.

**Criador:** Ivan Marques

---

## Objetivo

O projeto foi desenvolvido para simular uma arquitetura de dados aplicada ao agronegócio, consolidando fontes heterogêneas em uma camada analítica capaz de apoiar a investigação da seguinte pergunta:

> Como os preços do café arábica e robusta se comportam em relação à SELIC, ao IPCA e às condições climáticas observadas em Patrocínio/MG e Franca/SP?

O pipeline implementa as camadas:

- **Raw:** preparação dos arquivos recebidos.
- **Bronze:** ingestão dos dados em formato Parquet.
- **Silver:** limpeza, tipagem, padronização, deduplicação e aplicação de schema.
- **Gold:** criação de dimensões, fatos e tabelas analíticas no DuckDB.

---

## Fontes de dados

| Fonte | Conteúdo | Tipo | Formato |
|---|---|---|---|
| CEPEA/ESALQ | Preços do café arábica e robusta | Arquivo | Excel |
| Banco Central do Brasil — SGS | SELIC e IPCA | API REST | JSON |
| INMET/BDMEP | Clima de Patrocínio/MG e Franca/SP | Arquivo | CSV |

### Séries econômicas

- **SELIC:** código SGS `11`, com periodicidade diária.
- **IPCA:** código SGS `433`, com periodicidade mensal.

---

## Arquitetura

```text
Fontes públicas
      |
      v
+-------------+
|     Raw     |
| CSV / XLS   |
| JSON / API  |
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
| schema/data |
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

### Principais tecnologias

- Python
- PySpark
- Apache Spark
- DuckDB
- Parquet
- SQL
- Docker
- Docker Compose
- uv

### Principais bibliotecas Python

- `pyspark`: leitura, transformação e escrita das camadas Bronze e Silver.
- `duckdb`: modelagem relacional e consultas analíticas da camada Gold.
- `requests`: consumo da API pública do Banco Central.
- `spark-excel`: leitura das planilhas Excel do CEPEA.
- `pathlib`: manipulação segura de caminhos.
- `logging`: rastreabilidade da execução.
- `contextlib`: gerenciamento da conexão com o DuckDB.

As versões exatas devem ser consultadas no arquivo `pyproject.toml` e no `uv.lock`.

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

A estrutura pode variar levemente conforme a organização adotada no repositório.

---

## Pré-requisitos

### Execução local

- Python 3.12
- Java 17
- uv
- Git

### Execução com Docker

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

Confirme a instalação:

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

A aplicação utiliza Java 17 para execução local do Spark.

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

Verifique também:

- o nome da aba do Excel;
- a posição inicial da tabela, por exemplo: `'Plan 1'!A4`;
- os caminhos `path_raw`, `path_raw_cleaned`, `path_bronze`, `path_silver` e `path_gold`.

## 6. Executar a aplicação

Pela raiz do projeto:

```bash
uv run python main.py
```

Ou, caso o módulo principal seja `app.src.run`:

```bash
uv run python -m app.src.run
```

---

# Execução por camada

## Raw

Responsável por:

- extrair metadados dos arquivos INMET;
- localizar o cabeçalho tabular;
- padronizar os nomes das colunas;
- criar o arquivo `path_raw_cleaned`;
- salvar os metadados em JSON.

Exemplo:

```python
job_raw.run_all()
```

## Bronze

Responsável por:

- ler CSV, Excel e JSON;
- adicionar metadados de ingestão;
- persistir os dados em Parquet.

Exemplo:

```python
job_bronze.run_jobs()
```

## Silver

Responsável por:

- aplicar os schemas;
- converter datas;
- converter valores numéricos;
- tratar valores nulos;
- remover duplicidades;
- selecionar apenas as colunas previstas no contrato.

Exemplo:

```python
job_silver.run_all()
```

## Gold

Responsável por:

- ler os Parquets da Silver;
- criar tabelas dimensão e fato;
- gerar tabelas analíticas no DuckDB.

Exemplo:

```python
job_gold.process_analytics()
```

---

# Download das séries do Banco Central

Para baixar apenas a SELIC:

```python
manager.download_tax_series(
    payload=payload,
    tax_name="selic",
)
```

Para baixar apenas o IPCA:

```python
manager.download_tax_series(
    payload=payload,
    tax_name="ipca",
)
```

Para baixar todas as séries configuradas:

```python
manager.download_tax_series(
    payload=payload,
)
```

A API SGS possui limitação de período por requisição. Para séries longas, o downloader deve dividir o intervalo em janelas de até dez anos e concatenar os resultados.

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

## 2. Executar a pipeline

```bash
docker compose run --rm culttivo-pipeline
```

Ou:

```bash
docker compose up --build
```

## 3. Acompanhar os logs

```bash
docker compose logs -f culttivo-pipeline
```

## 4. Executar uma camada específica

Caso o projeto utilize a variável `PIPELINE_LAYER`:

```bash
PIPELINE_LAYER=bronze docker compose run --rm culttivo-pipeline
```

```bash
PIPELINE_LAYER=silver docker compose run --rm culttivo-pipeline
```

```bash
PIPELINE_LAYER=gold docker compose run --rm culttivo-pipeline
```

Valores permitidos:

```text
raw
bronze
silver
gold
all
```

---

## Persistência dos dados no Docker

O diretório local deve ser montado no container:

```yaml
volumes:
  - ./storage:/app/storage
```

Assim, os arquivos gerados permanecem disponíveis no host após o encerramento do container.

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

Dentro da CLI:

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

Valores climáticos ausentes não devem ser convertidos automaticamente para zero:

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

DuckDB foi utilizado por ser:

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
- A ingestão dos arquivos CEPEA depende da estrutura da planilha fornecida.

---

# Melhorias futuras

- Testes unitários e de integração.
- Validação de qualidade com Great Expectations ou Soda.
- Orquestração com Apache Airflow.
- Catálogo e lineage.
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
