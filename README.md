# Pipeline de dados Culltivo

Este projeto lê arquivos Excel e JSON com Spark, faz download de séries do Banco Central e organiza a lógica em módulos pequenos para facilitar manutenção.

## Arquitetura

- [main.py](main.py): ponto de entrada da aplicação.
- [pipeline.py](pipeline.py): orquestra o fluxo principal.
- [readers.py](readers.py): abstrai a leitura de Excel e JSON com uma fábrica de leitores.
- [spark_session.py](spark_session.py): centraliza a criação da sessão Spark.
- [downloader.py](downloader.py): encapsula o download das séries do BCB.
- [logging_config.py](logging_config.py): configura logs de execução.

## Como executar

```bash
cd pipelines
uv run main.py
```

## Exemplo de uso programático

```python
from pipeline import Pipeline

pipeline = Pipeline()
pipeline.download_series(11, "01/01/2025", "31/12/2026")
df = pipeline.read_file("../storage/raw/ROBUSTA_CEPEA_20260706165657.xls")
df.show(5)
```

## Princípios aplicados

- SRP (Single Responsibility Principle): cada módulo tem uma responsabilidade bem definida.
- OCP (Open/Closed Principle): a fábrica de leitores facilita a adição de novos formatos sem alterar o fluxo principal.
- DIP (Dependency Inversion Principle): o pipeline depende de abstrações e de uma sessão Spark criada por um componente dedicado.

## Logs

Os logs são emitidos no console e em [pipelines/logs/pipeline.log](logs/pipeline.log).


Gold

             dim_calendario
                    |
                    |
                    |
        +-----------+------------+
        |                        |
        |                        |
fact_preco_cafe        fact_clima
        |                        |
        +-----------+------------+
                    |
             gold_indicadores
MOdelo Fisico
                   dim_calendario
                          |
                          |
      +-------------------+------------------+
      |                   |                  |
      |                   |                  |
fact_preco        fact_clima        fact_indicadores
      \                  |                 /
       \                 |                /
        \                |               /
         \               |              /
          +--------------+-------------+
                         |
               gold_indicadores_cafe
