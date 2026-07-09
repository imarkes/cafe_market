import logging

from logging_config import configure_logging

from jobs.job_bronze import JobBronzeData
from pipeline import Pipeline
from jobs.job_silver import JobSilverData

configure_logging("INFO")
logger = logging.getLogger(__name__)

payload = {
    "ipca": {
        "COD_IPCA": 433,
        "start_date": "01/01/2020",
        "end_date": "31/12/2020",
        "partitions": ["data"],
        "path_raw": "../storage/raw/serie_433.json",
        "path_bronze": "../storage/bronze/bcb/ipca/serie_433.parquet",
        "path_silver": "../storage/silver/bcb/ipca/serie_433.parquet",
        "path_gold": "../storage/gold/bcb/ipca/serie_433.parquet",
    },
    "selic": {
        "COD_SELIC": 11,
        "start_date": "01/01/2020",
        "end_date": "31/12/2020",
        "partitions": ["data"],
        "path_raw": "../storage/raw/serie_11.json",
        "path_bronze": "../storage/bronze/bcb/selic/serie_11.parquet",
        "path_silver": "../storage/silver/bcb/selic/serie_11.parquet",
        "path_gold": "../storage/gold/bcb/selic/serie_11.parquet",
    },
    "robusta": {
        "dataAddress": "'Plan 1'!A4",  # !Atencao ao nome da planilha
        "partitions": ["data"],
        "path_raw": "../storage/raw/ROBUSTA_CEPEA_20260706165657.xls",
        "path_bronze": "../storage/bronze/cepea/robusta/robusta_cepea.parquet",
        "path_silver": "../storage/silver/cepea/robusta/robusta_cepea.parquet",
        "path_gold": "../storage/gold/cepea/robusta/robusta_cepea.parquet",
    },
    "arabica": {
        "dataAddress": "'Plan 1'!A4",  # !Atencao ao nome da planilha
        "partitions": ["data"],
        "path_raw": "../storage/raw/ARABICA_CEPEA_20260706165648.xls",
        "path_bronze": "../storage/bronze/cepea/arabica/arabica_cepa.parquet",
        "path_silver": "../storage/silver/cepea/arabica/arabica_cepa.parquet",
        "path_gold": "../storage/gold/cepea/arabica/arabica_cepa.parquet",
    },
    "inmet_patrocinio": {
        "partitions": ["data_medicao"],
        "path_raw": "../storage/raw/dados_A523_D_2025-01-01_2026-07-06.csv",
        "path_raw_cleaned": "../storage/raw/dados_A523_D_2025-01-01_2026-07-06_inmet_patrocinio_cleaned.csv",
        "path_bronze": "../storage/bronze/inmet/patrocinio/inmet_patrocinio.parquet",
        "path_silver": "../storage/silver/inmet/patrocinio/inmet_patrocinio.parquet",
        "path_gold": "../storage/gold/inmet/patrocinio/inmet_patrocinio.parquet",
    },
    "inmet_franca": {
        "partitions": ["data_medicao"],
        "path_raw": "../storage/raw/dados_A708_D_2025-01-01_2026-07-06.csv",
        "path_raw_cleaned": "../storage/raw/dados_A708_D_2025-01-01_2026-07-06_inmet_franca_cleaned.csv",
        "path_bronze": "../storage/bronze/inmet/franca/inmet_franca.parquet",
        "path_silver": "../storage/silver/inmet/franca/inmet_franca.parquet",
        "path_gold": "../storage/gold/inmet/franca/inmet_franca.parquet",
    },
}
def main() -> None:
    """
    Ponto de entrada da pipeline.

    Fluxo:

        Download (opcional)
              ↓
        Bronze
              ↓
        Silver
              ↓
        Gold
    """

    logger.info("Starting pipeline...")

    pipeline = Pipeline()

    #
    # Download opcional das séries do Banco Central
    #
    # pipeline.download_tax_series(
    #     code=payload["selic"]["COD_SELIC"],
    #     start_date=payload["selic"]["start_date"],
    #     end_date=payload["selic"]["end_date"],
    # )
    # pipeline.download_tax_series(
    #     code=payload["ipca"]["COD_IPCA"],
    #     start_date=payload["ipca"]["start_date"],
    #     end_date=payload["ipca"]["end_date"],
    # )
    # bronze = JobBronzeData(
    #     payload=payload,
    #     run=pipeline,
    # )
    #
    # Executa todas as fontes
    # bronze.run_all()

    ####
    # Executa sob demanda
    # bronze.process_source("robusta")
    # bronze.process_source("arabica")
    # bronze.process_source("selic")
    # bronze.process_source("ipca")
    # bronze.process_source("inmet_franca")
    # bronze.process_source("inmet_patrocinio")

    #------[Silver]-----
    # silver = JobSilverData(
    #     payload=payload,
    #     run=pipeline,
    # )
    # silver.process_data("inmet_franca")
    # silver.process_data("ipca")
    # silver.process_data("robusta")

    logger.info("Pipeline finished successfully.")

if __name__ == "__main__":
    main()
