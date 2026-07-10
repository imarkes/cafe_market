from __future__ import annotations
import logging
import os
from pathlib import Path

from app.src.jobs.job_raw import JobRawData
from .utils.logging_config import configure_logging

from .jobs.job_bronze import JobBronzeData
from .pipeline.manager import DataManager
from .jobs.job_silver import JobSilverData
from .jobs.job_gold import JobGoldData


configure_logging("INFO")
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STORAGE_PATH = PROJECT_ROOT / "storage"

payload = {
    "ipca": {
        "COD_IPCA": 433,
        "start_date": "01/01/2020",
        "end_date": "31/12/2020",
        "partitions": ["data"],
        "path_raw": f"{STORAGE_PATH}/raw/serie_433.json",
        "path_bronze": f"{STORAGE_PATH}/bronze/bcb/ipca/serie_433.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/bcb/ipca/serie_433.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/bcb/ipca/serie_433.parquet",
    },
    "selic": {
        "COD_SELIC": 11,
        "start_date": "01/01/2020",
        "end_date": "31/12/2020",
        "partitions": ["data"],
        "path_raw": f"{STORAGE_PATH}/raw/serie_11.json",
        "path_bronze": f"{STORAGE_PATH}/bronze/bcb/selic/serie_11.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/bcb/selic/serie_11.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/bcb/selic/serie_11.parquet",
    },
    "robusta": {
        "dataAddress": "'Plan 1'!A4",  # !Atencao ao nome da planilha
        "partitions": ["data"],
        "path_raw": f"{STORAGE_PATH}/raw/ROBUSTA_CEPEA_20260706165657.xls",
        "path_bronze": f"{STORAGE_PATH}/bronze/cepea/robusta/robusta_cepea.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/cepea/robusta/robusta_cepea.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/cepea/robusta/robusta_cepea.parquet",
    },
    "arabica": {
        "dataAddress": "'Plan 1'!A4",  # !Atencao ao nome da planilha
        "partitions": ["data"],
        "path_raw": f"{STORAGE_PATH}/raw/ARABICA_CEPEA_20260706165648.xls",
        "path_bronze": f"{STORAGE_PATH}/bronze/cepea/arabica/arabica_cepa.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/cepea/arabica/arabica_cepa.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/cepea/arabica/arabica_cepa.parquet",
    },
    "inmet_patrocinio": {
        "partitions": ["data_medicao"],
        "path_raw": f"{STORAGE_PATH}/raw/dados_A523_D_2025-01-01_2026-07-06.csv",
        "path_raw_cleaned": f"{STORAGE_PATH}/raw/dados_A523_D_2025-01-01_2026-07-06_inmet_patrocinio_cleaned.csv",
        "path_bronze": f"{STORAGE_PATH}/bronze/inmet/patrocinio/inmet_patrocinio.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/inmet/patrocinio/inmet_patrocinio.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/inmet/patrocinio/inmet_patrocinio.parquet",
    },
    "inmet_franca": {
        "partitions": ["data_medicao"],
        "path_raw": f"{STORAGE_PATH}/raw/dados_A708_D_2025-01-01_2026-07-06.csv",
        "path_raw_cleaned": f"{STORAGE_PATH}/raw/dados_A708_D_2025-01-01_2026-07-06_inmet_franca_cleaned.csv",
        "path_bronze": f"{STORAGE_PATH}/bronze/inmet/franca/inmet_franca.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/inmet/franca/inmet_franca.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/inmet/franca/inmet_franca.parquet",
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
    # layer = os.getenv("PIPELINE_LAYER", "all").lower()

    manager = DataManager()

    jobs = {
        # "donwloader":download_tax_series(payload=payload),
        "raw":JobRawData(payload=payload),
        "bronze": JobBronzeData(payload=payload, run=manager),
        "silver": JobSilverData(payload=payload, run=manager),
        "gold": JobGoldData(payload=payload),
    }
    # if layer == "all":
    #     jobs["bronze"].run_all()
    #     jobs["silver"].run_all()
    #     jobs["gold"].process_analytics()
    #     return
    ##------[Ingestion]-----
    # manager.download_tax_series(
    #     code=payload["selic"]["COD_SELIC"],
    #     start_date=payload["selic"]["start_date"],
    #     end_date=payload["selic"]["end_date"],
    # )
    # manager.download_tax_series(
    #     code=payload["ipca"]["COD_IPCA"],
    #     start_date=payload["ipca"]["start_date"],
    #     end_date=payload["ipca"]["end_date"],
    # )
    # ------[Raw]-----
    # jobs['raw'].prepare_file_csv_inmet("inmet_franca")

    # ------[Bronze]-----
    # Executa todas as fontes
    # jobs['bronze'].run_all()
    # Executa sob demanda
    # jobs['bronze'].process_source("robusta")
    # bronze.process_source("arabica")
    # bronze.process_source("selic")

    #------[Silver]-----
    # jobs['silver'].run_all()
    # jobs['silver'].process_data("inmet_patrocinio")
    # jobs['silver'].process_data("ipca")
    # jobs['silver'].process_data("robusta")

    #------[Gold]-----
    jobs['gold'].create_datawarehouse()
    logger.info("Pipeline finished successfully.")

if __name__ == "__main__":
    ...
