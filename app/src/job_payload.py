from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STORAGE_PATH = PROJECT_ROOT / "storage"

payload = {
    "ipca": {
        "COD_IPCA": 433,
        "start_date": "01/01/2025",
        "end_date": "31/12/2026",
        "partitions": ["data"],
        "path_raw": f"{STORAGE_PATH}/raw/serie_433.json",
        "path_bronze": f"{STORAGE_PATH}/bronze/bcb/ipca/serie_433.parquet",
        "path_silver": f"{STORAGE_PATH}/silver/bcb/ipca/serie_433.parquet",
        "path_gold": f"{STORAGE_PATH}/gold/bcb/ipca/serie_433.parquet",
    },
    "selic": {
        "COD_SELIC": 11,
        "start_date": "01/01/2025",
        "end_date": "31/12/2025",
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
