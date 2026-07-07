from jobs.job_raw import JobRawData
from logging_config import configure_logging
from pipeline import Pipeline

configure_logging("INFO")

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
        "sheet_name": "'Plan 1'!A4",  # !Atencao ao nome da planilha
        "partitions": ["Data"],
        "path_raw": "../storage/raw/ROBUSTA_CEPEA_20260706165657.xls",
        "path_bronze": "../storage/bronze/cepea/robusta/robusta_cepea.parquet",
        "path_silver": "../storage/silver/cepea/robusta/robusta_cepea.parquet",
        "path_gold": "../storage/gold/cepea/robusta/robusta_cepea.parquet",
    },
    "arabica": {
        "sheet_name": "'Plan 1'!A4",  # !Atencao ao nome da planilha
        "partitions": ["Data"],
        "path_raw": "../storage/raw/ARABICA_CEPEA_20260706165648.xls",
        "path_bronze": "../storage/bronze/cepea/arabica/arabica_cepa.parquet",
        "path_silver": "../storage/silver/cepea/arabica/arabica_cepa.parquet",
        "path_gold": "../storage/gold/cepea/arabica/arabica_cepa.parquet",
    },
    "inmet_patrocinio": {
        "partitions": ["Data"],
        "path_raw": "../storage/raw/dados_A523_D_2025-01-01_2026-07-06.csv",
        "path_bronze": "../storage/bronze/inmet/patrocinio/inmet_patrocinio.parquet",
        "path_silver": "../storage/silver/inmet/patrocinio/inmet_patrocinio.parquet",
        "path_gold": "../storage/gold/inmet/patrocinio/inmet_patrocinio.parquet",
    },
    "inmet_franca": {
        "partitions": ["Data"],
        "path_raw": "../storage/raw/dados_A708_D_2025-01-01_2026-07-06.csv",
        "path_bronze": "../storage/bronze/inmet/franca/inmet_franca.parquet",
        "path_silver": "../storage/silver/inmet/franca/inmet_franca.parquet",
        "path_gold": "../storage/gold/inmet/franca/inmet_franca.parquet",
    },
}
if __name__ == "__main__":
    job_raw = JobRawData(payload=payload, run=Pipeline())
    # job_raw.run_jobs()

    # run = Pipeline()

    # Realiza o donwload com base na série de códigos do Banco Central
    # run.download_tax_series(
    #     code=payload["data"].get("COD_SELIC"),
    #     start_date=payload["data"].get("start_date"),
    #     end_date=payload["data"].get("end_date"),
    # )
    # run.download_tax_series(
    #     code=payload["data"].get("COD_IPCA"),
    #     start_date=payload["data"].get("start_date"),
    #     end_date=payload["data"].get("end_date"),
    # )
    # df_row_robusta = run.create_dataframe(
    #     path=payload["path_raw"].get("path_robusta"),
    #     sheet_name=payload["path_raw"].get("sheet_name"),
    # )
    # df_row_arabica = run.create_dataframe(
    #     path=payload["path_raw"].get("path_arabica"),
    #     sheet_name=payload["path_raw"].get("sheet_name"),
    # )
    # df_row_arabica.show(2, truncate=False)

    # df_row_selic = run.create_dataframe(
    #     path=payload["path_raw"].get("path_selic")
    # )
    # df_row_ipca = run.create_dataframe(
    #     path=payload["path_raw"].get("path_ipca")
    # )

    # df_row_ipca = run.create_dataframe(
    #     path=payload["path_raw"].get("path_ipca")
    # )

    # df_row_inmet_patrocinio = run.create_dataframe(
    #     path=payload["path_raw"].get("path_inmet_patrocinio"),
    # )
    # df_row_inmet_patrocinio.show(20, truncate=False)
