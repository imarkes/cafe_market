from jobs.job_bronze import JobBrozeData
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
def main() -> None:
    """Ponto de entrada principal da pipeline."""
    run = Pipeline()

    # Exemplo para baixar os dados do Banco Central (SELIC e IPCA)
    # run.download_tax_series(
    #     code=payload["selic"]["COD_SELIC"],
    #     start_date=payload["selic"]["start_date"],
    #     end_date=payload["selic"]["end_date"],
    #     output_path=payload["selic"]["path_raw"],
    # )

    # Exemplo de execução do fluxo raw
    job_raw = JobRawData(payload=payload, run=run)
    job_raw.run_jobs()

    # # Exemplo de execução do fluxo bronze
    # job_bronze = JobBrozeData(payload=payload, run=run)
    # job_bronze.job_ipca(payload=payload)


if __name__ == "__main__":
    main()
