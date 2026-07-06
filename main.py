from logging_config import configure_logging
from pipeline import Pipeline

configure_logging("INFO")

payload = {
    "data": {
        "COD_SELIC": 11,
        "COD_IPCA": 433,
        "start_date": "01/01/2020",
        "end_date": "31/12/2020",
        "sheet_name": "'Plan 1'!A4",  # !Atencao ao nome da planilha
    },
    "path_raw": {
        "path_robusta": "../storage/raw/ROBUSTA_CEPEA_20260706165657.xls",
        "path_arabica": "../storage/raw/ARABICA_CEPEA_20260706165648.xls",
        "path_inmet_patrocinio": "../storage/raw/imet_patrocinio_20260706165657.xls",
        "path_inmet_franca": "../storage/raw/imet_sao_sebastiao_20260706165657.xls",
        "path_selic": "../storage/raw/serie_11.json",
        "path_ipca": "../storage/raw/serie_433.json",
    },
    "path_bronze": {},
    "path_silver": {},
    "path_gold": {},
}
if __name__ == "__main__":

    run = Pipeline()

    # Realiza o donwload com base na série de códigos do Banco Central
    run.download_tax_series(
        code=payload["data"].get("COD_SELIC"),
        start_date=payload["data"].get("start_date"),
        end_date=payload["data"].get("end_date"),
    )
    run.download_tax_series(
        code=payload["data"].get("COD_IPCA"),
        start_date=payload["data"].get("start_date"),
        end_date=payload["data"].get("end_date"),
    )
    df_row_robusta = run.create_dataframe(
        path=payload["path_raw"].get("path_robusta"),
        sheet_name=payload["path_raw"].get("sheet_name"),
    )
    df_row_arabica = run.create_dataframe(
        path=payload["path_raw"].get("path_arabica"),
        sheet_name=payload["path_raw"].get("sheet_name"),
    )

    df_row_selic = run.create_dataframe(
        path=payload["path_raw"].get("path_selic")
    )
    df_row_ipca = run.create_dataframe(
        path=payload["path_raw"].get("path_ipca")
    )
