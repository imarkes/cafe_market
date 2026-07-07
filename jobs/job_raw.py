import logging

from pipeline import Pipeline

logger = logging.getLogger(__name__)


class JobRawData:
    """Orquestra a ingestão de dados brutos a partir de arquivos e APIs."""

    def __init__(self, payload: dict, run: Pipeline) -> None:
        self.payload = payload
        self.run = run

    def run_jobs(self) -> None:
        """Executa todos os jobs de ingestão de dados."""
        logger.info("Starting raw data ingestion jobs...")
        # self.job_robusta(self.payload)
        # self.job_arabica(self.payload)
        # self.job_selic(self.payload)
        # self.job_ipca(self.payload)
        self.job_inmet_patrocinio(self.payload)
        self.job_inmet_franca(self.payload)

    # 1.CEPEA/ESALQ (ingestão de arquivo)
    def job_robusta(self, payload: dict):
        """Lê o arquivo de Robusta e persiste a versão bruta no formato Parquet."""

        path_raw_robusta = payload["robusta"].get("path_raw")
        path_bronze_robusta = payload["robusta"].get("path_bronze")

        logger.info(
            "creating DataFrame for Robusta data from path: %s", path_raw_robusta
        )
        df = self.run.create_dataframe(
            path=path_raw_robusta,
            sheet_name=payload["robusta"].get("sheet_name"),
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_robusta)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_robusta,
        )
        return df

    def job_arabica(self, payload: dict):
        """Lê o arquivo de Arabica e persiste a versão bruta no formato Parquet."""

        path_raw_arabica = payload["arabica"].get("path_raw")
        path_bronze_arabica = payload["arabica"].get("path_bronze")

        logger.info(
            "creating DataFrame for Arabica data from path: %s", path_raw_arabica
        )
        df = self.run.create_dataframe(
            path=path_raw_arabica,
            sheet_name=payload["arabica"].get("sheet_name"),
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_arabica)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_arabica,
        )
        return df

    # 2. SELIC e IPCA
    def job_selic(self, payload: dict):
        """Lê o arquivo de SELIC e persiste a versão bruta no formato Parquet."""

        path_raw_selic = payload["selic"].get("path_raw")
        path_bronze_selic = payload["selic"].get("path_bronze")

        logger.info("creating DataFrame for SELIC data from path: %s", path_raw_selic)
        df = self.run.create_dataframe(path=path_raw_selic)

        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_selic)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_selic,
        )
        return df

    def job_ipca(self, payload: dict):
        """Lê o arquivo de IPCA e persiste a versão bruta no formato Parquet."""

        path_raw_ipca = payload["ipca"].get("path_raw")
        path_bronze_ipca = payload["ipca"].get("path_bronze")

        logger.info("creating DataFrame for IPCA data from path: %s", path_raw_ipca)
        df = self.run.create_dataframe(path=path_raw_ipca)

        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_ipca)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_ipca,
        )
        return df

    # 3. INMET / BDMEP
    def job_inmet_patrocinio(self, payload: dict):
        """Lê o arquivo de INMET (Patrocínio) e persiste a versão bruta no formato Parquet."""

        path_inmet_patrocinio = payload["inmet_patrocinio"].get("path_raw")
        path_bronze_patrocinio = payload["inmet_patrocinio"].get("path_bronze")

        logger.info(
            "creating DataFrame for INMET (Patrocinio) data from path: %s",
            path_inmet_patrocinio,
        )
        df = self.run.create_dataframe(
            path=path_inmet_patrocinio,
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_patrocinio)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_patrocinio,
        )
        return df

    def job_inmet_franca(self, payload: dict):
        """Lê o arquivo de INMET (Franca) e persiste a versão bruta no formato Parquet."""

        path_inmet_franca = payload["inmet_franca"].get("path_raw")
        path_bronze_franca = payload["inmet_franca"].get("path_bronze")

        logger.info(
            "creating DataFrame for INMET (Franca) data from path: %s",
            path_inmet_franca,
        )
        df = self.run.create_dataframe(
            path=path_inmet_franca,
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_franca)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_franca,
        )
        return df


if __name__ == "__main__":
    ...
