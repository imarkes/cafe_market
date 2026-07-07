import logging

from pipeline import Pipeline

logger = logging.getLogger(__name__)

class JobBrozeData:
    def __init__(self, payload: dict, run: Pipeline) -> None:
        self.payload = payload
        self.run = run

    def job_ipca(self, payload: dict):
        """Retorna um DataFrame com os dados da série de IPCA."""
        path_bronze_ipca = payload["ipca"].get("path_bronze")
        path_silver_ipca = payload["ipca"].get("path_silver")
        logger.info(
            "creating DataFrame for IPCA data from path: %s", path_bronze_ipca
        )
        df = self.run.create_dataframe(
            path=path_bronze_ipca,
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_ipca)
        self.run.write_parquet(
            df=df,
            output_path=path_silver_ipca,
            partitions=payload["ipca"].get("partitions"),
        )
        return df
