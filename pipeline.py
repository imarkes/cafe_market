from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from downloader import TaxDataDownloader
from pyspark.sql import DataFrame, SparkSession
from readers import DataReaderFactory
from spark_session import create_spark_session

logger = logging.getLogger(__name__)


class Pipeline:
    """Camada de orquestração para leitura, escrita e extração de dados."""

    def __init__(self, spark: Optional[SparkSession] = None) -> None:
        self.spark = spark or create_spark_session()
        self.tax_series = TaxDataDownloader("../storage/raw")

    def download_tax_series(self, code: int, start_date: str, end_date: str) -> Path:
        """Baixa uma série do Banco Central e salva em disco."""

        logger.info(
            "Downloading tax series %s from %s to %s", code, start_date, end_date
        )
        return self.tax_series.download(code, start_date, end_date)

    def create_dataframe(self, path: str, sheet_name: str | None = None) -> DataFrame:
        """Cria um DataFrame a partir de um arquivo suportado."""
        logger.info("Creating DataFrame from path: %s", path)
        reader = DataReaderFactory.create(path, self.spark)
        options = (
            {"dataAddress": sheet_name} if Path(path).suffix.lower() == ".xls" else {}
        )
        return reader.read(path, **options)

    def write_parquet(
        self,
        df: DataFrame,
        output_path: str,
        partitions: Optional[list[str]] = None,
        mode: str = "overwrite",
    ) -> None:
        """Escreve um DataFrame em formato Parquet."""

        logger.info("Writing DataFrame to Parquet at: %s", output_path)
        writer = df.write.format("parquet").mode(mode).option("compression", "snappy")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(output_path)

    def read_parquet(self, path: str, options: Optional[dict] = None) -> DataFrame:
        """Lê um arquivo Parquet já persistido."""
        logger.info("Reading Parquet file: %s", path)
        read_options = options or {}
        return self.spark.read.options(**read_options).parquet(path)

    def process_data(self, df: DataFrame, process_function) -> DataFrame:
        """Aplica uma função de processamento a um DataFrame."""
        logger.info("Processing DataFrame with provided function.")
        return process_function(df)


if __name__ == "__main__":
    ...
