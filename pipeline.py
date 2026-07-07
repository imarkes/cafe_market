from __future__ import annotations

import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
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

    def _prepare_csv_for_reading(self, path: str, options: Optional[dict] = None) -> str:
        """Remove linhas de metadados de preâmbulo e devolve um caminho temporário com os dados limpos."""
        source_path = Path(path)
        if source_path.suffix.lower() != ".csv":
            return path

        lines = source_path.read_text(encoding="utf-8").splitlines()
        start_index = 0
        for index, line in enumerate(lines):

            stripped = line.strip()
            if not stripped:
                continue
            if ":" in stripped and ";" not in stripped and "," not in stripped:
                continue
            start_index = index
            break

        cleaned_lines = [line.rstrip() for line in lines[start_index:]]

        with NamedTemporaryFile("w", encoding="utf-8", suffix=".csv", delete=False) as handle:
            handle.write("\n".join(cleaned_lines) + "\n")
            temp_path = handle.name

        logger.info("Prepared CSV for Spark reading at %s", temp_path)
        return temp_path

    def create_dataframe(
        self, path: str, options: dict | None = None, sheet_name: str | None = None
    ) -> DataFrame:
        """Cria um DataFrame a partir de um arquivo suportado."""
        logger.info("Creating DataFrame from path: %s", path)
        reader = DataReaderFactory.create(path, self.spark)
        read_options = dict(options or {})
        if Path(path).suffix.lower() == ".xls":
            read_options["dataAddress"] = sheet_name or read_options.get(
                "dataAddress", "'Sheet1'!A1"
            )
        elif Path(path).suffix.lower() == ".csv":
            prepared_path = self._prepare_csv_for_reading(path, read_options)
            return reader.read(prepared_path, **read_options)
        return reader.read(path, **read_options)

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

        return self.spark.read.options(**(options or {})).parquet(path)


if __name__ == "__main__":
    ...
