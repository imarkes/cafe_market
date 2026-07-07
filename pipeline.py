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
    def __init__(self, spark: Optional[SparkSession] = None) -> None:
        self.spark = spark or create_spark_session()
        self.tax_series = TaxDataDownloader("../storage/raw")

    def download_tax_series(self, code: int, start_date: str, end_date: str) -> Path:
        return self.tax_series.download(code, start_date, end_date)

    def create_dataframe(self, path: str, sheet_name: str = None) -> DataFrame:
        reader = DataReaderFactory.create(path, self.spark)
        options = (
            {"dataAddress": sheet_name} if Path(path).suffix.lower() == ".xls" else {}
        )
        return reader.read(path, **options)

    def write_parquet(
        self,
        df: DataFrame,
        output_path: str,
        partitions: list[str] = [],
        mode: str = "overwrite",
    ) -> None:
        logger.info("Writing DataFrame to Parquet at: %s", output_path)
        return (
            df.write.format("parquet")
            .mode(mode)
            .partitionBy(*partitions)
            .option("compression", "snappy")
            .save(output_path)
        )


if __name__ == "__main__":
    ...
