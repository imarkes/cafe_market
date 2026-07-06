from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DataReader(Protocol):
    def read(self, path: str, **options: str) -> DataFrame: ...


class ExcelDataReader:
    """Cria dataframe baseado em um arquivo
    Excel usando o Spark. Suporta arquivos com extensão .xls e .xlsx."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, path: str, **options: str) -> DataFrame:
        logger.info("Reading Excel file: %s", path)
        return (
            self.spark.read.format("com.crealytics.spark.excel")
            .option("header", options.get("header", "true"))
            .option("inferSchema", options.get("inferSchema", "true"))
            .option("dataAddress", options.get("dataAddress", "'Sheet1'!A1"))
            .load(path)
        )


class JsonDataReader:
    """Cria dataframe baseado em um arquivo JSON usando o Spark."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, path: str, **options: str) -> DataFrame:
        logger.info("Reading JSON file: %s", path)
        return (
            self.spark.read.format("json")
            .option("inferSchema", options.get("inferSchema", "true"))
            .option("multiline", options.get("multiline", "true"))
            .load(path)
        )


class DataReaderFactory:
    """Fábrica para criar leitores de dados com base na extensão do arquivo."""

    @staticmethod
    def create(path: str, spark: SparkSession | None = None) -> DataReader:
        suffix = Path(path).suffix.lower()
        if suffix == ".xls":
            return (
                ExcelDataReader(spark)
                if spark is not None
                else ExcelDataReader.__new__(ExcelDataReader)
            )
        if suffix == ".json":
            return (
                JsonDataReader(spark)
                if spark is not None
                else JsonDataReader.__new__(JsonDataReader)
            )
        raise ValueError(f"Unsupported file extension: {suffix}")
