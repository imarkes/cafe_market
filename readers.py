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


class CsvDataReader:
    """Cria dataframe baseado em um arquivo CSV usando o Spark."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, path: str, **options: str) -> DataFrame:
        logger.info("Reading CSV file: %s", path)
        return (
            self.spark.read.format("csv")
            .option("header", options.get("header", "true"))
            .option("inferSchema", options.get("inferSchema", "true"))
            .option("delimiter", options.get("delimiter", ";"))
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
    def resolve_reader_class(path: str) -> type[DataReader]:
        """Retorna a classe do leitor compatível com o arquivo informado."""
        suffix = Path(path).suffix.lower()
        if suffix == ".xls":
            return ExcelDataReader
        if suffix == ".json":
            return JsonDataReader
        if suffix == ".csv":
            return CsvDataReader
        raise ValueError(f"Unsupported file extension: {suffix}")

    @staticmethod
    def create(path: str, spark: SparkSession | None = None) -> DataReader:
        """Cria uma instância do leitor apropriado para o caminho passado."""
        reader_class = DataReaderFactory.resolve_reader_class(path)
        if spark is None:
            return reader_class.__new__(reader_class)
        return reader_class(spark)
