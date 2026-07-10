from __future__ import annotations

import logging
from pathlib import Path

from pipeline.downloader import TaxDataDownloader
from pyspark.sql import DataFrame, SparkSession
from utils.readers import DataReaderFactory
from utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)


class DataManager:
    """
    Camada de abstração para operações de leitura e escrita de dados.

    Responsabilidades
    -----------------
    - Criar DataFrames Spark a partir de arquivos suportados.
    - Persistir DataFrames em Parquet.
    - Ler datasets Parquet.
    - Baixar séries temporais do Banco Central.

    A classe não contém regras de negócio.
    Toda transformação deve ocorrer na camada de Jobs.
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or create_spark_session()
        self.tax_series = TaxDataDownloader("../storage/raw")

    def download_tax_series(
        self,
        code: int,
        start_date: str,
        end_date: str,
    ) -> Path:
        """
        Baixa uma série histórica do Banco Central.

        Returns
        -------
        Path
            Caminho do arquivo salvo.
        """

        logger.info(
            "Downloading Central Bank series %s (%s -> %s)",
            code,
            start_date,
            end_date,
        )

        return self.tax_series.download(
            code,
            start_date,
            end_date,
        )

    def create_dataframe(
        self,
        path: str,
        options: dict | None = None,
    ) -> DataFrame:
        """
        Cria um DataFrame Spark a partir de um arquivo suportado.

        O tipo do arquivo é identificado automaticamente pelo
        ``DataReaderFactory``, que instancia o leitor apropriado
        (CSV, Excel, JSON, etc.).

        Parameters
        ----------
        path
            Caminho do arquivo de origem.

        options
            Opções específicas do leitor. As opções padrão são
            definidas por cada implementação de ``DataReader``.

        Returns
        -------
        DataFrame
            DataFrame Spark contendo os dados do arquivo.
        """

        logger.info("Reading dataset: %s", path)

        reader = DataReaderFactory.create(
            path=path,
            spark=self.spark,
        )

        return reader.read(
            path,
            **(options or {}),
        )

    def write_parquet(
        self,
        df: DataFrame,
        output_path: str,
        partitions: list[str] | None = None,
        mode: str = "overwrite",
    ) -> None:
        """
        Persiste um DataFrame em formato Parquet.

        Parameters
        ----------
        df
            DataFrame Spark.

        output_path
            Diretório de saída.

        partitions
            Colunas utilizadas para particionamento.

        mode
            Modo de escrita.
        """

        logger.info(
            "Writing parquet dataset: %s",
            output_path,
        )

        writer = df.write.format("parquet").mode(mode).option("compression", "snappy")

        if partitions:
            writer = writer.partitionBy(*partitions)

        writer.save(output_path)
