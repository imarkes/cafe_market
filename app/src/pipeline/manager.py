from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from ..pipeline.downloader import TaxDataDownloader
from ..utils.readers import DataReaderFactory
from ..utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

BCB_SERIES_CODE_KEYS = {
    "selic": "COD_SELIC",
    "ipca": "COD_IPCA",
}


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
        payload: dict[str, dict[str, Any]],
        tax_name: str | None = None,
    ) -> dict[str, Path]:
        """
        Baixa uma ou todas as séries econômicas do Banco Central.

        Quando ``tax_name`` é informado, apenas a série correspondente é
        processada. Quando não é informado, todas as séries registradas em
        ``BCB_SERIES_CODE_KEYS`` são baixadas.

        Parameters
        ----------
        payload
            Configuração das fontes de dados. Cada série deve possuir:

            - código da série, como ``COD_SELIC`` ou ``COD_IPCA``;
            - ``start_date``;
            - ``end_date``.

        tax_name
            Nome da série a ser baixada, como ``selic`` ou ``ipca``.
            Quando ``None``, todas as séries configuradas são baixadas.

        Returns
        -------
        dict[str, Path]
            Dicionário no formato ``nome_da_serie: caminho_do_arquivo``.

        Raises
        ------
        ValueError
            Quando a série informada não é suportada ou possui configuração
            incompleta no payload.

        KeyError
            Quando a série não existe no payload.
        """

        series_names = self._resolve_tax_series_names(tax_name)
        downloaded_files: dict[str, Path] = {}

        for series_name in series_names:
            source_config = payload.get(series_name)

            if not isinstance(source_config, dict):
                raise KeyError(
                    f"A série '{series_name}' não foi encontrada no payload."
                )

            code_key = BCB_SERIES_CODE_KEYS[series_name]

            code = source_config.get(code_key)
            start_date = source_config.get("start_date")
            end_date = source_config.get("end_date")

            missing_fields = [
                field_name
                for field_name, field_value in {
                    code_key: code,
                    "start_date": start_date,
                    "end_date": end_date,
                }.items()
                if field_value in (None, "")
            ]

            if missing_fields:
                raise ValueError(
                    f"Configuração incompleta para '{series_name}'. "
                    f"Campos ausentes: {', '.join(missing_fields)}."
                )

            logger.info(
                "Downloading BCB series '%s', code %s, from %s to %s.",
                series_name,
                code,
                start_date,
                end_date,
            )

            downloaded_path = self.tax_series.download(
                int(code),
                str(start_date),
                str(end_date),
            )

            downloaded_files[series_name] = downloaded_path

            logger.info(
                "BCB series '%s' successfully saved at: %s",
                series_name,
                downloaded_path,
            )

        return downloaded_files

    @staticmethod
    def _resolve_tax_series_names(tax_name: str | None) -> list[str]:
        """
        Resolve quais séries econômicas devem ser processadas.

        Parameters
        ----------
        tax_name
            Nome de uma série específica ou ``None`` para processar todas.

        Returns
        -------
        list[str]
            Lista de séries a serem baixadas.
        """

        if tax_name is None:
            return list(BCB_SERIES_CODE_KEYS)

        normalized_name = tax_name.strip().lower()

        if normalized_name not in BCB_SERIES_CODE_KEYS:
            supported_series = ", ".join(BCB_SERIES_CODE_KEYS)

            raise ValueError(
                f"Série econômica não suportada: '{tax_name}'. "
                f"Valores permitidos: {supported_series}."
            )

        return [normalized_name]

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
