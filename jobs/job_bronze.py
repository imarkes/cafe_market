from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path

from pipeline import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

logger = logging.getLogger(__name__)


class JobBronzeData:
    """
    Responsável pela ingestão da camada Bronze.

    A classe é totalmente genérica e utiliza apenas as
    configurações existentes no payload.

    Fluxo

        payload
            ↓
        resolve input
            ↓
        create dataframe
            ↓
        add metadata (quando existir)
            ↓
        write parquet
    """

    def __init__(
        self,
        payload: dict,
        run: Pipeline,
    ) -> None:

        self.payload = payload
        self.run = run

    def run_all(self) -> None:
        """
        Executa todos os jobs definidos no payload.
        """

        logger.info("Starting Bronze ingestion...")

        for source_name in self.payload:
            self.process_source(source_name)

        logger.info("Bronze ingestion finished.")

    def process_source(
        self,
        source_name: str,
    ) -> DataFrame:
        """
        Processa uma origem de dados.

        Parameters
        ----------
        source_name

            Nome da origem existente no payload.

        Returns
        -------
        DataFrame
        """

        source = self.payload[source_name]

        input_path = self._resolve_input_path(source)

        logger.info(
            "Reading source '%s' from %s",
            source_name,
            input_path,
        )

        df = self.run.create_dataframe(
            path=input_path,
            options=self._build_read_options(source),
        )

        metadata = self._load_metadata(source_name)

        if metadata:
            df = self._add_metadata(df, metadata)

        self.run.write_parquet(
            df=df,
            output_path=source["path_bronze"],
            partitions=source.get("partitions"),
        )

        logger.info(
            "Bronze dataset successfully created: %s",
            source_name,
        )
        df.printSchema()
        print("*-" * 50)
        return df

    def _resolve_input_path(
        self,
        source: dict,
    ) -> str:
        """
        Retorna o arquivo correto.

        Para arquivos tratados (INMET)
        utiliza path_raw_cleaned.

        Caso contrário utiliza path_raw.
        """

        return source.get(
            "path_raw_cleaned",
            source["path_raw"],
        )

    def _build_read_options(
        self,
        source: dict,
    ) -> dict:
        """
        Constrói as opções de leitura
        para o Reader correspondente.
        """

        options: dict = {}

        sheet = source.get("dataAddress")

        if sheet:
            options["dataAddress"] = sheet

        return options

    def _load_metadata(
        self,
        source_name: str,
    ) -> dict:

        metadata_path = Path("../storage/meta") / source_name / "metadata.json"

        if not metadata_path.exists():
            return {}

        with metadata_path.open(encoding="utf-8") as fp:

            return json.load(fp)

    def _add_metadata(
        self,
        df: DataFrame,
        metadata: dict,
    ) -> DataFrame:
        """
        Adiciona todas as chaves do metadata
        como colunas constantes.
        """

        for key, value in metadata.items():

            column = self._normalize_column_name(key)

            df = df.withColumn(
                column,
                lit(value),
            )

        return df

    @staticmethod
    def _normalize_column_name(
        column: str,
    ) -> str:
        """
        Converte um texto para snake_case.

        Exemplo

            Codigo Estacao

        torna-se

            codigo_estacao
        """

        column = unicodedata.normalize(
            "NFKD",
            column,
        )

        column = column.encode(
            "ascii",
            "ignore",
        ).decode("utf8")

        column = column.lower()

        column = re.sub(
            r"[^a-z0-9]+",
            "_",
            column,
        )

        return column.strip("_")
