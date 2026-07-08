import json
import logging
import re
import unicodedata
from pathlib import Path

from pipelines.pipeline_old import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class JobSilverData:
    """Padroniza e limpa os dados vindos da camada bronze."""

    def __init__(self, payload: dict, run: Pipeline) -> None:
        self.payload = payload
        self.run = run

    def run_jobs(self) -> None:
        """Executa a padronização de todas as fontes disponíveis."""
        logger.info("Starting silver standardization jobs...")
        self.job_ipca()
        self.job_selic()
        self.job_robusta()
        self.job_arabica()
        self.job_inmet_patrocinio()
        self.job_inmet_franca()

    def _prepare_source_metadata(
        self, source_payload: dict, source_name: str | None = None
    ) -> None:
        """Extrai metadados de cabeçalho e cria uma cópia limpa do arquivo bruto."""
        raw_path = source_payload.get("path_raw")
        if not raw_path:
            return

        source_path = Path(raw_path)
        if not source_path.exists():
            logger.warning(
                "Raw source not found for silver metadata preparation: %s",
                source_path,
            )
            return

        metadata_path = self._extract_header_metadata(
            source_path, output_dir=source_payload.get("metadata_dir")
        )
        cleaned_path = self._write_cleaned_source_file(
            source_path, output_dir=source_payload.get("metadata_dir")
        )
        source_payload["metadata_path"] = str(metadata_path)
        source_payload["path_raw_cleaned"] = str(cleaned_path)
        logger.info(
            "Prepared cleaned source file for %s at %s",
            source_name or source_path.stem,
            cleaned_path,
        )

    def _extract_header_metadata(
        self, raw_path: Path | str, output_dir: Path | str | None = None
    ) -> Path:
        """Extrai metadados de um arquivo bruto em formato chave: valor para um JSON."""
        source_path = Path(raw_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        metadata: dict[str, str] = {}
        for line in source_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            if key and value.strip():
                metadata[key.strip()] = value.strip()

        output_folder = (
            Path(output_dir) if output_dir is not None else source_path.parent
        )
        output_folder.mkdir(parents=True, exist_ok=True)
        metadata_path = output_folder / f"{source_path.stem}.metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("Extracted %s metadata fields from %s", len(metadata), source_path)
        return metadata_path

    def _write_cleaned_source_file(
        self, raw_path: Path | str, output_dir: Path | str | None = None
    ) -> Path:
        """Cria uma cópia limpa do arquivo bruto removendo o bloco de metadados de preâmbulo."""
        source_path = Path(raw_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

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
        output_folder = (
            Path(output_dir) if output_dir is not None else source_path.parent
        )
        output_folder.mkdir(parents=True, exist_ok=True)
        cleaned_path = output_folder / f"{source_path.stem}.cleaned{source_path.suffix}"
        cleaned_path.write_text("\n".join(cleaned_lines) + "\n", encoding="utf-8")
        logger.info("Wrote cleaned source file to %s", cleaned_path)
        return cleaned_path

    def _sanitize_column_name(col):
        # Remove acentos
        col = unicodedata.normalize("NFKD", col)
        col = col.encode("ASCII", "ignore").decode("utf-8")

        # lowercase
        col = col.lower()

        # troca espaços por _
        col = re.sub(r"\s+", "_", col)

        # remove caracteres especiais
        col = re.sub(r"[^a-z0-9_]", "", col)

        # remove múltiplos _
        col = re.sub(r"_+", "_", col)

        return col.strip("_")

    def _normalize_column_names(self, df: DataFrame, col_names: list[str]) -> DataFrame:
        """Renomeia as colunas de um DataFrame para os nomes esperados."""
        if not col_names:
            return df
        if len(col_names) != len(df.columns):
            logger.warning(
                "Column count mismatch while normalizing schema. Expected %s columns but received %s",
                len(col_names),
                len(df.columns),
            )
        return df.toDF(*col_names[: len(df.columns)])

    def _clean_numeric_column(self, df: DataFrame, column: str) -> DataFrame:
        """Converte coluna para numérica e trata valores inválidos."""
        return (
            df.withColumn(
                column, F.regexp_replace(F.col(column).cast("string"), r"[^0-9,.-]", "")
            )
            .withColumn(column, F.regexp_replace(F.col(column), ",", "."))
            .withColumn(
                column,
                F.when(F.col(column) == "", None).otherwise(
                    F.col(column).cast("double")
                ),
            )
        )

    def _clean_date_column(
        self, df: DataFrame, column: str, format_str: str = "dd/MM/yyyy"
    ) -> DataFrame:
        """Converte coluna para data e trata valores inválidos."""
        return df.withColumn(
            column,
            F.when(
                F.col(column).isNull() | (F.trim(F.col(column)) == ""), None
            ).otherwise(F.to_date(F.col(column).cast("string"), format_str)),
        )

    def job_ipca(self, payload: dict) -> DataFrame:
        """Padroniza os dados do IPCA vindos do bronze."""
        path_bronze = payload["ipca"]["path_bronze"]
        path_silver = payload["ipca"]["path_silver"]
        self._prepare_source_metadata(payload["ipca"], source_name="ipca")
        logger.info("Standardizing IPCA data from %s", path_bronze)

        df = self.run.read_parquet(path_bronze)
        if df is None:
            logger.warning(
                "No bronze dataframe available for IPCA at %s; skipping silver transformation",
                path_bronze,
            )
            return None

        df = (
            df.withColumnRenamed("data", "data")
            .withColumnRenamed("valor", "valor")
            .transform(lambda d: self._clean_date_column(d, "data", "dd/MM/yyyy"))
            .transform(lambda d: self._clean_numeric_column(d, "valor"))
            .withColumn("fonte", F.lit("IPCA"))
            .withColumn("data_mes", F.date_trunc("month", F.col("data")))
            .dropDuplicates(["data", "valor"])
        )
        self.run.write_parquet(df=df, output_path=path_silver, partitions=["data_mes"])
        return df

    def job_selic(self, payload: dict) -> DataFrame:
        """Padroniza os dados da SELIC vindos do bronze."""
        path_bronze = payload["selic"]["path_bronze"]
        path_silver = payload["selic"]["path_silver"]
        self._prepare_source_metadata(payload["selic"], source_name="selic")
        logger.info("Standardizing SELIC data from %s", path_bronze)

        df = self.run.read_parquet(path_bronze)
        if df is None:
            logger.warning(
                "No bronze dataframe available for SELIC at %s; skipping silver transformation",
                path_bronze,
            )
            return None

        df = (
            df.withColumnRenamed("data", "data")
            .withColumnRenamed("valor", "valor")
            .transform(lambda d: self._clean_date_column(d, "data", "dd/MM/yyyy"))
            .transform(lambda d: self._clean_numeric_column(d, "valor"))
            .withColumn("fonte", F.lit("SELIC"))
            .withColumn("data_mes", F.date_trunc("month", F.col("data")))
            .dropDuplicates(["data", "valor"])
        )
        self.run.write_parquet(df=df, output_path=path_silver, partitions=["data_mes"])
        return df

    def job_robusta(self, payload: dict) -> DataFrame:
        """Padroniza os dados de Robusta vindos do bronze."""
        path_bronze = payload["robusta"]["path_bronze"]
        path_silver = payload["robusta"]["path_silver"]
        self._prepare_source_metadata(payload["robusta"], source_name="robusta")
        logger.info("Standardizing Robusta data from %s", path_bronze)

        df = self.run.read_parquet(path_bronze)
        if df is None:
            logger.warning(
                "No bronze dataframe available for Robusta at %s; skipping silver transformation",
                path_bronze,
            )
            return None

        df = (
            df.withColumnRenamed("Data", "data")
            .withColumnRenamed("À vista R$", "valor_r")
            .withColumnRenamed("À vista US$", "valor_usd")
            .transform(lambda d: self._clean_date_column(d, "data", "dd/MM/yyyy"))
            .transform(lambda d: self._clean_numeric_column(d, "valor_r"))
            .transform(lambda d: self._clean_numeric_column(d, "valor_usd"))
            .withColumn("fonte", F.lit("CEPEA_ROBUSTA"))
            .withColumn("data_mes", F.date_trunc("month", F.col("data")))
            .dropDuplicates(["data"])
        )
        self.run.write_parquet(df=df, output_path=path_silver, partitions=["data_mes"])
        return df

    def job_arabica(self, payload: dict) -> DataFrame:
        """Padroniza os dados de Arabica vindos do bronze."""
        path_bronze = payload["arabica"]["path_bronze"]
        path_silver = payload["arabica"]["path_silver"]
        self._prepare_source_metadata(payload["arabica"], source_name="arabica")
        logger.info("Standardizing Arabica data from %s", path_bronze)

        df = self.run.read_parquet(path_bronze)
        if df is None:
            logger.warning(
                "No bronze dataframe available for Arabica at %s; skipping silver transformation",
                path_bronze,
            )
            return None

        df = (
            df.withColumnRenamed("Data", "data")
            .withColumnRenamed("À vista R$", "valor_r")
            .withColumnRenamed("À vista US$", "valor_usd")
            .transform(lambda d: self._clean_date_column(d, "data", "dd/MM/yyyy"))
            .transform(lambda d: self._clean_numeric_column(d, "valor_r"))
            .transform(lambda d: self._clean_numeric_column(d, "valor_usd"))
            .withColumn("fonte", F.lit("CEPEA_ARABICA"))
            .withColumn("data_mes", F.date_trunc("month", F.col("data")))
            .dropDuplicates(["data"])
        )
        self.run.write_parquet(df=df, output_path=path_silver, partitions=["data_mes"])
        return df

    def job_inmet_patrocinio(self) -> DataFrame:
        """Padroniza os dados climáticos do INMET Patrocínio vindos do bronze."""
        path_bronze = self.payload["inmet_patrocinio"]["path_bronze"]
        path_silver = self.payload["inmet_patrocinio"]["path_silver"]
        # self._prepare_source_metadata(payload["inmet_patrocinio"], source_name="inmet_patrocinio")
        col_names = [
            "data_medicao",
            "precipitacao_total_mm",
            "temp_max_c",
            "temp_media_c",
            "temp_min_c",
            "umidade_rel_media",
            "umidade_rel_minima",
        ]
        logger.info("Standardizing INMET Patrocínio data from %s", path_bronze)

        df = self.run.read_parquet(
            path_bronze,
            # options={
            # "header": "false",
            # #   "inferSchema": "true",
            #     "separator": ";"}
        )
        if df is None:
            logger.warning(
                "No bronze dataframe available for INMET Patrocínio at %s; skipping silver transformation",
                path_bronze,
            )
            return None

        # df = self.sanitize_column_name(df, col_names)

        # df = (
        #     df.withColumnRenamed("Nome: PATROCINIO", "metadata")
        #     .withColumn("fonte", F.lit("INMET_PATROCINIO"))
        #     .withColumn("data", F.lit(None).cast("date"))
        #     .withColumn("valor", F.lit(None).cast("double"))
        #     .dropDuplicates(["metadata", "fonte"])
        # )
        # self.run.write_parquet(df=df, output_path=path_silver, partitions=["fonte"])
        # df = df.filter(F.col("data").isNotNull())  # Filtra linhas com data nula

        df.show(5, truncate=False)  # Exibe o DataFrame para depuração
        return df

    def job_inmet_franca(self) -> DataFrame:
        """Padroniza os dados climáticos do INMET Franca vindos do bronze."""
        path_bronze = self.payload["inmet_franca"]["path_bronze"]
        path_silver = self.payload["inmet_franca"]["path_silver"]
        # self._prepare_source_metadata(payload["inmet_franca"], source_name="inmet_franca")
        logger.info("Standardizing INMET Franca data from %s", path_bronze)

        df = self.run.read_parquet(
            path_bronze,
            options={
                "header": "true",
                "delimiter": ";",
                "inferSchema": "true",
            },
        )
        if df is None:
            logger.warning(
                "No bronze dataframe available for INMET Franca at %s; skipping silver transformation",
                path_bronze,
            )
            return None

        col_names = [
            "data_medicao",
            "precipitacao_total_mm",
            "temp_max_c",
            "temp_media_c",
            "temp_min_c",
            "umidade_rel_media",
            "umidade_rel_minima",
        ]

        # df = self._cleane_csv_header(df, col_names)
        # df = (
        #     df.withColumnRenamed("Nome: FRANCA", "metadata")
        #     .withColumn("fonte", F.lit("INMET_FRANCA"))
        #     .withColumn("data", F.lit(None).cast("date"))
        #     .withColumn("valor", F.lit(None).cast("double"))
        #     .dropDuplicates(["metadata", "fonte"])
        # )

        # self.run.write_parquet(df=df, output_path=path_silver, partitions=["fonte"])
        df.show(5, truncate=False)
        return df

    def _cleane_csv_header(self, df: DataFrame, col_names: list[str]) -> DataFrame:
        """Renomeia e, quando necessário, separa as colunas de um CSV com base nos nomes recebidos."""
        if not col_names:
            return df

        if len(df.columns) == len(col_names):
            print("###entrei aqui.. ###")
            return df.toDF(*col_names)

        if len(df.columns) == 1:
            # 2. Cria um ID sequencial para conseguir identificar e pular as linhas de metadados
            indexed_df = df.withColumn("row_id", F.monotonically_increasing_id())

            # 3. Filtra mantendo apenas as linhas de dados (linha 1 em diante)
            data_lines_df = indexed_df.filter("row_id >= 1")

            source_col = data_lines_df.columns
            parsed_columns = [
                F.trim(F.split(F.col(source_col).cast("string"), ";")[idx]).alias(name)
                for idx, name in enumerate(col_names)
            ]
            return df.select(*parsed_columns)

        renamed_columns = col_names[: len(df.columns)]
        return df.toDF(*renamed_columns)
