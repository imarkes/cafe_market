import json
import logging
from pathlib import Path
from pyspark.sql.functions import lit
from pipelines.pipeline_old import Pipeline

logger = logging.getLogger(__name__)


class JobBronzeData:
    """Orquestra a ingestão de dados brutos a partir de arquivos e APIs."""

    def __init__(self, payload: dict, run: Pipeline) -> None:
        self.payload = payload
        self.run = run

    def run_jobs(self) -> None:
        """Executa todos os jobs de ingestão de dados."""
        logger.info("Starting raw data ingestion jobs...")
        self.job_robusta()
        self.job_arabica()
        self.job_selic()
        self.job_ipca()
        self.job_inmet_patrocinio()
        self.job_inmet_franca()

    # 1.CEPEA/ESALQ (ingestão de arquivo)
    def job_robusta(self):
        """Lê o arquivo de Robusta e persiste a versão bruta no formato Parquet."""

        path_raw_robusta = self.payload["robusta"].get("path_raw")
        path_bronze_robusta = self.payload["robusta"].get("path_bronze")
        self._prepare_source_metadata(self.payload["robusta"], source_name="robusta")
        logger.info(
            "creating DataFrame for Robusta data from path: %s", path_raw_robusta
        )
        df = self.run.create_dataframe(
            path=path_raw_robusta,
            sheet_name=self.payload["robusta"].get("sheet_name"),
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_robusta)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_robusta,
        )
        df.show(2)
        return df

    def job_arabica(self):
        """Lê o arquivo de Arabica e persiste a versão bruta no formato Parquet."""

        path_raw_arabica = self.payload["arabica"].get("path_raw")
        path_bronze_arabica = self.payload["arabica"].get("path_bronze")
        self._prepare_source_metadata(self.payload["arabica"], source_name="arabica")

        logger.info(
            "creating DataFrame for Arabica data from path: %s", path_raw_arabica
        )
        df = self.run.create_dataframe(
            path=path_raw_arabica,
            sheet_name=self.payload["arabica"].get("sheet_name"),
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_arabica)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_arabica,
        )
        df.show(2)
        return df

    # 2. SELIC e IPCA
    def job_selic(self):
        """Lê o arquivo de SELIC e persiste a versão bruta no formato Parquet."""

        path_raw_selic = self.payload["selic"].get("path_raw")
        path_bronze_selic = self.payload["selic"].get("path_bronze")
        self._prepare_source_metadata(self.payload["selic"], source_name="selic")
        logger.info("creating DataFrame for SELIC data from path: %s", path_raw_selic)
        df = self.run.create_dataframe(path=path_raw_selic)

        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_selic)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_selic,
        )
        df.show(2)
        return df

    def job_ipca(self):
        """Lê o arquivo de IPCA e persiste a versão bruta no formato Parquet."""

        path_raw_ipca = self.payload["ipca"].get("path_raw")
        path_bronze_ipca = self.payload["ipca"].get("path_bronze")
        self._prepare_source_metadata(self.payload["ipca"], source_name="ipca")
        logger.info("creating DataFrame for IPCA data from path: %s", path_raw_ipca)
        df = self.run.create_dataframe(path=path_raw_ipca)

        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_ipca)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_ipca,
        )
        df.show(2)
        return df

    # 3. INMET / BDMEP
    def job_inmet_patrocinio(self):
        """Lê o arquivo de INMET (Patrocínio) e persiste a versão bruta no formato Parquet."""

        path_raw = self.payload["inmet_patrocinio"].get("path_raw")
        path_bronze = self.payload["inmet_patrocinio"].get("path_bronze")
        # self._prepare_source_metadata(
        #     self.payload["inmet_patrocinio"], source_name="patrocinio"
        # )

        cleaned_path = self.payload["inmet_patrocinio"].get(
            "path_raw_cleaned", path_raw
        )

        logger.info(
            "creating DataFrame for INMET (Patrocinio) data from path: %s",
            cleaned_path,
        )
        df = self.run.create_dataframe(
            path=cleaned_path,
            options={
                "delimiter": ";",
                "header": "true",
                "inferSchema": "true",
            },
        )
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze)

        self.run.write_parquet(
            df=df,
            output_path=path_bronze,
        )
        df.show(2)
        return df

    def job_inmet_franca(self):
        """Lê o arquivo de INMET (Franca) e persiste a versão bruta no formato Parquet."""

        path_inmet_franca = self.payload["inmet_franca"].get("path_raw")
        path_bronze_franca = self.payload["inmet_franca"].get("path_bronze")

        cleaned_path_inmet_franca = self.payload["inmet_franca"].get(
            "path_raw_cleaned", path_inmet_franca
        )

        logger.info(
            "creating DataFrame for INMET (Franca) data from path: %s",
            cleaned_path_inmet_franca,
        )

        df = self.run.spark.read.format('csv').options(
            header=True, delimiter=";", nullValue="null").load(cleaned_path_inmet_franca)

        def add_metadata(df):

            meta_dir = Path("../storage/meta") / "inmet_franca"
            meta_dir.mkdir(parents=True, exist_ok=True)

            metadata_path = meta_dir / "metadata.json"

            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)

            df = (
                df
                .withColumn("estacao", lit(metadata["Nome"]))
                .withColumn("codigo_estacao", lit(metadata["Codigo Estacao"]))
                .withColumn("latitude", lit(metadata["Latitude"]))
                .withColumn("longitude", lit(metadata["Longitude"]))
                .withColumn("altitude", lit(metadata["Altitude"]))
                .withColumn("situacao", lit(metadata["Situacao"]))
                .withColumn("data_inicial", lit(metadata["Data Inicial"]))
                .withColumn("data_final", lit(metadata["Data Final"]))
            )
            return df
        df = add_metadata(df=df)
        logger.info("Writing DataFrame to Parquet at: %s", path_bronze_franca)
        self.run.write_parquet(
            df=df,
            output_path=path_bronze_franca,
        )

        return df

    def _prepare_source_metadata(
        self, source_payload: dict, source_name: str | None = None
    ) -> None:
        """Lê o arquivo bruto, extrai metadados, remove o cabeçalho e sobrescreve o arquivo limpo no mesmo path_raw."""

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

        metadata_folder = Path("../storage/meta") / (source_name or source_path.stem)
        metadata_path = self._extract_header_metadata(
            source_path,
            output_dir=metadata_folder,
        )
        cleaned_path = self._write_cleaned_source_file(source_path)
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
        """Extrai metadados em formato de cabeçalho de um arquivo bruto e salva como JSON."""
        source_path = Path(raw_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        metadata: dict[str, str] = {}
        lines = source_path.read_text(encoding="utf-8").splitlines()
        for line in lines:
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

    def _write_cleaned_source_file(self, raw_path: Path | str) -> Path:
        """Remove as linhas de metadados do arquivo bruto e salva uma cópia limpa com o sufixo _cleaned no path_raw."""
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
        cleaned_content = "\n".join(cleaned_lines).rstrip() + "\n"

        cleaned_path = source_path.with_name(
            f"{source_path.stem}_cleaned{source_path.suffix}"
        )
        cleaned_path.write_text(cleaned_content, encoding="utf-8")
        logger.info("Wrote cleaned source file to %s", cleaned_path)
        return cleaned_path


if __name__ == "__main__":
    ...
