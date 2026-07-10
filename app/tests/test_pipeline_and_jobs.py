import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.jobs.job_bronze_old import JobBronzeData
from app.src.jobs.job_raw import JobRawData
from app.src.jobs.job_silver import JobSilverData
from app.pipeline_old import Pipeline
from app.readers_old import CsvDataReader, DataReaderFactory, ExcelDataReader, JsonDataReader


class StubPipeline:
    def __init__(self) -> None:
        self.created_paths = []
        self.written_paths = []

    def create_dataframe(
        self, path: str, options: dict | None = None, sheet_name: str | None = None
    ):
        self.created_paths.append((path, sheet_name))
        return {"path": path, "options": options}

    def write_parquet(self, df, output_path: str, partitions=None, mode: str = "overwrite") -> None:
        self.written_paths.append((output_path, partitions or []))


def test_reader_factory_resolves_supported_classes() -> None:
    assert DataReaderFactory.resolve_reader_class("/tmp/file.csv") is CsvDataReader
    assert DataReaderFactory.resolve_reader_class("/tmp/file.xls") is ExcelDataReader
    assert DataReaderFactory.resolve_reader_class("/tmp/file.json") is JsonDataReader


def test_job_raw_runs_all_jobs_and_writes_outputs() -> None:
    payload = {
        "robusta": {"path_raw": "robusta.xls", "path_bronze": "robusta.parquet"},
        "arabica": {"path_raw": "arabica.xls", "path_bronze": "arabica.parquet"},
        "selic": {"path_raw": "selic.json", "path_bronze": "selic.parquet"},
        "ipca": {"path_raw": "ipca.json", "path_bronze": "ipca.parquet"},
        "inmet_patrocinio": {"path_raw": "inmet_patrocinio.csv", "path_bronze": "inmet_patrocinio.parquet"},
        "inmet_franca": {"path_raw": "inmet_franca.csv", "path_bronze": "inmet_franca.parquet"},
    }

    stub = StubPipeline()
    job = JobBronzeData(payload=payload, run=stub)
    job.run_jobs()

    assert len(stub.created_paths) == 6
    assert len(stub.written_paths) == 6


def test_job_silver_runs_for_supported_sources() -> None:
    payload = {
        "ipca": {"path_bronze": "ipca.parquet", "path_silver": "ipca_silver.parquet"},
        "selic": {
            "path_bronze": "selic.parquet",
            "path_silver": "selic_silver.parquet",
        },
        "robusta": {
            "path_bronze": "robusta.parquet",
            "path_silver": "robusta_silver.parquet",
        },
        "arabica": {
            "path_bronze": "arabica.parquet",
            "path_silver": "arabica_silver.parquet",
        },
        "inmet_patrocinio": {
            "path_bronze": "inmet_patrocinio.parquet",
            "path_silver": "inmet_patrocinio_silver.parquet",
        },
        "inmet_franca": {
            "path_bronze": "inmet_franca.parquet",
            "path_silver": "inmet_franca_silver.parquet",
        },
    }

    class SilverStubPipeline(StubPipeline):
        def read_parquet(self, path: str, options: dict | None = None):
            return None

    stub = SilverStubPipeline()
    job = JobSilverData(payload=payload, run=stub)
    job.run_jobs()

    assert len(stub.written_paths) == 0


def test_pipeline_forwards_reader_options_to_reader() -> None:
    class FakeReader:
        def __init__(self, spark):
            self.spark = spark

        def read(self, path: str, **options):
            return {"path": path, "options": options}

    fake_reader = FakeReader(None)

    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "sample.csv"
        csv_path.write_text("Data Medicao;valor\n2025-01-01;10\n", encoding="utf-8")

        with patch(
            "pipeline.DataReaderFactory.create", return_value=fake_reader
        ) as create_mock:
            pipeline = Pipeline.__new__(Pipeline)
            pipeline.spark = None
            pipeline.tax_series = None

            result = pipeline.create_dataframe(
                str(csv_path), options={"skipRows": 10, "delimiter": ";"}
            )

    assert create_mock.called
    assert result["options"] == {"skipRows": 10, "delimiter": ";"}


def test_normalize_column_names_for_generic_schema() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    try:
        df = spark.createDataFrame(
            [("2025-01-01", "0", "28,7", "22,4", "17,3", "65", "38")],
            ["_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6"],
        )
        job = JobSilverData(payload={}, run=StubPipeline())
        normalized = job._normalize_column_names(
            df,
            [
                "data_medicao",
                "precipitacao_total_mm",
                "temp_max_c",
                "temp_media_c",
                "temp_min_c",
                "umidade_rel_media",
                "umidade_rel_minima",
            ],
        )
        assert normalized.columns == [
            "data_medicao",
            "precipitacao_total_mm",
            "temp_max_c",
            "temp_media_c",
            "temp_min_c",
            "umidade_rel_media",
            "umidade_rel_minima",
        ]
    finally:
        spark.stop()


def test_extract_header_metadata_from_csv_file() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        raw_path = Path(tmp_dir) / "sample.csv"
        raw_path.write_text(
            "Nome: PATROCINIO\n"
            "Codigo Estacao: A523\n"
            "Data Medicao;valor\n"
            "2025-01-01;10\n",
            encoding="utf-8",
        )

        job = JobSilverData(payload={}, run=StubPipeline())
        metadata_path = job._extract_header_metadata(raw_path, output_dir=Path(tmp_dir))

        assert metadata_path.exists()
        assert json.loads(metadata_path.read_text(encoding="utf-8")) == {
            "Nome": "PATROCINIO",
            "Codigo Estacao": "A523",
        }


def test_prepare_file_csv_inmet_renames_columns_and_writes_cleaned_file() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_path = Path(tmp_dir) / "sample.csv"
            raw_path.write_text(
                "Nome: FRANCA\n"
                "Codigo Estacao: A708\n"
                "Data Medicao;PRECIPITACAO TOTAL, DIARIO (AUT)(mm);TEMPERATURA MAXIMA, DIARIA (AUT)(°C);TEMPERATURA MEDIA, DIARIA (AUT)(°C);TEMPERATURA MINIMA, DIARIA (AUT)(°C);UMIDADE RELATIVA DO AR, MEDIA DIARIA (AUT)(%);UMIDADE RELATIVA DO AR, MINIMA DIARIA (AUT)(%)\n"
                "2025-01-01;1;25;20;15;80;70\n",
                encoding="utf-8",
            )

            class SparkStubPipeline:
                def __init__(self, spark):
                    self.spark = spark

            job = JobRawData(
                payload={"inmet_franca": {"path_raw": str(raw_path)}},
                run=SparkStubPipeline(spark),
            )
            result = job.prepare_file_csv_inmet("inmet_franca")

            cleaned_path = Path(result["cleaned_path"])
            assert cleaned_path.exists()
            assert cleaned_path.read_text(encoding="utf-8").splitlines()[0] == (
                "data_medicao;precipitacao_total_mm;temp_max_c;temp_media_c;temp_min_c;umidade_rel_media;umidade_rel_minima"
            )
    finally:
        spark.stop()
