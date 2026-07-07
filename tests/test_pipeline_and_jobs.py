import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from jobs.job_raw import JobRawData
from readers import CsvDataReader, DataReaderFactory, ExcelDataReader, JsonDataReader


class StubPipeline:
    def __init__(self) -> None:
        self.created_paths = []
        self.written_paths = []

    def create_dataframe(self, path: str, sheet_name: str | None = None):
        self.created_paths.append((path, sheet_name))
        return {"path": path}

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
    job = JobRawData(payload=payload, run=stub)
    job.run_jobs()

    assert len(stub.created_paths) == 6
    assert len(stub.written_paths) == 6
