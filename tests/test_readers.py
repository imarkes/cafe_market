import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from readers import DataReaderFactory, ExcelDataReader, JsonDataReader


def test_factory_returns_excel_reader_for_xls_extension():
    reader = DataReaderFactory.create("/tmp/example.xls")
    assert isinstance(reader, ExcelDataReader)


def test_factory_returns_json_reader_for_json_extension():
    reader = DataReaderFactory.create("/tmp/example.json")
    assert isinstance(reader, JsonDataReader)
