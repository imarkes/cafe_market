from pathlib import Path


class BaseReader:

    format_name = ""

    def __init__(self, spark):
        self.spark = spark

    def read(self, path, **options):

        reader = self.spark.read.format(self.format_name)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(path)

class ParquetReader(BaseReader):
    format_name = "parquet"
    def read(self, path, **options):
        options.setdefault("header", True)
        options.setdefault("inferSchema", True)
        return super().read(path, **options)

class ExcelReader(BaseReader):

    format_name = "com.crealytics.spark.excel"

    def read(self, path, **options):

        options.setdefault("header", True)
        options.setdefault("inferSchema", True)
        options.setdefault("dataAddress", "'Sheet1'!A1")

        return super().read(path, **options)


class CsvReader(BaseReader):

    format_name = "csv"

    def read(self, path, **options):

        options.setdefault("header", True)
        options.setdefault("delimiter", ";")
        options.setdefault("nullValue", "null")

        return super().read(path, **options)


class JsonReader(BaseReader):

    format_name = "json"

    def read(self, path, **options):

        options.setdefault("inferSchema", True)
        options.setdefault("multiline", True)

        return super().read(path, **options)


_READERS = {
    ".csv": CsvReader,
    ".json": JsonReader,
    ".xls": ExcelReader,
    ".xlsx": ExcelReader,
    ".parquet":ParquetReader,
}


class DataReaderFactory:

    @classmethod
    def create(cls, path, spark):

        suffix = Path(path).suffix.lower()

        try:

            return _READERS[suffix](spark)

        except KeyError:

            raise ValueError(...)
