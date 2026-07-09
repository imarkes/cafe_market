import logging

from pipeline import Pipeline
from pyspark.sql import DataFrame
from schemas.schema import SCHEMAS
from pyspark.sql import functions as F
from transformations import (_clean_date_column, _clean_null_values,
_clean_numeric_column)

logger = logging.getLogger(__name__)

class JobSilverData:
    """Padroniza e limpa os dados vindos da camada bronze."""

    def __init__(self, payload: dict, run: Pipeline) -> None:
        self.payload = payload
        self.run = run

    def run_all(self) -> None:
        """
        Executa todos os jobs definidos no payload.
        """
        logger.info("Starting Silver Process...")

        for source_name in self.payload:
            self.process_source(source_name)

        logger.info("Silver Process finished.")

    def process_data(self, source_name) -> DataFrame:

        input_path = self.payload[source_name]["path_bronze"]
        output_path = self.payload[source_name]["path_silver"]

        df = self.run.create_dataframe(
            path=input_path, options={"header": "true", "inferSchema": "true"}
        )
        schema = SCHEMAS.get(source_name)
        if schema is None:
            raise ValueError(f"Schema not found for '{source_name}'.")

        for column, dtype in schema.items():
            if dtype == "double":
                df = _clean_numeric_column(
                    df,
                    column,
                )
            elif dtype == "date":
                df = _clean_date_column(
                    df,
                    column,
                )
            else:
                df = df.withColumn(
                    column,
                    F.col(column).cast(dtype),
                )
        df = _clean_null_values(df, list(schema.keys()))
        df = df.select(*schema.keys())

        self.run.write_parquet(
            df=df,
            output_path=output_path,
            partitions=self.payload[source_name]["partitions"],
        )

        logger.info(
            "Silver dataset successfully created: %s",
            source_name,
        )
        df.printSchema()
        print("*-" * 50)
        return df
