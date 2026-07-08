import logging

from pipeline import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

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
        print(input_path)

        df = self.run.create_dataframe(
            path=input_path, options={"header": "true", "inferSchema": "true"}
        )
        date_colum = self.payload[source_name]['partitions'][0]
        df = self._clean_date_column(df, column=date_colum)
        print(df.columns)
        # df = self._clean_numeric_column(df,)
        # df.show(5)

        # self.run.write_parquet(
        #     df=df,
        #     output_path=self.payload[source_name] / "path_bronze",
        #     partitions=source_name.get("partitions"),
        # )

        # logger.info(
        #     "Silver dataset successfully created: %s",
        #     source_name,
        # )
        # df.printSchema()
        # print("*-" * 50)
        return df

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
