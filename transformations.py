import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def _clean_numeric_column(df: DataFrame, column: list[str]) -> DataFrame:
    """Converte coluna para numérica e trata valores inválidos."""
    # for column in list_column:

    df = (
            df.withColumn(
                column,
                F.regexp_replace(F.col(column).cast("string"), r"[^0-9,.-]", ""),
            )
            .withColumn(column, F.regexp_replace(F.col(column), ",", "."))
            .withColumn(
                column,
                F.when(F.col(column) == "", None).otherwise(
                    F.col(column).cast("double")
                ),
            )
        )
    return df

def _clean_date_column(
    df: DataFrame, column: str, format_str: str = "dd/MM/yyyy"
) -> DataFrame:
    """Converte coluna para data e trata valores inválidos."""
    # return df.withColumn(
    #     column,
    #     F.when(
    #         F.col(column).isNull() | (F.trim(F.col(column)) == ""), None
    #     ).otherwise(F.to_date(F.col(column).cast("string"), format_str)),
    # )
    return (
    df.withColumn(

        column,

        F.coalesce(

            F.try_to_timestamp(
                F.col(column),
                F.lit("yyyy-MM-dd"),
            ),

            F.try_to_timestamp(
                F.col(column),
                F.lit("dd/MM/yyyy"),
            ),

            F.try_to_timestamp(
                F.col(column),
                F.lit("MM/dd/yyyy"),
            ),

        ).cast("date")

    )
)

def _select_columns(df:DataFrame, column):
    return df.select(column)
