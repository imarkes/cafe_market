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


DEFAULT_NULL_VALUES = (
    "",
    " ",
    "null",
    "NULL",
    "None",
    "none",
    "NaN",
    "nan",
    "N/A",
    "n/a",
    "-",
)


def _clean_null_values(
    df: DataFrame,
    columns: list[str] | None = None,
    null_values: tuple[str, ...] = DEFAULT_NULL_VALUES,
) -> DataFrame:
    """
    Padroniza representações textuais de valores nulos para NULL.

    Parameters
    ----------
    df
        DataFrame Spark.

    columns
        Lista de colunas a serem tratadas.
        Se None, todas as colunas serão processadas.

    null_values
        Valores que devem ser convertidos para NULL.

    Returns
    -------
    DataFrame
    """

    columns = columns or df.columns

    for column in columns:

        df = df.withColumn(
            column,
            F.when(
                F.lower(F.trim(F.col(column).cast("string"))).isin(
                    *[v.lower().strip() for v in null_values]
                ),
                F.lit(None),
            ).otherwise(F.col(column)),
        )

    return df
