from pathlib import Path

from app.src.database.conector import duckdb_connector

SQL_PATH = Path("./models")


def execute_sql_file(
    filename: str,
    **params,
):
    """
    Executa um arquivo SQL utilizando parâmetros.

    Example
    -------

    execute_sql_file(
        "fact_preco_cafe.sql",
        arabica="...",
        robusta="..."
    )
    """

    sql_file = SQL_PATH / filename

    if not sql_file.exists():
        raise FileNotFoundError(sql_file)

    sql = sql_file.read_text(
        encoding="utf8"
    )

    sql = sql.format(**params)

    with duckdb_connector() as con:
        con.execute(sql)

if __name__ == '__main__':
    ...
#     execute_sql_file(
#     "fact_preco_cafe.sql",
#     arabica="../storage/silver/cepea/arabica",
#     robusta="../storage/silver/cepea/robusta",
# )
