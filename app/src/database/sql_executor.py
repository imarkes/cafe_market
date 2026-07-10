from __future__ import annotations

from pathlib import Path
from typing import Any

from .conector import duckdb_connector


APP_DIR = Path(__file__).resolve().parents[2]
SQL_DIR = APP_DIR / "models"


def execute_sql_file(
    filename: str,
    **params: Any,
) -> None:
    """
    Lê, parametriza e executa um arquivo SQL no DuckDB.

    Parameters
    ----------
    filename
        Nome do arquivo SQL presente no diretório ``app/models``.

    **params
        Parâmetros utilizados para substituir placeholders do SQL
        por meio de ``str.format``.

    Raises
    ------
    FileNotFoundError
        Caso o arquivo SQL não seja encontrado.

    ValueError
        Caso o caminho informado não tenha extensão ``.sql``.
    """

    sql_file = SQL_DIR / filename

    if sql_file.suffix.lower() != ".sql":
        raise ValueError(
            f"O arquivo precisa possuir extensão .sql: {sql_file}"
        )

    if not sql_file.is_file():
        raise FileNotFoundError(
            "Arquivo SQL não encontrado.\n"
            f"Caminho procurado: {sql_file}\n"
            f"Diretório SQL: {SQL_DIR}"
        )

    sql = sql_file.read_text(encoding="utf-8")

    try:
        rendered_sql = sql.format(**params)
    except KeyError as error:
        missing_parameter = error.args[0]

        raise KeyError(
            f"O parâmetro '{missing_parameter}' usado em "
            f"'{filename}' não foi informado."
        ) from error

    with duckdb_connector() as connection:
        connection.execute(rendered_sql)

if __name__ == '__main__':
    ...
#     execute_sql_file(
#     "fact_preco_cafe.sql",
#     arabica="../storage/silver/cepea/arabica",
#     robusta="../storage/silver/cepea/robusta",
# )
