from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
from duckdb import DuckDBPyConnection

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@contextmanager
def duckdb_connector(db_name: str = "culttivo") -> Iterator[DuckDBPyConnection]:

    database_name = db_name if db_name.endswith(".duckdb") else f"{db_name}.duckdb"

    database_path = PROJECT_ROOT / database_name
    database_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = duckdb.connect(str(database_path))

    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
