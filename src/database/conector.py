from pathlib import Path

import duckdb


def duckdb_connector(db_name:str='culttivo'):
    DB_PATH = f"../storage/gold/{db_name}.duckdb"

    Path("../storage/gold").mkdir(parents=True, exist_ok=True)

    return duckdb.connect(DB_PATH)
