import logging

from database.sql_executor import execute_sql_file

logger = logging.getLogger(__name__)
class JobGoldData:

    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def create_datawarehouse(self):

        TABLES = [
            "fact_preco_cafe.sql",
            "fact_indicadores.sql",
            "fact_clima.sql",
            "dim_calendario.sql",
            "market_analysis.sql",
        ]
        context = {
            source_name: source["path_silver"]
            for source_name, source in self.payload.items()
        }

        for table in TABLES:
            logger.info("Creating table: %s", table)

            execute_sql_file(
                filename=table,
                **context,
            )
