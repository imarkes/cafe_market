import logging

from pipeline import Pipeline
from pyspark.sql import DataFrame
from schemas.schema import SCHEMAS
from pyspark.sql import functions as F
from transformations import _clean_date_column, _clean_numeric_column,_select_columns

logger = logging.getLogger(__name__)

class JobGoldData:
    def __init__(self, payload: dict, run: Pipeline) -> None:
        self.payload = payload
        self.run = run
