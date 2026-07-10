import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "Culltivo Pipeline") -> SparkSession:
    """Create and return a reusable Spark session for the pipeline."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.13:3.5.1_0.20.4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
    )

    logger.info("Creating Spark session")
    return builder.getOrCreate()
