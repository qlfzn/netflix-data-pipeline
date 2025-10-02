from .spark_service import SparkService
from utils import Logger
from pyspark.sql import DataFrame, functions
import os

class SilverService:
    def __init__(self) -> None:
        self.spark = SparkService().get_spark()
        self.logger = Logger(__name__).get_logger()
    
    def check_path_exists(self, path: str) -> bool:
        if not os.path.exists(path):
            self.logger.error(f"Path not found: {path}")
            return False
        return True
    
    def read_from_bronze(self, path: str) -> DataFrame:
        df = self.spark.read.parquet(path)
        row_count = df.count()
        self.logger.info(f"Read parquet from {path} successfully. Row count: {row_count}")
        return df
    
    def deduplicate(self, df: DataFrame, columns: list[str]) -> DataFrame:
        return df.dropDuplicates(subset=columns)

    def drop_null_values(self, df: DataFrame, columns: list[str]) -> DataFrame:
        return df.dropna(how="all", subset=columns)
    
    def log_processed_time(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("processed_at", functions.current_timestamp())
        return df
    
    def quarantine_rows()