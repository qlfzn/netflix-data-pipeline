from .spark_service import SparkService
from utils import Logger
from pyspark.sql import DataFrame

class BronzeService:
    def __init__(self) -> None:
        self.spark = SparkService().get_spark()
        self.logger = Logger(__name__).get_logger()

    def read_file(self, path: str) -> DataFrame:
        """
        Read input files and returns DataFrame
        """
        try: 
            df = self.spark.read \
            .option("inferSchema", True) \
            .csv(path=path)

            self.logger.info(f"Successfully read {path}. {df.count()} rows")
        except Exception as e:
            self.logger.error(f"Failed to read file: {e}")

        return df

    def write_to_processed(self, dataframe: DataFrame, dest_path: str):
        """
        Write extracted data to processed folder/bucket in parquet format
        """
        try:
            dataframe.write.parquet(path=dest_path)
            self.logger.info("Successfully write to parquet")
        except Exception as e:
            self.logger.error(f"Failed to write to parquet: {e}")