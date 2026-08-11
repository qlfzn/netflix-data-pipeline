from logging import Logger

from pyspark.sql import DataFrame, SparkSession


class BronzeService:
    def __init__(self, spark: SparkSession, logger: Logger) -> None:
        self.spark = spark
        self.logger = logger

    def read_file(self, path: str) -> DataFrame:
        """
        Read input files and returns DataFrame
        """
        df = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(path=path)
        )

        self.logger.info(f"Successfully read {path}. {df.count()} rows")

        return df

    def write_to_processed(self, dataframe: DataFrame, dest_path: str):
        """
        Write extracted data to processed folder/bucket in parquet format
        """
        dataframe.write.parquet(path=dest_path)
        self.logger.info("Successfully write to parquet")
