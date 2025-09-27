from .spark_service import SparkService
from pyspark.sql import DataFrame

class BronzeService:
    def __init__(self) -> None:
        self.spark = SparkService().get_spark()

    def read_file(self, path: str) -> DataFrame:
        try: 
            df = self.spark.read \
            .option("inferSchema", True) \
            .csv(path=path)

            print(f"Successfully read {path}. {df.count()} rows")
        except Exception as e:
            raise e

        return df

    def write_to_processed(self, dataframe: DataFrame, dest_path: str):
        """
        Write extracted data to processed folder/bucket in parquet format
        """
        try:
            dataframe.write.parquet(path=dest_path)
        except Exception as e:
            raise e