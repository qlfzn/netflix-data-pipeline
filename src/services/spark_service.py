from pyspark.sql import SparkSession
from pyspark import SparkConf

class SparkService:
    def __init__(self) -> None:
        conf = SparkConf()

        # define spark configs
        conf.set("spark.executor.memory", "4g")
        conf.set("spark.executor.cores", "4")
        conf.set("spark.executor.instances", "4")
        conf.set("spark.sql.shuffle.partitions", "64")

        try:
            self.spark = SparkSession.builder \
                        .appName("Netflix Data Pipeline") \
                        .config(conf=conf) \
                        .getOrCreate()
        except Exception as e:
            raise RuntimeError(f"Failed to initialise session: {e}")
    
    def get_spark(self) -> SparkSession:
        return self.spark