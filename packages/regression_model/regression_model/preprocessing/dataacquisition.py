from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from regression_model.config import config
import logging

_logger = logging.getLogger(__name__)

class Dataacquisition:
    def __init__(self):
        # Create spark context
        spark = SparkSession.builder.appName('Chicagotaxi').getOrCreate()
        self.spark = spark
        _logger.info(f"Created Spark Session")

    # Get data from csv file placed under project folder
    def getdatafromcsv(self) -> DataFrame:
        df = self.spark.read.option("inferSchema", "true").csv(str(config.DATA_FILE), header='true')
        _logger.info(f"Loaded data from csv file")
        return df

    # Get sample data from bigquery
    def sample_data(self) -> DataFrame:
        df = self.spark.read.format('bigquery') \
            .option("credentialsFile", 'key.json') \
            .option('parentproject', 'zeta-treat-276509') \
            .option('project', 'zeta-treat-276509') \
            .option('table', 'bigquery-public-data:chicago_taxi_trips.taxi_trips') \
            .option("filter",
                    "EXTRACT(MONTH from trip_start_timestamp) = 3 and "
                    "EXTRACT(DAYOFWEEK from trip_start_timestamp) = 3 and "
                    "EXTRACT(YEAR from trip_start_timestamp) = 2019") \
            .load()
        _logger.info(f"Loaded sample data from bigquery")
        return df

    # Get full set of latest data from bigquery
    def full_data(self) -> DataFrame:
        print('retrieving data from bigquery-public-data:chicago_taxi_trips.taxi_trips')
        df = self.spark.read.format('bigquery') \
            .option("credentialsFile", 'key.json') \
            .option('parentproject', 'zeta-treat-276509') \
            .option('project', 'zeta-treat-276509') \
            .option('table', 'bigquery-public-data:chicago_taxi_trips.taxi_trips') \
            .load()
        _logger.info(f"Loaded full set data from bigquery")
        return df
