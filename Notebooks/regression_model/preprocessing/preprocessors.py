from pyspark.ml import Transformer
from pyspark.sql.functions import dayofweek, year, month, hour
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


# Selecting features and extracting date fields
class Preprocessdataframe(Transformer, DefaultParamsReadable, DefaultParamsWritable):

    def __init__(self):
        super(Preprocessdataframe, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:

        df = df.withColumn('trip_start_timestamp_dt',F.to_timestamp(F.unix_timestamp('trip_start_timestamp', 'yyy-MM-dd HH:mm:ss Z').cast('timestamp')))
        df = df.withColumn('trip_start_timestamp_cst', F.from_utc_timestamp('trip_start_timestamp_dt', 'CST'))
        df = df.withColumn('Trip_Day_Of_Week', dayofweek(df.trip_start_timestamp))
        df = df.withColumn('Trip_Year', year(df.trip_start_timestamp))
        df = df.withColumn('Trip_Month', month(df.trip_start_timestamp))
        df = df.withColumn('Trip_Hour', hour(df.trip_start_timestamp_cst))
        return df


# Remove rows with null values and fare amount less than minimum
class Dataclean:
    def __init__(self, df: DataFrame):
        self.df = df

    def clean(self) -> DataFrame:
        self.df = self.df.dropna()
        self.df = self.df.filter(self.df.fare >= 2.70)
        return self.df
