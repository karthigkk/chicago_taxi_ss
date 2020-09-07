from regression_model.preprocessing.preprocessors import Preprocessdataframe
from mmlspark.featurize import AssembleFeatures
from mmlspark.lightgbm import LightGBMRegressor
from mmlspark.stages import UDFTransformer, DropColumns
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
# from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
# from pyspark.ml.regression import LinearRegression
from regression_model.config import config


class Buildpipeline:
    def __init__(self):
        pass

    @staticmethod
    def build_stages() -> list:
        get_week_day = udf(lambda z: z.weekday(), IntegerType())
        get_year = udf(lambda z: z.year, IntegerType())
        get_month = udf(lambda z: z.month, IntegerType())
        get_hour = udf(lambda z: z.hour, IntegerType())
        #preprocess = Preprocessdataframe()

        # build Pipeline
        # stringIndexer = StringIndexer(inputCol=config.CATEGORICAL_VAR, outputCol='Comp_Index')
        # encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=['Comp_classVec'])
        # inputfeatures = config.NUMERICAL_VARS + config.DERIVED_VARS + ['Comp_classVec']
        # assembler = VectorAssembler(inputCols=inputfeatures, outputCol='features')
        # lr = LinearRegression(featuresCol='features', labelCol=config.TARGET, maxIter=2, regParam=0.3, elasticNetParam=0.8)
        # STAGES = [preprocess, stringIndexer, encoder, assembler, lr]
        assembler = AssembleFeatures(
            columnsToFeaturize=config.NUMERICAL_VARS + config.DERIVED_VARS + config.CATEGORICAL_VAR,
            numberOfFeatures=1)
        lgbm = LightGBMRegressor(learningRate=0.001, numIterations=50, featuresCol='features', labelCol=config.TARGET)
        STAGES = [UDFTransformer(inputCol='trip_start_timestamp', outputCol='Trip_Day_Of_Week', udf=get_week_day),
                  UDFTransformer(inputCol='trip_start_timestamp', outputCol='Trip_Year', udf=get_year),
                  UDFTransformer(inputCol='trip_start_timestamp', outputCol='Trip_Month', udf=get_month),
                  UDFTransformer(inputCol='trip_start_timestamp', outputCol='Trip_Hour', udf=get_hour),
                  DropColumns(cols=['trip_start_timestamp']), assembler, lgbm]
        return STAGES
