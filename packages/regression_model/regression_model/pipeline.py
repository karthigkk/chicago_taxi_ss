from regression_model.preprocessing.preprocessors import Preprocessdataframe
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from regression_model.config import config


class Buildpipeline:
    def __init__(self):
        pass

    @staticmethod
    def build_stages() -> list:
        preprocess = Preprocessdataframe()

        # build Pipeline
        stringIndexer = StringIndexer(inputCol=config.CATEGORICAL_VAR, outputCol='Comp_Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=['Comp_classVec'])
        inputfeatures = config.NUMERICAL_VARS + config.DERIVED_VARS + ['Comp_classVec']
        assembler = VectorAssembler(inputCols=inputfeatures, outputCol='features')
        lr = LinearRegression(featuresCol='features', labelCol=config.TARGET, maxIter=2, regParam=0.3,
                              elasticNetParam=0.8)
        STAGES = [preprocess, stringIndexer, encoder, assembler, lr]
        return STAGES
