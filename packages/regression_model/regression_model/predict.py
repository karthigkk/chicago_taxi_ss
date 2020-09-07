from regression_model.config import config
from pyspark.ml import PipelineModel
from regression_model.preprocessing import validation
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from regression_model.errors import InvalidErrorMessage
from pyspark.mllib.evaluation import RegressionMetrics
from mmlspark.featurize import AssembleFeatures # This is to load model

from regression_model import __version__ as _version

import logging

_logger = logging.getLogger('regression_model')


class Predict:
    def __init__(self, input_data: DataFrame):
        self.input_data = input_data

    def make_prediction(self) -> DataFrame:
        # Make predictions
        # fare_pipe = PipelineModel.load(str(config.PIPELINE_NAME) + _version.decode("utf-8"))
        fare_pipe = PipelineModel.load(str(config.PIPELINE_NAME) + _version)
        _logger.info(f"Loaded Model for prediction")
        # fare_pipe = PipelineModel.load(str(config.PIPELINE_NAME) + _version)
        valid = validation.validate_inputs(self.input_data)
        _logger.info(f"Data Validation: {valid}")
        if valid == '0':
            results = fare_pipe.transform(self.input_data)
            _logger.info(f"Prediction Complete")
        else:
            results = self.input_data.withColumn("prediciton", F.lit(valid))
            _logger.warning(f"Missing colum values, prediction failed")
            raise InvalidErrorMessage(
                _logger.warning(f"Variables contain null values, "
                f"can't predict fare with null values for any variables")
            )

        return results

    def evaluate_results(self, results: DataFrame) -> (float, float, float):
        # Evaluate metrics
        valuesandpreds = results.rdd.map(lambda p: (float(p.prediction), p.fare))
        valuesandpreds.take(5)
        metric = RegressionMetrics(valuesandpreds)
        _logger.info(f"Test Mean Square Error: {metric.meanSquaredError}")
        _logger.info(f"Test Root Mean Square Error: {metric.rootMeanSquaredError}")
        _logger.info(f"Test R2: {metric.r2}")
        return metric.meanSquaredError, metric.rootMeanSquaredError, metric.r2