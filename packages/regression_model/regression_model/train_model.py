from regression_model.config import config
from pyspark.ml import Pipeline
from regression_model.pipeline import Buildpipeline
from pyspark.sql import DataFrame

from regression_model import __version__ as _version

import logging

_logger = logging.getLogger('regression_model')


class Trainmodel:
    def __init__(self, train_set: DataFrame):
        self.train_set = train_set

    def run_training(self) -> DataFrame:
        """Train the model."""
        _logger.info(f"Building pipeline")
        piplin = Buildpipeline()
        STAGES = piplin.build_stages()
        train_pip = Pipeline(stages=STAGES)
        result = train_pip.fit(self.train_set)
        result.write().overwrite().save(str(config.PIPELINE_NAME) + _version.decode("utf-8"))
        _logger.info(f"Training complete.  Saved pipelinemodel")
        return result
