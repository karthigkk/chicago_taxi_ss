from regression_model.config import config
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_inputs(input_data: DataFrame) -> str:
    # check for numerical variables with NA not seen during training
    for colum in config.FEATURES:
        if input_data.where(F.col(colum).isNull()).count() > 0:
            return 'Column ' + colum + ' cannot have null values'

    return '0'
