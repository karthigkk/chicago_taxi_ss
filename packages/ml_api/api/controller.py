from flask import Blueprint, request
import pandas as pd
from regression_model.predict import Predict
from api.config import get_logger
from pyspark.sql import SparkSession
from flask import jsonify

_logger = get_logger(logger_name=__name__)

ml_api_blueprint = Blueprint('prediction_app', __name__)


@ml_api_blueprint.route('/health', methods = ['GET'])
def health():
    if request.method == 'GET':
        _logger.info('health status OK')
        return 'ok'

@ml_api_blueprint.route('/v1/predict/regression', methods=['POST'])
def predict():
    if request.method == 'POST':
        test_data = request.get_json()
        spark = SparkSession.builder.appName('Chicagotaxi').getOrCreate()
        df = pd.DataFrame.from_dict(test_data)
        dataset = spark.createDataFrame(df)
        pred = Predict(dataset)
        predictions = pred.make_prediction()
        print(predictions.select(predictions['prediction']).collect()[0][0])
        d = {'Predicted Fare': predictions.select(predictions['prediction']).collect()[0][0], 'status': 'OK'}
        return jsonify(d)
