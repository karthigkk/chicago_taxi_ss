from flask import Blueprint, request
import pandas as pd
from api.config import get_logger

from flask import jsonify

_logger = get_logger(logger_name=__name__)

ml_api_blueprint = Blueprint('prediction_app', __name__)

from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .appName('Chicagotaxi') \
  .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta,com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc2') \
  .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
  .getOrCreate()

from regression_model.predict import Predict

@ml_api_blueprint.route('/health', methods = ['GET'])
def health():
    if request.method == 'GET':
        _logger.info('health status OK')
        return 'ok'

@ml_api_blueprint.route('/v1/predict/regression', methods=['POST'])
def predict():
    if request.method == 'POST':
        test_data = request.get_json()
        df = pd.DataFrame.from_dict(test_data)
        df['trip_start_timestamp'] = pd.to_datetime(df['trip_start_timestamp'], format='%Y%m%d %H:%M:%S')
        dataset = spark.createDataFrame(df)
        pred = Predict(dataset)
        predictions = pred.make_prediction()
        _logger.info(f"Predicted Fare: {predictions.select(predictions['prediction']).collect()[0][0]}")
        d = {'Predicted Fare': predictions.select(predictions['prediction']).collect()[0][0], 'status': 'OK'}
        return jsonify(d)
