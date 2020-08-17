from regression_model.preprocessing.dataacquisition import Dataacquisition
from regression_model.preprocessing.preprocessors import Dataclean
from regression_model.predict import Predict
from pyspark import SparkConf, SparkContext

def test_validate_prediction():
    conf = SparkConf().setAppName('appName').setMaster('local')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    da = Dataacquisition()
    df = da.getdatafromcsv()
    dc = Dataclean(df)
    df = dc.clean()
    df = df.limit(1)
    predict = Predict(df)
    Prediction = predict.make_prediction()
    pred_value = Prediction.select('prediction').collect()[0][0]

    assert pred_value is not None
    assert isinstance(pred_value, float)