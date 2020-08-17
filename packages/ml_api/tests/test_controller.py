from pyspark import SparkConf, SparkContext

def test_health_endpoint_returns_200(flask_test_client):
    # When
    response = flask_test_client.get('/health')

    # Then
    assert response.status_code == 200


def test_prediction_endpoint_returns_prediction(flask_test_client):
    # Given
    # Load the test data from the regression_model package
    # This is important as it makes it harder for the test
    # data versions to get confused by not spreading it
    # across packages.
    conf = SparkConf().setAppName('appName').setMaster('local')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    test_data = {'pickup_latitude':[41.980264315],
                 'pickup_longitude':[-87.913624596],
                 'dropoff_latitude':[42.001571027],
                 'dropoff_longitude':[-87.695012589],
                 'company':['City Service'],
                 'trip_start_timestamp':['2019-03-05 00:15:00 UTC']}
    #post_json = json.dump(test_data)
    #print(type(post_json))
    # When
    response = flask_test_client.post('/v1/predict/regression',
                                      json=test_data)

    # Then
    assert response.status_code == 200
    print(response)
    #prediction = response['prediction']
