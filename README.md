# Project Description

This project is to predict taxi fare based on chicago taxi fare data and Light GBM model.  This model is trained in AWS EMR.  An API is included to access the trained model.  API code is conainarized in docker and can be deployed in any container orchestration service.  In this case, this was deployed into AWS ECS. This is an individual project and does not reflect any promises from Chicago Taxis# chicago_taxi_ss

## Application Access

Applicaiton is deployed in AWS ECS.  UI can be access via the below url, which in turn calls the API
http://3.86.180.6:80

API can be directly access with the below details

URL - http://3.86.180.6:80/v1/predict/regression
Method - Post

Raw sample request:
{"pickup_latitude":[41.980264315],"pickup_longitude":[-87.913624596],"dropoff_latitude":[42.001571027],"dropoff_longitude":[-87.695012589],"company":["Blue Diamond"],"trip_start_timestamp":["2019-03-05 00:15:00"]} 

