from pathlib import Path
import pathlib
import sys
PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent
#PACKAGE_ROOT = Path('regression_model')

#--py-files s3://chicagotaxiproject/regression_model.zip

#s3://chicagotaxiproject/regression_model/trained_models/linear_regression
#s3://chicagotaxiproject/regression_model/VERSION

# Data file
#DATA_FILE = PACKAGE_ROOT / 'dataset/data.csv'
DATA_FILE = 'sample'

#Model Version
VERSION = PACKAGE_ROOT / 'VERSION'
# VERSION = sys.argv[2]

# Name of the pipeline
# PIPELINE_NAME = PACKAGE_ROOT / 'trained_models/linear_regression'
PIPELINE_NAME = sys.argv[1]

# Features
FEATURES = ['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude', 'company',
            'trip_start_timestamp']

# variables to log transform
NUMERICAL_VARS = ['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']

# Derived variables
DERIVED_VARS = ['Trip_Day_Of_Week', 'Trip_Year', 'Trip_Month', 'Trip_Hour']

# categorical variables to encode
CATEGORICAL_VAR = 'company'

TARGET = 'fare'

DATE_COL = 'trip_start_timestamp_dt'


