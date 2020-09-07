from regression_model.preprocessing.dataacquisition import Dataacquisition
from regression_model.preprocessing.preprocessors import Dataclean
from regression_model.predict import Predict
from regression_model.train_model import Trainmodel
from regression_model.config import config


if __name__ == '__main__':
    # read data
    da = Dataacquisition()
    if config.DATA_FILE == 'full':
        df = da.full_data()
    elif config.DATA_FILE == 'sample':
        df = da.sample_data()
    else:
        df = da.getdatafromcsv()

    # clean data of rows with null values and remove entries that have fare less than minimum
    dc = Dataclean(df)
    df = dc.clean()

    # splitting dataset into train and test set
    (train, test) = df.randomSplit([0.999, 0.001], seed=42)  # we are setting the seed here

    # Train Model
    model = Trainmodel(train)
    result = model.run_training()

    # Run predictions on test set
    pred = Predict(test)
    Prediction = pred.make_prediction()

    # evaluate the model
    mse, rmse, r2 = pred.evaluate_results(Prediction)

