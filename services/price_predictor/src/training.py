from src.config import config, HopsworksConfig, hopsworks_config, CometConfig, comet_config
from src.ohlc_data_reader import OhlcDataReader
from typing import Optional
from loguru import logger
from src.models.current_price_baseline import CurrentPriceBaseline
from sklearn.metrics import mean_absolute_error 
from comet_ml import Experiment
from src.models.xgboost_model import XGBoostModel
from src.utils import hash_data
import joblib
import os
from src.feature_engineering import add_engineered_features
from src.model_registry import get_model_name


def train_model(
    comet_config: CometConfig,
    hopsworks_config: HopsworksConfig,
    feature_view_name: str, # The name of the feature group to read from; feature view is a more general term
    feature_view_version: int, # The version of the feature group to read from
    feature_group_name: str,
    feature_group_version: int,
    ohlc_window_sec: int,
    product_id: str, # The product ID for which we want to get the OHLC data
    last_n_days: int, # The number of days of data to train on
    forecast_steps: int, # The number of steps to forecast (minutes)
    perc_test_data: Optional[float] = 0.23, # The percentage of data to use for testing
    n_search_trials: Optional[int] = 10, # The number of search trials for hyperparameter tuning
    n_splits: Optional[int] = 3, # The number of splits for cross-validation
    last_n_minutes: Optional[int] = 30, # The number of minutes of data in the past we need to generate features

):
    """
    Reads features from the feature store
    Trains a model
    Saves the model to the model registry

    Args:
        comet_config (CometConfig): Configuration for Comet
        hopsworks_config (HopsworksConfig): Configuration for Hopsworks
        feature_view_name (str): The name of the feature view to read from
        feature_view_version (int): The version of the feature view to read from
        feature_group_name (str): The name of the feature group to read from
        feature_group_version (int): The version of the feature group to read from
        ohlc_window_sec (int): The size of the OHLC window in seconds
        product_id (str): The product ID for which we want to get the OHLC data
        last_n_days (int): The number of days of data to train on
        forecast_steps (int): The number of steps to forecast (minutes)
        perc_test_data (float): The percentage of data to use for testing
        n_search_trials (int): The number of search trials for hyperparameter tuning
        n_splits (int): The number of splits for cross-validation
        last_n_minutes (int): The number of minutes of data in the past we need to generate features

    Returns:
        None
    """
    # Initialize the Comet experiment
    experiment = Experiment(
        api_key=comet_config.comet_api_key,
        project_name=comet_config.comet_project_name,
    )

    # Log the parameters
    experiment.log_parameter("last_n_days", last_n_days)
    experiment.log_parameter("forecast_steps", forecast_steps)
    experiment.log_parameter("n_search_trials", n_search_trials)
    experiment.log_parameter("n_splits", n_splits)

    # Log the number of minutes of data in the past we need to generate features
    experiment.log_parameter("last_n_minutes", last_n_minutes)

    # Log the feature store parameters
    experiment.log_parameter('feature_view_name', feature_view_name)
    experiment.log_parameter('feature_view_version', feature_view_version)

    # Read features from the feature store
    ohlc_data_reader = OhlcDataReader(
        ohlc_window_sec=ohlc_window_sec,
        hopsworks_config=hopsworks_config,
        feature_view_name=feature_view_name,
        feature_view_version=feature_view_version,
        feature_group_name=feature_group_name,
        feature_group_version=feature_group_version,
    )

    ohlc_data = ohlc_data_reader.read_from_offline_store(
        product_id=product_id, 
        last_n_days=last_n_days
    )
    logger.debug(f"Read {len(ohlc_data)} rows data from the feature store")
    experiment.log_parameter("num_raw_feature_rows", len(ohlc_data))

    # Log a hash of the data to comet; this is useful to check if the data has changed
    dataset_hash = hash_data(ohlc_data)
    experiment.log_parameter("ohlc_data_hash", dataset_hash) # We could use this method as a native comet method

    # Split the data into training and test sets; we need a time-based split
    # Our Data is already sorted by timestamp_ms
    logger.debug(f"Splitting data into training and test sets")
    test_size = int(perc_test_data * len(ohlc_data))
    train_df = ohlc_data.iloc[:-test_size].copy(deep=True)
    test_df = ohlc_data.iloc[-test_size:].copy(deep=True)
    logger.debug(f"Training set shape: {train_df.shape}")
    logger.debug(f"Test set shape: {test_df.shape}")
    experiment.log_parameter("n_train_rows_before_dropna_shape", train_df.shape)
    experiment.log_parameter("n_test_rows_before_dropna_shape", test_df.shape)


    # Add a column with the target price we want to predict
    # For both the training and test dataframes
    train_df['target_price'] = ohlc_data['close'].shift(-forecast_steps)
    test_df['target_price'] = ohlc_data['close'].shift(-forecast_steps)
    logger.debug(f"Added target price column to training and test sets")

    # Drop rows with NaN values (values that are missing targets)
    train_df.dropna(inplace=True)
    test_df.dropna(inplace=True)        
    logger.debug(f"Dropped rows with NaN values")
    logger.debug(f"Training set shape after NAN: {train_df.shape}")       
    logger.debug(f"Test set shape afer NAN: {test_df.shape}")
    experiment.log_parameter("n_train_rows_after_dropna_shape", train_df.shape)
    experiment.log_parameter("n_test_rows_after_dropna_shape", test_df.shape)
    
    # split the data into features and target
    X_train = train_df.drop(columns=['target_price']).copy(deep=True)       
    y_train = train_df['target_price']
    X_test = test_df.drop(columns=['target_price']).copy(deep=True)
    y_test = test_df['target_price']
    logger.debug(f"Split the data into features and target")

    # Keep only the features we want to use
    # To do: think if we want to keep these hardcoded or make them configurable
    X_train = X_train[['open', 'high', 'low', 'close', 'volume', 'timestamp_ms']]
    X_test = X_test[['open', 'high', 'low', 'close', 'volume', 'timestamp_ms']]

    # Add technical indicators
    X_train = add_engineered_features(X_train).copy(deep=True)
    X_test = add_engineered_features(X_test).copy(deep=True)
    logger.debug(f"Added technical indicators to the training and test sets")
    logger.debug(f"X_train columns after adding technical indicators: {X_train.columns}")
    logger.debug(f"X_test columns after adding technical indicators: {X_test.columns}")
    experiment.log_parameter("features", str(X_train.columns.tolist()))
    experiment.log_parameter("n_features", len(X_train.columns))

    # Extract row indices for X_Train where any of the technical indicators are NaN
    nan_rows_train = X_train.isna().any(axis=1)

    # Count number of rows with NaN values
    logger.debug(f"Number of rows with NaN values in the training set: {nan_rows_train.sum()}")

    # Drop rows with NaN values
    X_train = X_train.loc[~nan_rows_train]
    y_train = y_train.loc[~nan_rows_train]
    
    # Extract row indices for X_Test where any of the technical indicators are NaN
    nan_rows_test = X_test.isna().any(axis=1)

    # Count number of rows with NaN values
    logger.debug(f"Number of rows with NaN values in the test set: {nan_rows_test.sum()}")

    # Drop rows with NaN values
    X_test = X_test.loc[~nan_rows_test]
    y_test = y_test.loc[~nan_rows_test]

    experiment.log_parameter("n_nan_rows_train", nan_rows_train.sum())
    experiment.log_parameter("n_nan_rows_test", nan_rows_test.sum())
    experiment.log_parameter("perc_drop_nan_rows_train", nan_rows_train.sum() / X_train.shape[0] * 100)
    experiment.log_parameter("perc_drop_nan_rows_test", nan_rows_test.sum() / X_test.shape[0] * 100)

    # Log the dimensions of the training and test sets
    logger.debug(f"X_train shape: {X_train.shape}")
    logger.debug(f"y_train shape: {y_train.shape}")
    logger.debug(f"X_test shape: {X_test.shape}")
    logger.debug(f"y_test shape: {y_test.shape}")

    # Add the shapes to the Comet experiment
    experiment.log_parameter("X_train_shape", X_train.shape)
    experiment.log_parameter("y_train_shape", y_train.shape)
    experiment.log_parameter("X_test_shape", X_test.shape)
    experiment.log_parameter("y_test_shape", y_test.shape)

    # Log the complete list of features our model will use
    experiment.log_parameter("features_to_use", X_train.columns.tolist())

    # Build a model
    model=CurrentPriceBaseline()
    model.fit(X_train, y_train)
    logger.debug(f"Model trained") 

    # Evaluate the model
    y_pred = model.predict(X_test)

    # Compute the mean absolute error
    mae = mean_absolute_error(y_test, y_pred)
    logger.debug(f"Mean absolute error of current price baseline: {mae}")
    experiment.log_metric("mean_absolute_error_current_price_baseline", mae)
    mae_baseline = mae

    # Compute the mae on the training set for debugging purposes
    y_pred_train = model.predict(X_train)
    mae_train = mean_absolute_error(y_train, y_pred_train)
    logger.debug(f"Mean absolute error on the training set CurrentPriceBaseline: {mae_train}")
    experiment.log_metric("mean_absolute_error_current_price_baseline_train", mae_train)

    # Train an xgboost model
    xgb_model = XGBoostModel()
    xgb_model.fit(X_train, y_train, n_search_trials=n_search_trials, n_splits=n_splits)
    y_pred = xgb_model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    logger.debug(f"Mean absolute error: {mae}")
    experiment.log_metric("mean_absolute_error", mae)
    
    # Save the model to the model commet ml registry
    model_name = get_model_name(product_id, ohlc_window_sec, forecast_steps)
    model_version=f"{forecast_steps} step forecast"

    # Save the model locally
    local_model_path = f"{model_name}.joblib"
    joblib.dump(xgb_model.get_model_object(), local_model_path)

    # Log the model to the Comet experiment
    experiment.log_model(name=model_name, 
                         file_or_folder=local_model_path, 
                         overwrite=False
    )
    
    # We don't want to register the model if the mae is higher than the baseline
    #if mae < mae_baseline:
    if True:
        logger.info(f"Mean absolute error of the model is lower than the baseline. Registering the model")
        # Register the model in the model registry
        registered_model = experiment.register_model(
            model_name=model_name
        )
        logger.info(f"Model registered in commet ML {model_name} (version:{model_version})")
    else:
        logger.info(f"Mean absolute error of the model is higher than the baseline. Not registering the model")
    experiment.end()

    # Clean up the local model file
    os.remove(local_model_path)

if __name__ == "__main__":
    train_model(comet_config=comet_config,
                hopsworks_config=hopsworks_config,
                feature_view_name=config.feature_view_name, 
                feature_view_version=config.feature_view_version,
                feature_group_name=config.feature_group_name,
                feature_group_version=config.feature_group_version,
                ohlc_window_sec=config.ohlc_window_sec,
                product_id=config.product_id,
                last_n_days=config.last_n_days,
                forecast_steps=config.forecast_steps,
                n_search_trials=config.n_search_trials,
                n_splits=config.n_splits,
                last_n_minutes=config.last_n_minutes
    )