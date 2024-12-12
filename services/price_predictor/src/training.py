from src.config import config, HopsworksConfig, hopsworks_config, CometConfig, comet_config
from src.ohlc_data_reader import OhlcDataReader
from typing import Optional
from loguru import logger
from src.models.current_price_baseline import CurrentPriceBaseline
from sklearn.metrics import mean_absolute_error 
from comet_ml import Experiment

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

    Returns:
        None
    """
    # Initialize the Comet experiment
    experiment = Experiment(
        api_key=comet_config.comet_api_key,
        project_name=comet_config.comet_project_name,
    )


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

    # Split the data into training and test sets; we need a time-based split
    # Our Data is already sorted by timestamp_ms
    logger.debug(f"Splitting data into training and test sets")
    test_size = int(perc_test_data * len(ohlc_data))
    train_df = ohlc_data.iloc[:-test_size]
    test_df = ohlc_data.iloc[-test_size:]
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
    X_train = train_df.drop(columns=['target_price'])       
    y_train = train_df['target_price']
    X_test = test_df.drop(columns=['target_price'])
    y_test = test_df['target_price']
    logger.debug(f"Split the data into features and target")

    # Log the dimensions of the training and test sets
    logger.debug(f"X_train shape: {X_train.shape}")
    logger.debug(f"y_train shape: {y_train.shape}")
    logger.debug(f"X_test shape: {X_test.shape}")
    logger.debug(f"y_test shape: {y_test.shape}")

    experiment.log_parameter("X_train_shape", X_train.shape)
    experiment.log_parameter("y_train_shape", y_train.shape)
    experiment.log_parameter("X_test_shape", X_test.shape)
    experiment.log_parameter("y_test_shape", y_test.shape)

    # Build a model
    model=CurrentPriceBaseline()
    model.fit(X_train, y_train)
    logger.debug(f"Model trained") 

    # Evaluate the model
    y_pred = model.predict(X_test)

    # Compute the mean absolute error
    mae = mean_absolute_error(y_test, y_pred)
    logger.debug(f"Mean absolute error: {mae}")
    experiment.log_metric("mean_absolute_error", mae)
    
    # Save the model to the model registry
    experiment.end()

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

    )