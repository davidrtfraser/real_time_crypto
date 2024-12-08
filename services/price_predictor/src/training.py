from src.config import config, HopsworksConfig, hopsworks_config
from src.ohlc_data_reader import OhlcDataReader

def train_model(
    feature_view_name: str, # The name of the feature group to read from; feature view is a more general term
    feature_view_version: int, # The version of the feature group to read from
    feature_group_name: str,
    feature_group_version: int,
    ohlc_window_sec: int,
    product_id: str, # The product ID for which we want to get the OHLC data
    last_n_days: int, # The number of days of data to train on
    hopsworks_config: HopsworksConfig,
):
    """
    Reads features from the feature store
    Trains a model
    Saves the model to the model registry\;
    """

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

    breakpoint()
    # Train a model

    # Save the model to the model registry


if __name__ == "__main__":
    train_model(feature_view_name=config.feature_view_name, 
                feature_view_version=config.feature_view_version,
                feature_group_name=config.feature_group_name,
                feature_group_version=config.feature_group_version,
                ohlc_window_sec=config.ohlc_window_sec,
                product_id=config.product_id,
                last_n_days=config.last_n_days,
                hopsworks_config=hopsworks_config
    )