from pydantic import BaseModel
from src.config import config, comet_config, CometConfig
from src.hopsworks_api import push_value_to_feature_group
from src.model_registry import get_model_name
import json
import os
from comet_ml.api import API
import joblib
from src.price_predictor import PricePredictor

# This service runs continuously generating predictions for the price of a stock
def predict(
    product_id: str,
    ohlc_window_sec: int,
    forecast_steps: int,
    model_status: str,
):
    """Loads the modelf rom the registry and then endters a loop where it:
    - connects to the feature store
    - fetches the most recent OHLCV data
    - generates a prediction for the next five minutes
    - saves the prediction to the online feature group
    """
    # We create a predictor object that loads the model from the registry
    predictor = PricePredictor.from_model_registry(
        product_id=product_id,
        ohlc_window_sec=ohlc_window_sec,
        forecast_steps=forecast_steps,
        status=model_status
    )

    # This is where we should use CDC patterns to get the most recent data; instead we will just use a loop
    while True:
        prediction = predictor.predict()

        # # Save the prediction to the online feature group
        # predictor.push_value_to_feature_group(
        #     value=prediction,
        #     feature_group_name=config.online_feature_group_name,
        #     feature_group_version=config.online_feature_group_version,
        #     feature_group_primary_keys=config.online_feature_group_primary_keys,
        #     feature_group_event_time=config.online_feature_group_event_time,
        #     start_offline_materialization=config.start_offline_materialization
        # )

    # Code to generate a prediction
    print("Generating prediction...")

if __name__ == '__main__':
    # Generate a prediction
    predict(
        product_id=config.product_id,
        ohlc_window_sec=config.ohlc_window_sec,
        forecast_steps=config.forecast_steps,
        model_status=config.model_status
    )