
def get_model_name(
        product_id: str,
        ohlc_window_sec: int,
        forecast_steps: int,
    ) -> str:
    """
    Returns the name of the model for a given product_id, ohlcv_window_sec, and forecast_steps in the model registry
    The model name is used to identify the model in the model registry.
    """
    return f"price_predictor_{product_id.replace('/', '_')}_{ohlc_window_sec}s_{forecast_steps}steps"
