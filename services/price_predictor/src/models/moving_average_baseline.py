import pandas as pd

class MovingAverageBaseline:
    """
    A simple moving average baseline model, which predicts the price as the average of the last window_size prices
    """
    def __init__(self, window_size: int):
        self.window_size = window_size

    def fit(self, X: pd.DataFrame, y: pd.Series):
        """
        Fit the model to the data.

        Args:
            data (pd.DataFrame): _description_
            target (pd.Series): _description_
        """        
        pass

    def predict(self, data: pd.DataFrame) -> pd.DataFrame:
        # data['prediction'] = data['price'].rolling(window=self.window_size).mean()
        # return data
        raise NotImplementedError("This method is not implemented yet")
        pass
