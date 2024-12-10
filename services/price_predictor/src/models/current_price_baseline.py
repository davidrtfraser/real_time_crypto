import pandas as pd

class CurrentPriceBaseline:
    def __init__(self):
        pass


    def fit(self, X: pd.DataFrame, y: pd.Series):
        """
        Fit the model to the data.

        Args:
            data (pd.DataFrame): _description_
            target (pd.Series): _description_
        """        
        pass

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Predicts the next price using the current price.

        Args:
            data (pd.DataFrame): _description_

        Returns:
            pd.Series: _description_
        """
        return X['close']