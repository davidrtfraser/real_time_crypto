import talib
import pandas as pd

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add technical indicators to the dataframe

    Args:
        df (pd.DataFrame): The input dataframe is expected to have the following columns:
        - open
        - high
        - low
        - close
        - volume

    Returns:
        df (pd.DataFrame): The ouptut dataframe will have the original features and the new technical indicators:
        - open
        - high
        - low
        - close
        - volume
        - 'SMA_7'
        - 'SMA_14'
        - 'SMA_28'
        - 'EMA_7'
        - 'EMA_14'
        - 'EMA_28'
        - 'RSI_14'
        - 'MACD'
        - 'MACD_Signal'
        - 'BB_UPPER'
        - 'BB_MIDDLE'
        - 'BB_LOWER'
        - 'STOCH_K'
        - 'STOCH_D'
        - 'ATR'
        - 'CCI'
        - 'CMF'
    """

    # Add a simple moving average with a:
    # - timeperiod of 7
    # - timeperiod of 14
    # - timeperiod of 28
    df['SMA_7'] = talib.SMA(df['close'], timeperiod=7)
    df['SMA_14'] = talib.SMA(df['close'], timeperiod=14)
    df['SMA_28'] = talib.SMA(df['close'], timeperiod=28)

    # Add a Expotential Moving Average with a:
    # - timeperiod of 7
    # - timeperiod of 14
    # - timeperiod of 28
    df['EMA_7'] = talib.EMA(df['close'], timeperiod=7)
    df['EMA_14'] = talib.EMA(df['close'], timeperiod=14)
    df['EMA_28'] = talib.EMA(df['close'], timeperiod=28)
    
    # Add a Relative Strength Index with a:
    # - timeperiod of 14
    df['RSI_14'] = talib.RSI(df['close'], timeperiod=14)

    # Add a Moving Average Convergence Divergence with a:
    # - fastperiod of 12
    # - slowperiod of 26
    # - signalperiod of 9
    macd, macd_signal, _ = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['MACD'] = macd
    df['MACD_Signal'] = macd_signal

    # Add a Bollinger Bands with a:
    # - timeperiod of 20, nbdevup of 2, nbdevdn of 2
    upper, middle, lower = talib.BBANDS(df['close'], timeperiod=20, nbdevup=2, nbdevdn=2)
    df['BB_UPPER'] = upper
    df['BB_MIDDLE'] = middle
    df['BB_LOWER'] = lower

    # Add a Stoachastic Oscillator with a:
    # - fastk_period of 14, slowk_period of 3, slowd_period of 3
    slowk, slowd = talib.STOCH(df['high'], df['low'], df['close'], fastk_period=14, slowk_period=3, slowd_period=3)
    df['STOCH_K'] = slowk
    df['STOCH_D'] = slowd

    # Add a Average True Range with a:
    # - timeperiod of 14
    df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)

    # Add a Commodity Channel Index with a:
    # - timeperiod of 14
    df['CCI'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=14)

    # Caikin Money Flow
    df['CMF'] = talib.ADOSC(df['high'], df['low'], df['close'], df['volume'], fastperiod=3, slowperiod=10)

    return df

def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add temporal features to the dataframe

    Args:
        df (pd.DataFrame): The input dataframe is expected to have the following columns:
        - timestamp_ms

    Returns:
        df (pd.DataFrame): The ouptut dataframe will have the original features and the new temporal features:
        - timestamp_ms
        - 'hour'
        - 'day'
        - 'month'
        - 'weekday'
    """
    df['hour'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.hour
    df['day'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.day
    df['month'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.month
    df['weekday'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.weekday

    return df
