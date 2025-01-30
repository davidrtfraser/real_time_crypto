import talib
import pandas as pd

def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add engineered features to the dataframe

    Args:
        df (pd.DataFrame): The input dataframe is expected to have the following columns:
        - open
        - high
        - low
        - close
        - volume

    Returns:
        df (pd.DataFrame): The ouptut dataframe will have the original features and the new engineered features:
        - AD
        - ADOSC3
        - ADOSC7
        - ADOSC14
        - OBV
        - ADX7
        - ADX14
        - ADX28
        - ADXR7
        - ADXR14
        - ADXR28
        - APO12_26
        - APO6_18
        - AROON_UP7
        - AROON_DOWN7
        - AROON_UP14
        - AROON_DOWN14
        - AROON_UP28
        - AROON_DOWN28
        - AROONOSC7
        - AROONOSC14
        - AROONOSC28
        - BOP
        - CCI7
        - CCI14
        - CCI28
        - CMO7
        - CMO14
        - CMO28
        - DX7
        - DX14
        - DX28
        - MACD12_26
        - MACD6_18
        - MACDEXT12_26
        - MACDEXT6_18
        - MACDFIX7
        - MACDFIX14
        - MACDFIX28
        - MFI7
        - MFI14
        - MFI28
        - MINUS_DI7
        - MINUS_DI14
        - MINUS_DI28
        - MINUS_DM7
        - MINUS_DM14
        - MINUS_DM28
        - MOM7
        - MOM14
        - MOM28
        - PLUS_DI7
        - PLUS_DI14
        - PLUS_DI28
        - PLUS_DM7
        - PLUS_DM14
        - PLUS_DM28
        - PPO12_26
        - PPO6_18
        - ROC7
        - ROC14
        - ROC28
        - ROCP7
        - ROCP14
        - ROCP28
        - ROCR7
        - ROCR14
        - ROCR28
        - RSI7
        - RSI14
        - RSI28
        - STOCH
        - STOCHF
        - STOCHRSI
        - TRIX7
        - TRIX14
        - TRIX28
        - ULTOSC
        - WILLR7
        - WILLR14
        - WILLR28
        - ATR7
        - ATR14
        - ATR28
        - NATR7
        - NATR14
        - NATR28
        - TRANGE
        - BB7_UPPERBAND
        - BB7_MIDDLEBAND
        - BB7_LOWERBAND
        - BB14_UPPERBAND
        - BB14_MIDDLEBAND
        - BB14_LOWERBAND
        - BB28_UPPER
        - BB28_MIDDLE
        - BB28_LOWER
        - DEMA3
        - DEMA14
        - DEMA28
        - EMA_7
        - EMA_14
        - EMA_28
        - HT_TRENDLINE
        - KAMA
        - MA
        - MAMA
        - FAMA
        - MAVP
        - MIDPOINT7
        - MIDPOINT14
        - MIDPOINT28
        - MIDPRICE7
        - MIDPRICE14
        - MIDPRICE28
        - SAR
        - SAREXT
        - SMA_7
        - SMA_14
        - SMA_28
        - T3_5
        - T3_14
        - T3_28
        - TEMA7
        - TEMA14
        - TEMA28
        - TRIMA7
        - TRIMA14
        - TRIMA28
        - WMA7
        - WMA14
        - WMA28
        - timestamp_ms
        - hour
        - day
        - month
        - weekday

    """
    # Add momentum indicators
    df = add_momentum_indicators(df).copy(deep=True)
    
    # Add volatility indicators
    df = add_volatility_indicators(df).copy(deep=True)
    
    # Add overlap indicators
    df = add_overlap_indicators(df).copy(deep=True)
    
    # Add temporal features
    df = add_temporal_features(df).copy(deep=True)

    # Add volumne indicators
    df = add_volume_indicators(df).copy(deep=True)

    return df

def add_volume_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): 

    Returns:
        pd.DataFrame: The ouptut dataframe will have the original features and the new volume indicators:
        - AD
        - ADOSC3
        - ADOSC7
        - ADOSC14
        - OBV
    """
    # Accumulation/Distribution Line
    df['AD'] = talib.AD(df['high'], df['low'], df['close'], df['volume'])

    # Chaikin A/D Line
    df['ADOSC3'] = talib.ADOSC(df['high'], df['low'], df['close'], df['volume'], fastperiod=3, slowperiod=10)
    df['ADOSC7'] = talib.ADOSC(df['high'], df['low'], df['close'], df['volume'], fastperiod=7, slowperiod=14)
    df['ADOSC14'] = talib.ADOSC(df['high'], df['low'], df['close'], df['volume'], fastperiod=14, slowperiod=28)

    # On Balance Volume
    df['OBV'] = talib.OBV(df['close'], df['volume'])
    return df

def add_momentum_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """

    Args:
        df (pd.DataFrame): 

    Returns:
        pd.DataFrame: The ouptut dataframe will have the original features and the new momentum indicators:
        - ADX
        - ADXR
        - APO
        - AROON
        - AROONOSC
        - BOP
        - CCI
        - CMO
        - DX
        - MACD
        - MACDEXT
        - MACDFIX
        - MFI
        - MINUS_DI
        - MINUS_DM
        - MOM
        - PLUS_DI
        - PLUS_DM
        - PPO
        - ROC
        - ROCP
        - ROCR
        - ROCR100
        - RSI
        - STOCH
        - STOCHF
        - STOCHRSI
        - TRIX
        - ULTOSC
        - WILLR
    """
    # Average Directional Movement Index
    df['ADX7'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=7)
    df['ADX14'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
    df['ADX28'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=28)

    # Average Directional Movement Index Rating
    df['ADXR7'] = talib.ADXR(df['high'], df['low'], df['close'], timeperiod=7)
    df['ADXR14'] = talib.ADXR(df['high'], df['low'], df['close'], timeperiod=14)
    df['ADXR28'] = talib.ADXR(df['high'], df['low'], df['close'], timeperiod=28)

    # Absolute Price Oscillator
    df['APO12_26'] = talib.APO(df['close'], fastperiod=12, slowperiod=26, matype=0)
    df['APO6_18'] = talib.APO(df['close'], fastperiod=6, slowperiod=18, matype=0)

    # Aroon
    df['AROON_UP7'], df['AROON_DOWN7'] = talib.AROON(df['high'], df['low'], timeperiod=7)
    df['AROON_UP14'], df['AROON_DOWN14'] = talib.AROON(df['high'], df['low'], timeperiod=14)
    df['AROON_UP28'], df['AROON_DOWN28'] = talib.AROON(df['high'], df['low'], timeperiod=28)

    # Aroon Oscillator
    df['AROONOSC7'] = talib.AROONOSC(df['high'], df['low'], timeperiod=7)
    df['AROONOSC14'] = talib.AROONOSC(df['high'], df['low'], timeperiod=14)
    df['AROONOSC28'] = talib.AROONOSC(df['high'], df['low'], timeperiod=28)

    # Balance of Power
    df['BOP'] = talib.BOP(df['open'], df['high'], df['low'], df['close'])

    # Commodity Channel Index
    df['CCI7'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=7)
    df['CCI14'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=14)
    df['CCI28'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=28)

    # Chande Momentum Oscillator
    df['CMO7'] = talib.CMO(df['close'], timeperiod=7)
    df['CMO14'] = talib.CMO(df['close'], timeperiod=14)
    df['CMO28'] = talib.CMO(df['close'], timeperiod=28)

    # Directional Movement Index
    df['DX7'] = talib.DX(df['high'], df['low'], df['close'], timeperiod=7)
    df['DX14'] = talib.DX(df['high'], df['low'], df['close'], timeperiod=14)
    df['DX28'] = talib.DX(df['high'], df['low'], df['close'], timeperiod=28)

    # Moving Average Convergence Divergence
    df['MACD12_26'] , df['MACD_Signal12_26'], _ = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['MACD6_18'] , df['MACD_Signal6_18'], _ = talib.MACD(df['close'], fastperiod=6, slowperiod=18, signalperiod=9)

    # Moving Average Convergence Divergence with controllable moving average type
    df['MACDEXT12_26_A'], df['MACDEXT12_26_B'], _ = talib.MACDEXT(df['close'], fastperiod=12, slowperiod=26, signalperiod=9, fastmatype=0, slowmatype=0, signalmatype=0)
    df['MACDEXT6_18_A'], df['MACDEXT6_18_B'], _ = talib.MACDEXT(df['close'], fastperiod=6, slowperiod=18, signalperiod=9, fastmatype=0, slowmatype=0, signalmatype=0)

    # Moving Average Convergence Divergence Fix 12/26
    df['MACDFIX7_A'], df['MACDFIX7_B'], _ = talib.MACDFIX(df['close'], signalperiod=7)
    df['MACDFIX14_A'], df['MACDFIX14_B'], _ = talib.MACDFIX(df['close'], signalperiod=14)
    df['MACDFIX28_A'], df['MACDFIX28_B'], _ = talib.MACDFIX(df['close'], signalperiod=28)

    # Money Flow Index
    df['MFI7'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=7)
    df['MFI14'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=14)
    df['MFI28'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=28)

    # Directional Movement Index
    df['MINUS_DI7'] = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=7)
    df['MINUS_DI14'] = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
    df['MINUS_DI28'] = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=28)

    # Directional Movement
    df['MINUS_DM7'] = talib.MINUS_DM(df['high'], df['low'], timeperiod=7)
    df['MINUS_DM14'] = talib.MINUS_DM(df['high'], df['low'], timeperiod=14)
    df['MINUS_DM28'] = talib.MINUS_DM(df['high'], df['low'], timeperiod=28)

    # Momentum
    df['MOM7'] = talib.MOM(df['close'], timeperiod=7)
    df['MOM14'] = talib.MOM(df['close'], timeperiod=14)
    df['MOM28'] = talib.MOM(df['close'], timeperiod=28)

    # Plus Directional Indicator
    df['PLUS_DI7'] = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=7)
    df['PLUS_DI14'] = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
    df['PLUS_DI28'] = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=28)

    # Plus Directional Movement
    df['PLUS_DM7'] = talib.PLUS_DM(df['high'], df['low'], timeperiod=7)
    df['PLUS_DM14'] = talib.PLUS_DM(df['high'], df['low'], timeperiod=14)
    df['PLUS_DM28'] = talib.PLUS_DM(df['high'], df['low'], timeperiod=28)

    # Percentage Price Oscillator
    df['PPO12_26'] = talib.PPO(df['close'], fastperiod=12, slowperiod=26, matype=0)
    df['PPO6_18'] = talib.PPO(df['close'], fastperiod=6, slowperiod=18, matype=0)

    # Rate of Change
    df['ROC7'] = talib.ROC(df['close'], timeperiod=7)
    df['ROC14'] = talib.ROC(df['close'], timeperiod=14)
    df['ROC28'] = talib.ROC(df['close'], timeperiod=28)

    # Rate of Change Percentage
    df['ROCP7'] = talib.ROCP(df['close'], timeperiod=7)
    df['ROCP14'] = talib.ROCP(df['close'], timeperiod=14)
    df['ROCP28'] = talib.ROCP(df['close'], timeperiod=28)

    # Rate of Change Ratio
    df['ROCR7'] = talib.ROCR(df['close'], timeperiod=7)
    df['ROCR14'] = talib.ROCR(df['close'], timeperiod=14)
    df['ROCR28'] = talib.ROCR(df['close'], timeperiod=28)

    # Relative Strength Index
    df['RSI7'] = talib.RSI(df['close'], timeperiod=7)
    df['RSI14'] = talib.RSI(df['close'], timeperiod=14)
    df['RSI28'] = talib.RSI(df['close'], timeperiod=28)

    df['STOCHK'], df['STOCHD'] = talib.STOCH(df['high'], df['low'], df['close'], fastk_period=5, slowk_period=3, slowd_period=3)
    df['STOCHF_A'], df['STOCHF_B'] = talib.STOCHF(df['high'], df['low'], df['close'], fastk_period=3, fastd_period=3)
    df['STOCHRSI_A'], df['STOCHRSI_B'] = talib.STOCHRSI(df['close'], timeperiod=14, fastk_period=5, fastd_period=3)

    # Triple Exponential Moving Average
    df['TRIX7'] = talib.TRIX(df['close'], timeperiod=7)
    df['TRIX14'] = talib.TRIX(df['close'], timeperiod=14)
    df['TRIX28'] = talib.TRIX(df['close'], timeperiod=28)

    # Ultimate Oscillator
    df['ULTOSC'] = talib.ULTOSC(df['high'], df['low'], df['close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)

    # Williams' %R
    df['WILLR7'] = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=7)
    df['WILLR14'] = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=14)
    df['WILLR28'] = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=28)
    
    return df

def add_volatility_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: The ouptut dataframe will have the original features and the new volatility indicators:
        - ATR7
        - ATR14
        - ATR28
        - NATR7
        - NATR14
        - NATR28
        - TRANGE
    """
    # Average True Range
    df['ATR7'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=7)
    df['ATR14'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    df['ATR28'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=28)

    # Normalized Average True Range
    df['NATR7'] = talib.NATR(df['high'], df['low'], df['close'], timeperiod=7)
    df['NATR14'] = talib.NATR(df['high'], df['low'], df['close'], timeperiod=14)
    df['NATR28'] = talib.NATR(df['high'], df['low'], df['close'], timeperiod=28)

    # True Range
    df['TRANGE'] = talib.TRANGE(df['high'], df['low'], df['close'])
    return df

def add_overlap_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): The input dataframe

    Returns:
        pd.DataFrame: The ouptut dataframe will have the original features and the new overlap indicators:
        - BB7_UPPERBAND
        - BB7_MIDDLEBAND
        - BB7_LOWERBAND
        - BB14_UPPERBAND
        - BB14_MIDDLEBAND
        - BB14_LOWERBAND
        - BB28_UPPER
        - BB28_MIDDLE
        - BB28_LOWER
        - DEMA3
        - DEMA14
        - DEMA28
        - EMA_7
        - EMA_14
        - EMA_28
        - HT_TRENDLINE
        - KAMA
        - MA
        - MAMA
        - FAMA
        - MAVP
        - MIDPOINT7
        - MIDPOINT14
        - MIDPOINT28
        - MIDPRICE7
        - MIDPRICE14
        - MIDPRICE28
        - SAR
        - SAREXT
        - SMA_7
        - SMA_14
        - SMA_28
        - T3_5
        - T3_14
        - T3_28
        - TEMA7
        - TEMA14
        - TEMA28
        - TRIMA7
        - TRIMA14
        - TRIMA28
        - WMA7
        - WMA14
        - WMA28

    """
    # Bollinger Bands
    df['BB7_UPPERBAND'], df['BB7_MIDDLEBAND'], df['BB7_LOWERBAND'] = talib.BBANDS(df['close'], timeperiod=7, nbdevup=2, nbdevdn=2, matype=0)
    df['BB14_UPPERBAND'], df['BB14_MIDDLEBAND'], df['BB14_LOWERBAND'] = talib.BBANDS(df['close'], timeperiod=14, nbdevup=2, nbdevdn=2, matype=0)
    df['BB28_UPPER'], df['BB28_MIDDLE'], df['BB28_LOWER'] = talib.BBANDS(df['close'], timeperiod=28, nbdevup=2, nbdevdn=2)
    
    # Double Exponential Moving Average
    df['DEMA3'] = talib.DEMA(df['close'], timeperiod=3)
    df['DEMA14'] = talib.DEMA(df['close'], timeperiod=14)
    df['DEMA28'] = talib.DEMA(df['close'], timeperiod=28)

    # Exponential Moving Average
    df['EMA_7'] = talib.EMA(df['close'], timeperiod=7)
    df['EMA_14'] = talib.EMA(df['close'], timeperiod=14)
    df['EMA_28'] = talib.EMA(df['close'], timeperiod=28)

    df['HT_TRENDLINE'] = talib.HT_TRENDLINE(df['close'])
    df['KAMA'] = talib.KAMA(df['close'], timeperiod=30)
    df['MA'] = talib.MA(df['close'], timeperiod=30, matype=0)

    # Moving Average with controllable moving average type
    # df['MAVP'] = talib.MAVP(df['close'], periods=[2, 3, 4, 5, 6, 7, 8, 9, 10], minperiod=2, maxperiod=30)
    
    # Midpoint
    df['MIDPOINT7'] = talib.MIDPOINT(df['close'], timeperiod=7)
    df['MIDPOINT14'] = talib.MIDPOINT(df['close'], timeperiod=14)
    df['MIDPOINT28'] = talib.MIDPOINT(df['close'], timeperiod=28)

    # Midpoint Price over period
    df['MIDPRICE7'] = talib.MIDPRICE(df['high'], df['low'], timeperiod=7)
    df['MIDPRICE14'] = talib.MIDPRICE(df['high'], df['low'], timeperiod=14)
    df['MIDPRICE28'] = talib.MIDPRICE(df['high'], df['low'], timeperiod=28)

    # Parabolic SAR
    df['SAR'] = talib.SAR(df['high'], df['low'], acceleration=0, maximum=0)
    df['SAREXT'] = talib.SAREXT(df['high'], df['low'], startvalue=0, offsetonreverse=0, accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0, accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)
    
    # Simple Moving Average
    df['SMA_7'] = talib.SMA(df['close'], timeperiod=7)
    df['SMA_14'] = talib.SMA(df['close'], timeperiod=14)
    df['SMA_28'] = talib.SMA(df['close'], timeperiod=28)

    # T3
    df['T3_5'] = talib.T3(df['close'], timeperiod=5, vfactor=0)
    df['T3_14'] = talib.T3(df['close'], timeperiod=14, vfactor=0)
    df['T3_28'] = talib.T3(df['close'], timeperiod=28, vfactor=0)

    # Triple Exponential Moving Average
    df['TEMA7'] = talib.TEMA(df['close'], timeperiod=7)
    df['TEMA14'] = talib.TEMA(df['close'], timeperiod=14)
    df['TEMA28'] = talib.TEMA(df['close'], timeperiod=28)

    # Triangular Moving Average
    df['TRIMA7'] = talib.TRIMA(df['close'], timeperiod=7)
    df['TRIMA14'] = talib.TRIMA(df['close'], timeperiod=14)
    df['TRIMA28'] = talib.TRIMA(df['close'], timeperiod=28)

    # Weighted Moving Average
    df['WMA7'] = talib.WMA(df['close'], timeperiod=7)
    df['WMA14'] = talib.WMA(df['close'], timeperiod=14)
    df['WMA28'] = talib.WMA(df['close'], timeperiod=28)
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
    # Add temporal features
    df['hour'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.hour
    df['day'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.day
    df['month'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.month
    df['weekday'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.weekday

    return df
