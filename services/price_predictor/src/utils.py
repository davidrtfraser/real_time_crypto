import pandas as pd

# Function to create a consistent hash of the data
def hash_data(df: pd.DataFrame) -> str:
    """
    Create a consistent hash of the data
    
    Args:
        df (pd.DataFrame): The data to hash
        
    Returns:
        str: The hash of the data
    """
    return pd.util.hash_pandas_object(df).sum()