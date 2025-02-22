from typing import List
import hopsworks
from src.config import hopsworks_config
import pandas as pd

project = hopsworks.login(
    api_key_value=hopsworks_config.hopsworks_api_key,
    #url=config.hopsworks_url,
    project=hopsworks_config.hopsworks_project_name
)

# Get the feature store connection
feature_store = project.get_feature_store()

# Push the message to the feature store
def push_value_to_feature_group( 
    value: List[dict],
    feature_group_name: str,
    feature_group_version: int,
    feature_group_primary_keys: List[str],
    feature_group_event_time: str,
    start_offline_materialization: bool,
):
    """
    Pushes a value to a feature_group_name in the Feature store.

    Args:
        value List(dict): The value to push to the feature group.
        feature_group_name (str): The name of the feature group.
        feature_group_version (int): The version of the feature group.
        feature_group_primary_keys (List[str]): The primary key of the feature group.
        feature_group_event_time (str): The event time of the feature group.
        start_offline_materialization (bool): Whether to start offline materialization; if True, the data will be materialized immediately.

    Returns:
        None
    """

    # Push the value to the feature group
    feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        primary_key=feature_group_primary_keys,
        event_time=feature_group_event_time,
        online_enabled=start_offline_materialization, # Store historical data and enable online feature serving
        #expectation_suite=expectation_suite_transactions # This lets us validate the data; really useful for data quality monitoring
        # this checks the content of the data and raises an error if the data does not match the schema
    )

    # Transform the value into a pandas DataFrame
    value_df = pd.DataFrame(value)

    # Push the value to the feature store; offline store for large amounts of data; online store for real-time data and fast access
    feature_group.insert(value_df, 
                         write_options={"start_offline_materialization":start_offline_materialization}
                         ) # Do not materialize the data immediately we want it only in the online store
