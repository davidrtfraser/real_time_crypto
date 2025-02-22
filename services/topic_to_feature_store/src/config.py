from pydantic_settings import BaseSettings
from typing import List, Optional

class AppConfig(BaseSettings):
    kafka_broker_address: str
    kafka_input_topic: str
    kafka_consumer_group: str
    feature_group_name: str
    feature_group_version: int
    feature_group_primary_keys: List[str]
    feature_group_event_time: str
    start_offline_materialization: bool
    batch_size: Optional[int] = 1 # Here we set the default value to 1

    class Config:
        env_file = ".env"
    
class HopsworksConfig(BaseSettings):
    hopsworks_api_key: str
    hopsworks_project_name: str

    class Config:
        env_file = "credentials.env"

config = AppConfig()
hop_config = HopsworksConfig()