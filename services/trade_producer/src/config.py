from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):
    kafka_broker_address: str
    kafka_topic: str
    product_id: str

    # This is the first time Paulo has used this construct to load the environment variables
    # He usually uses the model_config to load the environment variables
    # model_config = {'env_file': '.env'}
    class Config:
        env_file = '.env'

config = AppConfig()