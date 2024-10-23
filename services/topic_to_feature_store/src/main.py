from quixstreams import Application
from loguru import logger
from src.config import config
import json
from src.hopsworks_api import push_value_to_feature_group
from typing import List

def topic_to_feature_store (
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_consumer_group: str,
    feature_group_name: str,
    feature_group_version: int,
    feature_group_primary_keys: List[str],
    feature_group_event_time: str,
    start_offline_materialization: bool,
    batch_size: int
    # we will probably need some feature store credentials here
):
    """
    Reads incoming messages from a Kafka topic (kafka_input_topic) and writes them to a feature store (feature_group_name).

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_input_topic (str): The name of the Kafka topic to read from.
        kafka_consumer_group (str): The name of the Kafka consumer group.
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to write to.
        feature_group_primary_keys (List[str]): The primary key of the feature group.
        feature_group_event_time (str): The event time of the feature group.
        start_offline_materialization (bool): Whether to start offline materialization; if True, the data will be materialized immediately.
        batch_size (int): The number of messages to consume in a batch to accumlate in memory before writing to the feature store.
        # feature store credentials

    Returns:
        None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
        # auto_offset_reset="latest",
        ) # this line is not in the original code
       

    batch = []

    # Create a consumer and start consuming messages
    with app.get_consumer() as consumer: # Checks when last message was consumed and commits offsets
        consumer.subscribe(topics=[kafka_input_topic])

        while True:
            msg = consumer.poll(0.1) # Here we wait for 1/10 of a second for a message

            if msg is None:
                continue
            elif msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
        
            value=msg.value()

            # Decode the message (bytes) to a string
            value = json.loads(value.decode("utf-8"))

            # Append the message to the batch
            batch.append(value)

            # If the batch is not full, continue
            if len(batch) < batch_size:
                logger.debug(f"Batch has size {len(batch)} < {batch_size}. Continuing...")
                continue

            # If the batch is full, push the batch to the feature store
            logger.debug(f"Batch has size {len(batch)} >= {batch_size}. Pushing to feature store...")
            push_value_to_feature_group(
                batch,
                feature_group_name,
                feature_group_version,
                feature_group_primary_keys,
                feature_group_event_time,
                start_offline_materialization,
            )
            # Clear the batch
            batch  = []

if __name__ == "__main__":
    topic_to_feature_store (
        kafka_broker_address = config.kafka_broker_address,
        kafka_input_topic = config.kafka_input_topic,
        kafka_consumer_group = config.kafka_consumer_group,
        feature_group_name = config.feature_group_name,
        feature_group_version = config.feature_group_version,
        feature_group_primary_keys = config.feature_group_primary_keys,
        feature_group_event_time = config.feature_group_event_time,
        start_offline_materialization = config.start_offline_materialization,
        batch_size=config.batch_size,
    )