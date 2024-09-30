from typing import List
from quixstreams import Application
from loguru import logger
from src.kraken_websocket_api import KrakenWebsocketAPI, Trade
from src.config import config


def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    product_id: str,
):
    """
    Reads trades from the Kraken WEbsocket API and produces them to a Kafka topic.
    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic (str): The name of the Kafka topic to save the trades.
        product_id (str): The product ID to get the trades from.
    Returns:
        None
    """

    # Create an application with Kafka configuration
    app = Application(broker_address=kafka_broker_address)
    # Define the Kafka topic with JSON serialization
    topic = app.topic(name=kafka_topic, value_serializer='json')

    # Create a Kraken API object
    kraken_api = KrakenWebsocketAPI(product_id=product_id)

    # Create a producer (helps save data to the topic)
    with app.get_producer() as producer:
        while True:
            # We use type annotation when we define the variable
            trades: List[Trade] = kraken_api.get_trades()

            # Serialize the event using the defined topic
            # Transform the event into a sequence of bytes
            for trade in trades:
                # The point of the key is to make sure that all trades of the same product go to the same partition
                # This way, the order of the trades is preserved and we can get data in parallel
                # We get horizontal scalability
                # trade.model_dump() is a method that serializes the trade object into a dictionary
                message = topic.serialize(key=trade.product_id, value=trade.model_dump())
                # Produce a message to the topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.debug(f"Pushed trade to Kafka: {trade}") 


if __name__ == '__main__':

    
    produce_trades(
        kafka_broker_address = config.kafka_broker_address,
        kafka_topic = config.kafka_topic,
        product_id = config.product_id,
    )

