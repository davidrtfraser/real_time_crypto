from typing import List
from quixstreams import Application
from loguru import logger
from src.config import config
from src.trade_data_source.trade import Trade
from src.trade_data_source.base import TradeSource


def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    trade_data_source: TradeSource,
):
    """
    Reads trades from the Kraken WEbsocket API and produces them to a Kafka topic.
    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic (str): The name of the Kafka topic to save the trades.
        trade_data_source (TradeSource): The source of the trade data.
    Returns:
        None
    """

    # Create an application with Kafka configuration
    app = Application(broker_address=kafka_broker_address)
    # Define the Kafka topic with JSON serialization
    topic = app.topic(name=kafka_topic, value_serializer='json')

    # Create a producer (helps save data to the topic)
    with app.get_producer() as producer:
        while not trade_data_source.is_done():
        # while trade_data_source.is_done() is False:

            trades: List[Trade] = trade_data_source.get_trades()

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

    if config.live_or_historical == 'live':
        from src.trade_data_source.kraken_websocket_api import KrakenWebsocketAPI
        kraken_api = KrakenWebsocketAPI(product_id=config.product_id)
    elif config.live_or_historical == 'historical':
        from src.trade_data_source.kraken_rest_api import KrakenRestAPI
        kraken_api = KrakenRestAPI(
            product_id=config.product_id,
            last_n_days=config.last_n_days,
            )
    else:
        raise ValueError("Invalid value for live_or_historical")

    produce_trades(
        kafka_broker_address = config.kafka_broker_address,
        kafka_topic = config.kafka_topic,
        trade_data_source = kraken_api,
    )

