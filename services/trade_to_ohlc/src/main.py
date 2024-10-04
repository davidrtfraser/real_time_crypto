from quixstreams import Application
from datetime import timedelta
from loguru import logger
from src.config import config

def init_ohlcv_candel(trade: dict):
    """
    Returns the initial state of the OHLCV candle.
    """
    return {
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'],
        'product_id': trade['product_id'],
        #'timestamp_ms': trade['timestamp_ms'],
    }

def update_ohlvc_candel(candle: dict, trade: dict):
    """
    Updates the OHLCV candle with the new trade data.
    """
    # Open price is the price of the first trade in the window
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']
    #candle['timestamp_ms'] = trade['timestamp_ms']

    return candle

def transform_trade_to_ohlcv(
        kafka_broker_address: str,
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_consumer_group: str,
        ohlcv_window_seconds: int
):
    """
    Reads incoming traged from the given Kafka topic, transforms them into OHLC data and writes them to the output Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_input_topic (str): The name of the Kafka topic to read the trades from.
        kafka_output_topic (str): The name of the Kafka topic to write the OHLC data to.
        kafka_consumer_group (str): The name of the Kafka consumer group.
    Returns:
        None
    """

    # Create an application with Kafka configuration
    app = Application(broker_address=kafka_broker_address,
                      # We need to set the consumer group to read the data from the topic
                      consumer_group=kafka_consumer_group)

    # Define the Kafka topic with JSON serialization
    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Create a Quix Steam Dataframe
    sdf = app.dataframe(input_topic)

    # Check if we are actually reading the trades
    sdf.update(logger.debug)

    # Aggregates trades into OHLCV candles
    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlcv_window_seconds))
        .reduce(reducer=update_ohlvc_candel, initializer=init_ohlcv_candel)
        # .current() # would be used if we wanted to get the current state of the window
        .final() # is used to get the final state of the window (we wait for the window to close)
    )

    # Print the output to the console
    sdf.update(logger.debug)

    # Unpack the dictionary to separate columns
    sdf["open"] = sdf["value"]["open"]
    sdf["high"] = sdf["value"]["high"]
    sdf["low"] = sdf["value"]["low"]
    sdf["close"] = sdf["value"]["close"]
    sdf["volume"] = sdf["value"]["volume"]
    sdf["timestamp_ms"] = sdf["end"]
    sdf["product_id"] = sdf["value"]["product_id"]

    #Keep only the columns we need
    sdf = sdf[["product_id","timestamp_ms", "open", "high", "low", "close", "volume"]]

    # Print the output to the console
    sdf.update(logger.debug)

    # Write the output to the Kafka topic
    sdf = sdf.to_topic(output_topic)

    # Write the output to the Kafka topic
    app.run(sdf)

if __name__ == '__main__':
    transform_trade_to_ohlcv(
        kafka_broker_address = config.kafka_broker_address,
        kafka_input_topic = config.kafka_input_topic,
        kafka_output_topic = config.kafka_output_topic,
        kafka_consumer_group = config.kafka_consumer_group,
        ohlcv_window_seconds = config.ohlcv_window_seconds
    )

