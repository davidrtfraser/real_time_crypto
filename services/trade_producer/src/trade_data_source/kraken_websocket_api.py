from typing import List
from datetime import datetime, timezone
from websocket import create_connection
from loguru import logger
import json
from src.trade_data_source.trade import Trade
from src.trade_data_source.base import TradeSource


# from pydantic import BaseModel
# class Trade(BaseModel):
#     # We use pydantic to validate the data; this is a dataclass from pydantic
#     # BaseModel is a data class that we inherit from
#     product_id: str
#     quantity: float
#     price: float
#     timestamp_ms: int

class KrakenWebsocketAPI(TradeSource):
    """
    Class for reading data from Kraken Websocket API
    """
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        """"
        Initializes the KrakenWebsocketAPI instange
        
        Args:
            product_id (str): The product id to get the trades from
        """

        self.product_id = product_id

        # Establish connection to the Kraken Websocket API
        self._ws = create_connection(self.URL)
        logger.debug(f"Connection Established")

        # Subscribe to the given trades for the given product_id(S)
        self._subscribe(product_id)

    def get_trades(self)->List[Trade]:
        """
        Returns the latest batch of trades from the Kraken Websocket API

        Args:
            None
        Returns:
            List[Trade]: A list of trades
        """
        # Get the data from the websocket
        message = self._ws.recv()
        
        if 'heartbeat' in message:
            # When we receive a heartbeat, we return an empty list
            # message that communicates connection is still alive
            logger.debug('Received heartbeat')
            return []
        
        # Parse the message as dictionary
        message = json.loads(message)
        
        # Extract the data from the message
        trades = []
        for trade in message['data']:
            # EXTRACT THE FOLLOWING FIELDS FROM THE TRADE
            # -product_id
            # -quantity
            # -price
            # -timestamp_ms

            # We use pydandtic to validate the data
            trades.append(
                Trade(
                    product_id=trade['symbol'],
                    price=trade['price'],
                    quantity=trade['qty'],
                    timestamp_ms=self.to_ms(trade['timestamp']),
                )
            )
        return trades 

    def is_done(self)->bool:
        """
        Returns True if the Kraken Websocket API connection is closed
        """
        # False because we are streaming from a websocket; but if we were doing batches
        # of trades, we would return True when we are done with last batch
        False
    
    def _subscribe(self, product_id: str):
        # Everything is public in python; but it is good practice to use _ for private methods; this is not
        # supposed to be called from someone else outside of the class
        """
        Subscribes to the given product_ids
        """
        logger.info(f'Subscribing to the trades for {product_id}')
        
        # Subscribe to the given product_ids
        msg =  {
            "method": "subscribe",
            "params": {
                "symbol": [product_id],
                "channel": "trade",
                "snapshot": False,
            },
        }
        self._ws.send(json.dumps(msg))
        logger.info(f'Subscrition worked')

        # For each product_id we dump
        # the first 2 messages we got from the websocket because they contain
        # no trade data, just confirmation of subscription
        for product_id in [product_id]:
            _ = self._ws.recv()
            _ = self._ws.recv()
    
    # Static method because it does not depend on the instance of the class
    # Decorators are used to modify the behavior of functions or methods
    @staticmethod
    def to_ms(timestamp: str)->int:
        """
        Converts a Kraken timestamp to Unix timestamp in milliseconds
        Args:
            timestamp (str): The timestamp to convert
        Returns:
            int: The Unix timestamp in milliseconds
        """

        # Convert the timestamp to Unix timestamp in milliseconds
        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)
