from abc import ABC, abstractmethod
from typing import List

# Observe how we use absolute import here
from src.trade_data_source.trade import Trade

class TradeSource(ABC):
    @abstractmethod
    def get_trades(self) -> List[Trade]:
        """"
        Get a list of trades from whatever source you connect to.
        """
        pass

    @abstractmethod
    def is_done(self) -> bool:
        """
        Check if the source has no more trades to return True; False otherwise.
        """
        pass