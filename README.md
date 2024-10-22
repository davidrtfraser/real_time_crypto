# real_time_crypto TO DOS

## Session 1
- [X] Redpanda up and running
- [X] Push some fake data to Redpanda
- [X] Push real-time (real data) from Kraken websocket API

## Session 2
- [X] Extract config parameters (you want to decouple configuration from source code)
- [X] Dockerize it
- [ ] HOMEWORK: adjust code so that instead of a single product id, the trade producer uses several product ids = ['BTC/USD', 'BTC/EUR']
HINT: you will need to update:
 - the config types
 - the Kraken Websocket API class

- [X] Trade to ohlc service
- [X] Homework: Dockerize the trade to OHLC service
HINT: you need to extract config parameters and dockerize the trade_to_ohlc service

- [X] Topic to feature store service -> service only reads data from kafka I.e. consumer

## Session 3 
- [ ] Start the backfill of historical data
    - [X] implement a Kraken historical data reader (trade producer)
    - [X] Adjust timestamps used to buckert trades into windows (trade to ohlc)
    - [ ] Save historical OHLCV features to batches in the offline store (topic_to_feature_store)