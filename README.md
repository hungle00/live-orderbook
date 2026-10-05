# Live Crypto Order Book

A real-time BTC/USDT order book showing bids, asks, and the mid-price, powered by the Binance WebSocket stream.

## Tech stack

- Nodejs
- Fastify
- WebSockex for the Binance WebSocket connection

## How to run
An internet connection is required to receive live data from Binance.

```sh
node server
```

Open [http://localhost:4000/](http://localhost:4000/) to view the order book.
