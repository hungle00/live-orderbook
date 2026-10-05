import Fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import WebSocket from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import fastifyStatic from '@fastify/static';

const fastify = Fastify({ logger: true });

fastify.register(fastifyWebsocket);

const orderbooks = new Map();
const clients = new Set();
const binanceConnections = new Map();
const INITIAL_RECONNECT_DELAY = 1000;
const MAX_RECONNECT_DELAY = 30000;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

fastify.register(fastifyStatic, {
  root: path.join(__dirname, 'public'),
  prefix: '/',
});

function hasSubscribers(symbol) {
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN && client.subscribedSymbol === symbol) {
      return true;
    }
  }
  return false;
}

function parseDepthMessage(raw, symbol) {
  let parsed;
  try {
    parsed = JSON.parse(raw.toString());
  } catch (err) {
    fastify.log.warn({ err, symbol, payload: raw.toString().slice(0, 500) }, 'Invalid JSON from Binance');
    return null;
  }

  if (!Array.isArray(parsed?.bids) || !Array.isArray(parsed?.asks)) {
    fastify.log.warn({ symbol, payload: parsed }, 'Unexpected message from Binance');
    return null;
  }

  const parseLevels = (levels) => levels.map((level) => {
    if (!Array.isArray(level) || level.length < 2) {
      throw new TypeError('Depth level must contain price and quantity');
    }

    const price = Number(level[0]);
    const quantity = Number(level[1]);
    if (!Number.isFinite(price) || !Number.isFinite(quantity) || price <= 0 || quantity < 0) {
      throw new TypeError('Depth level contains an invalid price or quantity');
    }

    return [price, quantity];
  });

  try {
    return {
      symbol,
      bids: parseLevels(parsed.bids),
      asks: parseLevels(parsed.asks),
      updated_at: Date.now(),
    };
  } catch (err) {
    fastify.log.warn({ err, symbol, payload: parsed }, 'Invalid depth data from Binance');
    return null;
  }
}

function connectBinance(symbol) {
  if (binanceConnections.has(symbol) || !hasSubscribers(symbol)) {
    return;
  }

  const state = {
    socket: null,
    retryTimer: null,
    retryDelay: INITIAL_RECONNECT_DELAY,
  };
  binanceConnections.set(symbol, state);

  const openConnection = () => {
    if (binanceConnections.get(symbol) !== state || !hasSubscribers(symbol)) {
      binanceConnections.delete(symbol);
      return;
    }

    const ws = new WebSocket(`wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@depth5@100ms`);
    state.socket = ws;

    ws.on('open', () => {
      fastify.log.info({ symbol }, 'Connected to Binance depth stream');
    });

    ws.on('message', (raw) => {
      const orderbookData = parseDepthMessage(raw, symbol);
      if (!orderbookData) {
        return;
      }

      state.retryDelay = INITIAL_RECONNECT_DELAY;
      orderbooks.set(symbol, orderbookData);
      broadcast(symbol, orderbookData);
    });

    ws.on('close', (code, reason) => {
      fastify.log.warn(
        { symbol, code, reason: reason.toString() },
        'Disconnected from Binance depth stream',
      );
      if (binanceConnections.get(symbol) !== state) {
        return;
      }
      state.socket = null;

      if (!hasSubscribers(symbol)) {
        binanceConnections.delete(symbol);
        return;
      }

      const delay = state.retryDelay;
      state.retryDelay = Math.min(state.retryDelay * 2, MAX_RECONNECT_DELAY);
      state.retryTimer = setTimeout(openConnection, delay);
    });

    ws.on('error', (err) => {
      fastify.log.error({ err, symbol }, 'Binance WebSocket error');
    });
  };

  openConnection();
}

function stopBinance(symbol) {
  const state = binanceConnections.get(symbol);
  if (!state) {
    return;
  }

  binanceConnections.delete(symbol);
  clearTimeout(state.retryTimer);
  if (state.socket && state.socket.readyState !== WebSocket.CLOSED) {
    state.socket.terminate();
  }
}

function broadcast(symbol, data) {
  const message = JSON.stringify({ type: 'ORDERBOOK_UPDATE', payload: data });

  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN && client.subscribedSymbol === symbol) {
      client.send(message, (err) => {
        if (err) {
          fastify.log.warn({ err, symbol }, 'Failed to send orderbook update to browser');
        }
      });
    }
  }
}

fastify.register(async function (fastify) {
  fastify.get('/ws/orderbook', { websocket: true }, (socket) => {
    clients.add(socket);
    socket.subscribedSymbol = 'BTCUSDT';

    const initialOrderbook = orderbooks.get(socket.subscribedSymbol);
    if (initialOrderbook) {
      socket.send(JSON.stringify({ type: 'ORDERBOOK_UPDATE', payload: initialOrderbook }));
    }
    connectBinance(socket.subscribedSymbol);

    socket.on('message', (message) => {
      let request;
      try {
        request = JSON.parse(message.toString());
      } catch (err) {
        fastify.log.warn({ err }, 'Invalid JSON message from browser');
        return;
      }

      const symbol = typeof request?.symbol === 'string' ? request.symbol.trim().toUpperCase() : '';
      if (request?.action !== 'SUBSCRIBE' || !/^[A-Z0-9]{5,20}$/.test(symbol)) {
        fastify.log.warn({ action: request?.action, symbol }, 'Invalid orderbook subscription request');
        return;
      }

      const previousSymbol = socket.subscribedSymbol;
      socket.subscribedSymbol = symbol;

      const orderbook = orderbooks.get(symbol);
      if (orderbook && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: 'ORDERBOOK_UPDATE', payload: orderbook }));
      }
      connectBinance(symbol);

      if (previousSymbol !== symbol && !hasSubscribers(previousSymbol)) {
        stopBinance(previousSymbol);
      }
    });

    socket.on('close', () => {
      const symbol = socket.subscribedSymbol;
      clients.delete(socket);
      if (symbol && !hasSubscribers(symbol)) {
        stopBinance(symbol);
      }
    });
  });
});

const start = async () => {
  try {
    await fastify.listen({ port: 4000 });
    fastify.log.info('Fastify Orderbook Server runs at http://localhost:4000');
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
