export class Orderbook {
  constructor(symbol) {
    this.symbol = symbol;
    this.bids = [];
    this.asks = [];
    this.updatedAt = null;
  }

  update(bids, asks) {
    const validatedBids = this.#validateLevels(bids);
    const validatedAsks = this.#validateLevels(asks);
    this.bids = validatedBids;
    this.asks = validatedAsks;
    this.updatedAt = Date.now();
  }

  toJSON() {
    return {
      symbol: this.symbol,
      bids: this.bids,
      asks: this.asks,
      updated_at: this.updatedAt,
    };
  }

  #validateLevels(levels) {
    if (!Array.isArray(levels)) {
      throw new TypeError('Orderbook levels must be an array');
    }

    return levels.map((level) => {
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
  }
}
