'use strict';

// Near-expiry family: in the final 5 minutes, when the market is statistically decided, buy the favoured side.

module.exports = [
  {
    name: 'near_expiry_certainty',
    description: 'tau<=300s and |z|>=2: buy the favoured side (fair>=0.5) if its ask<=0.95. Taker at ask.',
    decide(ctx) {
      if (ctx.tau_sec > 300) return null;
      if (!(Math.abs(ctx.z) >= 2)) return null;
      const side = ctx.fair_yes >= 0.5 ? 'YES' : 'NO';
      const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
      if (ask > 0.95) return null;
      return { side, qty: 1, limit_price: ask };
    },
  },
];
