'use strict';

// Ladder family: daily threshold ladders (series ending in "D"). Several strikes share one event;
// the engine marks the strike nearest spot as ctx.is_nearest_strike (computed from spot at minute T).

module.exports = [
  {
    name: 'ladder_nearest_money',
    description: 'Series ending in D: on the strike nearest spot, buy the side with fair in [0.35,0.65] and ask<=0.55 (larger edge if both qualify). Taker at ask.',
    decide(ctx) {
      if (!/D$/.test(ctx.series)) return null;
      if (!ctx.is_nearest_strike) return null;
      let best = null;
      for (const side of ['YES', 'NO']) {
        const fair = side === 'YES' ? ctx.fair_yes : ctx.fair_no;
        const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
        if (fair < 0.35 || fair > 0.65 || ask > 0.55) continue;
        const edge = fair - ask;
        if (!best || edge > best.edge) best = { side, ask, edge };
      }
      if (!best) return null;
      return { side: best.side, qty: 1, limit_price: best.ask };
    },
  },
];
