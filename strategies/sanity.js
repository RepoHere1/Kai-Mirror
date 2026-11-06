'use strict';

// SANITY CHECK ONLY. Not a trading idea.
// Buys NO at the first decision minute where a NO offer exists. Its win rate must equal the
// share of NO settlements among the markets it traded (checks settlement mapping and payoff logic).

module.exports = [
  {
    name: 'sanity_always_no',
    description: 'SANITY CHECK ONLY: buy NO at the first decision minute with an offered NO ask (one entry per market).',
    max_entries_per_market: 1,
    decide(ctx) {
      const ask = ctx.quotes.no_ask;
      if (!(ask > 0 && ask < 1)) return null;
      return { side: 'NO', qty: 1, limit_price: ask };
    },
  },
];
