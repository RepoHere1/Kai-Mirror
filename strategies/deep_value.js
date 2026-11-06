'use strict';

// Deep-value family: buy the side the model rates >= 60% when it is cheap (ask <= 0.35).
// Taker entry at the ask: limit_price = ask. Edge = fair - ask must be >= 0.02.

const MIN_FAIR = 0.60;
const MIN_ASK = 0.05;
const MAX_ASK = 0.35;
const MIN_EDGE = 0.02;

function valueDecision(ctx, side) {
  const fair = side === 'YES' ? ctx.fair_yes : ctx.fair_no;
  const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
  if (fair < MIN_FAIR || ask < MIN_ASK || ask > MAX_ASK) return null;
  if (fair - ask < MIN_EDGE) return null;
  return { side, qty: 1, limit_price: ask };
}

module.exports = [
  {
    name: 'deep_no_value',
    description: 'Buy NO when fair_no>=0.60, 0.05<=no_ask<=0.35, edge(fair_no-no_ask)>=0.02. Taker at ask.',
    decide(ctx) {
      return valueDecision(ctx, 'NO');
    },
  },
  {
    name: 'deep_yes_value',
    description: 'Mirror of deep_no_value: buy YES when fair_yes>=0.60, 0.05<=yes_ask<=0.35, edge>=0.02. Taker at ask.',
    decide(ctx) {
      return valueDecision(ctx, 'YES');
    },
  },
];
