'use strict';

// Spot-move family. Both strategies read the last 5-minute spot move (ln(S_t / S_t-5m)).
// moveSide(): the side that pays if spot keeps moving in the direction of that move.
//   'above' markets: spot up -> YES;  'below' markets: spot up -> NO.
//   'between' bins have no monotone link to spot direction -> no signal (null).

function moveSide(ctx) {
  if (ctx.yes_direction === 0) return null;
  const up = ctx.spot_move_5m > 0;
  return up === (ctx.yes_direction > 0) ? 'YES' : 'NO';
}

module.exports = [
  {
    name: 'momentum_with_spot',
    description: 'Buy the side that wins if the last 5-min spot move continues: fair>=0.55 and ask<=0.60. Taker at ask.',
    decide(ctx) {
      if (!Number.isFinite(ctx.spot_move_5m) || ctx.spot_move_5m === 0) return null;
      const side = moveSide(ctx);
      if (!side) return null;
      const fair = side === 'YES' ? ctx.fair_yes : ctx.fair_no;
      const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
      if (fair < 0.55 || ask > 0.60) return null;
      return { side, qty: 1, limit_price: ask };
    },
  },
  {
    name: 'fade_extreme_move',
    description: 'If |5-min spot move| > 2*sigma_5m (sigma_5m = sigma_per_sec*sqrt(300)), buy the side opposite the move if ask<=0.45. Taker at ask.',
    decide(ctx) {
      const mv = ctx.spot_move_5m;
      if (!Number.isFinite(mv) || !(ctx.sigma_5m > 0)) return null;
      if (Math.abs(mv) <= 2 * ctx.sigma_5m) return null;
      const favoured = moveSide(ctx);
      if (!favoured) return null;
      const side = favoured === 'YES' ? 'NO' : 'YES';
      const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
      if (ask > 0.45) return null;
      return { side, qty: 1, limit_price: ask };
    },
  },
];
