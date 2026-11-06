'use strict';

// Filter family: the deep-value rule (fair>=0.60, ask in [0.05,0.35], edge>=0.02) gated by
//   - a volatility-regime filter (vol_regime_calm_value)
//   - a time-of-day filter (utc_session_value)
// and a price-band rule (longshot_band) that trades a different part of the price curve.

function valueSide(ctx) {
  for (const side of ['YES', 'NO']) {
    const fair = side === 'YES' ? ctx.fair_yes : ctx.fair_no;
    const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
    if (fair >= 0.60 && ask >= 0.05 && ask <= 0.35 && fair - ask >= 0.02) return { side, limit_price: ask };
  }
  return null;
}

module.exports = [
  {
    name: 'vol_regime_calm_value',
    description: 'Deep-value rule only in a calm regime: sigma_15m <= sigma_60m (short-term realized vol not above long-term). Taker at ask.',
    decide(ctx) {
      if (!(ctx.sigma_per_sec_60m > 0)) return null;
      if (ctx.sigma_per_sec > ctx.sigma_per_sec_60m) return null;
      const v = valueSide(ctx);
      return v ? { side: v.side, qty: 1, limit_price: v.limit_price } : null;
    },
  },
  {
    name: 'utc_session_value',
    description: 'Deep-value rule only during 13:00-21:00 UTC (US cash hours, time-of-day filter). Taker at ask.',
    decide(ctx) {
      const hour = new Date(ctx.ts * 1000).getUTCHours();
      if (hour < 13 || hour >= 21) return null;
      const v = valueSide(ctx);
      return v ? { side: v.side, qty: 1, limit_price: v.limit_price } : null;
    },
  },
  {
    name: 'longshot_band',
    description: 'Price-band rule: buy the cheap tail when ask in [0.03,0.15] and fair_side - ask >= 0.05. Taker at ask.',
    decide(ctx) {
      let best = null;
      for (const side of ['YES', 'NO']) {
        const fair = side === 'YES' ? ctx.fair_yes : ctx.fair_no;
        const ask = side === 'YES' ? ctx.quotes.yes_ask : ctx.quotes.no_ask;
        if (ask < 0.03 || ask > 0.15) continue;
        const edge = fair - ask;
        if (edge < 0.05) continue;
        if (!best || edge > best.edge) best = { side, ask, edge };
      }
      return best ? { side: best.side, qty: 1, limit_price: best.ask } : null;
    },
  },
];
