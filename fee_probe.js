// Temporary probe: resolve the authoritative Kalshi fee model for KXBTC15M.
const k = require('./kalshi.js');
const out = [];
function log(...a) { out.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' ')); }
function unwrap(r) { return (r && r.data && typeof r.data === 'object' && !Array.isArray(r.data)) ? r.data : r; }

(async () => {
  for (const t of ['KXBTC15M', 'KXETH15M', 'KXHYPE15M']) {
    try {
      const raw = await k.get('/series/' + t);
      log('SERIES_RAW', t, JSON.stringify(raw).slice(0, 1200));
    } catch (e) { log('SERIES_ERR', t, e.message); }
  }
  try {
    const fc = await k.get('/series/fee_changes', { query: { series_ticker: 'KXBTC15M' } });
    log('SERIES_FEE_CHANGES', JSON.stringify(fc).slice(0, 800));
  } catch (e) { log('SERIES_FEE_CHANGES_ERR', e.message); }

  try {
    const mk = await k.get('/markets', { query: { series_ticker: 'KXBTC15M', status: 'open', limit: 5 } });
    const m = (mk.markets || [])[0] || {};
    log('MARKET_FEE_FIELDS', JSON.stringify(Object.keys(m).filter(x => /fee/i.test(x))));
    log('MARKET_FEE_VALUES', JSON.stringify({
      ticker: m.ticker, fee_type: m.fee_type, fee_multiplier: m.fee_multiplier,
      fee_multiplier_cents: m.fee_multiplier_cents,
      series_fee_multiplier: m.series_fee_multiplier,
    }));
  } catch (e) { log('MARKET_ERR', e.message); }

  // Ground truth: real fills carry the fees Kalshi actually charged.
  // The crypto series live on exchange_index 2; other positions may be on shard 0.
  const all = [];
  for (const acct of ['live', 'dry']) {
    for (const ei of [0, 2]) {
      try {
        const f = unwrap(await k.get('/portfolio/fills', { query: { limit: 200, exchange_index: ei } }, acct));
        for (const x of (f.fills || [])) all.push(Object.assign({ _acct: acct, _ei: ei }, x));
      } catch (e) { log('FILLS_ERR', acct, 'ei=' + ei, e.message.slice(0, 120)); }
    }
  }
  const seen = new Set();
  const uniq = all.filter(x => { if (seen.has(x.fill_id)) return false; seen.add(x.fill_id); return true; });
  log('TOTAL_FILLS', uniq.length);

  // ---- Fee model discrimination on TAKER fills (makers pay 0) ----------------
  const RATE = 0.07;
  const ceilTo = (x, step) => Math.ceil(x / step - 1e-9) * step;
  const floorTo = (x, step) => Math.floor(x / step + 1e-9) * step;
  const roundTo = (x, step) => Math.round(x / step) * step;

  const takers = uniq.filter(x => x.is_taker === true && Number(x.fee_cost) > 0);
  log('TAKER_FILLS_WITH_FEE', takers.length);

  const variants = {
    'quad_ceil4':  (p, q) => ceilTo(RATE * p * (1 - p) * q, 1e-4),
    'quad_floor4': (p, q) => floorTo(RATE * p * (1 - p) * q, 1e-4),
    'quad_round4': (p, q) => roundTo(RATE * p * (1 - p) * q, 1e-4),
    'quad_ceil6':  (p, q) => ceilTo(RATE * p * (1 - p) * q, 1e-6),
    'quad_round6': (p, q) => roundTo(RATE * p * (1 - p) * q, 1e-6),
    'min_ceil4':   (p, q) => ceilTo(RATE * Math.min(p, 1 - p) * q, 1e-4),
    'min_ceil6':   (p, q) => ceilTo(RATE * Math.min(p, 1 - p) * q, 1e-6),
    'lin_ceil4':   (p, q) => ceilTo(RATE * p * q, 1e-4),
  };
  const tally = {};
  for (const x of takers) {
    const qty = Number(x.count_fp);
    const yp = Number(x.yes_price_dollars), np = Number(x.no_price_dollars);
    const fee = Number(x.fee_cost);
    if (!isFinite(qty) || !isFinite(yp) || !isFinite(np)) continue;
    // the price of the side we traded: symmetric under P<->1-P for P*(1-P)
    const traded = x.side === 'yes' ? yp : np;
    for (const [name, fn] of Object.entries(variants)) {
      const v = fn(traded, qty);
      const ok = Math.abs(v - fee) < 5e-7;
      const k2 = name;
      tally[k2] = tally[k2] || { hit: 0, tot: 0 };
      tally[k2].tot++; if (ok) tally[k2].hit++;
    }
  }
  log('--- variant scores (taker fills, fee>0) ---');
  for (const k of Object.keys(tally).sort((a, b) => tally[b].hit / tally[b].tot - tally[a].hit / tally[a].tot)) {
    const t = tally[k];
    log('MODEL', k, t.hit + '/' + t.tot, (t.hit / t.tot).toFixed(3));
  }
  // Residual detail for the leading candidates.
  for (const name of ['quad_ceil4', 'quad_floor4', 'quad_round4', 'min_ceil4']) {
    log('--- residuals for ' + name + ' ---');
    let shown = 0;
    for (const x of takers) {
      const qty = Number(x.count_fp), yp = Number(x.yes_price_dollars), np = Number(x.no_price_dollars);
      const fee = Number(x.fee_cost);
      if (!isFinite(qty) || !isFinite(yp) || !isFinite(np)) continue;
      const traded = x.side === 'yes' ? yp : np;
      const v = variants[name](traded, qty);
      const d = v - fee;
      if (Math.abs(d) >= 5e-7 && shown < 12) {
        shown++;
        log('  RESID', JSON.stringify({
          t: x.ticker, side: x.side, qty: x.count_fp, traded,
          fee_cost: x.fee_cost, model: Number(v.toFixed(8)), diff: Number(d.toFixed(8)),
        }));
      }
    }
  }
  // How many takers have fee_cost that is NOT a multiple of 1e-4?
  let notTick = 0, ticks = new Set();
  for (const x of takers) {
    const f = Number(x.fee_cost);
    if (Math.abs(f / 1e-4 - Math.round(f / 1e-4)) > 1e-6) notTick++;
    ticks.add(Math.round(f * 1e4) / 1e4);
  }
  log('NON_1e4_QUANTISED_FEES', notTick, 'distinct_fee_values', ticks.size);
  // Zero-fee takers (should be none) and maker fees
  const makers = uniq.filter(x => x.is_taker !== true);
  log('MAKER_FILLS', makers.length, 'with_fee>0', makers.filter(x => Number(x.fee_cost) > 0).length);
})().catch(e => log('FATAL', e.message)).finally(() => {
  require('fs').writeFileSync('fee_probe_out.txt', out.join('\n') + '\n');
});
