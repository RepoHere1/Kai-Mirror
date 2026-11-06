// Standalone fee-model resolver using the recorded Kalshi fills.
const k = require('./kalshi.js');
const out = [];
function log(...a) { out.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' ')); }
function unwrap(r) { return (r && r.data && typeof r.data === 'object' && !Array.isArray(r.data)) ? r.data : r; }
const RATE = 0.07;
const ceilTo = (x, s) => Math.ceil(x / s - 1e-9) * s;

(async () => {
  const all = [];
  for (const ei of [0, 2]) {
    try {
      const f = unwrap(await k.get('/portfolio/fills', { query: { limit: 200, exchange_index: ei } }, 'live'));
      for (const x of (f.fills || [])) all.push(Object.assign({ _ei: ei }, x));
    } catch (e) { log('ERR', ei, e.message.slice(0, 100)); }
  }
  const seen = new Set();
  const uniq = all.filter(x => { if (seen.has(x.fill_id)) return false; seen.add(x.fill_id); return true; });
  const takers = uniq.filter(x => x.is_taker === true && Number(x.fee_cost) > 0);
  log('FILLS', uniq.length, 'TAKERS_WITH_FEE', takers.length);

  // Hypothesis A: fee = ceil_1e-4(0.07 * P*(1-P) * qty)
  // Hypothesis B: fee = ceil_1e-6(0.07 * P*(1-P) * qty)
  // Hypothesis C: fee = ceil_1e-4(0.07 * min(P,1-P) * qty)
  const H = {
    'A_quad_ceil4': (p, q) => ceilTo(RATE * p * (1 - p) * q, 1e-4),
    'B_quad_ceil6': (p, q) => ceilTo(RATE * p * (1 - p) * q, 1e-6),
    'C_min_ceil4':  (p, q) => ceilTo(RATE * Math.min(p, 1 - p) * q, 1e-4),
  };
  const tally = {};
  for (const x of takers) {
    const qty = Number(x.count_fp), yp = Number(x.yes_price_dollars), np = Number(x.no_price_dollars);
    const fee = Number(x.fee_cost);
    const traded = x.side === 'yes' ? yp : np;
    for (const [n, fn] of Object.entries(H)) {
      tally[n] = tally[n] || { hit: 0, tot: 0 };
      tally[n].tot++;
      if (Math.abs(fn(traded, qty) - fee) < 5e-7) tally[n].hit++;
    }
  }
  for (const n of Object.keys(tally)) log('HYP', n, tally[n].hit + '/' + tally[n].tot);

  // Every distinct (qty, price) taker fill, sorted by price, with both models.
  const rows = takers.map(x => {
    const qty = Number(x.count_fp), yp = Number(x.yes_price_dollars), np = Number(x.no_price_dollars);
    const traded = x.side === 'yes' ? yp : np;
    const raw = RATE * traded * (1 - traded) * qty;
    return { p: traded, qty, fee: Number(x.fee_cost), raw,
             A: ceilTo(raw, 1e-4), B: ceilTo(raw, 1e-6), t: x.ticker, side: x.side };
  }).sort((a, b) => a.p - b.p);
  log('--- all taker fills (p, qty, actual, raw, A, B) ---');
  for (const r of rows) {
    const okA = Math.abs(r.A - r.fee) < 5e-7, okB = Math.abs(r.B - r.fee) < 5e-7;
    log('ROW', JSON.stringify({ p: r.p, qty: r.qty, fee: r.fee, raw: +r.raw.toFixed(9), A: r.A, B: r.B, okA, okB, side: r.side }));
  }

  // LANE-RELEVANT: single contract, prices this lane trades.
  log('--- lane price table (qty=1) ---');
  for (const p of [0.90, 0.92, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99]) {
    const raw = RATE * p * (1 - p);
    log('LANE', 'p=' + p, 'raw=' + raw.toFixed(9),
      'ceil4=' + ceilTo(raw, 1e-4).toFixed(9),
      'ceil6=' + ceilTo(raw, 1e-6).toFixed(9),
      'MIN_ceil4=' + ceilTo(RATE * Math.min(p, 1 - p), 1e-4).toFixed(9),
      'OLD_cent_ceil=' + (Math.ceil(RATE * Math.min(p, 1 - p) * 100 - 1e-9) / 100));
  }
})().catch(e => log('FATAL', e.message)).finally(() => {
  require('fs').writeFileSync('fee_probe_out.txt', out.join('\n') + '\n');
});
