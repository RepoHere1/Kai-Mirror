// quant_prep.js - build the analysis dataset for the known-strike validation.
// READ-ONLY on: cf_results.json, state.json, cand_*.json. Writes quant_data.json, quant_basis.json.
const fs = require('fs');
const path = require('path');
const D = __dirname;

// ---------- math helpers ----------
function erf(x) {
  const z = Math.abs(x), t = 1 / (1 + 0.5 * z);
  const ans = t * Math.exp(-z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 +
    t * (-0.18628806 + t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 +
      t * 0.17087277)))))))));
  return x >= 0 ? ans : -ans;
}
const Phi = z => 0.5 * (1 + erf(z / Math.SQRT2));
const LOG = x => Math.log(x);

// ---------- load ----------
const cf = JSON.parse(fs.readFileSync(path.join(D, 'cf_results.json'), 'utf8'));
const SNAP = path.join(D, 'quant_state_snapshot.json');
const state = JSON.parse(fs.readFileSync(fs.existsSync(SNAP) ? SNAP : path.join(D, 'state.json'), 'utf8'));
const trades = state.all_no.dry.closed_trades || [];

const ASSET = { BTC: 'BTC-USD', ETH: 'ETH-USD', XRP: 'XRP-USD', HYPE: 'HYPE-USD' };
const candles = {};
for (const a of Object.keys(ASSET)) {
  const raw = JSON.parse(fs.readFileSync(path.join(D, `cand_${a.toLowerCase()}.json`), 'utf8'));
  const m = new Map();
  for (const k of Object.keys(raw)) {
    const r = raw[k];
    m.set(Number(r[0]), { t: Number(r[0]), low: r[1], high: r[2], open: r[3], close: r[4], vol: r[5] });
  }
  candles[ASSET[a]] = m;
}

// ---------- markets ----------
const assetOf = t => (t.startsWith('KXBTC') ? 'BTC' : t.startsWith('KXETH') ? 'ETH' : t.startsWith('KXXRP') ? 'XRP' :
  t.startsWith('KXHYPE') ? 'HYPE' : null);

const markets = [];
for (const ticker of Object.keys(cf)) {
  const m = cf[ticker];
  const asset = assetOf(ticker);
  if (!asset) continue;
  const closeT = Date.parse(m.close) / 1000;
  const wsT = closeT - 900;
  const refBarT = wsT - 60;
  const setBarT = closeT - 60;
  const C = candles[ASSET[asset]];
  const refBar = C.get(refBarT), setBar = C.get(setBarT);
  const rec = {
    ticker, asset, close: m.close, closeT, wsT, refBarT, setBarT,
    floor: m.floor, result: m.result, status: m.status,
    hasBars: !!(refBar && setBar),
  };
  if (refBar) {
    rec.ref_typ = (refBar.high + refBar.low + refBar.close) / 3;
    rec.ref_ohlc = (refBar.open + refBar.high + refBar.low + refBar.close) / 4;
    rec.ref_close = refBar.close;
    rec.ref_range_bps = (refBar.high - refBar.low) / refBar.close * 1e4;
  }
  if (setBar) {
    rec.set_typ = (setBar.high + setBar.low + setBar.close) / 3;
    rec.set_close = setBar.close;
    rec.set_range_bps = (setBar.high - setBar.low) / setBar.close * 1e4;
  }
  if (rec.hasBars) {
    rec.basis_typ_bps = (rec.floor / rec.ref_typ - 1) * 1e4;
    rec.basis_close_bps = (rec.floor / rec.ref_close - 1) * 1e4;
    rec.basis_ohlc_bps = (rec.floor / rec.ref_ohlc - 1) * 1e4;
    rec.proxy_up = rec.set_typ >= rec.ref_typ;
    rec.proxy_up_close = rec.set_close >= rec.ref_close;
    rec.official_up = m.result === 'yes';
    rec.proxy_agrees = rec.proxy_up === rec.official_up;
    rec.proxy_agrees_close = rec.proxy_up_close === rec.official_up;
  }
  markets.push(rec);
}
const MKT = new Map(markets.map(m => [m.ticker, m]));

// ---------- per-asset basis ----------
const basis = {};
for (const a of Object.keys(ASSET)) {
  const rows = markets.filter(m => m.asset === a && m.hasBars);
  const vals = rows.map(m => m.basis_typ_bps);
  const mean = vals.reduce((s, v) => s + v, 0) / vals.length;
  const sd = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / (vals.length - 1));
  const sorted = [...vals].sort((x, y) => x - y);
  const med = sorted.length % 2 ? sorted[(sorted.length - 1) / 2] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2;
  basis[a] = {
    n: vals.length, mean_bps: mean, se_bps: sd / Math.sqrt(vals.length), sd_bps: sd, median_bps: med,
    q25_bps: sorted[Math.floor(0.25 * (sorted.length - 1))], q75_bps: sorted[Math.floor(0.75 * (sorted.length - 1))],
    min_bps: sorted[0], max_bps: sorted[sorted.length - 1],
    mean_bps_close: rows.reduce((s, m) => s + m.basis_close_bps, 0) / rows.length,
    median_bps_close: [...rows.map(m => m.basis_close_bps)].sort((x, y) => x - y)[Math.floor(rows.length / 2)],
    mean_bps_ohlc: rows.reduce((s, m) => s + m.basis_ohlc_bps, 0) / rows.length,
    median_bps_ohlc: [...rows.map(m => m.basis_ohlc_bps)].sort((x, y) => x - y)[Math.floor(rows.length / 2)],
    mean_ref_range_bps: rows.reduce((s, m) => s + m.ref_range_bps, 0) / rows.length,
    proxy_acc: rows.filter(m => m.proxy_agrees).length / rows.length,
    proxy_acc_close: rows.filter(m => m.proxy_agrees_close).length / rows.length,
    proxy_n: rows.length,
  };
  basis[a].log_mean = LOG(1 + basis[a].mean_bps / 1e4);
  basis[a].log_median = LOG(1 + basis[a].median_bps / 1e4);
}


// ---------- trades ----------
const WS = [30, 60, 120];
const out = [];
let missingMarket = 0, noBars = 0, negTau = 0;
for (const tr of trades) {
  const m = MKT.get(tr.market_id);
  const rec = {
    id: tr.id, market_id: tr.market_id, side: (tr.side || 'NO').toUpperCase(),
    entry_price: tr.entry_price, exit_price: tr.exit_price, qty: tr.quantity,
    pnl_rec: tr.pnl, exit_reason: tr.exit_reason, fair_at_entry: tr.fair_at_entry,
    fee_paid: tr.fee_paid, opened_at: tr.opened_at, closed_at: tr.closed_at,
    has_result: false,
  };
  if (!m) { rec.skip = 'no_market_meta'; missingMarket++; out.push(rec); continue; }
  rec.asset = m.asset; rec.floor = m.floor; rec.basis_typ_bps = m.basis_typ_bps; rec.closeT = m.closeT; rec.result = m.result;
  const tOpen = Date.parse(tr.opened_at) / 1000;
  rec.t_open = tOpen;
  rec.tau = m.closeT - tOpen;
  if (rec.tau < 0) negTau++;
  const C = candles[ASSET[m.asset]];
  const barT = Math.floor(tOpen / 60) * 60 - 60;   // last fully-known 1-min bar at t_open
  rec.barT = barT;
  const bar = C.get(barT);
  if (!bar) { rec.skip = 'no_bar'; noBars++; out.push(rec); continue; }
  rec.X = bar.close;
  rec.X_typ = (bar.high + bar.low + bar.close) / 3;
  rec.sigma = {};
  for (const w of WS) {
    let sum = 0, k = 0;
    for (let i = 0; i < w; i++) {
      const b1 = C.get(barT - i * 60), b0 = C.get(barT - (i + 1) * 60);
      if (!b1 || !b0) break;
      sum += (LOG(b1.close) - LOG(b0.close)) ** 2; k++;
    }
    rec.sigma['w' + w] = k > 0 ? Math.sqrt(sum / (60 * k)) : null;
    rec.sigma['n' + w] = k;
  }
  rec.won = m.result === 'yes' ? rec.side === 'YES' : rec.side === 'NO';
  rec.has_result = true;
  const bAsset = basis[m.asset];
  const lnR = LOG(m.floor), lnX = LOG(rec.X);
  const mk = (meanLog, sd) => sd > 0 ? Phi((meanLog - lnR) / sd) : 0.5;
  const P = {};
  for (const w of WS) {
    const s = rec.sigma['w' + w];
    const tag = 'w' + w;
    if (!s) { P[tag] = {}; continue; }
    if (rec.tau >= 60) {
      P[tag] = {
        settle: mk(lnX + bAsset.log_mean, s * Math.sqrt(rec.tau - 40)),
        settle_nobasis: mk(lnX, s * Math.sqrt(rec.tau - 40)),
        settle_permkt: mk(lnX + LOG(1 + m.basis_typ_bps / 1e4), s * Math.sqrt(rec.tau - 40)),
        naive: mk(lnX, s * Math.sqrt(rec.tau)),
        naive_basis: mk(lnX + bAsset.log_mean, s * Math.sqrt(rec.tau)),
        settle_typ: mk(LOG(rec.X_typ) + bAsset.log_mean, s * Math.sqrt(rec.tau - 40)),
      };
    } else {
      const sdB = s * Math.sqrt(rec.tau / 3);
      const sdDamp = s * (rec.tau / 60) * Math.sqrt(rec.tau / 3);
      const p = {};
      p.settle = mk(lnX + bAsset.log_mean, sdB);
      p.settle_nobasis = mk(lnX, sdB);
      p.settle_permkt = mk(lnX + LOG(1 + m.basis_typ_bps / 1e4), sdB);
      p.settle_damped = mk(lnX + bAsset.log_mean, sdDamp);
      p.naive = mk(lnX, s * Math.sqrt(Math.max(rec.tau, 1)));
      p.naive_basis = mk(lnX + bAsset.log_mean, s * Math.sqrt(Math.max(rec.tau, 1)));
      const setBar = C.get(m.setBarT);
      if (setBar) {   // ORACLE: look-ahead, upper bound on the known-strike idea
        const aPart = (setBar.high + setBar.low + setBar.close) / 3;
        const A = (60 - rec.tau) / 60 * LOG(aPart) + (rec.tau / 60) * lnX;
        p.settle_oracle = mk(A + bAsset.log_mean, sdDamp);
      }
      P[tag] = p;
    }
  }
  rec.P = P;
  out.push(rec);
}

fs.writeFileSync(path.join(D, 'quant_data.json'), JSON.stringify({
  generated: new Date().toISOString(),
  n_trades: out.length, n_markets: markets.length,
  missing_market_meta: missingMarket, no_bar: noBars, negative_tau: negTau,
  basis, markets, trades: out,
}));
fs.writeFileSync(path.join(D, 'quant_basis.json'), JSON.stringify({ basis, markets }, null, 1));

console.log('trades', out.length, 'markets', markets.length, 'noMeta', missingMarket, 'noBar', noBars, 'negTau', negTau);
for (const a of Object.keys(ASSET)) {
  const b = basis[a];
  console.log(`${a}: n=${b.n} basis mean=${b.mean_bps.toFixed(2)}bps se=${b.se_bps.toFixed(2)} med=${b.median_bps.toFixed(2)} sd=${b.sd_bps.toFixed(2)} | exchange-proxy outcome acc=${(b.proxy_acc * 100).toFixed(1)}%`);
}
