// quant_chk.js - clean replication of the market-level horizon AUC (independent of validate.js)
const fs = require('fs'), path = require('path');
const D = __dirname;
const D0 = JSON.parse(fs.readFileSync(path.join(D, 'quant_data.json'), 'utf8'));
const markets = D0.markets, basis = D0.basis;
const ASSET = { BTC: 'BTC-USD', ETH: 'ETH-USD', XRP: 'XRP-USD', HYPE: 'HYPE-USD' };
const candles = {};
for (const a of Object.keys(ASSET)) {
  const raw = JSON.parse(fs.readFileSync(path.join(D, `cand_${a.toLowerCase()}.json`), 'utf8'));
  const m = new Map();
  for (const k of Object.keys(raw)) m.set(Number(raw[k][0]), { low: raw[k][1], high: raw[k][2], open: raw[k][3], close: raw[k][4] });
  candles[ASSET[a]] = m;
}
function erf(x) { const z = Math.abs(x), t = 1 / (1 + 0.5 * z); const a = t * Math.exp(-z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))); return x >= 0 ? a : -a; }
const Phi = z => 0.5 * (1 + erf(z / Math.SQRT2));
function auc(scores, ys) {
  const pos = [], neg = [];
  scores.forEach((s, i) => (ys[i] ? pos : neg).push(s));
  let s = 0, ties = 0;
  for (const a of pos) for (const b of neg) { if (a > b) s++; else if (a === b) ties++; else s--; }
  return (s + 0.5 * ties) / (pos.length * neg.length);
}
function sigmaAt(C, barT, w) {
  let sum = 0, k = 0;
  for (let i = 0; i < w; i++) { const b1 = C.get(barT - i * 60), b0 = C.get(barT - (i + 1) * 60); if (!b1 || !b0) break; sum += (Math.log(b1.close) - Math.log(b0.close)) ** 2; k++; }
  return k > 0 ? Math.sqrt(sum / (60 * k)) : null;
}
const avg = a => a.reduce((x, y) => x + y, 0) / a.length;
for (const H of [30, 60, 120, 300, 600, 900]) {
  const ps = [], ys = [], ds = [], recs = [];
  for (const m of markets) {
    if (!m.hasBars) continue;
    const C = candles[ASSET[m.asset]];
    const barT = Math.floor((m.closeT - H) / 60) * 60 - 60;
    const bar = C.get(barT); if (!bar) continue;
    const s = sigmaAt(C, barT, 60); if (!s) continue;
    const sd = H >= 60 ? s * Math.sqrt(H - 40) : s * Math.sqrt(H / 3);
    const d = (Math.log(bar.close) - Math.log(m.floor)) * 1e4;
    ps.push(Phi((Math.log(bar.close) + basis[m.asset].log_mean - Math.log(m.floor)) / sd));
    ys.push(m.result === 'yes'); ds.push(d);
    recs.push({ t: m.ticker, res: m.result, d, asset: m.asset });
  }
  const my = ds.filter((_, i) => ys[i]), mn = ds.filter((_, i) => !ys[i]);
  const avg = a => a.reduce((x, y) => x + y, 0) / a.length;
  console.log(`H=${String(H).padStart(4)} n=${ps.length} AUC(P)=${auc(ps, ys).toFixed(4)} AUC(d)=${auc(ds, ys).toFixed(4)} meanP=${avg(ps).toFixed(4)} meanD_yes=${avg(my).toFixed(2)} meanD_no=${avg(mn).toFixed(2)}`);
  if (H === 900 || H === 300) {
    const sorted = [...recs].sort((a, b) => Math.abs(b.d) - Math.abs(a.d)).slice(0, 8);
    console.log('   largest |d| outliers:', sorted.map(r => `${r.t}:${r.d.toFixed(1)}(${r.res})`).join(' '));
    const keep = recs.filter(r => Math.abs(r.d) < 10);
    console.log(`   trimmed(|d|<10bps): n=${keep.length} AUC(d)=${auc(keep.map(r => r.d), keep.map(r => r.res === 'yes')).toFixed(4)}`);
  }
}
