// quant_validate.js - validation metrics for probability models vs realised outcomes.
// P(side wins) evaluated per TRADE, scored against the realised settlement outcome.
const fs = require('fs'), path = require('path');
const D = __dirname;
const D0 = JSON.parse(fs.readFileSync(path.join(D, 'quant_data.json'), 'utf8'));
const allTrades = D0.trades.filter(t => t.has_result && t.tau >= 0 && t.P && t.P.w60 && t.P.w60.settle !== undefined);
const markets = D0.markets;

function erf(x) { const z = Math.abs(x), t = 1 / (1 + 0.5 * z); const a = t * Math.exp(-z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))); return x >= 0 ? a : -a; }
const Phi = z => 0.5 * (1 + erf(z / Math.SQRT2));
const mean = a => a.reduce((s, v) => s + v, 0) / a.length;
const sd = a => { const m = mean(a); return Math.sqrt(a.reduce((s, v) => s + (v - m) ** 2, 0) / (a.length - 1)); };
const clip = p => Math.min(1 - 1e-6, Math.max(1e-6, p));

// AUC (Mann-Whitney, ties counted at 0.5)
function aucOf(scores, ys) {
  const pos = [], neg = [];
  scores.forEach((s, i) => (ys[i] ? pos : neg).push(s));
  let gt = 0, eq = 0, lt = 0;
  for (const a of pos) for (const b of neg) { if (a > b) gt++; else if (a === b) eq++; else lt++; }
  const n = pos.length * neg.length;
  return { auc: (gt + 0.5 * eq) / n, gt, eq, lt, npos: pos.length, nneg: neg.length };
}

function metrics(rows, pf) {
  const ps = [], ys = [];
  for (const r of rows) {
    const p = pf(r);
    if (p === null || p === undefined || !isFinite(p)) continue;
    ps.push(clip(p));
    ys.push(((r.won !== undefined && r.won !== null) ? r.won : r.won_yes) ? 1 : 0);
  }
  const N = ps.length;
  if (!N) return { n: 0 };
  const briers = ps.map((p, i) => (p - ys[i]) ** 2);
  const lls = ps.map((p, i) => -(ys[i] ? Math.log(p) : Math.log(1 - p)));
  const A = aucOf(ps, ys);
  const auc = A.auc;
  const Q1 = auc / (2 - auc), Q2 = 2 * auc * auc / (1 + auc);
  const aucSe = Math.sqrt((auc * (1 - auc) + (A.npos - 1) * (Q1 - auc * auc) + (A.nneg - 1) * (Q2 - auc * auc)) / (A.npos * A.nneg));
  const bins = [];
  for (let b = 0; b < 10; b++) bins.push({ lo: b / 10, hi: (b + 1) / 10, n: 0, sum: 0, hits: 0 });
  for (let i = 0; i < N; i++) { const b = Math.min(9, Math.floor(ps[i] * 10)); bins[b].n++; bins[b].sum += ps[i]; bins[b].hits += ys[i]; }
  const cal = bins.filter(b => b.n > 0).map(b => ({ lo: b.lo, hi: b.hi, n: b.n, pred: b.sum / b.n, obs: b.hits / b.n,
    se: Math.sqrt(Math.max(1e-12, (b.hits / b.n) * (1 - b.hits / b.n) / b.n)) }));
  const wr = mean(ys);
  return {
    n: N, win_rate: wr, win_rate_se: Math.sqrt(wr * (1 - wr) / N),
    mean_pred: mean(ps),
    brier: mean(briers), brier_se: sd(briers) / Math.sqrt(N),
    logloss: mean(lls), logloss_se: sd(lls) / Math.sqrt(N),
    auc, auc_se: aucSe, n_pos: A.npos, n_neg: A.nneg,
    calibration: cal,
  };
}

// paired difference in Brier/logloss with a MARKET-CLUSTER bootstrap SE
function pairedDiff(rows, pfA, pfB) {
  const byMkt = new Map();
  for (const r of rows) {
    if (!byMkt.has(r.market_id)) byMkt.set(r.market_id, []);
    byMkt.get(r.market_id).push(r);
  }
  const perMkt = [...byMkt.values()].map(rs => {
    let db = 0, dl = 0, k = 0;
    for (const r of rs) {
      const a = pfA(r), b = pfB(r);
      if (!isFinite(a) || !isFinite(b)) continue;
      const pa = clip(a), pb = clip(b);
      const y = r.won ? 1 : 0;
      db += (pa - y) ** 2 - (pb - y) ** 2;
      dl += -(y ? Math.log(pa) : Math.log(1 - pa)) + (y ? Math.log(pb) : Math.log(1 - pb));
      k++;
    }
    return { db: db / k, dl: dl / k, k };
  });
  const tot = perMkt.reduce((s, m) => s + m.k, 0);
  const dB = perMkt.reduce((s, m) => s + m.db * m.k, 0) / tot;
  const dL = perMkt.reduce((s, m) => s + m.dl * m.k, 0) / tot;
  let rnd = 12345; const rand = () => (rnd = (rnd * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  const B = 2000, sb = [], sl = [];
  for (let i = 0; i < B; i++) {
    let ab = 0, al = 0, kk = 0;
    for (let j = 0; j < perMkt.length; j++) { const m = perMkt[rand() * perMkt.length | 0]; ab += m.db * m.k; al += m.dl * m.k; kk += m.k; }
    sb.push(ab / kk); sl.push(al / kk);
  }
  return { d_brier: dB, d_brier_se: sd(sb), d_logloss: dL, d_logloss_se: sd(sl), n_markets: perMkt.length };
}

// ---------------- models ----------------
const BASIS = D0.basis;
// z-score for P(YES) under the known-strike settlement model (positive => YES likely)
function zYes(t, variant, w) {
  const s = t.sigma['w' + w]; if (!s) return null;
  const lnX = Math.log(t.X), lnR = Math.log(t.floor);
  let mean, sd;
  if (variant === 'naive') { sd = s * Math.sqrt(Math.max(t.tau, 1)); mean = lnX; }
  else if (t.tau >= 60) { sd = s * Math.sqrt(t.tau - 40); mean = lnX; }
  else { sd = s * Math.sqrt(t.tau / 3); mean = lnX; }
  if (variant === 'nobasis') { /* no index-basis correction */ }
  else if (variant === 'permkt') { mean = lnX + Math.log(1 + t.basis_typ_bps / 1e4); }
  else mean = lnX + BASIS[t.asset].log_mean;
  return (mean - lnR) / sd;
}
function probYes(t, variant, w, k) {
  const z = zYes(t, variant, w);
  return z === null ? null : Phi(z / (k || 1));
}
const sideProb = (t, pYes) => t.side === 'NO' ? 1 - pYes : pYes;
const mkModel = (variant, w, k) => t => { const p = probYes(t, variant, w, k); return p === null ? null : sideProb(t, p); };
const MODELS = {
  market_implied: t => t.entry_price,
  lane_fair: t => (t.side === 'NO' ? t.fair_at_entry : 1 - t.fair_at_entry),
};
// z-inflation factor that maximises log-likelihood
function fitK(rows, variant, w) {
  let best = null;
  for (let k = 0.2; k <= 8.0001; k += 0.02) {
    let ll = 0, n = 0;
    for (const t of rows) {
      const p = probYes(t, variant, w, k); if (p === null) continue;
      const q = clip(t.side === 'NO' ? 1 - p : p);
      ll += t.won ? Math.log(q) : Math.log(1 - q); n++;
    }
    if (!best || ll > best.ll) best = { k: Math.round(k * 100) / 100, ll, n };
  }
  return best;
}
const SUBSETS = {
  ALL: t => true,
  tau_ge_60: t => t.tau >= 60,
  tau_lt_120: t => t.tau < 120,
  tau_lt_60: t => t.tau < 60,
  tau_120_300: t => t.tau >= 120 && t.tau < 300,
  tau_300_900: t => t.tau >= 300 && t.tau < 900,
  tau_ge_900: t => t.tau >= 900,
  side_NO: t => t.side === 'NO',
  side_YES: t => t.side === 'YES',
};
const W = 'w60';
const out = { subsets: {}, paired: {}, sigma_windows: {}, calibration_k: {}, market_level: {} };
const kAll = fitK(allTrades, 'settle', W);
const kNear = fitK(allTrades.filter(SUBSETS.tau_lt_120), 'settle', W);
out.calibration_k = { k_all: kAll, k_tau_lt_120: kNear };
const VARIANTS = [
  ['naive_spot', 'naive', 1],
  ['settle60', 'settle', 1],
  ['settle60_nobasis', 'nobasis', 1],
  ['settle60_permkt', 'permkt', 1],
  ['settle60_cal', 'settle', kAll.k],
];
for (const [sname, sf] of Object.entries(SUBSETS)) {
  const rows = allTrades.filter(sf);
  const res = {};
  for (const [mn, mf] of Object.entries(MODELS)) res[mn] = metrics(rows, mf);
  for (const [mn, variant, k] of VARIANTS) res[mn] = metrics(rows, mkModel(variant, W, k));
  out.subsets[sname] = { n: rows.length, models: res };
  out.paired[sname] = {};
  for (const [mn, variant, k] of VARIANTS) out.paired[sname][mn] = pairedDiff(rows, mkModel(variant, W, k), t => MODELS.market_implied(t));
  out.paired[sname].lane_fair = pairedDiff(rows, t => MODELS.lane_fair(t), t => MODELS.market_implied(t));
}
for (const w of ['w30', 'w60', 'w120']) {
  out.sigma_windows[w] = {
    ALL: metrics(allTrades, mkModel('settle', w, 1)),
    tau_lt_120: metrics(allTrades.filter(SUBSETS.tau_lt_120), mkModel('settle', w, 1)),
    tau_ge_60: metrics(allTrades.filter(SUBSETS.tau_ge_60), mkModel('settle', w, 1)),
  };
}

