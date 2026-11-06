'use strict';
// Independent re-verification of the forensics numbers, plus the take-profit
// policy counterfactual. Reads state.json + cf_results.json only. Read-only.
const fs = require('fs');
const B = 'C:/Users/darri/Kai-Mirror/';
const st = JSON.parse(fs.readFileSync(B + 'state.json', 'utf8'));
const cf = JSON.parse(fs.readFileSync(B + 'cf_results.json', 'utf8'));
const FEE = (p) => 0.07 * p * (1 - p);

const trades = st.all_no.dry.closed_trades.filter(
  (t) => cf[t.market_id] && cf[t.market_id].result && cf[t.market_id].close
);
const won = (t) => {
  const r = cf[t.market_id].result;
  return (t.side || 'NO') === 'NO' ? r === 'no' : r === 'yes';
};
const qty = (t) => t.quantity || 1;

// Exit as actually booked (real Kalshi fee on both legs; settlement exit is free).
function realExit(t) {
  const q = qty(t), e = t.entry_price, x = t.exit_price;
  if (t.exit_reason === 'settled') return (won(t) ? q : 0) - e * q - FEE(e) * q;
  return x * q - e * q - FEE(e) * q - FEE(x) * q;
}
// Counterfactual: ride to settlement (no exit fee on the settlement leg).
function holdExit(t) {
  const q = qty(t), e = t.entry_price;
  return (won(t) ? q : 0) - e * q - FEE(e) * q;
}

const tot = (g, f) => g.reduce((a, t) => a + f(t), 0);
const line = (k, v) => console.log('  ' + k.padEnd(46) + v);

console.log('=== A. FEES / P&L RECONCILIATION (' + trades.length + ' trades) ===');
let legacy = 0, real = 0, hold = 0;
for (const t of trades) {
  const q = qty(t), e = t.entry_price, x = t.exit_price;
  legacy += x * q - e * q - 0.0175 * x * q;      // legacy model: exit fee only
  real += realExit(t);
  hold += holdExit(t);
}
line('legacy fee model (exit fee only)', '$' + legacy.toFixed(2));
line('REAL fee model (both legs)', '$' + real.toFixed(2));
line('hold-to-settlement counterfactual', '$' + hold.toFixed(2));

console.log('\n=== B. P&L BY EXIT REASON (real fees) ===');
const byR = {};
for (const t of trades) (byR[t.exit_reason] = byR[t.exit_reason] || []).push(t);
console.log('  reason        n      real$     hold$     delta$');
for (const k of Object.keys(byR).sort()) {
  const g = byR[k];
  const r = tot(g, realExit), h = tot(g, holdExit);
  console.log('  ' + k.padEnd(12) + String(g.length).padStart(5) +
    (r.toFixed(2)).padStart(10) + (h.toFixed(2)).padStart(10) + (r - h).toFixed(2).padStart(10));
}

console.log('\n=== C. POLICY COUNTERFACTUAL: keep stop-loss, remove take-profit ===');
let withTP = 0, noTP = 0;
for (const t of trades) {
  const r = realExit(t);
  withTP += r;
  noTP += t.exit_reason === 'take_profit' ? holdExit(t) : r;
}
line('WITH take-profit (as actually booked)', '$' + withTP.toFixed(2));
line('WITHOUT take-profit (those ride to settle)', '$' + noTP.toFixed(2));
line('=> removing take-profit changes P&L by', '$' + (noTP - withTP).toFixed(2));

console.log('\n=== D. WHY: the take-profit cohort split by settlement outcome ===');
const tp = byR.take_profit || [];
const tpWin = tp.filter(won), tpLose = tp.filter((t) => !won(t));
let forgone = 0, saved = 0;
for (const t of tpWin) forgone += (1 - t.exit_price) * qty(t);
for (const t of tpLose) saved += realExit(t) - holdExit(t);
line('TP trades that later SETTLED AS A WIN (n)', tpWin.length);
line('  hold would have paid $1.00/contract; forgone $', forgone.toFixed(2));
line('TP trades that later SETTLED AS A LOSS (n)', tpLose.length);
line('  hold would have paid $0.00/contract; TP saved $', saved.toFixed(2));
line('  net of exit fees', '$' + (saved - forgone).toFixed(2));

console.log('\n=== E. REALIZED P&L BY DAY (real fee model) ===');
const byDay = {};
for (const t of trades) (byDay[t.opened_at.slice(0, 10)] = byDay[t.opened_at.slice(0, 10)] || []).push(t);
console.log('  date          n     real$    hold$   exit reasons');
for (const d of Object.keys(byDay).sort()) {
  const g = byDay[d];
  const rs = {};
  for (const t of g) rs[t.exit_reason] = (rs[t.exit_reason] || 0) + 1;
  console.log('  ' + d + String(g.length).padStart(5) +
    (tot(g, realExit).toFixed(2)).padStart(10) + (tot(g, holdExit).toFixed(2)).padStart(10) +
    '   ' + JSON.stringify(rs));
}

console.log('\n=== F. CALIBRATION: predicted fair vs realized win rate ===');
const bins = {};
for (const t of trades) {
  const f = t.fair_at_entry;
  if (f == null) continue;
  const k = (Math.floor(f * 10) / 10).toFixed(1);
  (bins[k] = bins[k] || { n: 0, w: 0, p: 0 }).n++;
  bins[k].w += won(t) ? 1 : 0;
  bins[k].p += f;
}
console.log('  fair bin      n   mean predicted   realized');
for (const k of Object.keys(bins).sort()) {
  const g = bins[k];
  if (g.n < 20) continue;
  console.log('  ' + k.padEnd(10) + String(g.n).padStart(5) +
    (g.p / g.n).toFixed(3).padStart(15) + (100 * g.w / g.n).toFixed(1).padStart(10) + '%');
}

console.log('\n=== G. DATA INTEGRITY: entries opened after market close ===');
const bad = trades.filter((t) => (Date.parse(cf[t.market_id].close) - Date.parse(t.opened_at)) / 1000 < 0);
line('count', bad.length);
line('dates involved', JSON.stringify([...new Set(bad.map((t) => t.opened_at.slice(0, 10)))]));
line('newest trade in book', trades.map((t) => t.opened_at).sort().pop());

console.log('\n=== H. CONCENTRATION: positions opened per single market, per day ===');
const dm = {};
for (const t of trades) {
  const k = t.opened_at.slice(0, 10) + '|' + t.market_id;
  dm[k] = (dm[k] || 0) + 1;
}
const byDayMax = {};
for (const k of Object.keys(dm)) {
  const d = k.split('|')[0];
  byDayMax[d] = Math.max(byDayMax[d] || 0, dm[k]);
}
for (const d of Object.keys(byDayMax).sort()) {
  console.log('  ' + d + String(byDayMax[d]).padStart(6) + '  max on one market');
}
