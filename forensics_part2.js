'use strict';
const fs = require('fs');
const B = 'C:/Users/darri/Kai-Mirror/';
const state = JSON.parse(fs.readFileSync(B + 'state.json', 'utf8'));
const cf = JSON.parse(fs.readFileSync(B + 'cf_results.json', 'utf8'));
const trades = state.all_no.dry.closed_trades;
const FEE = (p) => 0.07 * p * (1 - p);
const rows = [];
for (const t of trades) {
  const m = cf[t.market_id];
  rows.push({
    mid: t.market_id, side: t.side || 'NO', entry: t.entry_price, exit: t.exit_price,
    qty: t.quantity || 1, cost: t.cost, proceeds: t.proceeds,
    actual: t.pnl, reason: t.exit_reason, fair: t.fair_at_entry, feePaid: t.fee_paid,
    opened_at: t.opened_at, closed_at: t.closed_at,
    close: m ? m.close : null, floor: m ? m.floor : null, result: m ? m.result : null,
    won: m ? ((t.side || 'NO') === 'NO' ? m.result === 'no' : m.result === 'yes') : null,
  });
}
const known = rows.filter(r => r.result);
// ---- P&L RECONCILIATION: find why recorded pnl != recomputed pnl ----
console.log('=== P&L RECONCILIATION ===');
// Hypothesis: legacy trades used fee_entry = 0 and fee_exit = 1.75% of PROCEEDS.
let legOld = 0, legNew = 0, legHybrid = 0, legNone = 0;
const sample = known.slice(0, 400);
for (const r of sample) {
  const ex = r.exit;
  if (ex == null) continue;
  const proceeds = ex * r.qty;
  const fOld = 0.0175 * proceeds;
  const fNew = 0.07 * r.entry * (1 - r.entry) * r.qty + 0.07 * ex * (1 - ex) * r.qty;
  const fHyb = 0.07 * r.entry * (1 - r.entry) * r.qty + 0.0175 * proceeds;
  const fNone = 0;
  const target = r.actual;
  if (Math.abs(proceeds - r.entry * r.qty - fOld - target) < 0.006) legOld++;
  if (Math.abs(proceeds - r.entry * r.qty - fNew - target) < 0.006) legNew++;
  if (Math.abs(proceeds - r.entry * r.qty - fHyb - target) < 0.006) legHybrid++;
  if (Math.abs(proceeds - r.entry * r.qty - fNone - target) < 0.006) legNone++;
}
console.log('sample n=' + sample.length + ' with exit_price');
console.log('  legacy (no entry fee, 1.75% of proceeds at exit) matches:', legOld);
console.log('  new Kalshi 0.07*p*(1-p) both legs matches:', legNew);
console.log('  hybrid matches:', legHybrid, ' zero-fee matches:', legNone);

// ---- RECOMPUTE EVERYTHING UNDER THE REAL FEE MODEL ----
const FEEV = (p) => 0.07 * p * (1 - p);
// mode 'hold' = the counterfactual where EVERY position is held to settlement.
// The previous version ignored `mode` entirely, so the "hold-to-settlement"
// column silently equalled the "realistic" column (both $47.76 in fp2_out.txt).
function pnlReal(r, mode) {
  if (mode === 'hold') {
    // Settlement pays $1 on a win and $0 on a loss; the settlement exit is fee-free.
    const gross = r.won ? r.qty : 0;
    return gross - r.entry * r.qty - FEEV(r.entry) * r.qty;
  }
  const ex = (r.reason === 'settled' || (r.reason === 'time_decay' && r.exit == null)) ? null : r.exit;
  if (ex == null) {  // already held to settlement
    const gross = r.won ? r.qty : 0;
    return gross - r.entry * r.qty - FEEV(r.entry) * r.qty;   // settlement exit is fee-free
  }
  const gross = ex * r.qty;
  const fee = FEEV(r.entry) * r.qty + FEEV(ex) * r.qty;
  return gross - r.entry * r.qty - fee;
}
// The hold-to-settlement counterfactual used by every comparison below.
const heldPnl = (r) => pnlReal(r, 'hold');
const sumReal = (g, f) => g.reduce((a, r) => a + f(r), 0);
console.log('\n=== ALL 1759 TRADES UNDER THE REAL FEE MODEL ===');
console.log('recorded (legacy fees)  $' + sumReal(known, r => r.actual).toFixed(2));
console.log('realistic (both legs)   $' + sumReal(known, r => pnlReal(r)).toFixed(2));
console.log('hold-to-settlement      $' + sumReal(known, r => pnlReal(r, 'hold')).toFixed(2));
console.log('\nby exit reason (realistic fees):');
const byR = {};
for (const r of known) (byR[r.reason] = byR[r.reason] || []).push(r);
for (const k of Object.keys(byR).sort()) {
  const g = byR[k];
  console.log(' ', k.padEnd(12), 'n', String(g.length).padStart(5),
    'legacy$', sumReal(g, r => r.actual).toFixed(2).padStart(9),
    'real$', sumReal(g, r => pnlReal(r)).toFixed(2).padStart(9),
    'hold$', sumReal(g, r => pnlReal(r, 'hold')).toFixed(2).padStart(9));
}

// ---- NEGATIVE TAU: entries opened after market close ----
console.log('\n=== NEGATIVE/INVALID TAU ===');
const badTau = known.filter(r => r.close && (Date.parse(r.close) - Date.parse(r.opened_at)) / 1000 < 0);
console.log('trades opened AFTER their market close:', badTau.length);
for (const r of badTau.slice(0, 10)) {
  console.log(' ', r.mid, 'opened', r.opened_at, 'close', r.close,
    'tau', ((Date.parse(r.close) - Date.parse(r.opened_at)) / 1000).toFixed(0) + 's', 'reason', r.reason, 'pnl', r.actual);
}
const noClose = known.filter(r => !r.close);
console.log('trades with no close time:', noClose.length);

// ---- MULTIPLE POSITIONS PER MARKET (overlapping exposure) ----
console.log('\n=== OVERLAPPING EXPOSURE ===');
const byMkt = {};
for (const r of known) (byMkt[r.mid] = byMkt[r.mid] || []).push(r);
const counts = Object.values(byMkt).map(g => g.length);
console.log('positions per market: max', Math.max(...counts), 'mean', (counts.reduce((a, b) => a + b, 0) / counts.length).toFixed(2));
const multi = Object.entries(byMkt).filter(([k, g]) => g.length > 3).slice(0, 5);
for (const [k, g] of multi) console.log(' ', k, 'n', g.length, 'sides', g.map(x => x.side + '@' + x.entry).join(' '));

const withFair = known.filter(r => r.fair != null);

console.log('=== 2. HOLD-TO-SETTLEMENT vs CURRENT EXITS ===');
const byReason = {};
for (const r of known) (byReason[r.reason] = byReason[r.reason] || []).push(r);
for (const k of Object.keys(byReason).sort()) {
  const g = byReason[k];
  console.log(k.padEnd(12), 'n', String(g.length).padStart(5),
    'actual', g.reduce((a, r) => a + r.actual, 0).toFixed(2).padStart(9),
    'held', g.reduce((a, r) => a + heldPnl(r), 0).toFixed(2).padStart(9),
    'delta', (g.reduce((a, r) => a + heldPnl(r) - r.actual, 0)).toFixed(2).padStart(9));
}
console.log('TOTAL'.padEnd(12), 'n', String(known.length).padStart(5),
  'actual', known.reduce((a, r) => a + r.actual, 0).toFixed(2).padStart(9),
  'held', known.reduce((a, r) => a + heldPnl(r), 0).toFixed(2).padStart(9),
  'delta', known.reduce((a, r) => a + heldPnl(r) - r.actual, 0).toFixed(2).padStart(9));

console.log('\n=== 3. STOP-LOSS COUNTERFACTUAL ===');
const sl = known.filter(r => r.reason === 'stop_loss');
console.log('stop_loss n', sl.length, 'would have won if held:', sl.filter(r => r.won).length,
  'actual', sl.reduce((a, r) => a + r.actual, 0).toFixed(2),
  'held', sl.reduce((a, r) => a + heldPnl(r), 0).toFixed(2));
const slb = {};
for (const r of sl) { const b = Math.floor(r.entry * 10) / 10; (slb[b] = slb[b] || []).push(r); }
for (const b of Object.keys(slb).sort((a, x) => a - x)) {
  const g = slb[b]; const w = g.filter(r => r.won).length;
  console.log(' entry', Number(b).toFixed(1), 'n', String(g.length).padStart(4),
    'held_win%', ((w / g.length) * 100).toFixed(1).padStart(6),
    'actual', g.reduce((a, r) => a + r.actual, 0).toFixed(2).padStart(8),
    'held', g.reduce((a, r) => a + heldPnl(r), 0).toFixed(2).padStart(8));
}

console.log('\n=== 4. entry timing (tau to close) ===');
const tb = {};
for (const r of known) {
  if (!r.close) continue;
  const tau = (Date.parse(r.close) - Date.parse(r.opened_at)) / 1000;
  if (!isFinite(tau) || tau < 0) continue;
  const b = tau < 60 ? 'a<60s' : tau < 180 ? 'b60-180s' : tau < 300 ? 'c180-300s' : tau < 600 ? 'd300-600s' : 'e>600s';
  (tb[b] = tb[b] || []).push(r);
}
for (const b of Object.keys(tb).sort()) {
  const g = tb[b]; const w = g.filter(r => r.won).length;
  const imp = g.reduce((a, r) => a + r.entry, 0) / g.length;
  console.log(' tau', b.padEnd(10), 'n', String(g.length).padStart(4),
    'win%', ((w / g.length) * 100).toFixed(1).padStart(6),
    'implied', imp.toFixed(3), 'edge_pp', ((w / g.length - imp) * 100).toFixed(1).padStart(6),
    'held$', g.reduce((a, r) => a + heldPnl(r), 0).toFixed(2).padStart(8));
}

console.log('\n=== 5. lane model (fair_at_entry) informativeness ===');
const fb = {};
for (const r of withFair) { const b = Math.floor(r.fair * 10) / 10; (fb[b] = fb[b] || []).push(r); }
for (const b of Object.keys(fb).sort((a, x) => a - x)) {
  const g = fb[b]; const w = g.filter(r => r.won).length;
  const imp = g.reduce((a, r) => a + r.entry, 0) / g.length;
  const fair = g.reduce((a, r) => a + r.fair, 0) / g.length;
  console.log(' fair', Number(b).toFixed(1), 'n', String(g.length).padStart(4),
    'model_win%', (fair * 100).toFixed(1).padStart(6),
    'realized', ((w / g.length) * 100).toFixed(1).padStart(6) + '%',
    'edge_pp', ((fair - imp) * 100).toFixed(1).padStart(6),
    'held$', g.reduce((a, r) => a + heldPnl(r), 0).toFixed(2).padStart(8));
}

console.log('\n=== 7. EXIT CONSISTENCY AUDIT ===');
const tp = known.filter(r => r.reason === 'take_profit');
const sl2 = known.filter(r => r.reason === 'stop_loss');
for (const [nm, g] of [['take_profit', tp], ['stop_loss', sl2]]) {
  // Direction matters, and it differs per exit reason:
  //   take_profit on a NO must exit ABOVE entry (a "profit" that lost money is wrong).
  //   stop_loss  on a NO must exit BELOW entry (a "stop" that made money is wrong).
  // The earlier version read `r.exit_price` (undefined on these rows) so every
  // comparison was false and this audit silently reported "0 wrong-side exits".
  let wrong = 0;
  for (const r of g) {
    if (r.exit == null) continue;
    const gainOnNo = r.side === 'NO' ? (r.exit > r.entry) : (r.exit < r.entry);
    const wantedGain = nm === 'take_profit';
    if (gainOnNo !== wantedGain) wrong++;
  }
  console.log(nm, 'n', g.length, 'exits with the WRONG sign vs entry:', wrong,
    '(' + (100 * wrong / g.length).toFixed(1) + '%)');
}
// recompute pnl from scratch and compare
let mism = 0, maxd = 0;
for (const r of known) {
  const ex = r.reason === 'settled' ? null : r.exit;
  const feeEx = ex == null ? 0 : 0.07 * ex * (1 - ex) * r.qty;
  const feeEn = 0.07 * r.entry * (1 - r.entry) * r.qty;
  const gross = ex == null ? (r.won ? 1 * r.qty : 0) : ex * r.qty;
  const pnl = gross - r.entry * r.qty - feeEn - feeEx;
  const d = Math.abs(pnl - r.actual);
  if (d > 0.011) { mism++; if (d > maxd) maxd = d; }
}
console.log('pnl recompute mismatches (>1.1c):', mism, 'of', known.length, ' maxdiff', maxd.toFixed(4));

// settled group detail (the ALL NO winners the user cited)
const st = known.filter(r => r.reason === 'settled');
console.log('\n--- settled winners n=' + st.length + ' ---');
console.log('entry hist', JSON.stringify(st.reduce((a, x) => { const k = x.entry; a[k] = (a[k] || 0) + 1; return a; }, {})));
console.log('total pnl', st.reduce((a, r) => a + r.actual, 0).toFixed(2),
  'avg', (st.reduce((a, r) => a + r.actual, 0) / st.length).toFixed(3));
const stTau = st.map(r => (Date.parse(r.close) - Date.parse(r.opened_at)) / 1000).filter(x => isFinite(x));
console.log('tau at entry: min', Math.min(...stTau).toFixed(0), 'max', Math.max(...stTau).toFixed(0),
  'mean', (stTau.reduce((a, b) => a + b, 0) / stTau.length).toFixed(0));

console.log('\n=== 8. QUANTILE VIEW: P&L vs hold, by entry price decile ===');
const sorted = [...known].sort((a, b) => a.entry - b.entry);
const D = 10, per = Math.floor(sorted.length / D);
console.log('bucket      n   avgEntry  win%   actual$   held$   delta$');
for (let i = 0; i < D; i++) {
  const g = sorted.slice(i * per, (i + 1) * per);
  const w = g.filter(r => r.won).length;
  const act = g.reduce((a, r) => a + r.actual, 0);
  const held = g.reduce((a, r) => a + heldPnl(r), 0);
  console.log('d' + i, String(g.length).padStart(5),
    (g.reduce((a, r) => a + r.entry, 0) / g.length).toFixed(3).padStart(9),
    ((w / g.length) * 100).toFixed(1).padStart(6),
    act.toFixed(2).padStart(9), held.toFixed(2).padStart(8), (held - act).toFixed(2).padStart(8));
}

console.log('\n=== 9. WHAT ACTUALLY MADE MONEY (by exit reason x side) ===');
for (const side of ['NO', 'YES']) {
  for (const rsn of ['take_profit', 'stop_loss', 'time_decay', 'settled']) {
    const g = known.filter(r => r.side === side && r.reason === rsn);
    if (!g.length) continue;
    const act = g.reduce((a, r) => a + r.actual, 0);
    const held = g.reduce((a, r) => a + heldPnl(r), 0);
    console.log(side.padEnd(4), rsn.padEnd(12), 'n', String(g.length).padStart(5),
      'win%', ((g.filter(r => r.won).length / g.length) * 100).toFixed(1).padStart(6),
      'actual$', act.toFixed(2).padStart(9), 'held$', held.toFixed(2).padStart(9),
      'delta$', (held - act).toFixed(2).padStart(9));
  }
}

console.log('\n=== 10. THE THREE CITED WINNERS (reconstruct) ===');
for (const r of st.filter(x => x.actual > 0.2)) {
  console.log(r.mid, 'side', r.side, 'entry', r.entry, 'exit', r.exit,
    'qty', r.qty, 'pnl', r.actual, 'fair', r.fair,
    'tau', ((Date.parse(r.close) - Date.parse(r.opened_at)) / 1000).toFixed(0) + 's',
    'opened', r.opened_at);
}

