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
    mid: t.market_id, side: t.side || 'NO', entry: t.entry_price, qty: t.quantity || 1,
    actual: t.pnl, reason: t.exit_reason, fair: t.fair_at_entry,
    opened_at: t.opened_at, closed_at: t.closed_at,
    close: m ? m.close : null, floor: m ? m.floor : null,
    result: m ? m.result : null,
    won: m ? ((t.side || 'NO') === 'NO' ? m.result === 'no' : m.result === 'yes') : null,
  });
}
const known = rows.filter(r => r.result);
const heldPnl = (r) => r.won ? (1 - r.entry - FEE(r.entry)) * r.qty : -(r.entry + FEE(r.entry)) * r.qty;
console.log('total trades', rows.length, 'with settlement', known.length,
  'unique markets', new Set(known.map(r => r.mid)).size);

console.log('\n=== 1. CALIBRATION by side x entry price ===');
console.log('side bucket      n  wins  winrate implied edge_pp  EV_held EV_actual');
const buckets = {};
for (const r of known) {
  const b = Math.floor(r.entry * 20) / 20;
  const k = r.side + '@' + b.toFixed(2);
  (buckets[k] = buckets[k] || []).push(r);
}
for (const k of Object.keys(buckets).sort()) {
  const g = buckets[k];
  const wins = g.filter(r => r.won).length;
  const wr = wins / g.length;
  const implied = g.reduce((a, r) => a + r.entry, 0) / g.length;
  const evHeld = g.reduce((a, r) => a + heldPnl(r), 0) / g.length;
  const evAct = g.reduce((a, r) => a + r.actual, 0) / g.length;
  console.log(k.split('@')[0].padEnd(4), k.split('@')[1].padEnd(6),
    String(g.length).padStart(5), String(wins).padStart(6), (wr * 100).toFixed(1).padStart(7) + '%',
    implied.toFixed(3).padStart(8), ((wr - implied) * 100).toFixed(1).padStart(8),
    evHeld.toFixed(4).padStart(8), evAct.toFixed(4).padStart(9));
}
const sumImp = known.reduce((a, r) => a + r.entry, 0);
const sumWin = known.filter(r => r.won).length;
console.log('\nOVERALL implied', (sumImp / known.length).toFixed(4),
  'realized', (sumWin / known.length).toFixed(4),
  'gap_pp', ((sumWin / known.length - sumImp / known.length) * 100).toFixed(2),
  'n=', known.length);

console.log('\n--- calibration curve ---');
const cb = {};
for (const r of known) { const b = Math.floor(r.entry * 10) / 10; (cb[b] = cb[b] || []).push(r); }
for (const b of Object.keys(cb).sort((a, x) => a - x)) {
  const g = cb[b]; const wr = g.filter(r => r.won).length / g.length;
  const imp = g.reduce((a, r) => a + r.entry, 0) / g.length;
  console.log(' implied', Number(b).toFixed(1), 'n', String(g.length).padStart(5),
    'realized', (wr * 100).toFixed(1) + '%', 'diff_pp', ((wr - imp) * 100).toFixed(1));
}
module.exports = { known, heldPnl, FEE, buckets, withFair: known.filter(r => r.fair != null) };
