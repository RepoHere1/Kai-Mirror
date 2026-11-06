// quant_chk2.js - independent check of the H=900 (window start) relationship
const fs = require('fs'), path = require('path');
const D = __dirname;
const D0 = JSON.parse(fs.readFileSync(path.join(D, 'quant_data.json'), 'utf8'));
const ASSET = { BTC: 'BTC-USD', ETH: 'ETH-USD', XRP: 'XRP-USD', HYPE: 'HYPE-USD' };
const candles = {};
for (const a of Object.keys(ASSET)) {
  const raw = JSON.parse(fs.readFileSync(path.join(D, `cand_${a.toLowerCase()}.json`), 'utf8'));
  const m = new Map();
  for (const k of Object.keys(raw)) m.set(Number(raw[k][0]), { low: raw[k][1], high: raw[k][2], open: raw[k][3], close: raw[k][4] });
  candles[ASSET[a]] = m;
}
const rows = [];
for (const m of D0.markets) {
  if (!m.hasBars) continue;
  const C = candles[ASSET[m.asset]];
  const barWs = C.get(m.wsT - 60);          // bar covering [ws-60, ws]
  const barWsClose = C.get(m.wsT);           // bar covering [ws, ws+60]
  const setBar = C.get(m.closeT - 60);       // settlement minute
  if (!barWs || !barWsClose || !setBar) continue;
  const setTyp = (setBar.high + setBar.low + setBar.close) / 3;
  rows.push({
    t: m.ticker, asset: m.asset, res: m.result, floor: m.floor,
    xWs: barWs.close, xWsPlus: barWsClose.close, setTyp,
    refTyp: m.ref_typ,
    d_ws_bps: (barWs.close - m.floor) / m.floor * 1e4,
    d_ref_bps: (m.ref_typ - m.floor) / m.floor * 1e4,
    move_bps: (setTyp - m.refTyp) / m.refTyp * 1e4,
  });
}
const yes = rows.filter(r => r.res === 'yes'), no = rows.filter(r => r.res === 'no');
const avg = a => a.reduce((s, v) => s + v, 0) / a.length;
const med = a => { const s = [...a].sort((x, y) => x - y); return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2; };
console.log(`n=${rows.length} yes=${yes.length} no=${no.length}`);
console.log(`d_ws   (price@ws - floor): yes mean=${avg(yes.map(r => r.d_ws_bps)).toFixed(2)} med=${med(yes.map(r => r.d_ws_bps)).toFixed(2)} | no mean=${avg(no.map(r => r.d_ws_bps)).toFixed(2)} med=${med(no.map(r => r.d_ws_bps)).toFixed(2)}`);
console.log(`d_ref  (refAvg  - floor): yes mean=${avg(yes.map(r => r.d_ref_bps)).toFixed(2)} med=${med(yes.map(r => r.d_ref_bps)).toFixed(2)} | no mean=${avg(no.map(r => r.d_ref_bps)).toFixed(2)} med=${med(no.map(r => r.d_ref_bps)).toFixed(2)}`);
console.log(`move   (setTyp - refTyp): yes mean=${avg(yes.map(r => r.move_bps)).toFixed(2)} med=${med(yes.map(r => r.move_bps)).toFixed(2)} | no mean=${avg(no.map(r => r.move_bps)).toFixed(2)} med=${med(no.map(r => r.move_bps)).toFixed(2)}`);
console.log(`\nrank check: fraction of (yes,no) pairs with d_yes > d_no`);
const ys = yes.map(r => r.d_ws_bps), ns = no.map(r => r.d_ws_bps);
let gt = 0, eq = 0, lt = 0;
for (const a of ys) for (const b of ns) { if (a > b) gt++; else if (a === b) eq++; else lt++; }
console.log(`  gt=${gt} eq=${eq} lt=${lt} -> AUC=${((gt + 0.5 * eq) / (gt + eq + lt)).toFixed(4)}`);
// same for the realised move
console.log('\nfirst 12 rows:');
for (const r of rows.slice(0, 12)) console.log(`${r.t} res=${r.res} floor=${r.floor} xWs=${r.xWs.toFixed(4)} refTyp=${r.refTyp.toFixed(4)} d_ws=${r.d_ws_bps.toFixed(2)} d_ref=${r.d_ref_bps.toFixed(2)} move=${r.move_bps.toFixed(2)}`);

// replicate the quant_chk.js H=900 computation exactly and compare
{
  const ds = [], ys = [];
  for (const m of D0.markets) {
    if (!m.hasBars) continue;
    const C = candles[ASSET[m.asset]];
    const barT = Math.floor((m.closeT - 900) / 60) * 60 - 60;
    const bar = C.get(barT); if (!bar) continue;
    const d = (Math.log(bar.close) - Math.log(m.floor)) * 1e4;
    ds.push(d); ys.push(m.result === 'yes');
  }
  const pos = [], neg = [];
  ds.forEach((s, i) => (ys[i] ? pos : neg).push(s));
  let s = 0, ties = 0;
  for (const a of pos) for (const b of neg) { if (a > b) s++; else if (a === b) ties++; else s--; }
  console.log(`\nreplication AUC=${((s + 0.5 * ties) / (pos.length * neg.length)).toFixed(4)} n=${ds.length} npos=${pos.length} nneg=${neg.length}`);
  console.log('first 6 d:', ds.slice(0, 6).map(x => x.toFixed(2)).join(' '), 'ys:', ys.slice(0, 6).map(x => x ? 'Y' : 'n').join(''));
  console.log('d at index of first 12 rows from the other calc:', rows.slice(0, 12).map(r => r.d_ws_bps.toFixed(2)).join(' '));
}

let g2 = 0, e2 = 0, l2 = 0;
for (const a of yes.map(r => r.move_bps)) for (const b of no.map(r => r.move_bps)) { if (a > b) g2++; else if (a === b) e2++; else l2++; }
console.log(`  move AUC=${((g2 + 0.5 * e2) / (g2 + e2 + l2)).toFixed(4)}`);
// per asset
for (const a of Object.keys(ASSET)) {
  const yy = rows.filter(r => r.asset === a && r.res === 'yes'), nn = rows.filter(r => r.asset === a && r.res === 'no');
  let g = 0, e = 0, l = 0;
  for (const x of yy.map(r => r.d_ws_bps)) for (const y of nn.map(r => r.d_ws_bps)) { if (x > y) g++; else if (x === y) e++; else l++; }
  console.log(`  ${a}: n=${yy.length + nn.length} AUC(d_ws)=${((g + 0.5 * e) / (g + e + l)).toFixed(4)} yesMean=${avg(yy.map(r => r.d_ws_bps)).toFixed(2)} noMean=${avg(nn.map(r => r.d_ws_bps)).toFixed(2)}`);
}
console.log('\nfirst 12 rows:');
for (const r of rows.slice(0, 12)) console.log(`${r.t} res=${r.res} floor=${r.floor} xWs=${r.xWs.toFixed(4)} refTyp=${r.refTyp.toFixed(4)} d_ws=${r.d_ws_bps.toFixed(2)} d_ref=${r.d_ref_bps.toFixed(2)} move=${r.move_bps.toFixed(2)}`);
