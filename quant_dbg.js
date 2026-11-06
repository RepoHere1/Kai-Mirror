const fs = require('fs'), path = require('path');
const D = __dirname;
const D0 = JSON.parse(fs.readFileSync(path.join(D, 'quant_data.json'), 'utf8'));
const markets = D0.markets;
const ASSET = { BTC: 'BTC-USD', ETH: 'ETH-USD', XRP: 'XRP-USD', HYPE: 'HYPE-USD' };
const candles = {};
for (const a of Object.keys(ASSET)) {
  const raw = JSON.parse(fs.readFileSync(path.join(D, `cand_${a.toLowerCase()}.json`), 'utf8'));
  const m = new Map();
  for (const k of Object.keys(raw)) m.set(Number(raw[k][0]), { low: raw[k][1], high: raw[k][2], open: raw[k][3], close: raw[k][4] });
  candles[ASSET[a]] = m;
}
const defs = {
  A_set_gt_ref: m => m.set_typ > m.ref_typ,
  B_X30_gt_ref: m => { const C = candles[ASSET[m.asset]]; const b = C.get(m.closeT - 60 - 60); return b ? b.close > m.ref_typ : null; },
  C_X30_gt_floor: m => { const C = candles[ASSET[m.asset]]; const b = C.get(m.closeT - 60 - 60); return b ? b.close > m.floor : null; },
  D_set_gt_floor: m => m.set_typ > m.floor,
  E_set_gt_X30: m => { const C = candles[ASSET[m.asset]]; const b = C.get(m.closeT - 60 - 60); return b ? m.set_typ > b.close : null; },
  F_ref_gt_X30: m => { const C = candles[ASSET[m.asset]]; const b = C.get(m.closeT - 60 - 60); return b ? m.ref_typ > b.close : null; },
  G_set_avg_gt_refavg: m => m.set_typ > m.ref_typ,
};
console.log('ticker | close | floor | ref_typ | set_typ | X30 | result');
let i = 0;
for (const m of markets) {
  if (i++ > 12) break;
  const C = candles[ASSET[m.asset]];
  const b = C.get(m.closeT - 120);
  console.log(`${m.ticker} close=${m.close} floor=${m.floor} ref=${m.ref_typ.toFixed(4)} set=${m.set_typ.toFixed(4)} X30=${b ? b.close.toFixed(4) : 'NA'} result=${m.result}`);
}
console.log('\nagreement of each definition with official result:');
for (const [name, f] of Object.entries(defs)) {
  let n = 0, ok = 0;
  for (const m of markets) { const v = f(m); if (v === null || v === undefined) continue; n++; if (v === (m.result === 'yes')) ok++; }
  console.log(`${name.padEnd(22)} n=${n} agree=${(ok / n * 100).toFixed(1)}%`);
}
// distribution: |floor - ref_typ| vs |floor - set_typ|
const d1 = markets.filter(m => m.hasBars).map(m => Math.abs(m.floor - m.ref_typ) / m.ref_typ * 1e4);
const d2 = markets.filter(m => m.hasBars).map(m => Math.abs(m.floor - m.set_typ) / m.set_typ * 1e4);
const avg = a => a.reduce((s, v) => s + v, 0) / a.length;
console.log(`mean |floor-ref_typ| = ${avg(d1).toFixed(2)}bps ; mean |floor-set_typ| = ${avg(d2).toFixed(2)}bps`);


{
  // Relationship between the exchange path and the official outcome, market by market.
  const rows = [];
  for (const m of markets) {
    if (!m.hasBars) continue;
    const C = candles[ASSET[m.asset]];
    const barWs = C.get(m.wsT - 60), barClose = C.get(m.closeT - 60);
    if (!barWs || !barClose) continue;
    const xWs = barWs.close, setTyp = (barClose.high + barClose.low + barClose.close) / 3;
    rows.push({
      t: m.ticker, res: m.result, floor: m.floor, refTyp: m.ref_typ, setTyp, xWs,
      d1_bps: (xWs - m.floor) / m.floor * 1e4,
      d2_bps: (setTyp - m.refTyp) / m.refTyp * 1e4,
      d3_bps: (xWs - m.refTyp) / m.refTyp * 1e4,
    });
  }
  const yes = rows.filter(r => r.res === 'yes'), no = rows.filter(r => r.res === 'no');
  const avg = a => a.reduce((s, v) => s + v, 0) / a.length;
  console.log(`\n--- path vs outcome (n=${rows.length}) ---`);
  console.log(`mean d1 (price@ws - floor)  yes=${avg(yes.map(r => r.d1_bps)).toFixed(2)}bps  no=${avg(no.map(r => r.d1_bps)).toFixed(2)}bps`);
  console.log(`mean d2 (setTyp - refTyp)    yes=${avg(yes.map(r => r.d2_bps)).toFixed(2)}bps  no=${avg(no.map(r => r.d2_bps)).toFixed(2)}bps`);
  console.log(`mean d3 (price@ws - refTyp)  yes=${avg(yes.map(r => r.d3_bps)).toFixed(2)}bps  no=${avg(no.map(r => r.d3_bps)).toFixed(2)}bps`);
  const agree = (key) => { let n = 0, ok = 0; for (const r of rows) { n++; if ((r[key] > 0) === (r.res === 'yes')) ok++; } return (ok / n * 100).toFixed(1) + '%'; };
  console.log(`sign(d1) vs result: ${agree('d1_bps')} | sign(d2) vs result: ${agree('d2_bps')} | sign(d3) vs result: ${agree('d3_bps')}`);
  console.log('\nsample rows:');
  for (const r of rows.slice(0, 14)) console.log(`${r.t} res=${r.res} floor=${r.floor} xWs=${r.xWs.toFixed(4)} refTyp=${r.refTyp.toFixed(4)} setTyp=${r.setTyp.toFixed(4)} d1=${r.d1_bps.toFixed(2)} d2=${r.d2_bps.toFixed(2)}`);
  // where does the price@ws sit relative to floor, split by outcome?
  const q = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(p * (s.length - 1))]; };
  console.log(`\nd1 quantiles  yes: p25=${q(yes.map(r => r.d1_bps), .25).toFixed(2)} med=${q(yes.map(r => r.d1_bps), .5).toFixed(2)} p75=${q(yes.map(r => r.d1_bps), .75).toFixed(2)}`);
  console.log(`d1 quantiles   no: p25=${q(no.map(r => r.d1_bps), .25).toFixed(2)} med=${q(no.map(r => r.d1_bps), .5).toFixed(2)} p75=${q(no.map(r => r.d1_bps), .75).toFixed(2)}`);
  console.log(`d2 quantiles  yes: p25=${q(yes.map(r => r.d2_bps), .25).toFixed(2)} med=${q(yes.map(r => r.d2_bps), .5).toFixed(2)} p75=${q(yes.map(r => r.d2_bps), .75).toFixed(2)}`);
  console.log(`d2 quantiles   no: p25=${q(no.map(r => r.d2_bps), .25).toFixed(2)} med=${q(no.map(r => r.d2_bps), .5).toFixed(2)} p75=${q(no.map(r => d2 => 0) && q(no.map(r => r.d2_bps), .75)).toFixed(2)}`);
}


{
  const NCDF = z => { // local normal CDF (avoids TDZ on the shared const)
    const erf = x => { const zz = Math.abs(x), t = 1 / (1 + 0.5 * zz); const a = t * Math.exp(-zz * zz - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))); return x >= 0 ? a : -a; };
    return 0.5 * (1 + erf(z / Math.SQRT2));
  };
  console.log('\nbasis object:', JSON.stringify(D0.basis.BTC));
  const H = 30;
  for (const want of ['no', 'yes']) {
    let shown = 0;
    for (const m of markets) {
      if (m.result !== want || shown >= 6) continue;
      const C = candles[ASSET[m.asset]];
      const tH = m.closeT - H, barT = Math.floor(tH / 60) * 60 - 60;
      const bar = C.get(barT);
      let sum = 0, k = 0;
      for (let i = 0; i < 60; i++) { const b1 = C.get(barT - i * 60), b0 = C.get(barT - (i + 1) * 60); if (!b1 || !b0) break; sum += (Math.log(b1.close) - Math.log(b0.close)) ** 2; k++; }
      const s = Math.sqrt(sum / (60 * k));
      const sd = s * Math.sqrt(H / 3);
      const num = Math.log(bar.close) + D0.basis[m.asset].log_mean - Math.log(m.floor);
      console.log(`${m.ticker} res=${m.result} X=${bar.close} floor=${m.floor} num_bps=${(num * 1e4).toFixed(3)} sd_bps=${(sd * 1e4).toFixed(4)} z=${(num / sd).toFixed(3)} p=${NCDF(num / sd).toFixed(4)}`);
      shown++;
    }
  }
}


{
  console.log('\n--- row dump for 10 "no" markets and 6 "yes" markets ---');
  const dump = (m) => {
    const C = candles[ASSET[m.asset]];
    const b = C.get(m.closeT - 120);
    const typ = (b.high + b.low + b.close) / 3;
    const z = (Math.log(b.close) + D0.basis[m.asset].log_mean - Math.log(m.floor)) / (5e-5 * Math.sqrt(10));
    return `${m.ticker} floor=${m.floor} Xclose=${b.close.toFixed(4)} Xtyp=${typ.toFixed(4)} Xopen=${b.open} ln(X/floor)bps=${((Math.log(b.close/m.floor))*1e4).toFixed(2)} z=${z.toFixed(2)} result=${m.result}`;
  };
  let c = 0, c2 = 0;
  for (const m of markets) { if (m.result === 'no' && c++ < 10) console.log(dump(m)); }
  for (const m of markets) { if (m.result === 'yes' && c2++ < 6) console.log(dump(m)); }
}


// --- lag scan: which 1-minute bar/offset actually determines the official result? ---
{
  const barAvg = (C, t) => { const b = C.get(t); return b ? (b.high + b.low + b.close) / 3 : null; };
  console.log('\nlag scan: sign(60s-avg at [close-60*(k+1), close-60k] vs floor) vs official result');
  for (let k = -2; k <= 20; k++) {
    let n = 0, ok = 0;
    for (const m of markets) {
      if (!m.hasBars) continue;
      const C = candles[ASSET[m.asset]];
      const a = barAvg(C, m.closeT - 60 * k);
      if (a === null) continue;
      n++; if ((a > m.floor) === (m.result === 'yes')) ok++;
    }
    console.log(`  k=${String(k).padStart(3)}  agree=${(ok / n * 100).toFixed(1)}%  (n=${n})`);
  }
  console.log('\nlag scan on the REFERENCE side (offset j from window start): floor vs 60s-avg at [ws-60*(j+1), ws-60j]');
  for (let j = -3; j <= 6; j++) {
    let n = 0, d = 0;
    for (const m of markets) {
      if (!m.hasBars) continue;
      const C = candles[ASSET[m.asset]];
      const a = barAvg(C, m.wsT - 60 * j);
      if (a === null) continue;
      n++; d += Math.abs(a - m.floor) / m.floor * 1e4;
    }
    console.log(`  j=${String(j).padStart(3)}  mean |avg-floor| = ${(d / n).toFixed(2)}bps (n=${n})`);
  }
}


// --- direct contingency for the H=30 model, replicating validate.js math ---
function erf(x){const z=Math.abs(x),t=1/(1+0.5*z);const a=t*Math.exp(-z*z-1.26551223+t*(1.00002368+t*(0.37409196+t*(0.09678418+t*(-0.18628806+t*(0.27886807+t*(-1.13520398+t*(1.48851587+t*(-0.82215223+t*0.17087277)))))))));return x>=0?a:1-a;}
const Phi=z=>0.5*(1+erf(z/Math.SQRT2));
{
  const H = 30, rows = [];
  for (const m of markets) {
    if (!m.hasBars) continue;
    const C = candles[ASSET[m.asset]];
    const tH = m.closeT - H, barT = Math.floor(tH/60)*60 - 60;
    const bar = C.get(barT); if (!bar) continue;
    let sum=0,k=0;
    for (let i=0;i<60;i++){const b1=C.get(barT-i*60),b0=C.get(barT-(i+1)*60); if(!b1||!b0)break; sum+=(Math.log(b1.close)-Math.log(b0.close))**2;k++;}
    const s = Math.sqrt(sum/(60*k));
    const sd = H>=60 ? s*Math.sqrt(H-40) : s*Math.sqrt(H/3);
    const p = Phi((Math.log(bar.close) + D0.basis[m.asset].log_mean - Math.log(m.floor))/sd);
    rows.push({ won_yes: m.result==='yes', p });
  }
  let tp=0,fp=0,tn=0,fn=0, sPos=0, nPos=0, nNeg=0;
  for (const r of rows) {
    if (r.won_yes) { nPos++; if (r.p>0.5) tp++; else fn++; sPos += r.p; }
    else { nNeg++; if (r.p>0.5) fp++; else tn++; }
  }
  console.log(`\nH=30 contingency: n=${rows.length} p>0.5&yes=${tp} p<0.5&yes=${fn} p>0.5&no=${fp} p<0.5&no=${tn}`);
  console.log(`mean p | yes=${(sPos/nPos).toFixed(3)} acc=${((tp+tn)/rows.length*100).toFixed(1)}%`);
  let s=0,ties=0; const pos=[],neg=[];
  for (const r of rows) (r.won_yes?pos:neg).push(r.p);
  for (const a of pos) for (const b of neg){ if(a>b)s++; else if(a===b)ties++; else s--; }
  console.log(`AUC(P(YES) vs yes) = ${((s+0.5*ties)/(pos.length*neg.length)).toFixed(4)} (npos=${pos.length}, nneg=${neg.length})`);
}
