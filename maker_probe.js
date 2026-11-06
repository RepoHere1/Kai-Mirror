const k=require('./kalshi.js');
(async()=>{
  const out=[];
  let all=[];
  for (const acct of ['live']) for (const ei of [0,2]) {
    try { const f=await k.get('/portfolio/fills',{query:{limit:200,exchange_index:ei}},acct);
      const d=(f&&f.data&&typeof f.data==='object'&&!Array.isArray(f.data))?f.data:f;
      for(const x of (d.fills||[])) all.push(Object.assign({_a:acct,_e:ei},x));
    } catch(e){ out.push('err '+acct+' '+ei+' '+e.message.slice(0,60)); }
  }
  const uniq=all.filter((x,i)=>all.findIndex(y=>y.fill_id===x.fill_id)===i);
  out.push('fills '+uniq.length);
  const mk=uniq.filter(x=>x.is_taker!==true), tk=uniq.filter(x=>x.is_taker===true);
  out.push('maker '+mk.length+' fee>0:'+mk.filter(x=>Number(x.fee_cost)>0).length);
  out.push('taker '+tk.length+' fee>0:'+tk.filter(x=>Number(x.fee_cost)>0).length);
  for(const x of mk.slice(0,8)) out.push(' MK '+x.ticker+' '+x.side+' qty='+x.count_fp+' yp='+x.yes_price_dollars+' np='+x.no_price_dollars+' fee='+x.fee_cost+' taker='+x.is_taker);
  for(const x of tk.slice(0,8)) out.push(' TK '+x.ticker+' '+x.side+' qty='+x.count_fp+' yp='+x.yes_price_dollars+' np='+x.no_price_dollars+' fee='+x.fee_cost);
  require('fs').writeFileSync('maker_probe.txt', out.join('\n')+'\n');
  console.log(out.join('\n'));
})().catch(e=>{require('fs').writeFileSync('maker_probe.txt','FATAL '+e.message);console.log('FATAL',e.message);});
