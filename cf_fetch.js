const k=require('./kalshi.js');
const fs=require('fs');
(async()=>{
  const st=JSON.parse(fs.readFileSync('state.json','utf8'));
  const b=(st.all_no&&st.all_no.dry)||{};
  const closed=b.closed_trades||[];
  const mids=[...new Set(closed.map(t=>t.market_id).filter(Boolean))];
  fs.writeFileSync('cf_markets.json', JSON.stringify(mids));
  const out={};
  const CH=40;
  for(let i=0;i<mids.length;i+=CH){
    const chunk=mids.slice(i,i+CH);
    let done=false, tries=0;
    while(!done && tries<6){
      try{
        const r=await k.raw('live','GET','/markets',{query:{tickers:chunk.join(',')},noRetry429:true});
        const ms=(r&&r.markets)||[];
        for(const m of ms) out[m.ticker]={result:m.result||'',status:m.status,title:m.title,yes_sub:m.yes_sub_title,no_sub:m.no_sub_title,rules:m.rules_primary,floor:m.floor_strike,cap:m.cap_strike,close:m.close_time};
        done=true;
      }catch(e){ tries++; await new Promise(r=>setTimeout(r,1500*tries)); }
    }
    fs.writeFileSync('cf_results.json', JSON.stringify(out));
    await new Promise(r=>setTimeout(r,350));
  }
  console.log('markets',mids.length,'resolved',Object.keys(out).length);
})().catch(e=>{console.log('FATAL',e.message);});
