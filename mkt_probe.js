const k=require('./kalshi.js');
const fs=require('fs');
const T=['KXBTC15M-26NOV031330-30','KXHYPE15M-26NOV031330-30'];
(async()=>{
  const out=[];
  for(const t of T){
    let tries=0, ok=false;
    while(!ok && tries<8){
      try{
        const r=await k.raw('live','GET','/markets/'+t,{noRetry429:true});
        const m=(r&&r.market)||r;
        out.push(t+' :: '+JSON.stringify({title:m.title,yes_sub:m.yes_sub_title,no_sub:m.no_sub_title,rules:m.rules_primary,strike_type:m.strike_type,floor:m.floor_strike,cap:m.cap_strike,close:m.close_time,status:m.status,result:m.result,yes_bid:m.yes_bid_dollars,no_bid:m.no_bid_dollars,vol:m.volume_fp,oi:m.open_interest_fp}));
        ok=true;
      }catch(e){ tries++; await new Promise(r=>setTimeout(r,2000*tries)); if(tries>=8) out.push(t+' ERR '+e.message); }
    }
    await new Promise(r=>setTimeout(r,800));
  }
  fs.writeFileSync('mkt_probe.txt', out.join('\n')+'\n');
  console.log(out.join('\n'));
})().catch(e=>console.log('FATAL',e.message));
