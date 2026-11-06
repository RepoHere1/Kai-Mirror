import json, urllib.request, time, os
RANGES=json.load(open('cand_ranges.json'))
out={}
for prod,(s,e) in RANGES.items():
    fn='cand_%s.json'%prod.replace('-USD','').lower()
    if os.path.exists(fn):
        try:
            out[prod]=json.load(open(fn)); print('cached',prod,len(out[prod])); continue
        except Exception: pass
    rows={}
    t=int(s)-120; e=int(e)+120
    while t < e:
        u='https://api.exchange.coinbase.com/products/%s/candles?granularity=60&start=%d&end=%d'%(prod,t,min(t+300,e))
        try:
            req=urllib.request.Request(u, headers={'User-Agent':'kai/1.0'})
            with urllib.request.urlopen(req, timeout=30) as f:
                got=json.load(f)
            for x in got: rows[int(x[0])]=x
        except Exception as ex:
            print('ERR',prod,t,ex)
        t+=300
        time.sleep(0.3)
    out[prod]=rows
    json.dump(rows, open(fn,'w'))
    print('fetched',prod,len(rows))
print('DONE')
