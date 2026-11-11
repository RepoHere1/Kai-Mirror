const PORTS = { MIRROR: 8787, LIVE: 8788 };
(async () => {
  for (const [tag, port] of Object.entries(PORTS)) {
    try {
      const h = await (await fetch(`http://127.0.0.1:${port}/api/health`)).json();
      const logs = await (await fetch(`http://127.0.0.1:${port}/api/logs?limit=400`)).json();
      const arr = (logs.lines || []).map((s) => { try { return JSON.parse(s); } catch (e) { return null; } }).filter(Boolean);
      const kinds = {};
      for (const l of arr) kinds[l.kind] = (kinds[l.kind] || 0) + 1;
      const shadowOpens = arr.filter((l) => l.kind === 'shadow_open').slice(-4);
      const soscans = arr.filter((l) => l.kind === 'shadow_only_scan').slice(-2);
      console.log(`${tag}: pid=${h.pid} boot=${h.boots} ok=${h.ok} lag=${h.loop_lag_ms_max}ms`);
      console.log(`  kinds: ${Object.entries(kinds).map(([k, v]) => k + '=' + v).join(' ')}`);
      soscans.forEach((l) => console.log('  SHADOW_ONLY_SCAN', JSON.stringify(l)));
      shadowOpens.forEach((l) => console.log('  SHADOW_OPEN', l.market_id, l.side, l.strategy, '@' + l.price));
    } catch (e) { console.log(tag, 'ERR', e.message); }
  }
})();
