# Kai-Mirror — the dry book (the lab)

Paper-money mirror of Kai-LIVE: same laws, same lanes, same engines, zero
real money. Evidence earned here is the only path to live promotion
(`alpha_gate.js`). Node.js, one process, one port: **8787**.

## Operator law (read first)

`OPERATOR_LAWS.md` is binding on the paper book too:

1. **NO fee-attached exits, ever.** Stops stored null, no code path fires
   one — the dry book must predict live behavior exactly.
2. **Data-buy doctrine.** Per-order cap $0.50, ride to settlement,
   evidence is the product.

## Architecture (what each file is FOR)

| File | Purpose |
|---|---|
| `server.js` | HTTP API + dashboard + lane orchestration + boot wiring |
| `kalshi.js` | Signed Kalshi REST client, cache + rate/429-aware |
| `kalshi_ws.js` | Kalshi market websocket feed |
| `coinbase_ws.js` | Coinbase spot websocket (fair value / settlement truth) |
| `perp_feed.js` | Perp basis feed |
| `quote_stream.js` | Quote aggregation for scanners |
| `series_kind.js` | Series taxonomy (15m vs 1h etc.) |
| `fill_sim.js` | THE HONEST FILLER: maker intents only become positions on real prints |
| `harvester.js` | Candidate harvester feeding the book |
| `brain_dry.js` + `kilo.js` | LLM advisor + its client (recommendations only; legacy signal generators kept for API compatibility) |
| `certainty.js` | Certainty lane: WS-verdict favorite harvesting |
| `conviction.js` | Conviction press sizing from settled-trade evidence |
| `calibration.js` | The vaccine: model calibration vs outcomes |
| `data_facts.js` | Settled-trade fact store (detective input) |
| `alpha_gate.js` | Promotion gate: dry evidence is the only key |
| `hold_rule.js` | Hold-window rules (allowed exit reasons) |
| `detective.js` | Always-on edge miner (own db + Kalshi/Polymarket/Manifold), walk-forward verified, NEVER trades |
| `backtest.js` + `strategies/` | Historical backtester (candle-cached, batch-budgeted) |
| `dry-home.html`, `chart.umd.min.js`, `static/` | Dashboard (10 lane cards incl. certainty) |
| `ops/kctl.ps1`, `ops/watchdog.ps1`, `ops/watchdog_keepalive.ps1` | Process control |
| `tools/` | Operational probes + `check-server.js` (end-to-end verifier) |

## Running

```powershell
# canonical control
powershell -File ops/kctl.ps1 mirror restart
# dashboard
http://127.0.0.1:8787/
# end-to-end check against the RUNNING server
node tools/check-server.js
# backtest (writes backtest_results.json + backtest_cache/, both gitignored)
node backtest.js run --days 14
node backtest.js summary
```

## API highlights

`/api/health` · `/api/strategies` (10 lanes) · `/api/certainty` ·
`/api/detective` (edge-miner findings) · `/api/no-lanes/book`

## Tests

```bash
node hold_rule.test.js   # hold-window rules
node alpha_gate.test.js  # promotion gate
```

## SHADOW 2.0 + trading-hours law (2026-11-08)

- **Fast settle** (30s tick): when a shadow's window closes (ts+tau), Coinbase
  spot truth settles it (`close >= strike => YES`, the detective's own-db rule).
  Kalshi's later finalize cross-checks; conflicts log `shadow_settle_conflict`.
- **Zombie sweep**: pendings closed >2h and unpriceable get `outcome=-1`
  (abandoned sentinel — excluded from stats and the boot backfill). The ~318
  Mirror pendings from before this law were the direct motivation.
- **Scoreboard** `GET /api/shadow`: expectancy (cents/contract) per hour, lane,
  price-band and side from settled rows — the promotion dossier (which hours,
  lanes and zones actually pay; proof, not opinion).
- **Hours law**: 21Z/23Z dry entries refused (detective: toxic cells); scan and
  shadow evidence keeps flowing from every hour. Config
  `state.runtime.trading_hours.blocked_utc` (empty = off).

## Hygiene law (2026-11-07)

Session debris (one-off fix scripts, probe droppings, snapshot dumps) is
never committed — `.gitignore` enforces the patterns. Git history keeps
everything removed in the 2026-11-07 purge if archaeology is ever needed.
The 190 losing trades and 136 calibration rows are DATA LAW: never delete.
