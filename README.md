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

## FADE-THE-MODEL lane (operator order 2026-11-08)

"Do the opposite of what you think, in dry." The pure inverse of every loser
cannot win (fees + spread + noise hit both sides), but the repo's forensics
convicted the MODEL of systematic overstatement: btc_1h NO@6-8c claimed fair
0.65+ went 0/1136 — the market side (YES@93c) won 1136/1136 at ~+7c each.
So `anti_model` buys the OTHER side of a refused candidate in DRY when the
calibration ledger convicts the zone WITH EVIDENCE:

- conviction: claimed - adjusted >= 0.10 on the gate verdict,
- evidence: bucket or price-zone has >= 20 settled rows (never fades on noise),
- economics: anti price (1-bid) in 0.55-0.97 and empirical anti edge >= 3c
  (anti edge = bid - adjusted: the market-overpricing gap the LEDGER measured),
- same laws as every lane: $0.50/order cap, max 6 concurrent, 10-min per-ticker
  cooldown, never both sides of one market, 21Z/23Z hours law.

Every filter names its skip reason (`state.lanes.anti_model.last_skip`). Each
entry is double-booked: a dry position (real settlement P&L) AND a shadow row
(strategy `anti_model`) — so `/api/shadow` grades the idea by its own settled
rows. If the fade prints, the operator was right; if fees eat it, the numbers
say so. Config: `state.runtime.all_no.anti_model`.

## SABOTAGE BOOK (operator order 2026-11-08: "code that loses money in dry")

`state.sabotage` — a quarantined book that is DESIGNED to lose and measures
the inversion hypothesis with real settlements:

1. **Buys the overpriced side of every refusal**: every candidate the engine
   refuses — vaccine conviction, probability floor, price cap, lottery floor,
   momentum veto, edge minimum, edge cap — the sabotage book buys the
   MODEL-OVERPRICED side of that pair (price above fair), so EV is negative
   by construction (2026-11-08 fix: noise refusals at ~50c were coin flips;
   selection now structural). 3-min per-ticker cooldown.
2. **Mirrors every engine entry**: every position the dry engine books
   (all_no, certainty, anti_model fades), the sabotage book takes the
   OPPOSITE side at the complement price.

QUARANTINE IS ABSOLUTE: it never writes to the calibration ledger, the
shadow system, or the main dry book — engineered losers must not poison the
learning instrument. Settlement: Coinbase spot at close (`close >= strike`),
zombie-close at +3h booked as a loss (this book tries to lose).

`GET /api/inverse` shows both columns — sabotage P&L and the EXACT mirror
(arithmetic opposite of every settled trade) — with a verdict at n>=50:
if the mirror pays >2c/contract, inversion is real; if not, the numbers say
so. $100 dry balance, $0.50/trade, max 30 concurrent, auto-recharge below $10
(it can never pause - total drain tracked in `stats.recharged`). No hold-rule:
this book wants fast settles. Volume: every refused candidate, every pass.

## Hygiene law (2026-11-07)

Session debris (one-off fix scripts, probe droppings, snapshot dumps) is
never committed — `.gitignore` enforces the patterns. Git history keeps
everything removed in the 2026-11-07 purge if archaeology is ever needed.
The 190 losing trades and 136 calibration rows are DATA LAW: never delete.
