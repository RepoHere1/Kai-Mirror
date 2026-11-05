'use strict';
// ===========================================================================
// THE HARVESTER (2026-11-04) — mass maker-order lane, EARNED-EDGE ONLY.
// ===========================================================================
// THE IDEA (operator order): Kalshi charges NOTHING to place or cancel a
// limit order — fees exist only on fills. So post resting maker orders
// EVERYWHERE the EMPIRICAL ledger says we are being paid real edge, and
// cancel them fast when the reason disappears. Never chase, never cross.
//
// WHAT "FACTUAL EDGE" MEANS HERE (and only here):
//   The vaccine (calibration.js) converts the model's lying claim into an
//   empirical posterior per bucket. The harvester posts a bid at the touch
//   only when ALL of the following hold:
//     1. the bucket has a PROVEN record: n >= 50 settled observations, and
//     2. adjusted_prob - price >= HARVEST_MIN_EDGE (default 0.08), and
//     3. spread <= 0.06 (we join the touch; wide books are traps), and
//     4. the position count caps (2 per market, 8 total) hold.
//   A bid that no real print touches within the TTL is CANCELLED unfilled —
//   that is the "smart and sane cancel deal": orders live for at most
//   HARVEST_TTL_SEC (90s), so we never hold stale quotes whose profitable
//   window has closed.
//
// FILL HONESTY: fills are simulated by fill_sim.js — an intent becomes a
// position ONLY when a REAL executed print crosses our price inside the TTL.
// No print, no position. This is exactly how a live post-only order behaves,
// and it counts adverse selection for free.
//
// SCOPE HONESTY (v1): it harvests the noBook() candidate set (every Kalshi
// series the scanner covers, both sides). A raw all-markets quote builder is
// v2 once the ledger proves the lane. PAPER ONLY in the lab; no LIVE wiring
// until its own settled record says it should exist.
// ===========================================================================

const HARVEST_MIN_EDGE = 0.08;   // empirical posterior must beat price by 8c
const HARVEST_MIN_N = 50;        // bucket must have 50+ settled observations
const HARVEST_MAX_SPREAD = 0.06; // thin books only
const HARVEST_TTL_SEC = 90;      // resting order lifetime — then cancel
const MAX_PER_MARKET = 2;        // resting/filled positions per market
const MAX_ACTIVE = 8;            // total open paper positions

function create(deps) {
  const CALIB = deps.CALIB;
  const FILLSIM = deps.FILLSIM;
  const getState = deps.getState;
  const log = deps.log || function () {};
  const persistTrade = deps.persistTrade;   // optional (server wires it)

  function st() {
    const s = getState();
    if (!s.harvester) {
      s.harvester = {
        enabled: true,               // paper lane; runs only in the lab
        positions: [],               // filled paper positions
        stats: { intents: 0, cancelled_unfilled: 0, filled: 0,
                 settled: 0, wins: 0, contracts: 0, pnl: 0 },
      };
    }
    return s.harvester;
  }

  // Called each engine pass with the noBook() candidate rows.
  function run(rows) {
    const h = st();
    if (!h.enabled || !FILLSIM || !CALIB) return;
    for (const r of rows || []) {
      try {
        if (r.limit_entry == null) continue;
        if (r.spread != null && r.spread > HARVEST_MAX_SPREAD) continue;
        const g = CALIB.gate({
          ticker: r.market_id, side: r.side, strategy: 'maker_harvester',
          claimed: r.fair, price: r.limit_entry, minEdge: HARVEST_MIN_EDGE,
        });
        if (!g.ok || g.n < HARVEST_MIN_N) continue;
        const openSame = h.positions.filter(p => p.market_id === r.market_id).length;
        if (openSame >= MAX_PER_MARKET) continue;
        if (h.positions.length >= MAX_ACTIVE) break;
        // Post the intent: maker at the touch, 90s TTL, tagged for routing.
        FILLSIM.intent({
          ticker: r.market_id, side: r.side || 'NO', bid: r.limit_entry, qty: r.qty,
          ttl_ms: HARVEST_TTL_SEC * 1000,
          book: {
            market_id: r.market_id, entry: r.limit_entry, qty: r.qty,
            fair: r.fair, edge: r.edge, kelly: r.kelly,
            stop: r.stop, take_profit: r.take_profit, confidence: r.confidence,
            expiration: r.expiration,
            extra: { strike: r.strike, sigma: r.sigma, spot_product: r.spot_product,
                     side: r.side, adjusted: g.adjusted, harvester: true },
          },
        });
        h.stats.intents++;
      } catch (e) { /* never break the engine pass */ }
    }
  }

  // A FILLSIM-confirmed real print filled our resting bid -> paper position.
  function bookFill(a) {
    const h = st();
    const x = a.extra || {};
    const pos = {
      id: 'hv-' + Math.random().toString(36).slice(2, 8),
      market_id: a.market_id, strategy: 'maker_harvester', side: x.side || 'NO',
      entry_price: a.entry, quantity: a.qty, cost: Number((a.qty * a.entry).toFixed(2)),
      claimed_fair: a.fair, adjusted: x.adjusted != null ? x.adjusted : null,
      sigma_per_sec: x.sigma != null ? x.sigma : null,
      strike: x.strike != null ? x.strike : null,
      opened: new Date().toISOString(),
    };
    h.positions.push(pos);
    h.stats.filled++;
    h.stats.contracts += a.qty;
    log('harvester_fill', { market_id: pos.market_id, side: pos.side, entry: pos.entry_price, qty: a.qty, adjusted: pos.adjusted });
  }

  // Settled sweep: called for EVERY finalized market from the scan loop.
  function settleResult(ticker, resultYes) {
    const h = st();
    let n = 0;
    for (const pos of h.positions.filter(p => p.market_id === ticker)) {
      const won = (pos.side === 'YES') === (resultYes === true);
      const exit = won ? 1 : 0;
      const pnl = Number((pos.quantity * exit - pos.cost).toFixed(2));
      h.stats.settled++;
      if (won) h.stats.wins++;
      h.stats.pnl = Number((h.stats.pnl + pnl).toFixed(2));
      h.positions = h.positions.filter(p => p.id !== pos.id);
      // One ledger: the harvester's outcomes feed the vaccine too.
      if (CALIB) CALIB.recordOutcome({
        market_id: pos.market_id, strategy: 'maker_harvester', side: pos.side,
        claimed: pos.claimed_fair, price: pos.entry_price, outcome: won,
        pnl, sigma: pos.sigma_per_sec, strike: pos.strike, ts: pos.opened,
      });
      if (persistTrade) {
        persistTrade({
          market_id: pos.market_id, title: null, side: pos.side, strategy: 'maker_harvester',
          entry_price: pos.entry_price, exit_price: exit, quantity: pos.quantity,
          cost_basis: pos.cost, proceeds: Number((pos.quantity * exit).toFixed(2)),
          pnl, exit_reason: 'settled', result: won ? 'won' : 'lost',
          hold_seconds: Math.round((Date.now() - new Date(pos.opened).getTime()) / 1000),
          exit_timestamp: new Date().toISOString(), opened: pos.opened,
          settled_price: exit, settlement_source: 'kalshi',
          fair_at_entry: pos.claimed_fair, sigma_per_sec: pos.sigma_per_sec,
          strike: pos.strike, horizon: 'harvest',
        });
      }
      log('harvester_settled', { market_id: pos.market_id, side: pos.side, won, pnl });
      n++;
    }
    return n;
  }

  function snapshot() {
    const h = st();
    const s = Object.assign({}, h.stats);
    s.ev_per_contract = s.settled > 0 ? Number((s.pnl / Math.max(1, s.settled)).toFixed(4)) : null;
    s.win_rate = s.settled > 0 ? Number((s.wins / s.settled).toFixed(4)) : null;
    return { enabled: h.enabled, stats: s, open_positions: h.positions.length,
             positions: h.positions.slice(0, 20),
             rules: { min_edge: HARVEST_MIN_EDGE, min_n: HARVEST_MIN_N,
                      max_spread: HARVEST_MAX_SPREAD, ttl_sec: HARVEST_TTL_SEC,
                      max_per_market: MAX_PER_MARKET, max_active: MAX_ACTIVE } };
  }

  return { run, bookFill, settleResult, snapshot };
}

module.exports = { create };
