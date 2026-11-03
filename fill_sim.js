'use strict';
// ===========================================================================
// FILL REALISM for the paper book (Kai-Mirror / DRY).
//
// THE LIE IT KILLS: the lab used to book EVERY intent as an instant fill at
// the bid. Real post-only maker bids do NOT fill on demand - they fill only
// when somebody actually sells into that level, and they fill *most* when the
// market is moving against the position (adverse selection). Evidence that this
// mattered: identical cheap entries settled 51% of the time in the paper book
// but 21% in the real account, because the real account only gets filled on
// the prints that hurt.
//
// THE TRUTH IT USES (public, unauthenticated, already fetched by the scanner):
//   q.last    <- last_price_dollars, a REAL executed print in YES dollars
//   q.volume  <- volume_fp, cumulative REAL traded contracts
//   q.yes_bid / q.yes_bid_size - the live touch and displayed size at our level
//
// FILL RULE (conservative): a resting bid at our level executes only when
//   (1) cumulative volume has ADVANCED since the intent (a real trade happened), and
//   (2) a real print executed AT OR BELOW our bid in YES terms (someone sold
//       into us), and
//   (3) the print arrives inside the TTL. Otherwise the intent is CANCELLED
//       UNFILLED - which is a result, not a failure, and is counted.
// ===========================================================================

function toYes(side, px) {
  const p = Number(px);
  if (!Number.isFinite(p)) return null;
  return side === 'YES' ? p : Number((1 - p).toFixed(4));
}

function create(deps) {
  const log = deps.log || function () {};
  const getState = deps.getState;

  const pending = new Map();   // id -> intent

  // Counters live in state so they survive restarts and accumulate evidence.
  function counters() {
    const s = getState();
    if (!s.fill_realism) {
      s.fill_realism = {
        intents: 0, filled: 0, unfilled: 0, adverse: 0,
        contracts_intent: 0, contracts_filled: 0,
        wait_ms_total: 0, print_seen_adverse: 0,
      };
    }
    return s.fill_realism;
  }

  // Register a maker intent instead of booking it.
  function intent(rec) {
    const c = counters();
    const yesLevel = toYes(rec.side, rec.bid);
    if (!rec.ticker || !Number.isFinite(yesLevel)) return null;
    const id = rec.id || (rec.ticker + '-' + Date.now() + '-' + Math.floor(Math.random() * 1e6));
    c.intents += 1;
    c.contracts_intent += rec.qty || 1;
    pending.set(id, {
      id, ticker: rec.ticker, side: rec.side, bid: Number(rec.bid),
      yes_level: yesLevel, qty: rec.qty || 1, book: rec.book || null,
      ttl_ms: Number(rec.ttl_ms || 45000), at: Date.now(),
      vol0: Number.isFinite(rec.volume) ? Number(rec.volume) : null,
      last_seen: Number.isFinite(rec.last) ? Number(rec.last) : null,
      mid_at_intent: Number.isFinite(rec.mid) ? rec.mid : yesLevel,
    });
    log('sim_intent', { id, ticker: rec.ticker, side: rec.side, bid: rec.bid, qty: rec.qty, ttl_ms: rec.ttl_ms || 45000 });
    return id;
  }

  // Feed it a REAL quote. Returns events so the caller decides what to book.
  function observe(q) {
    const events = [];
    if (!q || !q.ticker) return events;
    const now = Date.now();
    const vol = Number(q.volume);
    const last = Number(q.last);
    const c = counters();
    for (const [id, it] of pending) {
      if (it.ticker !== q.ticker) continue;

      // First sighting only sets the volume BASELINE - a fill must be proved by
      // volume that advances AFTER the intent existed, never by pre-existing volume.
      if (!Number.isFinite(it.vol0)) { it.vol0 = Number.isFinite(vol) ? vol : 0; continue; }
      const grew = Number.isFinite(vol) && vol > it.vol0 + 1e-9;
      if (grew) it.vol0 = vol;

      // A resting bid executes only when a REAL print goes THROUGH our level.
      // Direction depends on which side we are bidding (last_price is YES cents):
      //   buy YES at X -> seller hits us -> YES print <= X
      //   buy NO  at Y -> seller hits us -> NO  print <= Y == YES print >= 1-Y
      const printed = Number.isFinite(last) && last > 0;
      const crossed = printed && (it.side === 'YES'
        ? last <= it.yes_level + 1e-9
        : last >= it.yes_level - 1e-9);
      if (printed) it.last_seen = last;

      if (grew && crossed) {
        pending.delete(id);
        c.filled += 1;
        c.contracts_filled += it.qty;
        const wait = now - it.at;
        c.wait_ms_total += wait;
        // Adverse selection measured in OUR side's dollars: we were filled only
        // after our own side's touch had already fallen below our level.
        const touch = Number(it.side === 'YES' ? q.yes_bid : q.no_bid);
        const adverse = Number.isFinite(touch) && touch < it.bid - 1e-9;
        if (adverse) { c.adverse += 1; }
        events.push({ type: 'filled', intent: it, wait_ms: wait, adverse, fill_price: it.bid, last_print: last });
        log('sim_fill', { id, ticker: it.ticker, side: it.side, px: it.bid, qty: it.qty, wait_ms: wait, adverse, print: last });
        continue;
      }

      if (now - it.at > it.ttl_ms) {
        pending.delete(id);
        c.unfilled += 1;
        events.push({ type: 'unfilled', intent: it });
        log('sim_unfilled', { id, ticker: it.ticker, side: it.side, px: it.bid, qty: it.qty, reason: printed ? 'no print through our level inside TTL' : 'no real print inside TTL' });
      }
    }
    return events;
  }

  function stats() {
    const c = counters();
    const fillable = c.intents;
    return {
      ...c,
      pending: pending.size,
      fill_rate: fillable ? Number((100 * c.filled / fillable).toFixed(1)) : 0,
      unfilled_rate: fillable ? Number((100 * c.unfilled / fillable).toFixed(1)) : 0,
      adverse_pct: c.filled ? Number((100 * c.adverse / c.filled).toFixed(1)) : 0,
      avg_wait_ms: c.filled ? Math.round(c.wait_ms_total / c.filled) : 0,
      note: 'paper fills require a REAL print at/below our bid inside the TTL; intents that never print are booked as UNFILLED, not as trades',
    };
  }

  return { intent, observe, stats, pendingCount: () => pending.size, _pending: pending };
}

module.exports = { create };
