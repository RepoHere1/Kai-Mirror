'use strict';
// PAPER SEED REBASE (operator order 2026-11-12: "you can't have that balance
// in fake reality"): the DRY books were seeded at $303 - a snapshot of the
// REAL Kalshi account's cash - and simulated fills then compounded on top of
// it, so the tape printed a growing "balance" that looked like the real
// account earning money in a mode where nothing real ever trades. Paper
// money must start from a DECLARED paper balance, never a mirror of real
// cash. rebaseBooks() swaps the legacy seed for each book's declared paper
// start and shifts the stored balance by the same delta, so every booked
// trade, pnl and open position is preserved exactly - only the fake origin
// changes. Idempotent: guarded by state.paper_seed_rebase_v1.

const LEGACY_REAL_CASH_PAPER_SEED = 303;

function rebaseBooks(state, opts) {
  if (!state || typeof state !== 'object') return { rebased: [], skipped: 'no state' };
  if (state.paper_seed_rebase_v1) return { rebased: [], skipped: 'already applied' };
  const allNoStart = Number.isFinite(opts && opts.allNoPaperStart) ? opts.allNoPaperStart : 250;
  const dryStart = Number.isFinite(opts && opts.dryPaperStart) ? opts.dryPaperStart : 10000;
  const rebased = [];

  for (const modeKey of Object.keys(state.all_no || {})) {
    const bk = state.all_no[modeKey];
    if (!bk || typeof bk !== 'object') continue;
    if (bk.starting_balance !== LEGACY_REAL_CASH_PAPER_SEED) continue;
    const delta = allNoStart - LEGACY_REAL_CASH_PAPER_SEED;
    if (Number.isFinite(bk.balance)) bk.balance = Number((bk.balance + delta).toFixed(4));
    bk.starting_balance = allNoStart;
    rebased.push({ book: 'all_no.' + modeKey, from: LEGACY_REAL_CASH_PAPER_SEED, to: allNoStart, delta });
  }

  if (state.dry && state.dry.starting_balance === LEGACY_REAL_CASH_PAPER_SEED) {
    state.dry.starting_balance = dryStart;
    rebased.push({ book: 'dry', from: LEGACY_REAL_CASH_PAPER_SEED, to: dryStart, delta: dryStart - LEGACY_REAL_CASH_PAPER_SEED });
  }

  state.paper_seed_rebase_v1 = true;
  return { rebased };
}

module.exports = { LEGACY_REAL_CASH_PAPER_SEED, rebaseBooks };
