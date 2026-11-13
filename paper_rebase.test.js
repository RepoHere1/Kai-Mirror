'use strict';
// PAPER SEED REBASE unit tests: the $303 real-cash seed is swapped for
// declared paper starts, balance shifts by the same delta, history is
// untouched, and the migration can never run twice.
const assert = require('assert');
const { LEGACY_REAL_CASH_PAPER_SEED, rebaseBooks } = require('./paper_rebase.js');

// 1) The live case: all_no.dry seeded 303 (real-cash snapshot) rebases to the
//    declared 250 paper start; stored balance moves by the same -53 delta so
//    equity history is preserved; pnl/trades are not touched.
const state = {
  all_no: {
    dry: { starting_balance: 303, balance: 361.2329, total_pnl: 57.73, positions: [], closed_trades: [{ id: 't1', pnl: 0.58 }] },
    live: { starting_balance: 250, balance: 250, total_pnl: 0, positions: [], closed_trades: [] },
  },
  dry: { starting_balance: 303, balance: 0, positions: [], closed: [] },
};
const r1 = rebaseBooks(state, { allNoPaperStart: 250, dryPaperStart: 10000 });
assert.deepStrictEqual(r1.rebased.map(x => x.book), ['all_no.dry', 'dry']);
assert.strictEqual(state.all_no.dry.starting_balance, 250);
assert.strictEqual(state.all_no.dry.balance, 308.2329); // 361.2329 - 53
assert.strictEqual(state.all_no.dry.total_pnl, 57.73);  // history untouched
assert.strictEqual(state.all_no.dry.closed_trades.length, 1);
assert.strictEqual(state.all_no.live.starting_balance, 250); // already honest - untouched
assert.strictEqual(state.all_no.live.balance, 250);
assert.strictEqual(state.dry.starting_balance, 10000);       // lane book: declared 10k paper
assert.strictEqual(state.paper_seed_rebase_v1, true);

// 2) Idempotent: a second call rebases nothing (the seed can never return).
const r2 = rebaseBooks(state, { allNoPaperStart: 250, dryPaperStart: 10000 });
assert.deepStrictEqual(r2.rebased, []);

// 3) Custom operator-set paper starts are NEVER touched - only the exact
//    legacy real-cash seed is swapped.
const s3 = { all_no: { dry: { starting_balance: 500, balance: 610 } }, dry: { starting_balance: 777 } };
const r3 = rebaseBooks(s3, {});
assert.deepStrictEqual(r3.rebased, []);
assert.strictEqual(s3.all_no.dry.starting_balance, 500);
assert.strictEqual(s3.dry.starting_balance, 777);

// 4) Degenerate shapes are safe (no all_no, no dry, flag already set).
assert.deepStrictEqual(rebaseBooks({}, {}).rebased, []);
assert.deepStrictEqual(rebaseBooks(null, {}).rebased, []);
assert.strictEqual(LEGACY_REAL_CASH_PAPER_SEED, 303);

console.log('paper_rebase.test.js: ALL PASS');
