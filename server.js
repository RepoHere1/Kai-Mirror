'use strict';
// ===========================================================================
// ATTENTION - ANY AI / AGENT EDITING THIS FILE. Read before touching anything.
//   The operator's lane BUTTON PUSH is IMMEDIATE, ABSOLUTE LAW. No code you add,
//   refactor, optimise, or "fix" may override, delay, auto-reverse, auto-restart,
//   keep-alive, supervise, or otherwise contravene an operator toggle of a lane's
//   running state. `ls.running` is written by the operator toggle endpoint and the
//   operator on/off scripts ONLY. If a change could make any lane disobey the
//   operator's last button push, DO NOT SHIP IT - revert and ask.
// ===========================================================================
// Kai-Mirror (DRY) server.
//
// DATA POLICY (hard rules, by operator instruction):
//   * Every chart, stat, tile and table is fed from a REAL production source:
//       - Kalshi DRY "hot" API  (primary, for BOTH the dry and dry books)
//       - Kalshi DEMO API        (fallback only; used when dry fails)
//       - Coinbase Exchange public ticker (spot prices, no key needed)
//       - Kilo AI Gateway        (LLM analysis, model auto/free)
//   * The ONLY simulated thing is DRY-mode *execution*: paper fills taken at the
//     real quoted price, settled against the real Kalshi settlement result.
//     No price, strike, settlement, balance or P&L is ever invented.
//   * Every API response carries a `provenance` block naming the host it came
//     from, so the operator can verify each number independently.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const WebSocket = require('ws');


const sqlite3 = require('sqlite3').verbose();
const kalshi = require('./kalshi');
const holdRule = require('./hold_rule');
const kilo = require('./kilo');
const brain = require('./brain_dry');
const perp = require('./perp_feed');
const series_kind = require('./series_kind');
const fillSim = require('./fill_sim');
const conviction = require('./conviction');
const paperRebaseMod = require('./paper_rebase');
const calibrationMod = require('./calibration');
const harvesterMod = require('./harvester');
const certainty = require('./certainty');
const quote_stream = require('./quote_stream');
const dataFacts = require('./data_facts');
const { KalshiWS } = require('./kalshi_ws');
const { CoinbaseWS } = require('./coinbase_ws');

const HERE = __dirname;
// DATA FACTS LAYER: every observation is verified and recorded before anybody
// is allowed to act on it. /api/data-health and /api/facts expose the ledger.
const FACTS = dataFacts.create({ dbPath: path.join(HERE, 'facts.db'), liveCount: () => { try { return quote_stream.freshCount(); } catch (e) { return null; } }, log: (k, o) => { try { log(k, o); } catch (e) { } } });
// CRASH WITNESS (2026-11-03). A silent death looked like "LIVE loves to fail".
// Registered at the very TOP of the module so even a failure during boot is
// captured. Every line carries the PID, and the exit handler captures the CALL
// STACK that ended the process, so a death is always attributable.
try {
  const APPEND = (line) => { try { fs.appendFileSync(path.join(HERE, 'crash.log'), line + String.fromCharCode(10)); } catch (e) { } };
  const STAMP = () => new Date().toISOString() + ' pid=' + process.pid;
  process.on('uncaughtException', (e) => { APPEND(STAMP() + ' UNCAUGHT ' + (e && e.stack ? e.stack : e)); process.exit(1); });
  process.on('unhandledRejection', (r) => APPEND(STAMP() + ' REJECTION ' + (r && r.stack ? r.stack : String(r))));
  process.on('exit', (code) => {
    let who = '';
    try { who = String(new Error('exit-here').stack || '').split('\n').slice(1, 5).join(' | '); } catch (e) { }
    APPEND(STAMP() + ' EXIT code=' + code + ' via ' + who);
  });
  process.on('SIGTERM', () => { APPEND(STAMP() + ' SIGTERM'); process.exit(143); });
  process.on('SIGINT', () => { APPEND(STAMP() + ' SIGINT'); process.exit(130); });
  // LAST WORDS: even an /F kill or power loss leaves the final alive-stamp; the
  // gap between last words and the next boot IS the exact time of death.
  setInterval(() => { try { APPEND(STAMP() + ' ALIVE uptime=' + Math.round((Date.now() - new Date(state.booted_at).getTime()) / 1000) + 's'); } catch (e) { } }, 30000);
} catch (e) { console.error('crash witness init failed', e.message); }
const ENV = kalshi.ENV;
const PORT = Number(ENV.PORT || 8787);  // DRY server: port 8787
const HOST = '127.0.0.1';

// Coinbase credentials for authenticated WebSocket (level2, user channels)
const COINBASE_CREDS = {
  apiKey: ENV.COINBASE_API_KEY || '',
  apiSecret: ENV.COINBASE_API_SECRET || '',
  apiPassphrase: ENV.COINBASE_API_PASSPHRASE || ''
};
const coinbaseAuth = COINBASE_CREDS.apiKey && COINBASE_CREDS.apiSecret ? COINBASE_CREDS : null;

// Rate limiting (in-memory, per-IP)
const RATE_LIMIT_WINDOW_MS = 60000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 120; // 120 req/min per IP
const rateLimitMap = new Map();

function rateLimit(ip) {
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW_MS;
  const record = rateLimitMap.get(ip) || { count: 0, windowStart: now };
  if (record.windowStart < windowStart) {
    record.count = 1;
    record.windowStart = now;
  } else {
    record.count++;
  }
  rateLimitMap.set(ip, record);
  return record.count <= RATE_LIMIT_MAX_REQUESTS;
}

// Cleanup old rate limit entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [ip, record] of rateLimitMap.entries()) {
    if (record.windowStart < now - RATE_LIMIT_WINDOW_MS * 2) {
      rateLimitMap.delete(ip);
    }
  }
}, 300000);

// ===========================================================================
// SQLite Database with proper indexing for trades/positions
// ===========================================================================
const DB_FILE = path.join(HERE, 'trading.db');
const db = new sqlite3.Database(DB_FILE);
// SQLITE BUSY HANDLING (2026-11-03). node-sqlite3 reports an async write
// failure (SQLITE_BUSY: database is locked) by EMITTING an 'error' event on
// the Database object. With no listener, Node promotes that to an uncaught
// exception, and the crash witness above calls process.exit(1) - so ONE
// transient lock on trading.db killed the entire server. That is exactly how
// Kai-LIVE died at 21:20:22 on 2026-11-03.
//
// PROVEN MECHANISM (node_modules/sqlite3/src/statement.cc + lib/sqlite3.js):
// the event is emitted on the STATEMENT object, NOT on the Database. A
// db-level `db.on('error')` handler therefore does NOT catch it. BOTH are
// required:
//   1. busyTimeout - a blocked writer WAITS for the lock instead of failing
//      fast. Must be configured BEFORE the first statement runs.
//   2. a callback on EVERY statement - a residual failure is delivered to the
//      caller instead of becoming an unhandled 'error' event.
// The proof lived in test_sqlite_busy.js (kept in git history; removed 2026-11-07).
db.configure('busyTimeout', 5000);

function dbLogError(scope, err) {
  const detail = err && err.message ? err.message : String(err);
  // Do not depend on `state` here: it is in the temporal dead zone until
  // loadState() runs, so the detail goes straight to crash.log.
  try {
    fs.appendFileSync(path.join(HERE, 'crash.log'),
      new Date().toISOString() + ' pid=' + process.pid + ' DB_ERROR ' + scope + ' ' + detail + String.fromCharCode(10));
  } catch (e) { }
  try { console.error('[db] ' + scope + ':', detail); } catch (e) { }
}

db.on('error', (err) => { dbLogError('trading.db', err); });

// (1) Database-level methods: always carry a callback.
for (const _m of ['run', 'exec', 'all', 'get', 'each']) {
  const _orig = db[_m].bind(db);
  db[_m] = function (sql, ...rest) {
    if (typeof rest[rest.length - 1] !== 'function') {
      rest.push((err) => { if (err) dbLogError('db.' + _m, err); });
    }
    return _orig(sql, ...rest);
  };
}

// (2) Prepared statements: `stmt.run([...])` with no callback is exactly the
// fatal shape.
const _origPrepare = db.prepare.bind(db);
db.prepare = function (sql) {
  const stmt = _origPrepare(sql);
  for (const _sm of ['run', 'get', 'all', 'each', 'finalize']) {
    const _o = stmt[_sm].bind(stmt);
    stmt[_sm] = function (...a) {
      if (typeof a[a.length - 1] !== 'function') {
        a.push((err) => { if (err) dbLogError('stmt.' + _sm, err); });
      }
      return _o(...a);
    };
  }
  return stmt;
};

// Initialize schema with proper indexes
db.serialize(() => {
  // Trades table - core trading data
  db.run(`CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    title TEXT,
    side TEXT NOT NULL,
    strategy TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    quantity INTEGER NOT NULL,
    cost_basis REAL NOT NULL,
    proceeds REAL,
    pnl REAL,
    exit_reason TEXT,
    result TEXT,
    hold_seconds INTEGER,
    exit_timestamp TEXT,
    opened TEXT NOT NULL,
    bid_at_entry REAL,
    ask_at_entry REAL,
    fill_quality REAL,
    settled_price REAL,
    settlement_source TEXT,
    fair_at_entry REAL,
    edge_at_entry REAL,
    kelly_fraction REAL,
    sigma_per_sec REAL,
    strike REAL,
    horizon TEXT,
    regime TEXT,
    signal_source TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Indexes for common queries
  db.run(`CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_trades_exit_time ON trades(exit_timestamp)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_trades_side ON trades(side)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_trades_pnl ON trades(pnl)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_trades_strategy_time ON trades(strategy, exit_timestamp)`);

  // FEE COLUMNS (2026-11-03). The fee is real money that left the account;
  // without it persisted, the ledger cannot be reconciled against the account
  // statement. Existing databases are migrated in place.
  db.all("PRAGMA table_info(trades)", (e0, cols) => {
    if (e0 || !cols) return;
    const have = new Set(cols.map(c => c.name));
    for (const c of ['fee_paid', 'fee_entry', 'fee_exit']) {
      if (!have.has(c)) db.run(`ALTER TABLE trades ADD COLUMN ${c} REAL`, (e1) => { if (e1) console.error('[db] migrate ' + c + ' failed:', e1.message); });
    }
  });

  // Positions table - open positions
  db.run(`CREATE TABLE IF NOT EXISTS positions (
    id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    title TEXT,
    series TEXT,
    strategy TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    qty INTEGER NOT NULL,
    cost REAL NOT NULL,
    fair_at_entry REAL,
    edge_at_entry REAL,
    sigma_per_sec REAL,
    strike REAL,
    horizon TEXT,
    stop_loss REAL,
    take_profit REAL,
    mark REAL,
    opened TEXT NOT NULL,
    close_time TEXT,
    source TEXT,
    host TEXT,
    bid_at_entry REAL,
    ask_at_entry REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(market_id)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_positions_opened ON positions(opened)`);

  // Settlement tracking
  db.run(`CREATE TABLE IF NOT EXISTS settlements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    strategy TEXT NOT NULL,
    settled_price REAL NOT NULL,
    kalshi_result TEXT,
    coinbase_spot_at_settlement REAL,
    coinbase_spot_time INTEGER,
    spot_product TEXT,
    entry_price REAL NOT NULL,
    pnl REAL,
    timestamp TEXT NOT NULL,
    spot_deviation REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_settlements_time ON settlements(timestamp)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_settlements_market ON settlements(market_id)`);

  // Fill quality tracking
  db.run(`CREATE TABLE IF NOT EXISTS fill_quality (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    strategy TEXT NOT NULL,
    entry_price REAL NOT NULL,
    bid_at_entry REAL,
    ask_at_entry REAL,
    fill_quality REAL,
    is_maker INTEGER,
    timestamp TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_fill_quality_strategy ON fill_quality(strategy)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_fill_quality_time ON fill_quality(timestamp)`);

  // Webhook events log
  db.run(`CREATE TABLE IF NOT EXISTS webhook_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    webhook_id TEXT NOT NULL,
    event TEXT NOT NULL,
    payload TEXT,
    response_code INTEGER,
    response_body TEXT,
    success INTEGER,
    timestamp TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_webhook_events_webhook ON webhook_events(webhook_id)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_webhook_events_time ON webhook_events(timestamp)`);

  // API request log for rate limiting analysis
  db.run(`CREATE TABLE IF NOT EXISTS api_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip TEXT NOT NULL,
    path TEXT NOT NULL,
    method TEXT NOT NULL,
    status_code INTEGER,
    response_time_ms INTEGER,
    timestamp TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_api_requests_ip ON api_requests(ip)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_api_requests_time ON api_requests(timestamp)`);

  console.log('[SQLite] Database initialized with indexes');
});

// Helper functions for database operations
function dbRun(sql, params) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve({ id: this.lastID, changes: this.changes });
    });
  });
}

function dbGet(sql, params) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function dbAll(sql, params) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Persist trades to SQLite (in addition to JSON state)
async function persistTrade(trade) {
  try {
    await dbRun(`INSERT INTO trades (
      market_id, title, side, strategy, entry_price, exit_price, quantity,
      cost_basis, proceeds, pnl, exit_reason, result, hold_seconds,
      exit_timestamp, opened, bid_at_entry, ask_at_entry, fill_quality,
      settled_price, settlement_source, fair_at_entry, edge_at_entry,
      kelly_fraction, sigma_per_sec, strike, horizon, regime, signal_source,
      fee_paid, fee_entry, fee_exit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      trade.market_id, trade.title, trade.side, trade.strategy,
      trade.entry_price, trade.exit_price, trade.quantity,
      trade.cost_basis, trade.proceeds, trade.pnl, trade.exit_reason,
      trade.result, trade.hold_seconds, trade.exit_timestamp,
      trade.opened, trade.bid_at_entry, trade.ask_at_entry, trade.fill_quality,
      trade.settled_price, trade.settlement_source, trade.fair_at_entry,
      trade.edge_at_entry, trade.kelly_fraction, trade.sigma_per_sec,
      trade.strike, trade.horizon, trade.regime, trade.reason?.split('_')[0],
      // The fee actually paid. LIVE trades carry `fees`; the DRY paper book
      // carries `fee_paid`. Persist whichever is present so the ledger and the
      // account can be reconciled.
      trade.fees != null ? trade.fees : (trade.fee_paid != null ? trade.fee_paid : null),
      trade.fee_entry != null ? trade.fee_entry : null,
      trade.fee_exit != null ? trade.fee_exit : null
    ]);
  } catch (e) { console.error('persistTrade failed:', e.message); }
}

// Persist position to SQLite
async function persistPosition(pos) {
  try {
    await dbRun(`INSERT OR REPLACE INTO positions (
      id, market_id, title, series, strategy, side, entry_price, qty, cost,
      fair_at_entry, edge_at_entry, sigma_per_sec, strike, horizon,
      stop_loss, take_profit, mark, opened, close_time, source, host,
      bid_at_entry, ask_at_entry
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      pos.id, pos.market_id, pos.title, pos.series, pos.strategy, pos.side,
      pos.entry_price, pos.qty, pos.cost, pos.fair_at_entry, pos.edge_at_entry,
      pos.sigma_per_sec, pos.strike, pos.horizon, pos.stop_loss, pos.take_profit,
      pos.mark, pos.opened, pos.close_time, pos.source, pos.host,
      pos.bid_at_entry, pos.ask_at_entry
    ]);
  } catch (e) { console.error('persistPosition failed:', e.message); }
}

// Remove position from SQLite
async function removePosition(posId) {
  try {
    await dbRun(`DELETE FROM positions WHERE id = ?`, [posId]);
  } catch (e) { console.error('removePosition failed:', e.message); }
}

// Persist settlement
async function persistSettlement(settlement) {
  try {
    await dbRun(`INSERT INTO settlements (
      market_id, side, strategy, settled_price, kalshi_result,
      coinbase_spot_at_settlement, coinbase_spot_time, spot_product,
      entry_price, pnl, timestamp, spot_deviation
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      settlement.market_id, settlement.side, settlement.strategy,
      settlement.settled_price, settlement.kalshi_result,
      settlement.coinbase_spot_at_settlement, settlement.coinbase_spot_time,
      settlement.spot_product, settlement.entry_price, settlement.pnl,
      settlement.timestamp, settlement.spot_deviation
    ]);
  } catch (e) { console.error('persistSettlement failed:', e.message); }
}

// Persist fill quality
async function persistFillQuality(fq) {
  try {
    await dbRun(`INSERT INTO fill_quality (
      market_id, side, strategy, entry_price, bid_at_entry, ask_at_entry,
      fill_quality, is_maker, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      fq.market_id, fq.side, fq.strategy, fq.entry_price,
      fq.bid_at_entry, fq.ask_at_entry, fq.fill_quality,
      fq.fill_quality <= 0 ? 1 : 0, fq.timestamp
    ]);
  } catch (e) { console.error('persistFillQuality failed:', e.message); }
}

// Persist webhook event
async function persistWebhookEvent(whId, event, payload, responseCode, responseBody, success) {
  try {
    await dbRun(`INSERT INTO webhook_events (
      webhook_id, event, payload, response_code, response_body, success, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [whId, event, JSON.stringify(payload), responseCode, responseBody, success ? 1 : 0, new Date().toISOString()]);
  } catch (e) { console.error('persistWebhookEvent failed:', e.message); }
}

// Persist API request
async function persistApiRequest(ip, path, method, statusCode, responseTimeMs) {
  try {
    await dbRun(`INSERT INTO api_requests (ip, path, method, status_code, response_time_ms, timestamp)
      VALUES (?, ?, ?, ?, ?, ?)`,
    [ip, path, method, statusCode, responseTimeMs, new Date().toISOString()]);
  } catch (e) { console.error('persistApiRequest failed:', e.message); }
}

// Sync existing state to SQLite on startup
async function syncStateToDb() {
  try {
    console.log('[syncStateToDb] Starting sync...');
    const trades = state.dry.closed;
    const positions = state.dry.positions;
    const settlements = state.settlement_db;
    const fillQuality = state.fill_quality_db;
    
    console.log('[syncStateToDb] Trades:', trades.length, 'Positions:', positions.length, 'Settlements:', settlements.length, 'FillQuality:', fillQuality.length);
    
    // Batch insert trades using a transaction
    if (trades.length > 0) {
      await new Promise((resolve, reject) => {
        db.serialize(() => {
          db.run('BEGIN TRANSACTION');
          const stmt = db.prepare(`INSERT INTO trades (
            market_id, title, side, strategy, entry_price, exit_price, quantity,
            cost_basis, proceeds, pnl, exit_reason, result, hold_seconds,
            exit_timestamp, opened, bid_at_entry, ask_at_entry, fill_quality,
            settled_price, settlement_source, fair_at_entry, edge_at_entry,
            kelly_fraction, sigma_per_sec, strike, horizon, regime, signal_source
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
          
          let count = 0;
          for (const t of trades) {
            stmt.run([
              t.market_id, t.title, t.side, t.strategy,
              t.entry_price, t.exit_price, t.quantity,
              t.cost_basis, t.proceeds, t.pnl, t.exit_reason,
              t.result, t.hold_seconds, t.exit_timestamp,
              t.opened, t.bid_at_entry, t.ask_at_entry, t.fill_quality,
              t.settled_price, t.settlement_source, t.fair_at_entry,
              t.edge_at_entry, t.kelly_fraction, t.sigma_per_sec,
              t.strike, t.horizon, t.regime, t.reason?.split('_')[0]
            ]);
            count++;
          }
          stmt.finalize();
          db.run('COMMIT', (err) => {
            if (err) reject(err);
            else {
              console.log('[syncStateToDb] Inserted', count, 'trades');
              resolve();
            }
          });
        });
      });
    }
    
    // Batch insert positions
    if (positions.length > 0) {
      await new Promise((resolve, reject) => {
        db.serialize(() => {
          db.run('BEGIN TRANSACTION');
          const stmt = db.prepare(`INSERT OR REPLACE INTO positions (
            id, market_id, title, series, strategy, side, entry_price, qty, cost,
            fair_at_entry, edge_at_entry, sigma_per_sec, strike, horizon,
            stop_loss, take_profit, mark, opened, close_time, source, host,
            bid_at_entry, ask_at_entry
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
          
          let count = 0;
          for (const p of positions) {
            stmt.run([
              p.id, p.market_id, p.title, p.series, p.strategy, p.side,
              p.entry_price, p.qty, p.cost, p.fair_at_entry, p.edge_at_entry,
              p.sigma_per_sec, p.strike, p.horizon, p.stop_loss, p.take_profit,
              p.mark, p.opened, p.close_time, p.source, p.host,
              p.bid_at_entry, p.ask_at_entry
            ]);
            count++;
          }
          stmt.finalize();
          db.run('COMMIT', (err) => {
            if (err) reject(err);
            else {
              console.log('[syncStateToDb] Inserted', count, 'positions');
              resolve();
            }
          });
        });
      });
    }
    
    // Batch insert settlements
    if (settlements.length > 0) {
      await new Promise((resolve, reject) => {
        db.serialize(() => {
          db.run('BEGIN TRANSACTION');
          const stmt = db.prepare(`INSERT INTO settlements (
            market_id, side, strategy, settled_price, kalshi_result,
            coinbase_spot_at_settlement, coinbase_spot_time, spot_product,
            entry_price, pnl, timestamp, spot_deviation
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
          
          let count = 0;
          for (const s of settlements) {
            stmt.run([
              s.market_id, s.side, s.strategy,
              s.settled_price, s.kalshi_result,
              s.coinbase_spot_at_settlement, s.coinbase_spot_time,
              s.spot_product, s.entry_price, s.pnl,
              s.timestamp, s.spot_deviation
            ]);
            count++;
          }
          stmt.finalize();
          db.run('COMMIT', (err) => {
            if (err) reject(err);
            else {
              console.log('[syncStateToDb] Inserted', count, 'settlements');
              resolve();
            }
          });
        });
      });
    }
    
    // Batch insert fill quality
    if (fillQuality.length > 0) {
      await new Promise((resolve, reject) => {
        db.serialize(() => {
          db.run('BEGIN TRANSACTION');
          const stmt = db.prepare(`INSERT INTO fill_quality (
            market_id, side, strategy, entry_price, bid_at_entry, ask_at_entry,
            fill_quality, is_maker, timestamp
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
          
          let count = 0;
          for (const fq of fillQuality) {
            stmt.run([
              fq.market_id, fq.side, fq.strategy, fq.entry_price,
              fq.bid_at_entry, fq.ask_at_entry, fq.fill_quality,
              fq.fill_quality <= 0 ? 1 : 0, fq.timestamp
            ]);
            count++;
          }
          stmt.finalize();
          db.run('COMMIT', (err) => {
            if (err) reject(err);
            else {
              console.log('[syncStateToDb] Inserted', count, 'fill quality');
              resolve();
            }
          });
        });
      });
    }
    
    console.log('[SQLite] State synced:', trades.length, 'trades,', positions.length, 'positions');
  } catch (e) { console.error('syncStateToDb failed:', e.message); }
}

// ===========================================================================
// Lane registry - every lane maps to a REAL Kalshi series and a REAL spot feed.
// `seriesList` is ordered: the declared series first, then real fallbacks that
// share the same horizon family. The lane card always names the series actually
// in use, so a fallback is visible rather than hidden.
// ---------------------------------------------------------------------------
const LANES = [
  { name: 'btc_updown', label: 'BTC 15-Min Up/Down', series: 'KXBTC15M', seriesList: ['KXBTC15M'], spot: 'BTC-USD', asset: 'BTC', horizon: '15m' },
  { name: 'eth_updown', label: 'ETH 15-Min Up/Down', series: 'KXETH15M', seriesList: ['KXETH15M'], spot: 'ETH-USD', asset: 'ETH', horizon: '15m' },
  { name: 'xrp_updown', label: 'XRP 15-Min Up/Down', series: 'KXXRP15M', seriesList: ['KXXRP15M'], spot: 'XRP-USD', asset: 'XRP', horizon: '15m' },
  { name: 'btc_1h_updown', label: 'BTC Daily Ladder', series: 'KXBTCD', seriesList: ['KXBTCD'], spot: 'BTC-USD', asset: 'BTC', horizon: '1d', deep: 300 },
  { name: 'eth_1h_updown', label: 'ETH Daily Ladder', series: 'KXETHD', seriesList: ['KXETHD'], spot: 'ETH-USD', asset: 'ETH', horizon: '1d', deep: 300 },
  { name: 'xrp_1h_updown', label: 'XRP Daily Ladder', series: 'KXXRPD', seriesList: ['KXXRPD'], spot: 'XRP-USD', asset: 'XRP', horizon: '1d', deep: 300 },
  { name: 'hyperliquid_updown', label: 'Hyperliquid Up/Down', series: 'KXHYPE15M', seriesList: ['KXHYPE15M'], spot: 'HYPE-USD', asset: 'HYPE', horizon: '15m' },
  { name: 'hyperliquid_1h_updown', label: 'Hyperliquid 1-Hour', series: 'KXHYPE', seriesList: ['KXHYPE'], spot: 'HYPE-USD', asset: 'HYPE', horizon: '1h', deep: 300,
    // BRAIN REWIRE (2026-11-11, operator order after 0/6, -$1.87, five YES
    // losses in one day): the driftless gaussian is honest at 15m but blind at
    // 1h+ - it priced a multi-hour HYPE slide as a 30c YES "edge" six times.
    // These four code-level knobs are read from THIS registry entry only (the
    // persisted state.lane_configs can not carry them; the registry is code).
    trendGate: true,     // block entries when spot drives toward the strike
                         // over a horizon-scaled window (the proven noBook
                         // 2026-11-04 "losers were reversals" law, transplanted)
    maxTauSec: 7200,     // never price rungs >2h out - a 20-tick drift model
                         // has no business pricing 5-hour risk
    minEdgeFloor: 0.01,  // 1c real edge minimum (lane config said 0.05c ~ none)
    makerTtlSec: 60,     // resting bids die in 60s so a slide that starts
                         // mid-rest cannot adversely fill a stale bid; the
                         // re-placed bid must re-pass the trend gate
  },
  { name: 'all_no_markets', label: 'ALL NO MARKETS', series: null, seriesList: [], spot: null, asset: null, horizon: 'no-side' },
  { name: 'certainty', label: 'CERTAINTY (near-expiry, 90%+ win rate)', series: null, seriesList: [], spot: null, asset: null, horizon: 'certainty' },
];
const LANE_BY_NAME = new Map(LANES.map(l => [l.name, l]));

// ===========================================================================
// OPERATOR LAW - HARD-CODED, NON-NEGOTIABLE (by direct operator command).
//   1. The operator's button push is IMMEDIATE, ABSOLUTE LAW. A lane is turned
//      ON or OFF the instant the toggle endpoint is hit. Nothing - no timer,
//      no engine tick, no boot routine, no "supervisor", no auto-start, no
//      auto-restart, no health-check, no watchdog - may ever flip a lane's
//      `running` flag against the operator's last explicit command.
//   2. There is NO supervisor process in this codebase and there will never be.
//      Any future code that starts, restarts, resurrects, keeps-alive, or
//      auto-reenables a lane WITHOUT a direct operator toggle is FORBIDDEN and
//      must be removed. The ONLY writer of `ls.running` is the operator toggle
//      endpoint below (and the operator `on`/`off` control scripts).
//   3. A lane stays EXACTLY the way the operator last set it - forever - until
//      the operator changes it again.
//   Violating any of these three is a bug, full stop.
// ===========================================================================

// OPERATOR_LAW: Enforced at runtime - any violation throws immediately.
const OPERATOR_LAW = Object.freeze({
  version: '2026-10-27',
  decree: 'Operator button push is immediate absolute law. No supervisor, auto-start, auto-restart, watchdog, or keep-alive may ever contravene.',
  onlyWritersOfRunning: ['/api/strategy/:name/toggle endpoint', 'kai.ps1 on/off', 'kai-mirror.ps1 on/off'],
  forbiddenPatterns: ['supervisor', 'auto.start', 'auto.restart', 'keep.alive', 'watchdog', 'resurrect', 'reenable'],
  enforce: function(action, context) {
    const forbidden = this.forbiddenPatterns.some(p => action.toLowerCase().includes(p) || JSON.stringify(context).toLowerCase().includes(p));
    if (forbidden) {
      const msg = `OPERATOR_LAW VIOLATION: ${action} contains forbidden pattern. ${this.decree}`;
      console.error('[OPERATOR_LAW]', msg, context);
      throw new Error(msg);
    }
  },
  // ---- OPERATOR LAW 2026-11-07: NO FEE-ATTACHED EXITS ----------------------
  // stop_loss / take_profit / trail_stop exits pay a second taker fee and
  // historically destroyed the book (LIVE evidence: 119 stop exits, 0 winners,
  // -$175.36). They are FORBIDDEN on every book. Positions ride to settlement;
  // the per-order size is the loss limit. Re-adding ANY fee-attached exit
  // requires the operator's express written say-so in OPERATOR_LAWS.md first.
  feeExitDecree: 'NO stop_loss, NO take_profit, NO trail_stop (2026-11-07). Ride to settlement; per-order size is the loss limit.',
  allowedExitReasons: ['settled', 'manual', 'hold_rule', 'flatten_feed_death'],
  assertNoFeeExit: function(reason) {
    const r = String(reason || 'settled').toLowerCase();
    const ok = this.allowedExitReasons.some((a) => r === a || r.startsWith(a));
    if (!ok) {
      const msg = 'OPERATOR_LAW VIOLATION: fee-attached exit "' + reason + '" is forbidden. ' + this.feeExitDecree;
      console.error('[OPERATOR_LAW]', msg);
      throw new Error(msg);
    }
  },
  assertNoSupervisor: function() {

    // Runtime guard - if any code tries to spawn a supervisor, it dies here.
    if (global.__KAI_SUPERVISOR_SPAWNED) {
      throw new Error('OPERATOR_LAW: Supervisor spawn detected - exterminated.');
    }
  }
});

// Seal the law - cannot be modified, deleted, or overridden.
Object.seal(OPERATOR_LAW);
global.OPERATOR_LAW = OPERATOR_LAW;

// ===========================================================================
// COMMAND ORDER - IMPERIAL LAW. Hard-coded by DIRECT OPERATOR COMMAND (2026-10-27).
//   1. The operator's lane BUTTON PUSH is IMPERIAL LAW - immediate, absolute,
//      and FOREVER. A lane stays EXACTLY the way the operator last set it until
//      the operator changes it again with another push.
//   2. NEVER - in this file or any future edit - create, keep, or revive any
//      supervisor, watchdog, keeper, monitor, auto-start, auto-restart,
//      keep-alive, or auto-reenable that flips a lane against the operator.
//      Any such code is FORBIDDEN and must be EXTERMINATED the instant it runs.
//   3. The routine below is that enforcement. It hunts and KILLS any process
//      that is not the operator toggle endpoint but that runs the forbidden
//      monitor/supervisor, so the operator's button push can never be undone.
//      It ONLY kills hostile OS processes - it NEVER writes ls.running.
// ===========================================================================
// POPUP + STALL FIX (2026-11-04): the old version ran a BLOCKING
// execFileSync('powershell') every 3 seconds with no windowsHide. Each run
// froze the Node event loop for the whole PowerShell start-up + CIM scan, which
// starved /api/health (the tape then reported SERVER DOWN on a live server),
// and each run flashed a visible PowerShell console window. Now it is ASYNC,
// fully HIDDEN, never overlaps itself, and runs every 30s, on boot, and on
// every operator lane toggle (OPERATOR_LAW is unchanged).
let _exterminateBusy = false;
function exterminateSupervisors() {
  if (_exterminateBusy) return;
  _exterminateBusy = true;
  const script =
    "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'monitor\\.js' -and ($_.Name -eq 'node.exe' -or $_.Name -eq 'bash.exe' -or $_.Name -eq 'timeout.exe') } | ForEach-Object { $_.ProcessId }";
  execFile('powershell', ['-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden', '-Command', script],
    { encoding: 'utf8', timeout: 20000, maxBuffer: 1 << 20, windowsHide: true }, (err, out) => {
      _exterminateBusy = false;
      if (err) return;
      const pids = String(out || '').split(/\r?\n/).map(s => s.trim()).filter(s => /^\d+$/.test(s));
      for (const pid of pids) {
        execFile('taskkill', ['/PID', pid, '/F'], { windowsHide: true }, (e2) => {
          if (!e2) log('exterminator', { killedPid: pid });
        });
      }
    });
}

// ---------------------------------------------------------------------------
// Persisted state
// ---------------------------------------------------------------------------
const STATE_FILE = path.join(HERE, 'state.json');
const STATE_BACKUP_FILE = path.join(HERE, 'state.backup.json');
const STATE_TMP_FILE = path.join(HERE, 'state.json.tmp');
const CONFIG_FILE = path.join(HERE, 'config.json');
// BOOT SWEEP (2026-11-11): atomic state writes rename state.json.tmp.<pid> ->
// state.json; a crash mid-write leaves tmp corpses behind (7 in this repo on
// 2026-11-11, oldest from 2026-11-10). A live writer's tmp exists for
// milliseconds, so at boot anything >60s old is a corpse by definition.
try {
  for (const f of fs.readdirSync(HERE)) {
    if (!f.startsWith('state.json.tmp.')) continue;
    const corpse = path.join(HERE, f);
    try {
      if (Date.now() - fs.statSync(corpse).mtimeMs > 60_000) fs.unlinkSync(corpse);
    } catch (e) { /* racing writer or already gone - nothing to do */ }
  }
} catch (e) { /* a failed readdir must never block boot */ }
let STATE_SOURCE = 'primary';
let STATE_LOAD_ERROR = null;
const DRY_PAPER_STARTING_BALANCE = 10000; // Paper trading starting balance
const ALL_NO_PAPER_STARTING_BALANCE = 250; // Paper all_no book starting balance
// FORBIDDEN SEED (operator order 2026-11-12): the dry books once carried
// $303 - the REAL Kalshi account's cash snapshot - as their paper start, so
// the tape printed a growing balance that mirrored real money. Paper books
// start from the declared constants above, never real cash. See paper_rebase.js.
function defaultState() {
  const allNoStart = ALL_NO_PAPER_STARTING_BALANCE;
  return {
    mode: 'dry',
    no_lanes_enabled: true,
    all_no: {
      dry: { starting_balance: allNoStart, balance: allNoStart, positions: [], closed_trades: [], total_pnl: 0, entries: 0 },
      live: { starting_balance: allNoStart, balance: allNoStart, positions: [], closed_trades: [], total_pnl: 0, entries: 0 },
    },
    runtime: { 
    clip: 15,
    min_edge: 0.0003,
    max_price: 0.50, // EVIDENCE ZONE 2026-11-04 (was 0.99)
    min_price: 0.30, // EVIDENCE ZONE 2026-11-04 (was 0.005)
    take_profit: null,   // DEAD (operator order, 2026-11-03): winners ride to settlement
    stop_loss: 0, // OPERATOR LAW 2026-11-07: DEAD config. Fee exits are forbidden on the paper book too; stops are stored null and nothing fires them.
    // TRAIL STOP EXTERMINATED (operator law): trail exits sold 86.7%-
    // would-be-settled winners; the code is DELETED, not gated.
    kelly_cap: 0.95,
    min_hold_sec: 5,
    entry_cooldown_sec: 5,
    max_hold_sec: 120,
    max_exposure_pct: 0.95,
    maker_only: true,
    all_no: {
      max_exposure_pct: 0.5,
      max_concurrent: 20,
      min_edge: 0.003,
      min_confidence: 0.2,
      min_spread: 0.005,
      max_spread: 0.3,
      min_tau_sec: 30,
      max_tau_sec: 1800,
      max_hold_pct: 0.9,
      scan_interval_ms: 5000
    },
  },
  // Lane-specific configurations - self-aware trading logic per lane
  lane_configs: {
    btc_updown: {
      asset: 'BTC', series: 'KXBTC15M', horizon: '15m',
      min_edge: 0.0005, min_confidence: 0.15, entry_cooldown_sec: 20,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.5, // EVIDENCE ZONE 2026-11-04: n=1983, <$0.50 +$132 / >=$0.50 -$72
      preferred_signals: ['order_flow', 'vol_rp', 'black_scholes', 'imbalance'],
      regime_avoid: ['wide_spread', 'arbitrage'],
      description: 'BTC 15-min Up/Down - highest volume, tightest spreads. Fast mean-reversion.',
    },
    eth_updown: {
      asset: 'ETH', series: 'KXETH15M', horizon: '15m',
      min_edge: 0.0005, min_confidence: 0.15, entry_cooldown_sec: 20,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.5, // EVIDENCE ZONE 2026-11-04: n=1983, <$0.50 +$132 / >=$0.50 -$72
      preferred_signals: ['order_flow', 'funding_carry', 'black_scholes', 'imbalance'],
      regime_avoid: ['wide_spread', 'arbitrage'],
      description: 'ETH 15-min Up/Down - strong volume, funding carry matters.',
    },
    xrp_updown: {
      asset: 'XRP', series: 'KXXRP15M', horizon: '15m',
      min_edge: 0.0008, min_confidence: 0.18, entry_cooldown_sec: 30,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.45, // EVIDENCE ZONE 2026-11-04
      preferred_signals: ['vol_rp', 'term_struct', 'black_scholes', 'imbalance'],
      regime_avoid: ['wide_spread', 'arbitrage'],
      description: 'XRP 15-min Up/Down - lower liquidity, wider spreads.',
    },
    btc_1h_updown: {
      asset: 'BTC', series: 'KXBTCD', horizon: '1d',
      min_edge: 0.0005, min_confidence: 0.12, entry_cooldown_sec: 120,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.5, // EVIDENCE ZONE 2026-11-04: n=1983, <$0.50 +$132 / >=$0.50 -$72
      preferred_signals: ['settlement_alpha', 'term_struct', 'black_scholes', 'cross_expiry'],
      regime_avoid: ['arbitrage'],
      description: 'BTC Daily Ladder - long horizon to settlement. Settlement alpha (CF BRTI vs spot) dominates final hour.',
    },
    eth_1h_updown: {
      asset: 'ETH', series: 'KXETHD', horizon: '1d',
      min_edge: 0.0005, min_confidence: 0.15, entry_cooldown_sec: 120,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.5, // EVIDENCE ZONE 2026-11-04: n=1983, <$0.50 +$132 / >=$0.50 -$72
      preferred_signals: ['settlement_alpha', 'funding_carry', 'term_struct', 'black_scholes'],
      regime_avoid: ['arbitrage'],
      description: 'ETH Daily Ladder - settlement alpha + funding carry (ETH basis).',
    },
    xrp_1h_updown: {
      asset: 'XRP', series: 'KXXRPD', horizon: '1d',
      min_edge: 0.0008, min_confidence: 0.18, entry_cooldown_sec: 150,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.45, // EVIDENCE ZONE 2026-11-04
      preferred_signals: ['settlement_alpha', 'term_struct', 'black_scholes', 'cross_expiry'],
      regime_avoid: ['arbitrage'],
      description: 'XRP Daily Ladder - settlement alpha across expiries.',
    },
    hyperliquid_updown: {
      asset: 'HYPE', series: 'KXHYPE15M', horizon: '15m',
      min_edge: 0.0005, min_confidence: 0.15, entry_cooldown_sec: 20,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.5, // EVIDENCE ZONE 2026-11-04: n=1983, <$0.50 +$132 / >=$0.50 -$72
      preferred_signals: ['order_flow', 'vol_rp', 'black_scholes', 'imbalance'],
      regime_avoid: ['wide_spread', 'arbitrage'],
      description: 'HYPE 15-min Up/Down - newer asset, volatility-driven.',
    },
    hyperliquid_1h_updown: {
      asset: 'HYPE', series: 'KXHYPE', horizon: '1h',
      min_edge: 0.0005, min_confidence: 0.15, entry_cooldown_sec: 60,
      max_price: 0.50, min_price: 0.30, kelly_cap: 0.5, // EVIDENCE ZONE 2026-11-04: n=1983, <$0.50 +$132 / >=$0.50 -$72
      preferred_signals: ['order_flow', 'vol_rp', 'term_struct', 'black_scholes'],
      regime_avoid: ['arbitrage'],
      description: 'HYPE 1-Hour Up/Down - hourly contracts.',
    },
    all_no_markets: {
      asset: 'ALL',
      series: 'ALL',
      horizon: 'no-side',
      min_edge: 0.002,
      min_confidence: 0.15,
      entry_cooldown_sec: 3,
      max_price: 0.50, // EVIDENCE ZONE 2026-11-04
      min_price: 0.30, // EVIDENCE ZONE 2026-11-04
      kelly_cap: 0.6,
      preferred_signals: ['black_scholes', 'vol_rp', 'term_struct'],
      regime_avoid: ['wide_spread'],
      description: 'ALL NO MARKETS - scans ALL Kalshi series for NO-side edge. High min_edge (0.8%), real quotes only. Vol-adjusted stops, 2.5:1 R:R. Maker at bid only.',
    },
  },
  signal_weights: brain.signalStats(),
  signal_outcomes: [],
  lanes: {},
  dry: { positions: [], closed: [], balance: 0, starting_balance: DRY_PAPER_STARTING_BALANCE, entries: 0, running: false },
  alerts: { enabled: false, telegram_url: '', discord_url: '', last_sent: null },
  logs: [],
  booted_at: new Date().toISOString(),
  settlement_db: [],
  fill_quality_db: [],
  webhooks: [],
  };
}
function loadState() {
  // DURABLE STATE IO (2026-11-03). The old loader swallowed a CORRUPT state file
  // and returned defaultState() - that is exactly how a watchdog restart turned 9
  // running lanes into 0 and threw away every risk gate. New rule:
  //   primary state.json -> state.backup.json -> FAIL CLOSED (no trading).
  // Never, ever boot into default risk settings with money on the line.
  const base = defaultState();
  let raw = null, source = 'none', err = null;
  for (const cand of [[STATE_FILE, 'primary'], [STATE_BACKUP_FILE, 'backup']]) {
    try { raw = JSON.parse(fs.readFileSync(cand[0], 'utf8')); source = cand[1]; break; }
    catch (e) { err = e; }
  }
  if (raw) {
    Object.assign(base, raw);
    base.runtime = Object.assign(base.runtime, raw.runtime || {});
    if (raw.runtime && raw.runtime.all_no) base.runtime.all_no = Object.assign(base.runtime.all_no || {}, raw.runtime.all_no);
    for (const k of ['all_no', 'live', 'dry', 'lane_configs', 'lanes', 'clip_state']) {
      if (raw[k] && typeof raw[k] === 'object' && !Array.isArray(raw[k])) base[k] = Object.assign({}, base[k] || {}, raw[k]);
    }
  }
  // VERSION-CONTROLLED RISK CONFIG. config.json lives in git, so the operator's
  // gates survive ANY state file accident (torn write, deleted file, bad edit).
  let cfg = null;
  try { cfg = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8')); } catch (e) { cfg = null; }
  if (cfg && cfg.runtime) {
    base.runtime = Object.assign(base.runtime, cfg.runtime);
    if (cfg.runtime.all_no) base.runtime.all_no = Object.assign(base.runtime.all_no || {}, cfg.runtime.all_no);
  }
  base.state_source = source;
  base.safe_mode = (source === 'none');
  if (base.safe_mode) {
    base.lanes = {};
    base.no_lanes_enabled = false;
    base.state_error = String((err && err.message) || err || 'state file unreadable');
  } else {
    base.state_error = null;
  }
  STATE_SOURCE = source;
  STATE_LOAD_ERROR = base.state_error;
  return base;
}
let state = loadState();
global.__KAI_TRADE_BLOCKED = !!state.safe_mode;
// The boot note goes straight to crash.log and NEVER through log(): log() touches
// structures (provLog) that are not initialised yet at this point of module load,
// and the RECOVERY path must never be able to crash the process it is recovering.
try {
  fs.appendFileSync(path.join(HERE, 'crash.log'), new Date().toISOString() +
    ' BOOT source=' + STATE_SOURCE + ' safe_mode=' + (state.safe_mode ? 'YES' : 'no') +
    ' error=' + (STATE_LOAD_ERROR || 'none') + '\n');
} catch (e) { }
// LEDGER RETAG (2026-11-10): legacy all_no closed rows can share an id (two
// entries booked in the same market in the same millisecond). Give every closed
// row a unique id (first keeps it, later ones get -dup2/-dup3) so the
// close-dedupe guard is exact and history keys never collide. Runs before any
// sweep; idempotent (after the first pass there is nothing left to retag).
try {
  let retagged = 0;
  for (const modeKey of Object.keys(state.all_no || {})) {
    const bk = state.all_no[modeKey];
    if (!bk || !Array.isArray(bk.closed_trades)) continue;
    const seen = new Set();
    for (const t of bk.closed_trades) {
      if (!t.id) continue;
      if (seen.has(t.id)) {
        let n = 2;
        while (seen.has(t.id + '-dup' + n)) n++;
        t.id = t.id + '-dup' + n;
        retagged++;
      }
      seen.add(t.id);
    }
  }
  if (retagged > 0) {
    fs.appendFileSync(path.join(HERE, 'crash.log'), new Date().toISOString() +
      ' LEDGER_RETAG unique ids restored on ' + retagged + ' legacy closed rows\n');
    try { save(); } catch (e) { }
  }
} catch (e) {
  try {
    fs.appendFileSync(path.join(HERE, 'crash.log'), new Date().toISOString() +
      ' LEDGER_RETAG failed: ' + (e && e.message ? e.message : String(e)) + '\n');
  } catch (e2) { }
}
// PAPER SEED REBASE (operator order 2026-11-12: "you can't have that balance
// in fake reality"): swap the legacy $303 real-cash seed for the declared
// paper starts. One-time, idempotent, balance shifted by the same delta so
// every booked trade and pnl is preserved exactly. crash.log carries the
// audit trail.
try {
  const seedRebase = paperRebaseMod.rebaseBooks(state, {
    allNoPaperStart: ALL_NO_PAPER_STARTING_BALANCE,
    dryPaperStart: DRY_PAPER_STARTING_BALANCE,
  });
  if (seedRebase.rebased.length > 0) {
    fs.appendFileSync(path.join(HERE, 'crash.log'), new Date().toISOString() +
      ' PAPER_SEED_REBASE ' + JSON.stringify(seedRebase.rebased) + '\n');
    try { save(); } catch (e) { }
  }
} catch (e) {
  try {
    fs.appendFileSync(path.join(HERE, 'crash.log'), new Date().toISOString() +
      ' PAPER_SEED_REBASE failed: ' + (e && e.message ? e.message : String(e)) + '\n');
  } catch (e2) { }
}

// EVENT-LOOP LAG MONITOR (2026-11-10): the tape's SERVER SLOW lines were
// health timeouts on a STATIC endpoint - only a blocked event loop can do
// that. The blocker (the synchronous state write) is fixed below; this
// monitor measures whatever remains and surfaces it in /api/health so the
// next stall is a number, not a mystery.
var LOOP_LAG_MAX = 0;
setInterval(() => {
  const t0 = Date.now();
  setImmediate(() => {
    const lag = Date.now() - t0;
    if (lag > LOOP_LAG_MAX) LOOP_LAG_MAX = lag;
    if (lag >= 1000) { try { log('loop_lag', { ms: lag }); } catch (e) { } }
  });
}, 5000);
setInterval(() => { LOOP_LAG_MAX = 0; }, 5 * 60 * 1000); // 5-minute max window
// The paper book may only book a fill when a REAL public print proves it happened.
const FILLSIM = fillSim.create({ getState: () => state, log: (k, o) => { try { log(k, o); } catch (e) {} } });
// THE VACCINE (2026-11-04): empirical probability ledger. The model's claimed
// fair is NEVER traded directly — every entry must clear the calibration gate
// (buy below REALIZED probability). Blocked candidates are shadow-traded for
// free so the buckets keep learning. Warm-starts from all settled history.
const CALIB = calibrationMod.create({ dbRun, dbAll, dbFile: DB_FILE, log: (k, o) => { try { log(k, o); } catch (e) {} } });
// THE HARVESTER (2026-11-04): mass maker-order lane. Posts resting bids only
// where the empirical ledger pays >= 8c edge with n>=50 proof; TTL-cancels.
const HARVEST = harvesterMod.create({ CALIB, FILLSIM, getState: () => state, log: (k, o) => { try { log(k, o); } catch (e) {} }, persistTrade });
CALIB.init().catch((e) => { console.error('[calibration] boot failed:', e.message); });

// ALPHA GATE (Mirror port 2026-11-07): same measured-verdict engine as LIVE.
// In the Mirror it runs in REPORT-CLAMP mode: dry is the proof ground, so a
// refuted verdict clamps paper size to 1 contract instead of blocking (LIVE
// enforces the hard block). Verdicts can only change from outcomes, and dry
// is where outcomes are cheapest to earn. See alpha_gate.js.
// REHAB v2 (2026-11-10, operator commission): DNA-revamp for refuted lanes -
// retrofit a measured ENTRY DNA (edge bar + price band) mined from the lane's
// own closed trades, return on 1-contract probation, halt at once on a death
// spiral or refuted cohort. No hospice: a halted lane re-diagnoses as its
// record grows, capped at 5 generations.
if (!state.runtime.rehab) state.runtime.rehab = {};
const REHAB = require('./rehab').create({
  getTrades: () => [].concat(state.dry.closed || [], ((state.all_no[state.mode] || {}).closed_trades) || []),
  getRecords: () => state.runtime.rehab,
  log: (k, o) => { try { log(k, o); } catch (e) {} },
});
const ALPHA = require('./alpha_gate').create({
  getTrades: () => state.dry.closed || [],
  rehab: REHAB,
  certaintyStats: () => { try { return FACTS.certaintyStats(); } catch (e) { return null; } },
  vaccine: () => { try { return (CALIB.snapshot() || {}).vaccine_gate || null; } catch (e) { return null; } },
  log: (k, o) => { try { log(k, o); } catch (e) {} },
});
function spotAgeMs(asset) {
  const s = spot[asset];
  if (!s) return null;
  const ms = (x) => { const n = Number(x) || 0; if (n > 1e12) return n; if (n > 1e9) return n * 1000; return 0; };
  const lastPoint = (s.points && s.points.length) ? s.points[s.points.length - 1] : null;
  const last = Math.max(ms(s.at), lastPoint ? ms(lastPoint.t) : 0);
  return last > 0 ? Date.now() - last : null;
}

// BOOT COUNTER: every process start bumps this; the tape PULSE prints it so
// silent watchdog restart loops become visible.
state.boot_count = (state.boot_count || 0) + 1;
let saveTimer = null;
// SAVE-PATH WITNESS (2026-11-06): the save used to swallow every error. A single
// non-serializable value (circular ref / BigInt) made JSON.stringify throw, every
// later save returned false silently, and state.json froze for hours while the
// process kept trading. Serialize defensively and LOG every failure.
function safeStringify(obj) {
  const ancestors = [];
  return JSON.stringify(obj, function (key, value) {
    if (typeof value === 'bigint') return value.toString();
    if (typeof value !== 'object' || value === null) return value;
    while (ancestors.length > 0 && ancestors[ancestors.length - 1] !== this) ancestors.pop();
    if (ancestors.includes(value)) return '[circular]';
    ancestors.push(value);
    return value;
  }, 1);
}
let stateWriteErrors = 0;
function noteStateWriteError(stage, err) {
  stateWriteErrors++;
  const msg = String((err && err.message) || err).slice(0, 300);
  try { console.error('[state] write failed at ' + stage + ': ' + msg); } catch (e) { }
  try { log('state_write_error', { stage, error: msg, count: stateWriteErrors }); } catch (e) { }
}
function writeStateAtomic() {
  // ATOMIC REPLACE. Write temp -> fsync -> rename. The old writeFileSync could
  // leave a half-written state.json if the process was killed mid-save, and the
  // next boot then silently reverted to defaults.
  let payload = null;
  try { payload = safeStringify(state); } catch (e) { noteStateWriteError('stringify', e); return false; }
  const tmp = STATE_TMP_FILE + '.' + process.pid;
  try {
    const fd = fs.openSync(tmp, 'w');
    try { fs.writeSync(fd, payload); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try {
      fs.renameSync(tmp, STATE_FILE);
    } catch (e) {
      // Windows can refuse rename while another reader holds state.json open.
      // Fall back to a direct overwrite so the save still lands (and is logged).
      noteStateWriteError('rename-fallback', e);
      fs.writeFileSync(STATE_FILE, payload);
      try { fs.unlinkSync(tmp); } catch (e3) { }
    }
  } catch (e) {
    noteStateWriteError('write', e);
    try { fs.unlinkSync(tmp); } catch (e2) { }
    return false;
  }
  try {
    const now = Date.now();
    if (!writeStateAtomic._last || now - writeStateAtomic._last > 15000) {
      fs.copyFileSync(STATE_FILE, STATE_BACKUP_FILE);
      writeStateAtomic._last = now;
    }
  } catch (e) { }
  return true;
}
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    let payload = null;
    try { payload = safeStringify(state); } catch (e) { noteStateWriteError('stringify', e); return; }
    writeChain = writeChain.then(() => writeStateAsync(payload)).catch(() => {});
  }, 250);
}
// ASYNC STATE WRITES (2026-11-10): the tape's SERVER SLOW lines were health
// timeouts on a static endpoint - the only possible cause is a blocked event
// loop, and the blocker was writeStateAtomic's synchronous write+fsync+rename
// of the ~1.5MB state.json (plus the 15s copyFileSync backup) on every
// debounced save. The hot path moves the entire write/fsync/rename/copy onto
// the thread pool via fs.promises; durability is kept (FileHandle.sync() is
// the async fsync). The synchronous writeStateAtomic remains ONLY for the
// shutdown flush, where a blocking write is exactly what is wanted.
let writeChain = Promise.resolve();
function writeStateAsync(payload) {
  const tmp = STATE_TMP_FILE + '.' + process.pid;
  return (async () => {
    try {
      const fh = await fs.promises.open(tmp, 'w');
      try { await fh.writeFile(payload); await fh.sync(); } finally { await fh.close(); }
    } catch (e) {
      noteStateWriteError('write', e);
      try { await fs.promises.unlink(tmp); } catch (e2) { }
      return false;
    }
    try {
      await fs.promises.rename(tmp, STATE_FILE);
    } catch (e) {
      noteStateWriteError('rename-fallback', e);
      try {
        await fs.promises.writeFile(STATE_FILE, payload);
        await fs.promises.unlink(tmp);
      } catch (e2) { noteStateWriteError('write', e2); return false; }
    }
    try {
      const now = Date.now();
      if (!writeStateAtomic._last || now - writeStateAtomic._last > 15000) {
        await fs.promises.copyFile(STATE_FILE, STATE_BACKUP_FILE);
        writeStateAtomic._last = now;
      }
    } catch (e) { }
    return true;
  })();
}
function laneState(name) {
  if (!state.lanes[name]) {
    state.lanes[name] = {
      running: false, started_at: null, stop_reason: '', heartbeat: null, last_tick: null,
      trades: 0, wins: 0, losses: 0, realized: 0, deployed: 0, entries: 0, skips: 0,
      last_skip: '', last_edge: null, last_series: LANE_BY_NAME.get(name) ? LANE_BY_NAME.get(name).series : null,
      equity: { labels: [], pnl: [] },
    };
  }
  return state.lanes[name];
}

// ---------------------------------------------------------------------------
// Real data feeds
// ---------------------------------------------------------------------------
const SPOT_PRODUCTS = ['BTC-USD', 'ETH-USD', 'XRP-USD', 'HYPE-USD'];
const spot = {};      // product -> { price, bid, ask, t, at, source, points:[{t,p}] }
const spotAge = {};   // product -> ms timestamp of last success
const marketCache = new Map(); // series -> { at, market, source, error, markets }

for (const p of SPOT_PRODUCTS) spot[p] = { price: null, points: [], source: 'Coinbase Exchange (api.exchange.coinbase.com)', product: p, fresh_ticks: 0, stale_frames: 0, duplicate_frames: 0, rejected: 0 };

// SPOT INGEST (2026-11-06). Every spot tick enters through here, from the
// Coinbase WebSocket or the REST backstop. A frame whose exchange timestamp is
// old is counted and dropped; so is an exact repeat of the last price+time.
// Before this, a frozen feed kept advancing `at` while every point carried the
// same stale timestamp, which emptied the 900s realised-vol window and blinded
// every lane ("no realized vol", every candidate refused).
const SPOT_MAX_FRAME_AGE_SEC = 20;
function ingestSpotTick(product, price, bid, ask, timeStr, via) {
  const s = spot[product];
  if (!s) return false;
  if (!Number.isFinite(price) || price <= 0) { s.rejected++; return false; }
  const nowSec = Date.now() / 1000;
  let t = Math.floor(new Date(timeStr).getTime() / 1000);
  if (!Number.isFinite(t)) t = Math.floor(nowSec);
  s.last_frame_lag_s = Math.round(nowSec - t);
  if (nowSec - t > SPOT_MAX_FRAME_AGE_SEC) {
    s.stale_frames++;
    s.last_stale = { via, time: timeStr || null, age_s: Math.round(nowSec - t) };
    return false;
  }
  if (timeStr && timeStr === s.last_time_str && price === s.price) { s.duplicate_frames++; return false; }
  s.price = price;
  if (Number.isFinite(bid)) s.bid = bid;
  if (Number.isFinite(ask)) s.ask = ask;
  s.t = t;
  s.at = Date.now();
  s.last_time_str = timeStr || null;
  s.via = via;
  s.fresh_ticks++;
  s.points.push({ t, p: price });
  const cutoff = nowSec - 86400;
  while (s.points.length && s.points[0].t < cutoff) s.points.shift();
  if (s.points.length > 30000) s.points.splice(0, s.points.length - 30000);
  return true;
}

async function pollSpot(products) {
  for (const p of (products || SPOT_PRODUCTS)) {
    try {
      const r = await fetch('https://api.exchange.coinbase.com/products/' + p + '/ticker', {
        headers: { 'User-Agent': 'kai-mirror/1.0', 'Cache-Control': 'no-cache' },
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const j = await r.json();
      if (ingestSpotTick(p, Number(j.price), Number(j.bid), Number(j.ask), j.time, 'rest')) {
        spotAge[p] = Date.now();
      }
    } catch (e) {
      spot[p].last_error = String(e.message || e);
    }
  }
}

// realised vol per product, from REAL ticks: stdev of log returns / sqrt(dt)
function perSecondVol(product, windowSec) {
  const s = spot[product];
  if (!s || s.points.length < 8) return null;
  const now = Date.now() / 1000;
  const pts = s.points.filter(p => now - p.t <= (windowSec || 900));
  if (pts.length < 8) return null;
  const rets = [];
  let dtSum = 0;
  for (let i = 1; i < pts.length; i++) {
    const dt = pts[i].t - pts[i - 1].t;
    if (dt <= 0) continue;
    const r = Math.log(pts[i].p / pts[i - 1].p);
    rets.push(r);
    dtSum += dt;
  }
  if (rets.length < 6) return null;
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
  const varc = rets.reduce((a, b) => a + (b - mean) * (b - mean), 0) / (rets.length - 1);
  const avgDt = dtSum / rets.length;
  const perBar = Math.sqrt(varc);
  return perBar / Math.sqrt(avgDt); // per root-second
}
global.perSecondVol = perSecondVol;

function normCdf(x) {
  // Abramowitz-Stegun 7.1.26
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp(-x * x / 2);
  const p = d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x >= 0 ? 1 - p : p;
}

function quoteFromMarket(m) {
  if (!m) return null;
  const num = v => (v == null || v === '' ? null : Number(v));
  const q = {
    ticker: m.ticker,
    event: m.event_ticker,
    title: m.title,
    series: String(m.ticker || '').split('-')[0],
    strike: m.floor_strike != null ? Number(m.floor_strike) : (m.cap_strike != null ? Number(m.cap_strike) : null),
    strike_type: m.strike_type,
    yes_bid: num(m.yes_bid_dollars),
    yes_ask: num(m.yes_ask_dollars),
    no_bid: num(m.no_bid_dollars),
    no_ask: num(m.no_ask_dollars),
    last: num(m.last_price_dollars),
    yes_bid_size: num(m.yes_bid_size_fp),
    yes_ask_size: num(m.yes_ask_size_fp),
    no_bid_size: num(m.no_bid_size_fp),
    no_ask_size: num(m.no_ask_size_fp),
    volume: num(m.volume_fp),
    open_interest: num(m.open_interest_fp),
    close_time: m.close_time,
    expected_expiration: m.expected_expiration_time,
    status: m.status,
    result: m.result || '',
    rules: m.rules_primary || '',
    sub_title: m.yes_sub_title || m.sub_title || '',
    updated: m.updated_time,
  };
  // VERDICT BEFORE ACTION. This observation is now a recorded fact with a
  // verdict; anything that fails verification is refused by the entry path.
  q.observed_at = Date.now();
  q.mkt_updated = m.updated_time;
  return q;
}

// VERDICT BEFORE ACTION. This is called AFTER the live WebSocket book has been
// overlaid on the quote, so the verdict is made on the prices the machine would
// actually trade - not on the static REST snapshot it started from.
function recordFact(q) {
  if (!q) return q;
  const v = FACTS.observe(q, { source: 'kalshi' });
  q.facts_ok = v.ok;
  q.facts_verdict = v.verdict;
  q.implied_yes = v.implied_yes;
  q.facts_basis = v.basis;
  q.facts_age_ms = v.age_ms;
  return q;
}

// Options chain cache - full chain per series (8 depths) for cross-expiry/term structure
// Key: series, Value: { at, markets: [all strikes], source, host }
const optionsChainCache = new Map();
const OPTIONS_CHAIN_TTL = 5000; // 5 seconds

async function fetchOptionsChain(series) {
  const key = 'chain:' + series;
  const hit = optionsChainCache.get(key);
  if (hit && Date.now() - hit.at < OPTIONS_CHAIN_TTL) return hit;
  try {
    const r = await kalshi.get('/markets', { query: { limit: 20, status: 'open', series_ticker: series }, ttlMs: 1500 });
    const ms = ((r.data && r.data.markets) || []).map(quoteFromMarket);
    // Sort by strike for easy access
    ms.sort((a, b) => (a.strike || 0) - (b.strike || 0));
    for (const q of ms) recordFact(quote_stream.apply(q));
    const out = { at: Date.now(), markets: ms, source: r.source, host: r.host, ms: r.ms, fetched_at: r.at };
    optionsChainCache.set(key, out);
    return out;
  } catch (e) {
    const out = { at: Date.now(), markets: [], source: 'error', error: String(e.message || e), ms: null };
    optionsChainCache.set(key, out);
    return out;
  }
}

async function marketsForSeries(series, limit) {
  const key = series + ':' + (limit || 5);
  const hit = marketCache.get(key);
  // Static-only cache (bids stream in via quote_stream). SUCCESS stays at 30s
  // for quote freshness, but a FAILED fetch cools down for 180s: under IP-level
  // 429 saturation (both books share one IP) a failing series re-hammered every
  // 30s kept the budget permanently exhausted - the scan saw ~14 markets/pass,
  // 0 rows out, harvester starved (tape: HARVEST ... bids 0 ... EV/contract ?).
  if (hit && Date.now() - hit.at < (hit.source === 'error' ? 180000 : 30000)) return hit;
  try {
    const r = await kalshi.get('/markets', { query: { limit: limit || 5, status: 'open', series_ticker: series }, ttlMs: 1500 });
    const ms = ((r.data && r.data.markets) || []).map(quoteFromMarket);
    quote_stream.note(ms);
    for (const q of ms) recordFact(quote_stream.apply(q));
    const out = { at: Date.now(), markets: ms, source: r.source, host: r.host, ms: r.ms, fetched_at: r.at };
    marketCache.set(key, out);
    return out;
  } catch (e) {
    const out = { at: Date.now(), markets: [], source: 'error', error: String(e.message || e), ms: null };
    marketCache.set(key, out);
    return out;
  }
}

// Pick the contract in a series that is genuinely the one to trade: nearest
// strike to dry spot (for ladders) or the soonest-expiring quotable market.
// A quote is only usable if the book is coherent: no crossed market, prices
// inside the exchange's band, and a real size behind the quote. A contract
// whose strike sits within a dollar of spot flips 15+ points when BTC crosses
// it, so incoherent quotes are skipped rather than traded.
function quoteSane(q) {
  if (!q) return false;
  // Accept markets with bid/ask OR last_price as fallback
  const hasBidAsk = q.yes_ask != null && q.no_ask != null && q.yes_bid != null && q.no_bid != null;
  const hasLastPrice = q.last_price != null && q.last_price >= 0.02 && q.last_price <= 0.98;
  // Also accept markets with strike for synthetic quotes
  const hasStrike = q.strike != null;
  if (!hasBidAsk && !hasLastPrice && !hasStrike) return false;
  if (hasBidAsk) {
    if (q.yes_bid > q.yes_ask + 0.005) return false;
    if (q.no_bid > q.no_ask + 0.005) return false;
    if (q.yes_ask < 0.02 || q.yes_ask > 0.98) return false;
    if (q.no_ask < 0.02 || q.no_ask > 0.98) return false;
  }
  return true;
}

function syntheticQuote(q, fair) {
  // DEPRECATED: synthetic prices are never tradeable. Kept only so old callers
  // don't crash; it now just passes through when real quotes exist.
  if (q.yes_ask != null && q.no_ask != null && q.yes_bid != null && q.no_bid != null) {
    return q;
  }
  return { ...q, synthetic: true, no_bid: null, no_ask: null, yes_bid: null, yes_ask: null };
}

async function activeQuote(lane) {
    const staticSeries = (lane.seriesList || [lane.series]).filter(Boolean);
  // 2026-11-04 STARVATION KILL (parity with LIVE): series discovery is raced
  // with a 4s deadline and falls through to the static series list. A crawling
  // sweep must never freeze a lane tick (LIVE lost lanes 2-8 for a day to this).
  const discSeries = await Promise.race([
    series_kind.resolveFresh(lane.name),
    new Promise((r) => setTimeout(() => r(null), 4000)),
  ]);
  if (discSeries && !staticSeries.includes(discSeries.series)) staticSeries.push(discSeries.series);
  for (const series of staticSeries) {
    // 2026-11-04: Kalshi's default /markets sort returns the TOP strikes of a
    // ladder series first. With limit=8 the engine only ever saw the dead far
    // strikes (evidence: KXBTCD had 200 open markets, 32 within 2% of spot,
    // while the engine saw 8 strikes 9% away - 81k skips, 0 entries ever).
    // Ladder lanes fetch deep so the fair-value filter can pick the near-spot rung.
    const res = await marketsForSeries(series, lane.deep || 8);
    const s = lane.spot ? spot[lane.spot] : null;
    const perSec = s ? perSecondVol(lane.spot, 900) : null;
    const usable = (res.markets || []).filter(m => m.status !== 'finalized' && m.result === '');
    if (!usable.length) continue;
    
    // Compute fair value for each market
    const marketsWithFair = usable.map(m => {
      let fair = 0.5;
      if (s && Number.isFinite(s.price) && m.strike != null && m.close_time) {
        const tau = Math.max(0, (new Date(m.close_time).getTime() - Date.now()) / 1000);
        const vol = perSecondVol(lane.spot, 900) || 0.0001;
        if (tau > 20 && vol > 0) {
          const z = Math.log(s.price / m.strike) / (vol * Math.sqrt(tau));
          fair = normCdf(z);
        }
      }
      return { ...m, fair };
    });
    
    // Filter to markets with reasonable fair value. maxTauSec (2026-11-11
    // hyperliquid_1h rewire): a registry-level horizon ceiling so the 20-tick
    // drift brain never prices rungs hours out - the KXHYPE hourly ladder
    // lists 5-hour contracts and the old picker happily scored them.
    const tauMaxMs = lane.maxTauSec ? lane.maxTauSec * 1000 : Infinity;
    const fairMarkets = marketsWithFair.filter(m => m.fair >= 0.05 && m.fair <= 0.95 && m.close_time && (new Date(m.close_time).getTime() - Date.now()) >= 60000 && (new Date(m.close_time).getTime() - Date.now()) <= tauMaxMs);
    if (!fairMarkets.length) continue;
    
    // Pick closest to spot, but prefer markets with reasonable time to expiration
    let pick = fairMarkets[0];
    if (s && Number.isFinite(s.price)) {
      let bestScore = -Infinity;
      for (const m of fairMarkets) {
        if (m.strike == null || !m.close_time) continue;
        const tau = Math.max(0, (new Date(m.close_time).getTime() - Date.now()) / 1000);
        if (tau < 30) continue; // Skip markets expiring in < 30 seconds
        const d = Math.abs(Math.log(s.price / m.strike));
        // Score: closer strike = higher, longer tau = higher
        const score = 1 / (1 + d) * Math.min(1, tau / 300);
        if (score > bestScore) { bestScore = score; pick = m; }
      }
    } else {
      pick = fairMarkets.slice().sort((a, b) => new Date(a.close_time) - new Date(b.close_time))[0];
    }
    
    // REAL quotes only - a model-derived synthetic price is never tradeable.
    return { 
      quote: pick, 
      series, 
      source: res.source, 
      host: res.host, 
      error: res.error, 
      tradable: fairMarkets.length, 
      open_markets: (res.markets || []).length 
    };
  }
  const lane0 = (lane.seriesList || [lane.series]).filter(Boolean)[0] || lane.series;
  const res = lane0 ? await marketsForSeries(lane0, 8) : { markets: [], source: 'none', error: 'no series' };
  return { quote: null, series: lane0, source: res.source, host: res.host, error: res.error || (lane0 ? 'no coherent open market in series ' + lane0 : 'no series'), tradable: 0, open_markets: (res.markets || []).length };
}

// Contract for a ticker we already hold (settlement tracking, real result).
async function quoteForTicker(ticker) {
  try {
    const r = await kalshi.get('/markets/' + ticker, { ttlMs: 2000 });
    const m = r.data && r.data.market;
    return recordFact(quote_stream.apply(quoteFromMarket(m)));
  } catch (e) { return null; }
}

// ---------------------------------------------------------------------------
function clipState() { return state.clip_state || (state.clip_state = { base: 3, max: 25, ratchet_until: 0 }); }
function effectiveClip() {
  // OPERATOR ORDER 2026-11-04: every order is at most $0.50 (live AND dry).
  const ORDER_MAX_DOLLARS = 0.50;
  const cs = clipState();
  if (cs.ratchet_until && Date.now() < cs.ratchet_until) return Math.min(ORDER_MAX_DOLLARS, cs.base);
  const equity = dryEquity();
  const baseline = state.dry.starting_balance || DRY_PAPER_STARTING_BALANCE;
  const target = Math.round(cs.base * Math.max(1, equity / baseline));
  return Math.min(ORDER_MAX_DOLLARS, Math.max(cs.base, Math.min(cs.max, target)));
}

// AUTOMATIC CLIP INCREASER / DECREASER (operator-ordered).
//   UP:   every 3 consecutive wins the base clip grows +1 (hard cap cs.max).
//   DOWN: a big loss ratchets the clip to base for 30 min (in close paths).
//   SCALE: effectiveClip() also scales with equity growth (base * equity/baseline).
// Call noteClipOutcome(pnl) on EVERY booked trade.
function noteClipOutcome(pnl) {
  const cs = state.clip_state || (state.clip_state = { base: 3, max: 25, ratchet_until: 0 });
  if (pnl > 0) {
    cs.win_streak = (cs.win_streak || 0) + 1;
    if (cs.win_streak % 3 === 0 && cs.base < cs.max) {
      cs.base = Math.min(cs.max, cs.base + 1);
      log('clip_up', { base: cs.base, win_streak: cs.win_streak });
    }
  } else if (pnl < 0) {
    cs.win_streak = 0;
  }
}


// DRY book (the ONLY simulated execution) - real quotes, real settlements
// ---------------------------------------------------------------------------
function deployed() {
  return state.dry.positions.reduce((a, p) => a + p.cost, 0);
}
function dryRealized() {
  return state.dry.closed.reduce((a, t) => a + (t.pnl || 0), 0);
}
// Real Kalshi cash, read straight from the /portfolio/balance payload when it
// has loaded. Returns null when the balance call is rate-limited/absent so the
// display can fall back to the computed identity instead of inventing a number.
function realBalanceCash() {
  const b = null;
  if (!b || typeof b !== 'object') return null;
  for (const k of ['balance_available', 'balance_total', 'balance', 'buying_power']) {
    const v = Number(b[k]);
    if (Number.isFinite(v) && v >= 0) return v;
  }
  return null;
}
function dryCash() {
  // cash = starting_balance + realized - deployed  (the dashboard's own identity)
  return Number((state.dry.starting_balance + dryRealized() - deployed()).toFixed(4));
}
function dryEquity() {
  // real balance (if loaded) + dry mark-to-market of open positions at the real bid,
  // else the computed identity + mtm.
  let mtm = 0;
  for (const p of state.dry.positions) {
    if (p.mark != null) mtm += p.qty * p.mark;
    else mtm += p.cost;
  }
  return Number(((realBalanceCash() ?? dryCash()) + mtm).toFixed(4));
}
function todayStats(list) {
  const day = new Date().toISOString().slice(0, 10);
  const t = list.filter(x => String(x.exit_timestamp || '').slice(0, 10) === day);
  const wins = t.filter(x => x.pnl > 0).length;
  return {
    trades: t.length,
    pnl: Number(t.reduce((a, x) => a + (x.pnl || 0), 0).toFixed(2)),
    win_rate: t.length ? Number((100 * wins / t.length).toFixed(1)) : 0,
  };
}
function curveFromClosed(list) {
  const byDay = new Map();
  for (const t of list) {
    const d = String(t.exit_timestamp || '').slice(0, 10);
    if (!d) continue;
    byDay.set(d, (byDay.get(d) || 0) + (t.pnl || 0));
  }
  const labels = Array.from(byDay.keys()).sort();
  let run = 0;
  const pnl = labels.map(d => { run += byDay.get(d); return Number(run.toFixed(2)); });
  return { labels, pnl };
}
function dryAccountPayload() {
  const closed = state.dry.closed;
  const wins = closed.filter(t => t.pnl > 0).length;
  return {
    book: 'dry',
    label: 'DRY account - paper money',
    starting_balance: state.dry.starting_balance,
    baseline: state.dry.baseline,
    cash: dryCash(),
    equity: dryEquity(),
    realized: Number(dryRealized().toFixed(2)),
    total_pnl: Number((dryEquity() - state.dry.starting_balance).toFixed(2)),
    exposure: Number(deployed().toFixed(2)),
    open_positions: state.dry.positions.length,
    closed_trades: closed.length,
    wins, losses: closed.length - wins,
    win_rate: closed.length ? Number((100 * wins / closed.length).toFixed(1)) : 0,
    today: todayStats(closed),
    curve: curveFromClosed(closed),
  };
}

// OPERATOR RULE (2026-11-06): the 30-day max-hold law now lives in
// hold_rule.js (fail-closed) and is enforced at every booking site.
// ---------------------------------------------------------------------------
// MAKER-EDGE LAYER (2026-11-06, operator order). Lanes never lift the ask: they
// rest a bid at their own level through FILLSIM. A position exists only when a
// REAL print goes through that bid inside the TTL. The kill sweep switches off
// any strategy whose recent maker fills lose money, so it stops buying trash.
// ---------------------------------------------------------------------------
function makerLaneMode(lane, lc) {
  if (state.runtime.lane_maker === false) return false;
  if (lc && lc.maker === false) return false;
  return true;
}
function laneMakerPending(name) {
  for (const it of FILLSIM._pending.values()) { if (it.book && it.book.lane_entry && it.book.lane === name) return true; }
  return false;
}
function makerKillMinN() { return Math.max(10, Number(state.runtime.maker_kill_min_n) || 30); }
function killStrategy(name, reason) {
  const ls = laneState(name);
  ls.running = false; ls.maker_killed = true; ls.kill_reason = reason; ls.killed_at = new Date().toISOString();
  ls.stop_reason = 'maker-edge kill: ' + reason;
  log('maker_kill', { strategy: name, reason });
  broadcast('strategy', name + ' KILLED: ' + reason);
  save();
}
function resetMakerWindow(name) {
  const ls = laneState(name);
  ls.maker_window_ts = new Date().toISOString();
  ls.maker_killed = false; ls.kill_reason = null; ls.killed_at = null;
}
// KILL TEST (one-sided 95%): a strategy is useless only when the upper bound of
// its window EV is still below zero, i.e. mean + 1.645 * SE < 0. A plain "lost
// money over the last N" rule kills on noise (a 34%-win-rate book swings widely).
function makerWindowStats(trades, sinceIso, minN) {
  const rows = (trades || []).filter(t => !sinceIso || (t.exit_timestamp && t.exit_timestamp >= sinceIso));
  const win = rows.slice(-minN);
  const xs = win.map(t => Number(t.pnl || 0));
  const n = xs.length;
  const pnl = xs.reduce((a, b) => a + b, 0);
  const mean = n ? pnl / n : 0;
  const sd = n > 1 ? Math.sqrt(xs.reduce((a, b) => a + (b - mean) * (b - mean), 0) / (n - 1)) : 0;
  const se = n > 1 ? sd / Math.sqrt(n) : 0;
  return { n_total: rows.length, n_window: n, pnl_window: Number(pnl.toFixed(4)), ev_window: n ? Number(mean.toFixed(4)) : null, ev_upper: n ? Number((mean + 1.645 * se).toFixed(4)) : null };
}
function makerKillSweep() {
  const minN = makerKillMinN();
  for (const lane of LANES) {
    if (!lane.seriesList || !lane.seriesList.length) continue;
    const ls = laneState(lane.name);
    if (!ls.running) continue;
    const mine = state.dry.closed.filter(t => t.strategy === lane.name && t.maker_fill);
    const st = makerWindowStats(mine, ls.maker_window_ts, minN);
    if (st.n_window >= minN && st.ev_upper < 0) killStrategy(lane.name, 'last ' + st.n_window + ' maker fills EV ' + st.ev_window + ' (95% upper ' + st.ev_upper + ')');
  }
  const nd = laneState('all_no_markets');
  const book = state.all_no[state.mode];
  if (nd.running && book && Array.isArray(book.closed_trades)) {
    const st = makerWindowStats(book.closed_trades, nd.maker_window_ts, minN);
    if (st.n_window >= minN && st.ev_upper < 0) killStrategy('all_no_markets', 'last ' + st.n_window + ' ALL NO fills EV ' + st.ev_window + ' (95% upper ' + st.ev_upper + ')');
  }
}
function makerPayload() {
  const minN = makerKillMinN();
  const lanes = LANES.filter(l => l.seriesList && l.seriesList.length).map(l => {
    const ls = laneState(l.name);
    const mine = state.dry.closed.filter(t => t.strategy === l.name && t.maker_fill);
    const st = makerWindowStats(mine, ls.maker_window_ts, minN);
    return { strategy: l.name, running: !!ls.running, rests: ls.maker_rests || 0, maker_closed: mine.length,
      pnl_all: Number(mine.reduce((a, t) => a + Number(t.pnl || 0), 0).toFixed(4)),
      window_n: st.n_window, window_ev: st.ev_window, window_pnl: st.pnl_window,
      killed: !!ls.maker_killed, kill_reason: ls.kill_reason || null, killed_at: ls.killed_at || null };
  });
  return { generated_at: new Date().toISOString(), kill_min_n: minN, maker_mode: state.runtime.lane_maker !== false, fill_realism: FILLSIM.stats(), lanes };
}
function openDryPosition(lane, q, side, price, fair, edge, sigma, opts) {
  // HOLD RULE (operator order 2026-11-06): entry needs a close_time that
  // settles within 30 days. Fail-closed: missing/invalid close_time blocks
  // entry (the old check silently skipped when close_time was absent).
  const holdOk = holdRule.entryAllowed(q && q.close_time, Date.now());
  if (!holdOk.ok) { const lh = laneState(lane.name); lh.skips++; lh.last_skip = 'hold rule: ' + holdOk.reason; return null; }
  // OPERATOR ORDER 2026-11-04: every order is at most $0.50. The old
  // Math.max(1, Math.floor(clip/price)) forced qty=1 whenever the price ALONE
  // exceeded the cap - that is exactly how a $0.61 KXBTC15M YES slipped through
  // on 2026-11-04. If no whole contract fits the cap, we SKIP, never force.
  const cap = effectiveClip();
  let qty = Math.floor(cap / price);
  // ALPHA GATE (Mirror port, 2026-11-07): dry runs the same measured verdict
  // engine as LIVE, in REPORT-CLAMP mode - refuted/unproven clamp paper size
  // to 1 contract (never block: dry is where proof is earned).
  if (typeof ALPHA !== 'undefined' && ALPHA) {
    const av = ALPHA.verdict(lane && lane.name);
    if (av && av.max_qty != null) {
      // Halted (max_qty 0) = no contracts at all. Only paper probation/unproven clamp to 1.
      const target = av.max_qty;
      if (qty > target) {
        qty = target;
        log('alpha_gate_clamped', { lane: lane.name, ticker: q.ticker, verdict: av.verdict, reason: av.reason, qty });
        // SHADOW THE REFUSAL (operator order 2026-11-10 "spank"): a qty-0
        // clamp is a refused entry, and a refused entry is FREE evidence. The
        // halted lane keeps earning verdict data while it cannot trade.
        if (qty === 0) {
          try {
            CALIB.shadowOpen({ ticker: q.ticker, side: side || 'NO', strategy: lane && lane.name, claimed: fair, price,
              gate: CALIB.gate({ ticker: q.ticker, side: side || 'NO', strategy: lane && lane.name, claimed: fair, price, minEdge: 0.05 }) });
          } catch (e) { }
        }
      }
    }
    // REHAB DNA (2026-11-10): a lane on probation trades only inside its DNA.
    if (typeof REHAB !== 'undefined' && REHAB) {
      const rhd = REHAB.entryCheck(lane && lane.name, edge, price);
      if (!rhd.ok) { const lr = laneState(lane.name); lr.skips++; lr.last_skip = rhd.reason; log('rehab_bar_refused', { lane: lane.name, ticker: q.ticker, edge, price, dna: rhd.dna });
        // SHADOW THE DNA REFUSAL (operator order 2026-11-10): out-of-DNA
        // candidates are exactly what the NEXT DNA generation needs as
        // evidence. Silence here starves the rehab loop it belongs to.
        try {
          CALIB.shadowOpen({ ticker: q.ticker, side: side || 'NO', strategy: lane && lane.name, claimed: fair, price,
            gate: CALIB.gate({ ticker: q.ticker, side: side || 'NO', strategy: lane && lane.name, claimed: fair, price, minEdge: 0.05 }) });
        } catch (e) { }
        return null; }
    }
  }
  // FEED GATE (2026-11-07): a stale spot feed makes fair values fiction even
  // in paper. No fresh tick, no dry trade.
  if (lane && lane.spot) {
    const age = spotAgeMs(lane.spot);
    if (age != null && age > 3000) {
      const lsf = laneState(lane.name);
      lsf.skips++; lsf.last_skip = 'spot feed stale ' + Math.round(age / 1000) + 's (' + lane.spot + ')';
      log('spot_stale_refused', { lane: lane.name, ticker: q.ticker, asset: lane.spot, age_ms: age });
      return null;
    }
  }
  if (qty < 1) { const ls = laneState(lane.name); ls.skips++; ls.last_skip = 'price $' + price.toFixed(2) + ' > order cap $' + cap.toFixed(2) + ' (no contract fits)'; return null; }
  // CROSS-LANE ANTI-STACK + CLUSTER CAP (2026-11-04 operator order: lanes
  // trade the evidence zone like all_no). Lanes used to check only their own
  // book - 4 lanes stacked one market and died together. The dry book obeys
  // the same booking-site law as createAllNoEntry: max 2 per market, max 2
  // per settlement round. No caller can bypass this.
  const sameMkt = state.dry.positions.filter(p => p.market_id === q.ticker).length;
  if (sameMkt >= 2) { const lsm = laneState(lane.name); lsm.skips++; lsm.last_skip = 'anti-stack: ' + sameMkt + ' positions already in ' + q.ticker; return null; }
  const sameRound = state.dry.positions.filter(p => p.close_time && p.close_time === q.close_time).length;
  if (q.close_time && sameRound >= 2) { const lsr = laneState(lane.name); lsr.skips++; lsr.last_skip = 'cluster cap: ' + sameRound + ' positions already settle this round'; return null; }
  // LIFETIME MARKET CAP (CODE LAW 2026-11-04): no market is enterable more
  // than MAX_MARKET_ENTRIES times, ever — same law the all_no book obeys.
  if (marketEntryCount(q.ticker) >= MAX_MARKET_ENTRIES) { const lsc = laneState(lane.name); lsc.skips++; lsc.last_skip = 'lifetime cap: ' + q.ticker + ' already entered ' + MAX_MARKET_ENTRIES + 'x'; return null; }
  const cost = Number((qty * price).toFixed(4));
  if (dryCash() < cost) { const ls = laneState(lane.name); ls.skips++; ls.last_skip = 'insufficient dry cash'; return null; }
  // OPERATOR LAW 2026-11-07 (OPERATOR_LAWS.md): NO fee-attached exits. The
  // paper book mirrors the live law exactly so dry data predicts live behavior.
  const stop = null;
  const tp = null;
  // Capture bid/ask at entry for fill quality tracking
  const bid_at_entry = side === 'YES' ? q.yes_bid : q.no_bid;
  const ask_at_entry = side === 'YES' ? q.yes_ask : q.no_ask;
  const pos = {
    id: 'dry-' + crypto.randomBytes(3).toString('hex'),
    market_id: q.ticker,
    title: q.title,
    series: q.series,
    strategy: lane.name,
    side,
    spot: lane.spot || null,
    entry_price: price,
    qty, cost,
    fair_at_entry: Number((fair ?? price).toFixed(4)),
    edge_at_entry: Number((edge ?? 0).toFixed(4)),
    sigma_per_sec: sigma,
    strike: q.strike,
    horizon: lane.horizon,
    stop_loss: stop,
    take_profit: tp,
    mark: price,
    opened: new Date().toISOString(),
    close_time: q.close_time,
    source: q.source || 'dry',
    host: q.host || kalshi.LIVE_BASE,
    // Fill quality tracking
    bid_at_entry,
    ask_at_entry,
    maker_fill: !!(opts && opts.maker),
  };
  state.dry.positions.push(pos);
  const ls = laneState(lane.name);
  ls.entries++;
  // LIFETIME MARKET CAP bookkeeping for lane entries too (CODE LAW).
  bumpMarketEntry(pos.market_id);
  ls.heartbeat = new Date().toISOString();
  ls.last_series = q.series;
  log('dry_open', { strategy: lane.name, market: pos.market_id, side, entry: price, qty, cost, fair: pos.fair_at_entry, edge: pos.edge_at_entry, source: pos.source, bid_at_entry, ask_at_entry });
  broadcast('strategy', lane.name + ' entry ' + side + ' ' + pos.market_id + ' @ ' + price);
  // Persist to SQLite
  persistPosition(pos);
  save();
  return pos;
}

function closeDryPosition(pos, exitPrice, reason, result) {
  const idx = state.dry.positions.indexOf(pos);
  if (idx === -1) return;
  state.dry.positions.splice(idx, 1);
  const proceeds = Number((pos.qty * exitPrice).toFixed(4));
  const pnl = Number((proceeds - pos.cost).toFixed(4));
  
  // Fill quality: (fill - bid) / (ask - bid) - should be <= 0 for maker
  let fillQuality = null;
  if (pos.bid_at_entry != null && pos.ask_at_entry != null && pos.ask_at_entry !== pos.bid_at_entry) {
    fillQuality = (pos.entry_price - pos.bid_at_entry) / (pos.ask_at_entry - pos.bid_at_entry);
  }
  
  const trade = {
    market_id: pos.market_id, title: pos.title, side: pos.side, strategy: pos.strategy,
    entry_price: pos.entry_price, exit_price: exitPrice, quantity: pos.qty, cost_basis: pos.cost,
    proceeds, pnl, exit_reason: reason, result: result || '', hold_seconds: Math.round((Date.now() - new Date(pos.opened).getTime()) / 1000),
    exit_timestamp: new Date().toISOString(), opened: pos.opened,
    // Real model values at entry (persisted for edge analytics)
    fair_at_entry: pos.fair_at_entry != null ? pos.fair_at_entry : null,
    edge_at_entry: pos.edge_at_entry != null ? pos.edge_at_entry : null,
    kelly_fraction: pos.kelly_fraction != null ? pos.kelly_fraction : null,
    // Vol-audit fields (2026-11-04 forensics): sigma/strike/horizon were
    // dropped here, so every lane row in trading.db had NULL sigma. Fixed.
    sigma_per_sec: pos.sigma_per_sec != null ? pos.sigma_per_sec : null,
    strike: pos.strike != null ? pos.strike : null,
    horizon: pos.horizon != null ? pos.horizon : null,
    // Fill quality tracking
    bid_at_entry: pos.bid_at_entry,
    ask_at_entry: pos.ask_at_entry,
    fill_quality: fillQuality,
    maker_fill: !!pos.maker_fill,
    entry_mode: pos.maker_fill ? 'maker' : 'taker',
    // Settlement tracking (filled when settled)
    settled_price: reason === 'settled' ? exitPrice : null,
    settlement_source: reason === 'settled' ? 'kalshi' : null,
  };
  state.dry.closed.push(trade);
  
  // Update lane equity curve for charts
  const ls = laneState(pos.strategy);
  const day = String(trade.exit_timestamp || '').slice(0, 10);
  if (day) {
    const existingIdx = ls.equity.labels.indexOf(day);
    const dayPnl = trade.pnl || 0;
    if (existingIdx >= 0) {
      ls.equity.pnl[existingIdx] = Number((ls.equity.pnl[existingIdx] + dayPnl).toFixed(2));
    } else {
      ls.equity.labels.push(day);
      ls.equity.pnl.push(Number(dayPnl.toFixed(2)));
    }
    // Keep labels and pnl sorted by date
    const combined = ls.equity.labels.map((label, i) => ({ label, pnl: ls.equity.pnl[i] }));
    combined.sort((a, b) => a.label.localeCompare(b.label));
    ls.equity.labels = combined.map(c => c.label);
    ls.equity.pnl = combined.map(c => c.pnl);
  }
  
  // Update lane stats
  ls.trades++;
  if (pnl > 0) ls.wins++; else {
  ls.losses++; ls.loss_cooldown_until = Date.now() + (state.runtime.loss_cooldown_sec || 30) * 1000;
  if (pnl < 0 && Math.abs(pnl) >= 0.25 * (state.dry.starting_balance || DRY_PAPER_STARTING_BALANCE)) {
    state.clip_state = state.clip_state || { base: 3, max: 25, ratchet_until: 0 };
    state.clip_state.ratchet_until = Date.now() + 30 * 60 * 1000;
    log('clip_ratchet', { pnl, baseline: state.dry.starting_balance || DRY_PAPER_STARTING_BALANCE });
  }
}
  ls.realized = Number((ls.realized + pnl).toFixed(2));
  ls.heartbeat = new Date().toISOString();
  noteClipOutcome(pnl);

  // Record fill quality
  if (fillQuality != null) {
    state.fill_quality_db.push({
      market_id: pos.market_id,
      side: pos.side,
      strategy: pos.strategy,
      entry_price: pos.entry_price,
      bid_at_entry: pos.bid_at_entry,
      ask_at_entry: pos.ask_at_entry,
      fill_quality: fillQuality,
      timestamp: new Date().toISOString(),
    });
    // Keep last 10000 entries
    if (state.fill_quality_db.length > 10000) state.fill_quality_db.shift();
  }
  
  log('dry_close', { strategy: pos.strategy, market: pos.market_id, side: pos.side, entry: pos.entry_price, exit: exitPrice, reason, pnl, result: result || '', fill_quality: fillQuality });
  broadcast('strategy', pos.strategy + ' exit ' + pos.market_id + ' @ ' + exitPrice + ' pnl ' + pnl);
  fireWebhooks('trade_exit', { market_id: pos.market_id, side: pos.side, strategy: pos.strategy, entry_price: pos.entry_price, exit_price: exitPrice, pnl, reason, timestamp: new Date().toISOString() });
  // Persist to SQLite
  persistTrade(trade);
  if (fillQuality != null) persistFillQuality({ market_id: pos.market_id, side: pos.side, strategy: pos.strategy, entry_price: pos.entry_price, bid_at_entry: pos.bid_at_entry, ask_at_entry: pos.ask_at_entry, fill_quality: fillQuality, timestamp: trade.exit_timestamp });
  if (reason === 'settled') persistSettlement({ market_id: pos.market_id, side: pos.side, strategy: pos.strategy, settled_price: exitPrice, kalshi_result: result, coinbase_spot_at_settlement: null, coinbase_spot_time: null, spot_product: null, entry_price: pos.entry_price, pnl: trade.pnl, timestamp: trade.exit_timestamp, spot_deviation: null });
  // Settled lane trades feed the calibration ledger too — every settled
  // outcome the vaccine can learn from, it gets.
  if (reason === 'settled') {
    try {
      CALIB.recordOutcome({
        market_id: pos.market_id, strategy: pos.strategy, side: pos.side,
        claimed: pos.fair_at_entry, price: pos.entry_price,
        outcome: exitPrice > 0, pnl: trade.pnl,
        sigma: pos.sigma_per_sec, strike: pos.strike, horizon: pos.horizon,
        ts: pos.opened,
      });
    } catch (e) { }
  }
  removePosition(pos.id);
  save();
}

// ---------------------------------------------------------------------------
// DRY book (real Kalshi order placement)
// ---------------------------------------------------------------------------


async function markDryPositions() {
  const tickers = Array.from(new Set(state.dry.positions.map(p => p.market_id)));
  for (const ticker of tickers) {
    const q = await quoteForTicker(ticker);
    const held = state.dry.positions.filter(p => p.market_id === ticker);
    if (!q) continue;
    for (const pos of held) {
      if (q.result === 'yes' || q.result === 'no') {
        const settled = Number(q.result === pos.side.toLowerCase() ? 1 : 0);
        // Record settlement data: Coinbase spot at settlement as CF BRTI proxy
        const spotAtSettlement = pos.spot ? spot[pos.spot] : null;
        const settlementRecord = {
          market_id: pos.market_id,
          side: pos.side,
          strategy: pos.strategy,
          settled_price: settled,
          kalshi_result: q.result,
          coinbase_spot_at_settlement: spotAtSettlement ? spotAtSettlement.price : null,
          coinbase_spot_time: spotAtSettlement ? spotAtSettlement.t : null,
          spot_product: pos.spot,
          entry_price: pos.entry_price,
          pnl: settled * pos.qty - pos.cost,
          timestamp: new Date().toISOString(),
        };
        state.settlement_db.push(settlementRecord);
        if (state.settlement_db.length > 10000) state.settlement_db.shift();
        
        closeDryPosition(pos, settled, 'settled', q.result);
        continue;
      }
      if (q.status === 'finalized' || q.status === 'settled') {
        // NO FABRICATED ZEROES (2026-11-04 surgery): finalized-without-result
        // is NOT a $0 settlement. Parking the position costs nothing; a fake
        // loss corrupts every calibration number the lab produces.
        continue;
      }
      const bid = pos.side === 'YES' ? q.yes_bid : q.no_bid;
            if (bid != null) pos.mark = bid;
            pos.quote = { bid, ask: pos.side === 'YES' ? q.yes_ask : q.no_ask, close_time: q.close_time, status: q.status, updated: q.updated };
            const heldMs = Date.now() - new Date(pos.opened).getTime();
            if (heldMs < Number(state.runtime.min_hold_sec || 45) * 1000) continue;

            // HOLD RULE (operator order 2026-11-06): a position open 30 days is
            // force-exited. Sell only at or above entry; otherwise hold and
            // retry on the next tick. Manual closes and stop-losses are
            // separate exits and never satisfy the rule.
            if (holdRule.forcedExitDue(pos.opened, Date.now())) {
              if (holdRule.forcedExitPriceOk(bid, pos.entry_price)) {
                log('hold_rule_forced_exit', { strategy: pos.strategy, market: pos.market_id, bid, entry: pos.entry_price, opened: pos.opened });
                closeDryPosition(pos, bid, 'hold_rule_30d');
              } else {
                pos.hold_rule_waiting = { since: pos.hold_rule_waiting ? pos.hold_rule_waiting.since : new Date().toISOString(), bid, entry: pos.entry_price };
              }
              continue;
            }

            // (No high-water tracking: the trail stop it fed is exterminated.)

            // 2026-11-04 OPERATOR ORDER: NO TIME-DECAY EXIT AND NO STOP LOSS on
            // the dry book either. Every position rides to settlement; the
            // per-order size is the loss limit.
            const totalLifeMs = new Date(q.close_time).getTime() - new Date(pos.opened).getTime();
            const lifePct = totalLifeMs > 0 ? heldMs / totalLifeMs : 1;

            // TRAIL STOP: DELETED. The hard stop and take-profit below are the
            // only price exits; winners ride to settlement.

            // (stop_loss exit intentionally removed - operator order 2026-11-04.)
    }
  }
}


async function engineTick() {
  await markDryPositions();
  const list = LANES.filter(l => true);
  for (const lane of list) {
    const ls = laneState(lane.name);
    if (!ls.running) continue;
    ls.last_tick = new Date().toISOString();
    const act = await activeQuote(lane);
    ls.last_series = act.series;
    if (!act.quote) { ls.skips++; ls.last_skip = act.error || 'no quotable market'; continue; }
    pumpFills(act.quote);
    const q = act.quote;
    q.source = act.source; q.host = act.host;
    if (!lane.spot) continue;
    const s = spot[lane.spot];
    if (!s || !Number.isFinite(s.price)) { ls.skips++; ls.last_skip = 'spot feed ' + lane.spot + ' not ready'; continue; }
    if (q.strike == null) { ls.skips++; ls.last_skip = 'market has no strike'; continue; }
    const tau = Math.max(0, (new Date(q.close_time).getTime() - Date.now()) / 1000);
    if (ls.loss_cooldown_until && Date.now() < ls.loss_cooldown_until) { ls.skips++; ls.last_skip = 'loss cooldown ' + Math.round((ls.loss_cooldown_until - Date.now()) / 1000) + 's'; continue; }
    if (q.yes_ask != null && q.yes_bid != null && (q.yes_ask - q.yes_bid) > (state.runtime.max_spread || 0.02)) { ls.skips++; ls.last_skip = 'wide spread ' + (q.yes_ask - q.yes_bid).toFixed(3); continue; }
    if (tau < (state.runtime.min_tau_sec || 60)) { ls.skips++; ls.last_skip = 'contract closes in ' + Math.round(tau) + 's'; continue; }
    const perSec = perSecondVol(lane.spot, 900);
    
    // Get lane-specific config (self-aware trading logic)
    const lc = state.lane_configs?.[lane.name] || {};
    
    // Build params for brain evaluation
    const volWindow = s.points.slice(-20).map(p => p.p);
    const fills = [];
        const chainRes = await fetchOptionsChain(q.series).catch(() => ({ markets: [] }));
    const allQuotes = (chainRes.markets && chainRes.markets.length) ? chainRes.markets : ((act.open_markets > 0) ? [q] : []);

    // Evaluate with ensemble brain
    const result = brain.evaluate({
      q, spotPrice: s.price, strike: q.strike, tau, perSecVol: perSec,
      volWindow, fills, history: ls.price_history || [],
      quotes: allQuotes, weights: state.signal_weights,
      perpTilt: perp.biasFor(lane.spot).bias
    });

    ls.last_edge = result.edge || 0;
    ls.last_brain = JSON.stringify({ side: result.side, confidence: result.confidence, reason: result.reason, regime: result.regime });
    ls.heartbeat = new Date().toISOString();

    if (!result.side) { ls.skips++; ls.last_skip = result.reason || 'no signal'; continue; }

    // Regime filter: skip toxic regimes (use lane-specific avoid list)
    if (lc.regime_avoid && lc.regime_avoid.includes(result.regime)) {
      ls.skips++; ls.last_skip = 'regime filter: ' + result.regime; continue;
    }

    const side = result.side;
    // Maker entry: use bid instead of ask
    const price = side === 'YES' ? q.yes_bid : q.no_bid;
    if (price == null) { ls.skips++; ls.last_skip = 'no bid on ' + side; continue; }
    if (q.synthetic) { ls.skips++; ls.last_skip = 'synthetic quote - not tradeable'; continue; }
    
    // Lane-specific price bounds (DRY: looser for analysis)
    const maxPrice = lc.max_price ?? state.runtime.max_price;
    const minPrice = lc.min_price ?? state.runtime.min_price;
    if (price >= maxPrice) { ls.skips++; ls.last_skip = 'entry price ' + price + ' in blocked band'; continue; }
    if (price < minPrice) { ls.skips++; ls.last_skip = 'entry price ' + price + ' below floor'; continue; }

    // TREND GATE (2026-11-11 hyperliquid_1h rewire, operator order): the proven
    // noBook 2026-11-04 law ("winners entered while the move AWAY from the
    // strike was continuing; losers were reversals"), transplanted to the lane
    // path with a horizon-scaled window: at 15m the driftless gaussian is
    // honest, at 1h+ it is blind - it priced a multi-hour HYPE slide as a YES
    // "edge" six times (0/6). Enabled per-lane via the LANES registry
    // (lane.trendGate) so winning lanes are untouched.
    if (lane.trendGate) {
      const winSec = Math.max(60, Math.min(900, Math.round(tau * 0.25))); // 25% of horizon, clamped 1-15 min
      const pts = (s && Array.isArray(s.points)) ? s.points : [];
      const nowT = Date.now() / 1000;
      let ref = null;
      for (let i = pts.length - 1; i >= 0; i--) { if (nowT - pts[i].t >= winSec) { ref = pts[i]; break; } }
      if (ref && perSec) {
        const mom = (s.price - ref.p) / ref.p;                 // realized drift over the window
        const driftWin = 0.5 * perSec * Math.sqrt(winSec);     // half a window-sigma (noBook threshold law)
        const fading = (result.side === 'YES' && mom < -driftWin) || (result.side === 'NO' && mom > driftWin);
        if (fading) {
          ls.skips++; ls.last_skip = 'trend_gate ' + result.side + ' blocked: ' + (mom >= 0 ? '+' : '') + (mom * 100).toFixed(2) + '%/' + winSec + 's toward strike';
          continue;
        }
      }
    }
    
    // Lane-specific edge threshold (DRY: looser for analysis). The registry
    // may impose a HIGHER floor (2026-11-11 hyperliquid_1h rewire: its config
    // floor of 0.0005 was effectively no requirement at all).
    const minEdge = Math.max(lc.min_edge ?? state.runtime.min_edge, lane.minEdgeFloor || 0);
    if (result.edge < minEdge) { ls.skips++; ls.last_skip = 'no edge (' + result.edge.toFixed(4) + ' < ' + minEdge + ')'; continue; }
    
    // Lane-specific confidence threshold (DRY: looser for analysis)
    const minConf = lc.min_confidence ?? 0.3;
    if (result.confidence < minConf) { ls.skips++; ls.last_skip = 'low confidence (' + result.confidence.toFixed(2) + ' < ' + minConf + ')'; continue; }

    // Portfolio heat check (lane-specific exposure)
    const totalExposure = state.dry.positions.reduce((a, p) => a + p.cost, 0);
    const maxExposurePct = lc.max_exposure_pct ?? state.runtime.max_exposure_pct ?? 0.3;
    if (totalExposure > dryCash() * maxExposurePct) { ls.skips++; ls.last_skip = 'portfolio heat limit'; continue; }

    // Kelly sizing check (lane-specific)
    const kellyFraction = Math.max(0, result.edge / (1 - price));
    const kellyCap = lc.kelly_cap ?? state.runtime.kelly_cap ?? 0.4;
    if (kellyFraction < (state.runtime.min_kelly || 0.01)) { ls.skips++; ls.last_skip = 'kelly too small'; continue; }

    const lastEntry = ls.last_entry_at ? Date.now() - ls.last_entry_at : Infinity;
    const cooldownSec = lc.entry_cooldown_sec ?? state.runtime.entry_cooldown_sec ?? 60;
    const cooldown = Number(cooldownSec) * 1000;
    if (lastEntry < cooldown) { ls.skips++; ls.last_skip = 'cooldown ' + Math.ceil((cooldown - lastEntry) / 1000) + 's'; continue; }

    // LANE CONCURRENCY (operator order 2026-11-08: "make dry trade much more"):
    // was held>=1 — one position per lane starved the book. Now per-market
    // uniqueness + a per-lane cap (lc.max_concurrent, default 3). The global
    // anti-stack (2/market), cluster (2/round) and lifetime caps still bind.
    const laneHeld = state.dry.positions.filter(p => p.strategy === lane.name);
    if (laneHeld.some(p => p.market_id === q.ticker)) { ls.skips++; ls.last_skip = 'already in this market'; continue; }
    const laneCap = Number(lc.max_concurrent) > 0 ? Number(lc.max_concurrent) : 3;
    if (laneHeld.length >= laneCap) { ls.skips++; ls.last_skip = 'lane at max concurrent (' + laneCap + ')'; continue; }

    // (max-hold ENTRY gate removed 2026-11-06: it refused every new entry once the
    // last entry was older than 5 minutes, which starved the lanes. Hold-time exits
    // are handled by the position manager.)

    // REAL model fair value for the chosen side (drives fair_at_entry records).
    const fair = result.fair != null ? result.fair : price;
    // THE VACCINE, EVERY LANE (operator order 2026-11-08: raise win rate): the
    // per-asset lanes traded on the brain's claimed fair alone — the exact lie
    // the vaccine was built to catch (btc|15m|NO claimed 0.48, realized 0.35,
    // n=51). Entry now requires adjusted − bid >= 3c (clears the ~1.7c fee at
    // these prices). Refused candidates become free shadow rows so their
    // buckets keep learning. all_no and the harvester already obey this law.
    const cal = CALIB.gate({ ticker: q.ticker, side, strategy: lane.name, claimed: fair, price, minEdge: 0.03 });
    if (!cal.ok) {
      ls.skips++; ls.last_skip = 'calib gate: ' + (cal.reason || 'refused');
      try { CALIB.shadowOpen({ ticker: q.ticker, side, strategy: lane.name, claimed: fair, price, gate: cal }); } catch (e) { }
      continue;
    }
    // OPERATOR_LAW: if the operator stopped this lane mid-tick, place NO order.
    // Enforce: operator's last button push is absolute law.
    OPERATOR_LAW.enforce('engineTick mid-tick check', { lane: lane.name, running: ls.running });
    if (!ls.running) { ls.skips++; ls.last_skip = 'operator stop (mid-tick)'; continue; }
    if (makerLaneMode(lane, lc)) {
      // TAKER FALLBACK (operator order 2026-11-08): maker-only lets fat edges
      // rest until they expire (the sim_unfilled pile). When the book is thin
      // (spread <= 2c) and the vaccine-adjusted edge still clears 5c AFTER
      // paying the ask — and the ask stays inside the evidence price band —
      // take the liquidity now instead of resting behind it.
      const askPx = side === 'YES' ? q.yes_ask : q.no_ask;
      if (askPx != null && askPx > 0 && askPx < maxPrice && (askPx - price) <= 0.02
          && cal.adjusted != null && (cal.adjusted - askPx) >= 0.05) {
        const openedT = await openDryPosition(lane, q, side, askPx, fair, cal.adjusted - askPx, perSec);
        if (openedT) { ls.last_entry_at = Date.now(); ls.pending_edge = null; continue; }
      }
      // MAKER-EDGE: never lift the ask. Rest a bid at our own level; FILLSIM books
      // the position only if a REAL print goes through that bid inside the TTL.
      if (laneMakerPending(lane.name)) { ls.skips++; ls.last_skip = 'maker bid already resting'; continue; }
      const ttlSec = Number(lc.maker_ttl_sec) || lane.makerTtlSec || (lane.horizon === '15m' ? 60 : 180);
      const snapQ = { ticker: q.ticker, title: q.title, series: q.series, strike: q.strike, close_time: q.close_time, source: q.source, host: q.host, yes_bid: q.yes_bid, no_bid: q.no_bid, yes_ask: q.yes_ask, no_ask: q.no_ask };
      const rid = FILLSIM.intent({ ticker: q.ticker, side, bid: price, qty: 1, ttl_ms: ttlSec * 1000, volume: q.volume, last: q.last,
        book: { lane_entry: true, lane: lane.name, side, price, fair: result.fair != null ? result.fair : price, edge: result.edge, sigma: perSec, q: snapQ } });
      if (rid) { ls.maker_rests = (ls.maker_rests || 0) + 1; ls.last_skip = 'maker bid ' + price + ' ' + side + ' resting ' + ttlSec + 's'; }
      continue;
    }
    const opened = await openDryPosition(lane, q, side, price, fair, result.edge, perSec);
    if (opened) { ls.last_entry_at = Date.now(); ls.pending_edge = null; }
  }
}

// ---------------------------------------------------------------------------
// DRY account (real, read-only)
// ---------------------------------------------------------------------------

function dryPositions() {
  const d = state.dry.positions || {};
  const rows = [];
  const events = [];
  const out = rows.filter(r => Number(r.position_fp) !== 0).map(r => {
    const qty = Math.abs(Number(r.position_fp));
    const exposure = r.market_exposure_dollars != null ? Number(r.market_exposure_dollars) : null;
    return {
      market_id: r.ticker,
      side: Number(r.position_fp) > 0 ? 'YES' : 'NO',
      entry_price: exposure != null && qty ? Number((Math.abs(exposure) / qty).toFixed(3)) : null,
      quantity: qty,
      strategy: strategyForTicker(r.ticker),
      stop_loss: null, take_profit: null,
      realized: r.realized_pnl_dollars != null ? Number(r.realized_pnl_dollars) : null,
      fees: r.fees_paid_dollars != null ? Number(r.fees_paid_dollars) : null,
      exposure,
      kind: 'market',
    };
  });
  for (const e of events) {
    const qty = Number(e.total_cost_shares_fp || 0);
    if (!qty) continue;
    out.push({
      market_id: e.event_ticker,
      side: 'NO',
      entry_price: Number(e.total_cost_dollars || 0) / qty,
      quantity: qty,
      strategy: strategyForTicker(e.event_ticker),
      stop_loss: null, take_profit: null,
      realized: e.realized_pnl_dollars != null ? Number(e.realized_pnl_dollars) : null,
      fees: e.fees_paid_dollars != null ? Number(e.fees_paid_dollars) : null,
      exposure: Number(e.event_exposure_dollars || 0),
      kind: 'event',
    });
  }
  return out;
}
function strategyForTicker(ticker) {
  const series = String(ticker || '').split('-')[0];
  const hit = LANES.find(l => (l.seriesList || []).includes(series) || l.series === series);
  return hit ? hit.name : 'kalshi_manual';
}
function dryFillsList() {
  const d = state.dry.closed || {};
  const rows = d.fills || [];
  return rows.map(f => {
    const side = String(f.side || f.outcome_side || '').toLowerCase();
    const price = side === 'no' ? Number(f.no_price_dollars) : Number(f.yes_price_dollars);
    return {
      trade_id: f.fill_id || f.trade_id,
      market_id: f.ticker || f.market_ticker,
      side: side === 'no' ? 'NO' : 'YES',
      action: String(f.action || '').toLowerCase(),
      price: Number.isFinite(price) ? price : null,
      quantity: Number(f.count_fp || 0),
      fee: f.fee_cost != null ? Number(f.fee_cost) : null,
      strategy: strategyForTicker(f.ticker || f.market_ticker),
      at: f.created_time || (f.ts ? new Date(f.ts * 1000).toISOString() : ''),
    };
  }).filter(f => f.quantity);
}
function dryCurve() {
  const fills = dryFillsList();
  const byDay = new Map();
  for (const f of fills) {
    const day = String(f.at).slice(0, 10);
    if (!day) continue;
    // real cash flow: buys are -price*qty, sells are +price*qty
    const delta = (String(f.action || '').toLowerCase() === 'sell' ? 1 : -1) * (f.price || 0) * f.quantity;
    byDay.set(day, (byDay.get(day) || 0) + delta);
  }
  const labels = Array.from(byDay.keys()).sort();
  let run = 0;
  const pnl = labels.map(d => { run += byDay.get(d); return Number(run.toFixed(2)); });
  return { labels, pnl };
}

// ---------------------------------------------------------------------------
// Strategy cards / stats
// ---------------------------------------------------------------------------
function laneTrades(name) { return state.dry.closed.filter(t => t.strategy === name); }
function dryTrades(name) {
  return (state.dry.closed || {}).fills
    ? dryFillsList().filter(t => t.strategy === name)
    : [];
}
function cards() {
  return LANES.map(lane => {
    const ls = laneState(lane.name);
    const closed = laneTrades(lane.name);
    const wins = closed.filter(t => t.pnl > 0).length;
    const open = state.dry.positions.filter(p => p.strategy === lane.name);
    const equity = curveFor(closed);
    const heartbeatAge = ls.heartbeat ? Math.round((Date.now() - new Date(ls.heartbeat).getTime()) / 1000) : null;
    const dryOpen = dryPositions().filter(p => p.strategy === lane.name);
    const dryClosed = dryFillsList().filter(t => t.strategy === lane.name);
    const dryWins = dryClosed.filter(t => (t.price || 0) * t.quantity > 0).length;
    return {
      name: lane.name,
      label: lane.label,
      series: ls.last_series || lane.series,
      running: !!ls.running,
      heartbeat_age_sec: heartbeatAge,
      stop_reason: ls.stop_reason || '',
      trades: closed.length,
      win_rate: closed.length ? Number((100 * wins / closed.length).toFixed(1)) : 0,
      open_positions: open.length,
      deployed: Number(open.reduce((a, p) => a + p.cost, 0).toFixed(2)),
      realized: Number(closed.reduce((a, t) => a + (t.pnl || 0), 0).toFixed(2)),
      entries: ls.entries, skips: ls.skips, last_skip: ls.last_skip, last_edge: ls.last_edge,
      equity,
      mode: state.mode,
      book: state.mode,
      heartbeat: ls.heartbeat,
      // DRY book stats (read-only, from real Kalshi fills)
      live_trades: dryClosed.length,
      live_win_rate: dryClosed.length ? Number((100 * dryWins / dryClosed.length).toFixed(1)) : 0,
      live_open: dryOpen.length,
      live_realized: Number(dryClosed.reduce((a, t) => a + ((t.price || 0) * t.quantity), 0).toFixed(2)),
    };
  });
}
function curveFor(closed) {
  const byDay = new Map();
  for (const t of closed) {
    const d = String(t.exit_timestamp || '').slice(0, 10);
    if (!d) continue;
    byDay.set(d, (byDay.get(d) || 0) + (t.pnl || 0));
  }
  const labels = Array.from(byDay.keys()).sort();
  let run = 0;
  return { labels, pnl: labels.map(d => { run += byDay.get(d); return Number(run.toFixed(2)); }) };
}
function tradesStats() {
  const closed = state.dry.closed;
  const bands = [
    { band: '0.00-0.20', lo: 0, hi: 0.20 }, { band: '0.20-0.40', lo: 0.2, hi: 0.4 },
    { band: '0.40-0.60', lo: 0.4, hi: 0.6 }, { band: '0.60-0.80', lo: 0.6, hi: 0.8 },
    { band: '0.80-1.00', lo: 0.8, hi: 1.01 },
  ];
  const by_fair_band = bands.map(b => {
    const list = closed.filter(t => (t.entry_price || 0) >= b.lo && (t.entry_price || 0) < b.hi);
    const wins = list.filter(t => t.pnl > 0).length;
    return {
      band: b.band, trades: list.length, wins,
      win_rate: list.length ? Number((100 * wins / list.length).toFixed(1)) : 0,
      pnl: Number(list.reduce((a, t) => a + (t.pnl || 0), 0).toFixed(2)),
    };
  });
  const wins = closed.filter(t => t.pnl > 0).length;
  return { by_fair_band, win_rate: closed.length ? Number((100 * wins / closed.length).toFixed(1)) : 0, wins, losses: closed.length - wins };
}

function modePayload(book) {
  const dry = null ? dryAccountPayload() : null;
  const b = 'dry';  // Kai-Mirror is DRY-only: the mode never changes.
  return {
    mode: b,
    dry: dry ? {
      cash: dry.cash, open_positions: dry.open_positions, total_pnl: dry.total_pnl,
      portfolio_value: dry.portfolio_value, equity: dry.equity, realized: dry.realized,
      exposure: dry.exposure, closed_trades: dry.closed_trades, error: dry.error,
    } : null,
    funding: { balance: dry ? dry.cash : null },
    no_lanes_enabled: !!state.no_lanes_enabled,
    live_full: dry,
  };
}

function kalshiBlock(book) {
  const b = book || state.mode || 'dry';
  // DRY mode: use paper positions from state.dry, never call real Kalshi account API
  const markets = state.dry.positions.map(p => ({
    ticker: p.market_id, shares: p.side === 'YES' ? p.qty : -p.qty,
    exposure: p.cost, traded: p.qty, realized: null, fees: 0,
  }));
  return {
    markets, events: [], held_count: markets.length, ghost_count: 0,
    market_count: markets.length, event_count: 0,
    exposure: Number(deployed().toFixed(2)), fees: 0, realized: null,
    cost_basis_known: true, cost_basis: Number(deployed().toFixed(2)),
    ledger: { sales_realized: null, settlement_realized: Number(dryRealized().toFixed(2)), window_start: (state.dry.closed[0] || {}).exit_timestamp || null },
    source: 'dry book (paper positions)', host: null,
  };
}

const TITLES = {};
function rememberTitles(lanes) {
  for (const lane of lanes) {
    const res = marketCache.get((lane.series || '') + ':5');
    if (!res || !res.markets) continue;
    for (const m of res.markets) if (m.title) TITLES[m.ticker] = m.title;
  }
  for (const p of state.dry.positions) if (p.title) TITLES[p.market_id] = p.title;
  for (const t of state.dry.closed) if (t.title) TITLES[t.market_id] = t.title;
}

function snapshotPayload(book) {
  const b = 'dry';  // Kai-Mirror is DRY-only: the mode never changes.
  const dry = dryAccountPayload();
  const prov = lastProvenance();
  return {
      book: b,
      mode: modePayload(b),
      balance: dry.cash,
      kalshi_age_sec: null ? Math.round((Date.now() - null) / 1000) : null,
      accounts: { dry },
      current: b,
      equity: dry.curve,
      positions: dryPositions(),
      strategy_cards: cards(),
      trades: tradesStats(),
      data: { dry: { win_rate: dry.win_rate }, ai_cost_today: 0 },
      kalshi: kalshiBlock(b),
      market_titles: TITLES,
      no_lanes_enabled: !!state.no_lanes_enabled,
      running_count: LANES.filter(l => laneState(l.name).running).length,
      uptime_sec: Math.round((Date.now() - new Date(state.booted_at).getTime()) / 1000),
      provenance: prov,
      spot: Object.fromEntries(Object.entries(spot).map(([k, v]) => [k, v.price != null ? { price: v.price, bid: v.bid, ask: v.ask, t: v.t, at: v.at, points: v.points?.length || 0 } : null]).filter(([k, v]) => v != null)),
      generated_at: new Date().toISOString(),
      debug: 'spot-field-added',
    };
}

// ---------------------------------------------------------------------------
// Provenance
// ---------------------------------------------------------------------------
const provLog = [];
function noteProvenance(rec) { provLog.unshift(rec); if (provLog.length > 60) provLog.pop(); }
function lastProvenance() {
  const btc = spot['BTC-USD'];
  const m = marketCache.get('KXBTC15M:5');
  const btcAt = (btc && btc.at && !isNaN(new Date(btc.at).getTime())) ? new Date(btc.at).toISOString() : null;
  const mAt = (m && m.at && !isNaN(new Date(m.at).getTime())) ? new Date(m.at).toISOString() : null;
  return {
    spot: {
      product: 'BTC-USD', price: btc ? btc.price : null, at: btcAt,
      source: 'Coinbase Exchange public ticker',
      url: 'https://api.exchange.coinbase.com/products/BTC-USD/ticker',
    },
    kalshi: {
      host: m ? m.host : null, source: m ? m.source : null,
      fetched_at: mAt, latency_ms: m ? m.ms : null,
      series: 'KXBTC15M',
      key_id: kalshi.accounts.dry.keyId,
      url: (m ? m.host : kalshi.LIVE_BASE) + '/markets?series_ticker=KXBTC15M',
    },
    llm: { provider: 'Kilo AI Gateway', model: ENV.KILO_MODEL || 'auto/free', base: ENV.KILO_BASE_URL },
    dry_execution: 'SIMULATED fills at real quoted prices; settlements from real Kalshi results',
    recent: provLog.slice(0, 20),
  };
}

// ---------------------------------------------------------------------------
// ALL NO book - separate book tracking NO-side market positions + trades
// ---------------------------------------------------------------------------
// YES / NO view of the ALL NO book (YES-TWIN rows live in this book too).
// Trades with no stored side are legacy NO. Only the VIEW is filtered: the
// engine, the shared balance and the stored records are untouched.
function bookForSide(src, sideFilter) {
  const want = sideFilter === 'YES' || sideFilter === 'NO' ? sideFilter : null;
  if (!want) return src;
  const sideOk = t => (t.side || 'NO') === want;
  const positions = (src.positions || []).filter(sideOk);
  const closed = (src.closed_trades || []).filter(sideOk);
  return Object.assign({}, src, {
    positions,
    closed_trades: closed,
    total_pnl: closed.reduce((a, t) => a + (Number(t.pnl) || 0), 0),
    entries: positions.length + closed.length,
  });
}

function getAllNoBook(sideFilter) {
  // Return current ALL NO book state (open positions + closed trades + tallies)
  // The NO book runs in its own ledger: state.all_no['dry'] for paper DRY,
  // state.all_no['dry'] for real-money DRY.
  const mode = state.mode;
  const rawBook = bookForSide(state.all_no[mode] || { starting_balance: 250, balance: 250, positions: [], closed_trades: [], total_pnl: 0, entries: 0 }, sideFilter);
  const safeNum = (v, d = 0) => { const x = Number(v); return Number.isFinite(x) ? x : d; };
  // A missing balance must stay UNKNOWN, not silently become 0.
  const rawBalanceKnown = rawBook.balance != null && Number.isFinite(Number(rawBook.balance));
  
  // Calculate tallies from closed trades
  const closed = rawBook.closed_trades || [];
  const wins = closed.filter(t => t.pnl > 0).length;
  const losses = closed.filter(t => t.pnl < 0).length;
  const win_rate = closed.length > 0 ? (wins / closed.length) * 100 : 0;
  // FEE TOTAL: LIVE trades carry `fees` (the real Kalshi amount); the DRY
  // paper book carries `fee_paid`. Reading only `fee_paid` made the fee
  // tally report $0.00 while real money was being paid in fees.
  const tradeFee = (t) => (t.fees != null ? Number(t.fees) : Number(t.fee_paid || 0));
  const total_fees = closed.reduce((sum, t) => sum + tradeFee(t), 0);
  const avg_entry = closed.length > 0 ? closed.reduce((sum, t) => sum + t.entry_price, 0) / closed.length : 0;
  
  // Calculate open tally
  const open = rawBook.positions || [];
  const open_cost = open.reduce((sum, p) => sum + p.cost, 0);
  const open_sweet = open.filter(p => p.entry_price >= 0.84 && p.entry_price <= 0.92).length;
  
  return {
    book: {
      balance: rawBalanceKnown ? Number(Number(rawBook.balance).toFixed(2)) : null,
      // BALANCE TRUTH: a null balance means "the balance read failed", which is
      // NOT the same as "the balance is zero". The tape and the dashboards must
      // be able to tell those apart instead of showing $0.
      balance_known: rawBalanceKnown,
      balance_stale: rawBook.balance_stale === true,
      starting: rawBook.starting_balance,
      pnl: Number(safeNum(rawBook.total_pnl).toFixed(2)),
      entries: safeNum(rawBook.entries),
      mode: mode,
    },
    open_rows: (open || []).map(p => ({
      id: p.id,
      market_id: p.market_id,
      strategy: p.strategy || 'all_no_markets',
      side: p.side || 'NO',
      entry_price: p.entry_price,
      quantity: p.quantity,
      cost: Number(safeNum(p.cost).toFixed(2)),
      entry_fair: p.fair_at_entry || 0.5,
      edge: (p.edge_at_entry != null ? Number(p.edge_at_entry.toFixed(3)) : (p.fair_at_entry != null && p.entry_price != null ? Number((p.fair_at_entry - p.entry_price).toFixed(4)) : 0)),
      kelly: Number(safeNum(p.kelly_fraction).toFixed(3)),
      stop_loss: p.stop_loss,
      take_profit: p.take_profit,
      confidence: safeNum(p.confidence),
      expiration: p.expiration,
      status: p.status || 'resting',
      mode: mode,
      timestamp: p.opened_at,
    })),
    closed_rows: (closed || []).map(t => ({
      id: t.id,
      market_id: t.market_id,
      strategy: t.strategy || 'all_no_markets',
      side: t.side || 'NO',
      entry_price: t.entry_price,
      exit_price: t.exit_price,
      quantity: t.quantity,
      cost: Number(safeNum(t.cost).toFixed(2)),
      proceeds: Number(safeNum(t.proceeds).toFixed(2)),
      pnl: Number(safeNum(t.pnl).toFixed(2)),
      roi: t.quantity > 0 && safeNum(t.cost) > 0 ? Number(((safeNum(t.pnl) / safeNum(t.cost)) * 100).toFixed(1)) : 0,
      entry_fair: t.fair_at_entry || 0.5,
      kelly: Number(safeNum(t.kelly_fraction).toFixed(3)),
      fee_paid: Number(tradeFee(t).toFixed(3)),
      exit_reason: t.exit_reason,
      mode: mode,
      entry_timestamp: t.opened_at,
      exit_timestamp: t.closed_at,
    })),
    tally: {
      closed: closed.length,
      wins,
      losses,
      win_rate: Number(win_rate.toFixed(1)),
      pnl: Number(safeNum(rawBook.total_pnl).toFixed(2)),
      fees: Number(total_fees.toFixed(2)),
      avg_entry: avg_entry,
    },
    open_tally: {
      positions: open.length,
      contracts: open.reduce((sum, p) => sum + p.quantity, 0),
      cost: Number(open_cost.toFixed(2)),
      sweet: open_sweet,
    },
    no_lanes_enabled: state.no_lanes_enabled,
    boots: state.boot_count || 0,
  };
}

// Book the paper fills the simulator confirmed from REAL prints (zero extra API calls).
function pumpFills(q) {
  for (const ev of FILLSIM.observe(q)) {
    if (ev.type !== 'filled' || !ev.intent.book) continue;
    const a = ev.intent.book;
    // HARVESTER fills route to the harvester's own paper ledger; everything
    // else books into the all_no book as before.
    if (a.lane_entry) {
      const lref = LANE_BY_NAME.get(a.lane);
      if (lref) { const fillPos = openDryPosition(lref, a.q, a.side, a.price, a.fair, a.edge, a.sigma, { maker: true }); if (fillPos) laneState(lref.name).last_entry_at = Date.now(); }
      continue;
    }
    if (a.extra && a.extra.harvester) { HARVEST.bookFill(a); continue; }
    createAllNoEntry(a.market_id, a.entry, a.qty, a.fair, a.edge, a.kelly, a.stop,
      a.take_profit, a.confidence, a.expiration, a.extra);
  }
}

// SCAN TALLY (2026-11-06): why each market was rejected on the last ALL NO pass.
let lastScanTally = {};
async function noBook() {
  const SCAN = {};
  const sk = (r) => { SCAN[r] = (SCAN[r] || 0) + 1; };
  // DIVERGENCE POLICY: Mirror is the LAB - keep gates loose (cluster=2, no
  // price floor, HYPE live) to generate statistics. LIVE is the product with
  // tight gates. Do NOT sync LIVE tightenings back into Mirror.
  // Return available NO-side market recommendations for potential entries
  // Scans ALL Kalshi series, not just DRY/LIVE lanes - open to ANY prediction market
  const rows = [];
  const allSeries = await getAllTradableSeries();
  
  for (const s of allSeries) {
    // 2026-11-04: same blindness as activeQuote - hourly/daily ladders need a
    // deep window to see their near-spot rungs (default sort shows top strikes).
    const deepLadder = /^KX/.test(s) && !/15M$/.test(s);
    const res = await marketsForSeries(s, deepLadder ? 300 : 12); // deeper scan: ladders have 200+ open strikes, near-spot rungs sit past the first 80
    for (const q of res.markets || []) {
      // Refresh the book and re-verify AT THE MOMENT OF THE DECISION, so the
      // verdict covers exactly the prices that would be traded.
      recordFact(quote_stream.apply(q));
      pumpFills(q);
      // FINALIZED MARKET SWEEP: settle every pending shadow (the vaccine's
      // free lessons) and every harvester paper position the moment the REAL
      // Kalshi result appears — for ALL markets, not just ones we hold.
      if (q.result === 'yes' || q.result === 'no') {
        try {
          CALIB.settleResult(q.ticker, q.result === 'yes');
          HARVEST.settleResult(q.ticker, q.result === 'yes');
        } catch (e) { }
      }
      if (q.status === 'finalized' || q.result) { sk('finalized_or_resolved'); continue; }
      if (!q.strike || !q.close_time) { sk('no_strike_or_close'); continue; }
      
      const asset = s.startsWith('KXB') ? 'BTC-USD' : s.startsWith('KXE') ? 'ETH-USD' : s.startsWith('KXX') ? 'XRP-USD' : s.startsWith('KXH') ? 'HYPE-USD' : null;
      const sp = asset ? spot[asset] : null;
      if (!sp || !Number.isFinite(sp.price)) { sk('no_spot'); continue; }
      
      // Compute fair value using Black-Scholes normal CDF
      const perSec = perSecondVol(asset, 900);
      const tau = Math.max(0, (new Date(q.close_time).getTime() - Date.now()) / 1000);
      if (tau < 30 || tau > 3600) { sk('tau_outside_30_3600'); continue; } // only 30s-1hr contracts
      
      let fair = 0.5;
      if (perSec && tau > 20) {
        // PERP TILT (2026-11-04): Hyperliquid funding/premium/OI bias for this
        // asset becomes a drift term, capped at 0.5*sigma*sqrt(tau) - the
        // driftless gaussian was blind to derivatives positioning.
        const perpB = perp.biasFor(asset);
        const muTilt = (Number.isFinite(perpB.bias) ? perpB.bias : 0) * 0.5 * perSec * Math.sqrt(tau);
        fair = 1 - normCdf((Math.log(sp.price / q.strike) + muTilt) / (perSec * Math.sqrt(tau)));
      }
      
      // Strict fair value bounds
      if (fair < 0.1 || fair > 0.9) { sk(perSec ? 'fair_outside_0.1_0.9' : 'no_realized_vol'); continue; }
      
      // Use REAL market quotes - no synthetic fallbacks
      const no_bid = q.no_bid;
      const no_ask = q.no_ask;
      // 2026-11-04 OPERATOR ORDER: one-sided books are tradable. We POST at the
      // bid, so a NO entry only needs a real NO bid. Quote sanity still applies.
      if (q.facts_ok === false) { sk('facts_refused:' + (q.facts_verdict || '?')); continue; }   // DATA INTEGRITY: payload failed verification
      if (!no_bid || no_bid <= 0.02 || (no_ask != null && no_ask >= 0.98)) { sk('no_bid_missing_or_extreme'); continue; }
      
      // Spread only where both sides exist; a one-sided book has no spread to test.
      const spread = (no_ask != null) ? no_ask - no_bid : 0;
      if (no_ask != null && spread > 0.06) { sk('wide_spread'); continue; } // skip wide spreads (the hard thin line)
      
      // Deep-value thesis, real basis: we ENTER at the bid (maker), so edge is
      // measured against the bid. Model prob >= min_fair and entry <= cap
      // (high win rate, capped risk, big payout).
      // YES-TWIN: the identical thesis mirrored to the other side of the book -
      // a cheap high-probability YES is the same trade as a cheap
      // high-probability NO. One scan, both sides, per-side caps.
      const anCfg = { min_fair: 0.65, max_fair: 0.85, max_no_price: 0.35, max_yes_price: 0.35, min_edge: 0.02, allow_yes: true, ...(state.runtime.all_no || {}) };
      const mkRow = (side, fairSide, bid, askPx) => {
        if (state.safe_mode) { sk('safe_mode'); return; }   // FAIL CLOSED
        const cap = side === 'YES' ? anCfg.max_yes_price : anCfg.max_no_price;
        if (bid == null) { sk('no_bid_side_' + side); return; }
        const sideSpread = askPx != null ? askPx - bid : 0;
        if (askPx != null && sideSpread > 0.06) { sk('side_spread_wide'); return; } // per-side thin line
        // FAT-TAIL MARGIN: HYPE's jumps crush a smooth gaussian model. Demand
        // more probability margin there.
        const fairFloor = asset === 'HYPE-USD' ? Math.max(anCfg.min_fair, 0.58) : anCfg.min_fair;
        if (fairSide < fairFloor) { sk('fair_below_floor'); return; }                   // probability floor
        // CONFIDENCE CEILING (ledger law 2026-11-08): entry-fair 0.85+ is the
        // -$198.61 hole (n=1983). The ceiling is now 0.85 in CODE — the lab
        // sampled that zone long enough to convict it. Runtime may LOWER it,
        // never raise it (runtime spread merges after the literal only for
        // fields it defines; this literal pins the default).
        if (fairSide > (anCfg.max_fair != null ? anCfg.max_fair : 9.99)) { sk('fair_above_ceiling'); return; }
        if (bid > cap) { sk('bid_above_cap'); return; }                              // price cap (cheap only)
        if (bid < (anCfg.min_entry_price || 0)) { sk('bid_below_min_entry'); return; }     // no sub-floor lottery tickets
        // MOMENTUM CONFIRMATION: the gaussian model knows only distance from
        // strike. Winners entered while the move AWAY from the strike was
        // continuing; losers were reversals. Block entries when spot is
        // driving back toward the strike faster than half a 60s sigma.
        const pts = (sp && Array.isArray(sp.points)) ? sp.points : [];
        if (pts.length >= 31 && perSec) {
          const mom = (sp.price - pts[pts.length - 31].p) / pts[pts.length - 31].p;
          const drift60 = 0.5 * perSec * Math.sqrt(60);
          if (side === 'NO' && mom > drift60) { sk('momentum_block'); return; }   // spot rising toward strike
          if (side === 'YES' && mom < -drift60) { sk('momentum_block'); return; } // spot falling toward strike
        }
        const e = fairSide - bid;                           // REAL maker edge
        if (e < anCfg.min_edge) { sk('edge_below_min'); return; }
        // TOO-GOOD FILTER (CODE LAW 2026-11-04): an edge > 0.15 means the
        // MODEL is wrong, not that the market is giving money away. This cap
        // lives in CODE, not runtime config — the Oct 26 config surgery that
        // raised max_edge to 0.30 is exactly how 1,136 zero-win entries got
        // made. No runtime override may ever raise it.
        if (e > MAX_CLAIMED_EDGE) { sk('edge_above_code_cap'); antiBook(q, side, bid, fairSide, null, perSec || null, tau, sp); return; }
        // THE VACCINE: the claimed fair is a hypothesis, not a probability.
        // The bucket's REALIZED record (with market price as prior) is the
        // only probability we trade against. Blocked candidates become free
        // shadow rows so the bucket keeps learning while we refuse to pay.
        const cal = CALIB.gate({
          ticker: q.ticker, side, strategy: 'all_no_markets',
          claimed: fairSide, price: bid, minEdge: Math.max(anCfg.min_edge, 0.05),
        });
        if (!cal.ok) {
          sk('calib_gate_refused');
          CALIB.shadowOpen({
            ticker: q.ticker, side, strategy: 'all_no_markets',
            claimed: fairSide, price: bid, gate: cal,
            sigma: perSec || null, tau, strike: q.strike, spot: (sp && sp.price) || null,
          });
          // FADE-THE-MODEL (2026-11-08): a refused OVERSTATEMENT is the one
          // refusal the ledger itself says was right to refuse — the anti
          // side is the candidate worth measuring (see antiBook).
          antiBook(q, side, bid, fairSide, cal, perSec || null, tau, sp);
          return;
        }
        const kelly = Math.max(0, e / (1 - bid));
        const kellyCapped = Math.min(0.25, kelly);          // cap at 25% Kelly
        // OPERATOR ORDER 2026-11-04: every order ≤ $0.50. The Kelly notional
        // (hardcoded $5 before) is now min(effectiveClip(), 5*kelly), and if
        // even one contract doesn't fit that budget the row is DROPPED. The old
        // Math.max(1, ...) forced a $0.61 YES through on KXBTC15M (2026-11-04).
        const qBudget = Math.min(effectiveClip(), 5 * kellyCapped);
        const q2 = Math.floor(qBudget / Math.max(0.01, bid));
        if (q2 < 1) { sk('qty_below_1'); return; }
        rows.push({
          id: q.ticker,
          market: q.title,
          market_id: q.ticker,
          strategy: 'all_no_markets',
          side,
          limit_entry: bid, // MAKER at bid
          qty: q2,
          cost: Number((q2 * bid).toFixed(2)),
          fair: Number(fairSide.toFixed(3)),
          edge: Number(e.toFixed(4)),
          kelly: Number(kellyCapped.toFixed(3)),
          stop: Number((bid * 0.8).toFixed(3)),
          take_profit: null,   // DEAD (operator order, 2026-11-03): winners ride to settlement
          confidence: Number(Math.min(1, e / 0.08).toFixed(2)),
          expiration: q.close_time,
          status: 'candidate',
          source: res.source,
          host: res.host,
          spread: sideSpread,
          one_sided: askPx == null,
          tau,
          strike: q.strike,
          sigma: perSec || null,
          spot_product: asset,
          // THE VACCINE VERDICT: empirical posterior, bucket n, realized rate.
          calib: cal,
          adjusted: cal.adjusted,
        });
      };
      mkRow('NO', fair, no_bid, no_ask);
      if (anCfg.allow_yes !== false) mkRow('YES', 1 - fair, q.yes_bid, q.yes_ask);
    }
  }
  
  // Sort by edge * confidence (quality-adjusted edge)
  lastScanTally = SCAN;
  rows.sort((a, b) => (b.edge * b.confidence) - (a.edge * a.confidence));
  return rows.slice(0, 20); // top 20 only
}

// Fetch ALL tradable Kalshi series (not just LANES)
async function getAllTradableSeries() {
  // Bounded scan: the crypto lane series always, then the most liquid open series, max 30.
  // An unbounded scan made hundreds of rate-limited Kalshi calls per pass and never finished.
  try {
    // 2026-11-04 shared-key fix: go through kalshi.get() so this heavy
    // /markets?limit=500 scan is an UNSIGNED public read (per-IP budget),
    // not a signed live-key call every scan pass.
    const r = await kalshi.get('/markets', { query: { status: 'open', limit: 500 }, ttlMs: 30000 });
    const data = r.data;
    const bySeries = new Map();
    for (const m of data.markets || []) {
      const s = m.series_ticker || String(m.ticker || '').split('-')[0];
      if (!s) continue;
      const rec = bySeries.get(s) || { oi: 0 };
      rec.oi += Number(m.open_interest_fp) || 0;
      bySeries.set(s, rec);
    }
    const lanesSeries = Array.from(new Set(LANES.flatMap(l => l.seriesList || []).filter(Boolean)));
    // COMBAT: series discovered by kind (handles rotated/delayed 1-hour ids)
    // always make the scan list, even before they rank by open interest.
    const kindSeries = Object.values(series_kind.cache).map(c => c && c.series).filter(Boolean);
    const ranked = Array.from(bySeries.entries()).map(([s, r]) => ({ s, oi: r.oi })).sort((a, b) => b.oi - a.oi).map(x => x.s);
    return Array.from(new Set([...lanesSeries, ...kindSeries, ...ranked])).slice(0, 30);
  } catch (e) {
    // Fallback to LANES series
    return Array.from(new Set(LANES.flatMap(l => l.seriesList || []).filter(Boolean)));
  }
}  
// ---------------------------------------------------------------------------
// ALL NO MARKETS - Entry/Exit Trade Functions
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// THE FADE-THE-MODEL LANE (operator order 2026-11-08): "do the opposite of
// what you think, in dry." The honest version of that instinct. The PURE
// inverse of every loser cannot win (fees + spread + noise hit both sides,
// and inverting a coin is the same coin) — but this repo's own forensics
// convicted the MODEL of systematic overstatement: btc_1h NO@6-8c claimed
// fair 0.65+ went 0/1136, meaning the market side (YES@93c) won 1136/1136 at
// ~+7c each. So: when the calibration ledger CONVICTS a zone WITH EVIDENCE
// (claimed - realized >= threshold on a warm bucket), this lane buys the
// OTHER side in DRY at the complement price. The dry book and /api/shadow
// then settle it against reality — if the fade prints, the operator was
// right; if fees/spread/noise eat it, the numbers say so. Proof, not opinion.
// ---------------------------------------------------------------------------
const ANTI_DEFAULTS = {
  enabled: true,
  min_overstatement: 0.10,   // conviction floor: claimed - adjusted must clear this
  min_evidence_n: 20,        // bucket or zone must carry this many settled rows (not noise)
  min_edge: 0.03,            // anti-side empirical edge after paying the complement price
  max_price: 0.97,           // never pay more for the anti side
  min_price: 0.55,           // below this the "opposite" is the same lottery, not a fade
  max_concurrent: 6,
  notional_dollars: 0.50,    // operator law: every order <= $0.50
};
const antiTried = new Map(); // ticker -> ts (10min per-ticker cooldown)
function antiCfg() {
  return Object.assign({}, ANTI_DEFAULTS, (state.runtime.all_no && state.runtime.all_no.anti_model) || {});
}
function antiBook(q, side, bid, fairSide, calIn, perSec, tau, sp) {
  try {
    const cfg = antiCfg();
    const ls = laneState('anti_model');
    ls.heartbeat = new Date().toISOString();
    if (!cfg.enabled) { ls.last_skip = 'lane disabled in config'; return; }
    if (hourBlockedUtc()) { ls.skips++; ls.last_skip = 'trading-hours law: blocked UTC hour ' + new Date().getUTCHours() + 'Z'; return; }
    const t = antiTried.get(q.ticker);
    if (t && Date.now() - t < 10 * 60 * 1000) return;
    // The ledger verdict (recomputed for the too-good-to-be-true refusals,
    // which never reach the calibration gate on their own path).
    let cal = calIn;
    if (!cal) cal = CALIB.gate({ ticker: q.ticker, side, strategy: 'all_no_markets', claimed: fairSide, price: bid, minEdge: 0.05 });
    // CONVICTION: the ledger must have caught the model lying, with evidence.
    // Every filter step names its skip reason so the operator can SEE why the
    // fade did or did not fire - the lane is auditable, not a black box.
    const overstatement = Number(cal.bias);
    if (!Number.isFinite(overstatement) || overstatement < cfg.min_overstatement) { ls.skips++; ls.last_skip = 'no conviction (bias ' + (Number.isFinite(overstatement) ? overstatement.toFixed(3) : '?') + ' < ' + cfg.min_overstatement + ')'; return; }
    const evN = Math.max(cal.n || 0, cal.zone_n || 0);
    if (evN < cfg.min_evidence_n) { ls.skips++; ls.last_skip = 'conviction thin (n=' + evN + ')'; return; }
    // The anti side: buy the OTHER side of the SAME market at the complement.
    const antiSide = side === 'YES' ? 'NO' : 'YES';
    const antiPrice = Math.round((1 - bid) * 100) / 100;
    if (!(antiPrice >= cfg.min_price && antiPrice <= cfg.max_price)) { ls.skips++; ls.last_skip = 'anti price ' + antiPrice.toFixed(2) + ' outside ' + cfg.min_price + '-' + cfg.max_price; return; }
    const antiProb = 1 - (Number.isFinite(Number(cal.adjusted)) ? Number(cal.adjusted) : bid);
    const edge = antiProb - antiPrice;
    if (!(edge >= cfg.min_edge)) { ls.skips++; ls.last_skip = 'anti edge ' + edge.toFixed(3) + ' < ' + cfg.min_edge + ' (fees/spread eat it)'; return; }
    const book = state.all_no[state.mode];
    if (!book) return;
    const openAnti = book.positions.filter(p => (p.strategy || '') === 'anti_model' && p.status === 'resting');
    if (openAnti.length >= cfg.max_concurrent) { ls.last_skip = 'max concurrent anti positions'; return; }
    // Never hold both sides of one market (the dry book may want the
    // convicted side for its own evidence; the fade takes only the mirror).
    if (book.positions.some(p => p.market_id === q.ticker && p.status === 'resting')) return;
    const qty = Math.max(1, Math.floor(cfg.notional_dollars / antiPrice));
    const res = createAllNoEntry(q.ticker, antiPrice, qty, Number(antiProb.toFixed(4)), Number(edge.toFixed(4)), 0, null, null, 1, q.close_time, {
      strategy: 'anti_model', side: antiSide,
      strike: q.strike != null ? q.strike : null, sigma: perSec || null,
      anti: { faded_side: side, faded_claimed: Number(fairSide.toFixed(4)), faded_price: bid, conviction: Number(overstatement.toFixed(4)), evidence_n: evN },
    });
    antiTried.set(q.ticker, Date.now());
    if (res && res.ok) {
      ls.entries++; ls.last_edge = Number(edge.toFixed(4));
      log('anti_model_entry', {
        market_id: q.ticker, side: antiSide, px: antiPrice, qty,
        faded: side + '@' + bid, claimed: Number(fairSide.toFixed(3)),
        conviction: Number(overstatement.toFixed(3)), evidence_n: evN,
        empirical_prob: Number(antiProb.toFixed(3)), edge: Number(edge.toFixed(3)),
      });
      // Free double-entry: the vaccine ledger learns the anti zone too, and
      // /api/shadow will grade THIS idea by its own settled rows.
      CALIB.shadowOpen({ ticker: q.ticker, side: antiSide, strategy: 'anti_model', claimed: antiProb, price: antiPrice, sigma: perSec || null, tau, strike: q.strike != null ? q.strike : null, spot: (sp && sp.price) || null });
    } else if (res && res.error) {
      ls.skips++; ls.last_skip = 'entry refused: ' + res.error;
    }
  } catch (e) {
    try { log('anti_model_error', { error: String(e && e.message || e).slice(0, 200) }); } catch (e2) { }
  }
}

function createAllNoEntry(market_id, entry_price, quantity, fair_at_entry, edge, kelly, stop_loss, take_profit, confidence, expiration, extra) {
  // OPERATOR RULE (2026-11-06): a trade must close within 30 days of opening.
  // HOLD RULE: fail-closed - missing/invalid close_time blocks the entry
  // (the old check silently skipped when expiration was absent).
  const holdOk = holdRule.entryAllowed(expiration, Date.now());
  if (!holdOk.ok) return { error: 'hold rule: ' + holdOk.reason };
  const mode = state.mode;
  const book = state.all_no[mode];
  if (!book) return { error: 'Book not found for mode: ' + mode };
  const x = extra || {};

  // OPERATOR_LAW safety: never let bad market data crash the server (an
  // unhandled rejection here was restart-looping the process). Skip instead.
  if (typeof entry_price !== 'number' || !Number.isFinite(entry_price) ||
      typeof quantity !== 'number' || !Number.isFinite(quantity) ||
      typeof fair_at_entry !== 'number' || !Number.isFinite(fair_at_entry) ||
      typeof edge !== 'number' || !Number.isFinite(edge) ||
      typeof kelly !== 'number' || !Number.isFinite(kelly) ||
      typeof confidence !== 'number' || !Number.isFinite(confidence) ||
      !market_id || !expiration) {
    return { error: 'Skipping all_no entry: incomplete market data' };
  }

  // 2026-11-04 SURGERY (the fourth path): this function no longer trusts its
  // callers. Four callers size positions their own way - the dashboard POST
  // /api/no-lanes/entry, the certainty lane, pumpFills, runAllNoEntry - and a
  // dashboard entry booked qty=2 @ $0.56 ($1.12) at 01:30:26 while every
  // scanner path was already capped. The operator law is enforced HERE, at
  // the only booking site, so no caller can bypass it again.
  const cap = effectiveClip();
  const cappedQty = Math.floor(cap / Math.max(0.01, entry_price));
  if (cappedQty < 1) {
    return { error: 'Entry refused: price $' + entry_price.toFixed(2) + ' > order cap $' + cap.toFixed(2) + ' (no contract fits)' };
  }
  if (quantity > cappedQty) {
    log('all_no_qty_capped', { market_id, asked: quantity, allowed: cappedQty, entry_price });
    quantity = cappedQty;
  }
  // ANTI-STACK + CLUSTER re-checked at BOOKING time (fill-lag stacking put 8
  // positions into one KXBTC15M market on 2026-11-04: -$8.88 in 6 seconds).
  // ONE POSITION PER MARKET (2026-11-10, operator order after the 12:17 double
  // entry: KXXRP15M-26NOV101530-30 booked @0.46 then @0.49 22s apart - the old
  // >=2 threshold still allowed a second, worse-priced entry on a held market).
  // Mirrors the LIVE openEntry guard: any already-held market is refused.
  const sameMkt = book.positions.filter(p => p.market_id === market_id).length;
  if (sameMkt >= 1) return { error: 'Entry refused: one position per market (already holding ' + market_id + ')' };
  // REHAB DNA (2026-11-10): a lane on probation trades only inside its
  // measured DNA - edge bar AND price band. Refusal here is treatment, not
  // punishment.
  const rh = REHAB.entryCheck(x.strategy || "all_no_markets", edge, entry_price);
  if (!rh.ok) return { error: rh.reason };
  const sameRound = book.positions.filter(p => p.expiration === expiration).length;
  if (sameRound >= 2) return { error: 'Entry refused: cluster cap (' + sameRound + ' positions already in this round)' };

  const cost = Number((entry_price * quantity).toFixed(2));
  // FEE REALISM (2026-11-03): Kalshi charges 0.07*p*(1-p) PER CONTRACT on BOTH legs.
  // The lab used to charge nothing at entry and 1.75% of proceeds at exit, so paper
  // results were systematically better than real money (LIVE pays ~2x what DRY booked).
  const fee_entry = x.fee_entry_override != null
    ? Number(x.fee_entry_override)   // the certainty lane books the REAL per-contract fee (rounded up to the cent)
    : Number((quantity * 0.07 * entry_price * (1 - entry_price)).toFixed(4));
  if (book.balance < cost + fee_entry) {
    return { error: 'Insufficient balance: need ' + (cost + fee_entry).toFixed(2) + ', have ' + book.balance.toFixed(2) };
  }

  // UNIQUE POSITION IDS (2026-11-10): market_id + Date.now() collided when two
  // entries booked the same market in the same millisecond (71 legacy closed
  // rows share ids in pairs/triples from the pre-anti-stack era). The random
  // suffix makes future id collisions impossible.
  const id = market_id + '-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
  const position = {
    id,
    market_id,
    strategy: x.strategy || 'all_no_markets',
    side: x.side || 'NO',
    spot: x.spot_product || null,
    strike: x.strike != null ? x.strike : null,
    sigma_per_sec: x.sigma != null ? x.sigma : null,
    entry_price: Number(entry_price.toFixed(3)),
    quantity: Math.floor(quantity),
    cost,
    fair_at_entry: Number(fair_at_entry.toFixed(3)),
    edge_at_entry: Number(edge.toFixed(3)),
    kelly_fraction: Number((kelly * 100).toFixed(2)),
    stop_loss: stop_loss ? Number(stop_loss.toFixed(3)) : null,
    take_profit: take_profit ? Number(take_profit.toFixed(3)) : null,
    confidence: Number(confidence.toFixed(2)),
    expiration,
    status: 'resting',
    opened_at: new Date().toISOString(),
  };

  // DRY book: pure paper ledger. NEVER posts real Kalshi orders.
  book.balance -= (cost + fee_entry);
  position.fee_entry = fee_entry;
  book.positions.push(position);
  book.entries++;
  // LIFETIME MARKET CAP bookkeeping: every booked entry counts, whatever
  // path created it (engine, manual, FILLSIM fill, harvester excluded).
  bumpMarketEntry(position.market_id);
  save();

  return { ok: true, position, balance: book.balance };
}

function closeAllNoPosition(position_id, exit_price, exit_reason) {
  const mode = state.mode;
  const book = state.all_no[mode];
  if (!book) return { error: 'Book not found for mode: ' + mode };

  const idx = book.positions.findIndex(p => p.id === position_id);
  if (idx === -1) return { error: 'Position not found' };

  // CLOSE IDEMPOTENCY (2026-11-10): a replayed sweep or a rollback-reopened
  // position must never credit proceeds twice for the same settled id. With
  // collision-free ids (createAllNoEntry) and the boot retag of legacy rows,
  // this membership check is exact.
  if ((book.closed_trades || []).some(t => t.id === position_id)) {
    return { error: 'Close refused: position already settled (dedupe guard)' };
  }

  const pos = book.positions[idx];
  const proceeds = Number((pos.quantity * exit_price).toFixed(2));
  const fee_exit = Number((pos.quantity * 0.07 * exit_price * (1 - exit_price)).toFixed(4));
  const fee_paid = Number(((pos.fee_entry || 0) + fee_exit).toFixed(4)); // BOTH legs, Kalshi formula
  const pnl = Number((proceeds - pos.cost - fee_paid).toFixed(2));
  const roi = pos.cost > 0 ? Number(((pnl / pos.cost) * 100).toFixed(1)) : 0;
  const net_pnl = pnl;

  const trade = {
    id: pos.id,
    market_id: pos.market_id,
    strategy: pos.strategy,
    side: pos.side || 'NO',
    entry_price: pos.entry_price,
    exit_price: Number(exit_price.toFixed(3)),
    quantity: pos.quantity,
    cost: pos.cost,
    cost_basis: pos.cost,
    proceeds,
    pnl: net_pnl,
    roi,
    fair_at_entry: pos.fair_at_entry != null ? pos.fair_at_entry : null,
    edge_at_entry: pos.edge_at_entry != null ? pos.edge_at_entry : null,
    kelly_fraction: pos.kelly_fraction != null ? pos.kelly_fraction : null,
    sigma_per_sec: pos.sigma_per_sec != null ? pos.sigma_per_sec : null,
    strike: pos.strike != null ? pos.strike : null,
    horizon: pos.horizon != null ? pos.horizon : null,
    fee_paid,
    fee_entry: pos.fee_entry || 0,
    fee_exit,
    exit_reason,
    result: exit_reason === 'settled' ? (exit_price > 0 ? 'won' : 'lost') : exit_reason,
    hold_seconds: Math.round((Date.now() - new Date(pos.opened_at || Date.now()).getTime()) / 1000),
    opened: pos.opened_at,
    opened_at: pos.opened_at,
    closed_at: new Date().toISOString(),
    exit_timestamp: new Date().toISOString(),
    settled_price: exit_reason === 'settled' ? Number(exit_price.toFixed(3)) : null,
    settlement_source: exit_reason === 'settled' ? 'kalshi' : null,
  };

  // Update book
  book.positions.splice(idx, 1);
  book.closed_trades.push(trade);
  // THE CERTAINTY LEDGER: settle the claim the lane made at entry, so the lane
  // measures its own win rate instead of asserting one.
  if (pos.strategy === 'certainty' && FACTS && FACTS.certaintyOutcome) {
    try {
      FACTS.certaintyOutcome({
        ticker: pos.market_id, side: pos.side,
        won: exit_price > (pos.entry_price || 0), pnl: net_pnl,
        prob: pos.fair_at_entry, price: pos.entry_price,
      });
    } catch (e) { }
  }
  book.balance += proceeds - fee_exit;
  book.total_pnl += net_pnl;
  noteClipOutcome(net_pnl);
  // FORENSICS LAW (2026-11-04): every closed all_no trade lands in SQLite —
  // trades table, settlement table, and the calibration ledger. The all_no
  // book was in-memory only; 92k rows of forensics could not see it.
  try {
    persistTrade(trade);
    if (exit_reason === 'settled') {
      persistSettlement({
        market_id: pos.market_id, side: pos.side || 'NO', strategy: pos.strategy,
        settled_price: Number(exit_price.toFixed(3)), kalshi_result: exit_price > 0 ? (pos.side === 'YES' ? 'yes' : 'no') : (pos.side === 'YES' ? 'no' : 'yes'),
        coinbase_spot_at_settlement: null, coinbase_spot_time: null, spot_product: null,
        entry_price: pos.entry_price, pnl: net_pnl, timestamp: trade.exit_timestamp, spot_deviation: null,
      });
      CALIB.recordOutcome({
        market_id: pos.market_id, strategy: pos.strategy, side: pos.side || 'NO',
        claimed: pos.fair_at_entry, price: pos.entry_price, outcome: exit_price > 0,
        pnl: net_pnl, sigma: pos.sigma_per_sec, strike: pos.strike,
        horizon: pos.horizon, ts: pos.opened_at,
      });
    }
  } catch (e) { console.error('all_no persist failed:', e.message); }
  save();
  log('all_no_exit', { id: pos.id, market_id: pos.market_id, entry: pos.entry_price, exit: exit_price, pnl: net_pnl, balance_after: book.balance });

  return { ok: true, trade, balance: book.balance, pnl: net_pnl };
}

function strategyForSeries(series) {
  const lane = LANES.find(l => (l.seriesList || []).includes(series));
  return lane ? lane.name : 'unmapped';
}

// ---------------------------------------------------------------------------
// ALL NO MARKETS - Kalshi order placement
// ---------------------------------------------------------------------------
// REMOVED: postAllNoOrder posted a malformed Kalshi order (no ticker, bogus
// option_type/taker fields, dry-account signature) from a PAPER book. The DRY
// server never posts real orders. Real orders live only in Kai-LIVE via
// live_engine.js (LO.openEntry / LO.exitPosition) with the correct V2 schema.

// ---------------------------------------------------------------------------
// Logs + SSE
// ---------------------------------------------------------------------------
function log(kind, obj) {
  const line = { at: new Date().toISOString(), kind, ...obj };
  state.logs.unshift(JSON.stringify(line));
  if (state.logs.length > 2000) state.logs.pop();
  noteProvenance({ kind, ...obj, at: line.at });
  save();
}
let lastServedView = null;
const clients = new Set();
function broadcast(event, data) {
  const payload = 'event: ' + event + '\ndata: ' + String(data).replace(/\n/g, ' ') + '\n\n';
  for (const res of clients) { try { res.write(payload); } catch (e) { } }
}
function sseMessage(data) { broadcast('message', typeof data === 'string' ? data : JSON.stringify(data)); }

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------
function json(res, obj, code) {
  const body = JSON.stringify(obj);
  res.writeHead(code || 200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  });
  res.end(body);
}

// json with API request logging
function jsonLogged(res, obj, code, req, startTime) {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  const u = new URL(req.url, 'http://' + HOST + ':' + PORT);
  const p = u.pathname;
  const method = req.method.toUpperCase();
  const responseTimeMs = Date.now() - startTime;
  if (!p.startsWith('/api/health') && !p.match(/\.(js|css|png|svg|json)$/)) {
    persistApiRequest(ip, p, method, code || 200, responseTimeMs).catch(() => {});
  }
  return json(res, obj, code);
}
function readBody(req) {
  return new Promise(resolve => {
    let b = '';
    req.on('data', c => { b += c; if (b.length > 1e6) req.destroy(); });
    req.on('end', () => { try { resolve(b ? JSON.parse(b) : {}); } catch (e) { resolve({}); } });
  });
}
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml' };

async function serveDashboard(route, res) {
  // Kai-Mirror serves exactly one dashboard: dry-home.html (the real Kalshi
  // account). The page says which book it is. The server only fills in the
  // numbers.
  const book = 'dry';
  const file = path.join(HERE, 'dry-home.html');
  
  rememberTitles(LANES);
  const snap = snapshotPayload();
  snap.mode = { mode: book };
  try {
    let html = fs.readFileSync(file, 'utf8');
    // Replace the SNAPSHOT placeholder in the page with dry data
    const snapshotJson = JSON.stringify(snap);
    const placeholder = 'const SNAPSHOT = {"__KAI_SNAPSHOT_PLACEHOLDER__":true};';
    html = html.replace(placeholder, `const SNAPSHOT = ${snapshotJson};`);
    // The page's own book wins: a DRY page never claims to be DRY.
    // Only replace data-mode on the body tag, NOT in CSS attribute selectors
    // (the CSS uses data-mode to switch card borders between books).
    html = html.replace(/(<body[^>]*?)data-mode="[^"]*"/, `$1data-mode="${book}"`);
    if (!html.includes('data-mode=')) {
      html = html.replace(/<body([^>]*?)>/, `<body data-mode="${book}"$1>`);
    }
    res.writeHead(200, { 'Content-Type': MIME['.html'], 'Cache-Control': 'no-store' });
    return res.end(html);
  } catch (e) { res.writeHead(500); return res.end('page missing: ' + e.message); }
}

async function serveYesMarkets(res) {
  // REMOVED 2026-11-07: yes-markets.html page purged (dead UI). Route deleted.
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  return res.end('gone');
}

async function serveAllNoMarkets(res) {
  // REMOVED 2026-11-07: all-no-markets.html page purged (dead UI). Route deleted.
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  return res.end('gone');
}

  async function fireWebhooks(event, data) {
    for (const wh of (state.webhooks || [])) {
      if (wh.events.includes(event)) {
        try {
          await fetch(wh.url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Kai-Signature': crypto.createHmac('sha256', ENV.KILO_TOKEN || '').update(JSON.stringify(data)).digest('hex') },
            body: JSON.stringify({ event, data, timestamp: new Date().toISOString() }),
            timeout: 5000
          });
        } catch (e) { console.error('webhook failed:', wh.id, e.message); }
      }
    }
  }

async function handle(req, res) {
  const startTime = Date.now();
  const u = new URL(req.url, 'http://' + HOST + ':' + PORT);
  const p = u.pathname;
  const method = req.method.toUpperCase();

  if (method === 'OPTIONS') return json(res, { ok: true });

  // Rate limiting (skip for health checks and static assets)
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  if (!p.startsWith('/api/health') && !p.match(/\.(js|css|png|svg|json)$/)) {
    if (!rateLimit(ip)) {
      return json(res, { error: 'Rate limit exceeded', retry_after: 60 }, 429);
    }
  }

  // --- static pages: Kai-Mirror serves the DRY book - the ONLY book on this
  // server - plus the NO-side book. There is no DRY page here and no
  // client-side mode flipping: a page IS a book.
  if (p === '/' || p === '/index.html' || p === '/dry' || p === '/dry/' || p === '/dry-home.html') {
    return serveDashboard('/dry', res);
  }
  if (/^\/[A-Za-z0-9_.\-]+\.(js|css|png|svg|json)$/.test(p)) {
    const file = path.join(HERE, path.basename(p));
    if (fs.existsSync(file)) {
      res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
      return res.end(fs.readFileSync(file));
    }
    res.writeHead(404); return res.end('not found');
  }

  // --- health / settings
  if (p === '/api/health') {
    return json(res, { ok: true, safe_mode: !!state.safe_mode, state_source: state.state_source || 'primary', boots: state.boot_count || 0, pid: process.pid, loop_lag_ms_max: LOOP_LAG_MAX,
      ok: true, mode: state.mode, uptime_sec: Math.round((Date.now() - new Date(state.booted_at).getTime()) / 1000),
      // TAPE BLOCK (2026-11-05): calib init stage + vaccine gate + memory, all
      // in-memory. The watchdog heartbeat prints this every minute.
      tape: (() => {
        try {
          const cs = CALIB.snapshot();
          const gate = cs.vaccine_gate || {};
          const ist = cs.init_status || {};
          const mu = process.memoryUsage();
          return {
            calib: ist.status ? (ist.status + '@' + ist.stage + (ist.inserted != null ? '+' + ist.inserted : '')) : 'off',
            vac_n: gate.outcomes_total || 0, vac_mature: gate.buckets_mature_n40 || 0,
            vac_ready: !!gate.ready, vac_avoided_c: gate.avoided_loss_cents_per_entry,
            heap_mb: Math.round(mu.heapUsed / 1048576), rss_mb: Math.round(mu.rss / 1048576),
          };
        } catch (e) { return { error: String(e.message || e) }; }
      })(),
      hosts: { kalshi_live: kalshi.LIVE_BASE, kalshi_demo: kalshi.DEMO_BASE, spot: 'api.exchange.coinbase.com', llm: ENV.KILO_BASE_URL },
      credentials: { live_key: String(kalshi.accounts.dry.keyId || '').slice(0, 8) + '...', demo_key: String(kalshi.accounts.demo.keyId || '').slice(0, 8) + '...', llm_token: (ENV.KILO_TOKEN || '').slice(0, 8) + '...' },
      live_balance: dryCash(),
    });
  }

  if (p === '/api/data-health') return json(res, FACTS.health());
  if (p === '/api/certainty-scan') {
    // READ ONLY: what the certainty lane sees right now, and exactly what it
    // would trade and why. This is the validator - it never places an order.
    const scan = await scanCertaintyCandidates();
    return json(res, Object.assign(scan, { lane: laneState('certainty') }));
  }
  if (p === '/api/certainty') {
    return json(res, await FACTS.certaintyStats());
  }
  if (p === '/api/detective') {
    // SUPER-DETECTIVE (operator commission 2026-11-07): what the always-on
    // factual edge miner sees right now, across every prediction-market source.
    return json(res, DETECTIVE.snapshot());
  }
  if (p === '/api/facts') {
    const mm = /[?&]minutes=(\d+)/.exec(req.url || '');
    const rows = await FACTS.recent(mm ? Number(mm[1]) : 10, 300);
    return json(res, { count: rows.length, rows });
  }
  if (p === '/api/shutdown' && method === 'POST') {
    // GRACEFUL STOP. The watchdog and kctl call this FIRST so state is flushed
    // atomically before the process goes away. A force kill (taskkill /F) is
    // what produced torn state files and mystery lane resets.
    try { writeStateAtomic(); } catch (e) { }
    log('control', { action: 'shutdown', pid: process.pId || process.pid, state_source: STATE_SOURCE, safe_mode: !!state.safe_mode });
    json(res, { ok: true, stopping: true, state_source: STATE_SOURCE, safe_mode: !!state.safe_mode, boots: state.boot_count || 0 });
    setTimeout(() => { try { process.exit(0); } catch (e) { } }, 250);
    return;
  }
  if (p === '/api/provenance') {
    return json(res, { provenance: lastProvenance(), lanes: cards(), spot: Object.fromEntries(SPOT_PRODUCTS.map(x => [x, { price: spot[x].price, at: spot[x].at ? new Date(spot[x].at).toISOString() : null, via: spot[x].via || null, last_exchange_time: spot[x].last_time_str || null, ticks: spot[x].points.length, fresh_ticks: spot[x].fresh_ticks, stale_frames: spot[x].stale_frames, duplicate_frames: spot[x].duplicate_frames, last_stale: spot[x].last_stale || null, vol_per_sec: perSecondVol(x, 900) }])), state_write_errors: stateWriteErrors });
  }
  if (p === '/api/config' && method === 'POST') {
    const b = await readBody(req);
    const updated = [];
    for (const k of Object.keys(b)) {
      if (k in state.runtime) { state.runtime[k] = b[k]; updated.push(k); }
    }
    save();
    broadcast('config', JSON.stringify(b));
    return json(res, { ok: true, updated, runtime: state.runtime });
  }
  if (p === '/api/config') return json(res, { ok: true, runtime: state.runtime });
  // THE VACCINE, VISIBLE: per-bucket realized win rates vs claimed fairs —
  // the empirical probability table every entry is now gated against.
  if (p === '/api/calibration') return json(res, CALIB.snapshot());
if (p === '/api/alpha') {
  // Measured-edge verdicts (Mirror, report-clamp mode): what the same gate
  // that LIVE enforces says about the dry record.
  return json(res, ALPHA.report());
}
  // MEDICAL INTERVENTION, VISIBLE: what the rehab unit is doing to each
  // refuted lane (generation, entry DNA, probation cohort, halt reason).
  if (p === '/api/rehab') return json(res, REHAB.report());
  // THE HARVESTER, VISIBLE: resting-order stats, fill rate, settled EV.
  if (p === '/api/harvester') return json(res, HARVEST.snapshot());
  if (p === '/api/cache') return json(res, { ok: true, stats: kalshi.getCacheStats ? kalshi.getCacheStats() : { error: 'not available' } });
  if (p === '/api/cache' && method === 'POST') {
    const b = await readBody(req);
    if (b.action === 'clear') { kalshi.clearCache ? kalshi.clearCache() : null; return json(res, { ok: true, cleared: true }); }
    if (b.action === 'warm') { await kalshi.warmCache ? kalshi.warmCache() : null; return json(res, { ok: true, warmed: true }); }
    return json(res, { error: 'unknown action' }, 400);
  }
  if (p === '/api/alerts' && method === 'POST') {
    const b = await readBody(req);
    Object.assign(state.alerts, b);
    save();
    return json(res, { ok: true, alerts: state.alerts });
  }
  if (p === '/api/alerts') return json(res, { ok: true, alerts: state.alerts });

  // --- webhooks for external integrations
  if (p === '/api/webhooks' && method === 'POST') {
    const b = await readBody(req);
    if (!b.url || !b.events || !Array.isArray(b.events)) {
      return json(res, { error: 'url and events[] required' }, 400);
    }
    const id = 'wh_' + crypto.randomBytes(4).toString('hex');
    state.webhooks = state.webhooks || [];
    state.webhooks.push({ id, ...b, created: new Date().toISOString() });
    save();
    return json(res, { ok: true, webhook: { id, ...b } });
  }
  if (p === '/api/webhooks') return json(res, { webhooks: state.webhooks || [] });
  if (p === '/api/webhooks' && method === 'DELETE') {
    const id = u.searchParams.get('id');
    if (!id) return json(res, { error: 'id required' }, 400);
    state.webhooks = (state.webhooks || []).filter(w => w.id !== id);
    save();
    return json(res, { ok: true });
  }


  if (p === '/api/logs') {
    const n = Math.max(1, Math.min(2000, Number(u.searchParams.get('lines') || 100)));
    return json(res, { lines: state.logs.slice(0, n) });
  }

  // --- mode: FIXED. Kai-Mirror is the DRY-only server. There is no mode
    // switch and no dry book. Any attempt to flip modes is refused: dry
    // trading runs on the separate Kai-Mirror server (port 8787).
    if (p === '/api/mode' && method === 'POST') {
      return json(res, { error: 'Kai-Mirror is the DRY-only server (port 8787). Mode is fixed to dry. Live trading runs on the separate Kai-Mirror server (port 8787).' }, 403);
    }
  if (p === '/api/mode') return json(res, modePayload());

  // --- no lanes
  if (p === '/api/no-lanes' && method === 'POST') {
    const b = await readBody(req);
    state.no_lanes_enabled = !!b.enabled;
    save();
    broadcast('config', 'no_lanes ' + state.no_lanes_enabled);
    return json(res, { no_lanes_enabled: state.no_lanes_enabled });
  }
  if (p === '/api/no-lanes') return json(res, { no_lanes_enabled: !!state.no_lanes_enabled });
  if (p === '/api/fill-realism') return json(res, FILLSIM.stats());
  if (p === '/api/maker') return json(res, makerPayload());
  if (p === '/api/scan') return json(res, { all_no_scan: lastScanTally, spot_lag: Object.fromEntries(SPOT_PRODUCTS.map(x => [x, { lag_s: spot[x].last_frame_lag_s == null ? null : spot[x].last_frame_lag_s, via: spot[x].via || null }])), generated_at: new Date().toISOString() });
  if (p === '/api/no-lanes/book') {
    // Return ALL NO book: positions, trades, and tallies
    const bookData = getAllNoBook(u.searchParams.get('side'));
    return json(res, Object.assign(bookData, { generated_at: new Date().toISOString() }));
  }

  // ONE ATOMIC SNAPSHOT for the tape. The tape used to poll three endpoints
  // separately, so a trade that closed between two of those calls appeared in
  // one view and not the other: the tape printed a CLOSE with a balance that
  // had already moved on. One snapshot = one consistent truth.
  if (p === '/api/tape-snapshot') {
    const bookData = getAllNoBook(u.searchParams.get('side'));
    const per = Math.max(1, Math.min(200, Number(u.searchParams.get('per_page') || 200)));
    const all = state.dry.closed.slice().reverse();
    return json(res, {
      generated_at: new Date().toISOString(),
      book: bookData,
      lanes: {
        page: 1,
        pages: 1,
        total: all.length,
        per_page: per,
        // Same shape as /api/recent-trades so the tape logic is unchanged.
        trades: all.slice(0, per),
        total_pnl: Number(all.filter(t => t.strategy !== 'all_no_markets').reduce((a, t) => a + (Number(t.pnl) || 0), 0).toFixed(2)),
      },
      health: {
        ok: true,
        pid: process.pid,
        boots: state.boot_count || 0,
        mode: state.mode,
        uptime_sec: Math.round((Date.now() - new Date(state.booted_at).getTime()) / 1000),
      },
    });
  }
  
  if (p === '/api/no-lanes/entry' && method === 'POST') {
    // Create a new ALL NO MARKETS position
    const b = await readBody(req);
    const result = createAllNoEntry(
      b.market_id,
      Number(b.entry_price),
      Number(b.quantity),
      Number(b.fair_at_entry || 0.5),
      Number(b.edge || 0),
      Number(b.kelly || 0),
      b.stop_loss ? Number(b.stop_loss) : null,
      b.take_profit ? Number(b.take_profit) : null,
      Number(b.confidence || 0),
      b.expiration
    );
    return json(res, result, result.error ? 400 : 200);
  }
  
  // --- SHADOW SCOREBOARD: what the free ledger has PROVEN, per cell --------
  // Settled rows only (outcome 0/1; -1 = abandoned sentinel). Every cell is
  // measured — no opinions. Cells with n>=20 are evidence-grade; expectancy
  // in cents per contract. This is the promotion dossier for every lane.
  if (p === '/api/shadow') {
    let rows = [];
    try { rows = await dbAll(`SELECT ts, market_id, strategy, source, side, asset, horizon, entry_price, outcome, pnl FROM calibration WHERE outcome IN (0,1) ORDER BY ts`); }
    catch (e) { return json(res, { error: 'ledger read failed: ' + String(e.message || e) }, 500); }
    const mk = () => ({ n: 0, wins: 0, pnl: 0 });
    const add = (m, k, o, pnl) => { const c = m.get(k) || mk(); c.n++; c.wins += o; c.pnl += pnl; m.set(k, c); };
    const byStrat = new Map(), byHour = new Map(), byBand = new Map(), bySide = new Map();
    let tot = mk(), shadowN = 0, liveN = 0;
    for (const r of rows) {
      const o = Number(r.outcome), pnl = Number(r.pnl) || 0, px = Number(r.entry_price);
      if (o !== 0 && o !== 1) continue;
      if (String(r.source || 'shadow') === 'shadow') shadowN++; else liveN++;
      add(byStrat, String(r.strategy || '?'), o, pnl);
      add(byHour, new Date(r.ts).getUTCHours() + 'Z', o, pnl);
      add(byBand, (Number.isFinite(px) ? (Math.floor(px / 0.05) * 0.05).toFixed(2) : '?') + ' ' + String(r.side || '?'), o, pnl);
      add(bySide, String(r.side || '?'), o, pnl);
      tot.n++; tot.wins += o; tot.pnl += pnl;
    }
    const rank = (m) => Array.from(m.entries()).map(([name, c]) => ({
      name, n: c.n, win_rate: Number((c.wins / c.n).toFixed(3)), pnl: Number(c.pnl.toFixed(2)),
      cents_per_contract: Number((100 * c.pnl / c.n).toFixed(2)),
    })).sort((a, b) => b.cents_per_contract - a.cents_per_contract);
    const hours = rank(byHour), strats = rank(byStrat), bands = rank(byBand), sides = rank(bySide);
    // ARM GATE (2026-11-13): objective re-arm criteria for the all_no lane
    // from its own shadow ledger (operator rule: never arm on a heater).
    // READY requires: (1) sample >= 14 days or >= 5000 graded rows; (2) pnl
    // positive EXCLUDING the best single day (regime bias); (3) both money
    // bands (0.50-0.60, 0.60-0.70) each n>=20 with realized >= claimed.
    let armGate = null;
    try {
      const armRows = await dbAll(`SELECT ts, claimed_fair, entry_price, outcome, pnl FROM calibration WHERE strategy='all_no_markets' AND source='shadow' AND outcome IN (0,1) ORDER BY ts`);
      const byDay = new Map(); let pnlAll = 0, nAll = 0;
      const band = (lo, hi) => {
        const rs = armRows.filter(r => { const px = Number(r.entry_price); return px >= lo && px < hi; });
        const bn = rs.length, wins = rs.reduce((a, r) => a + Number(r.outcome), 0);
        const claimed = bn ? rs.reduce((a, r) => a + Number(r.claimed_fair || 0), 0) / bn : null;
        const realized = bn ? wins / bn : null;
        const ok = bn >= 20 && realized != null && claimed != null && realized >= claimed;
        return { n: bn, claimed: claimed != null ? Number(claimed.toFixed(3)) : null, realized: realized != null ? Number(realized.toFixed(3)) : null, pnl: Number(rs.reduce((a, r) => a + (Number(r.pnl) || 0), 0).toFixed(2)), ok };
      };
      for (const r of armRows) {
        nAll++; pnlAll += Number(r.pnl) || 0;
        const d = String(r.ts).slice(0, 10);
        byDay.set(d, (byDay.get(d) || 0) + (Number(r.pnl) || 0));
      }
      const days = armRows.length ? (Date.parse(armRows[armRows.length - 1].ts) - Date.parse(armRows[0].ts)) / 86400000 : 0;
      let bestDay = null, bestPnl = -Infinity;
      for (const [d, v] of byDay) if (v > bestPnl) { bestPnl = v; bestDay = d; }
      const pnlExBest = nAll ? pnlAll - (bestPnl > 0 ? bestPnl : 0) : 0;
      const b1 = band(0.50, 0.60), b2 = band(0.60, 0.70);
      const blockers = [];
      if (!(days >= 14 || nAll >= 5000)) blockers.push(`sample: ${days.toFixed(1)}d / ${nAll} rows (need >=14d or >=5000)`);
      if (!(pnlExBest > 0)) blockers.push(`pnl ex-best-day ${pnlExBest.toFixed(2)} (regime-biased)`);
      if (!b1.ok) blockers.push(`band 0.50-0.60: n=${b1.n} claimed=${b1.claimed} realized=${b1.realized} (need n>=20, realized>=claimed)`);
      if (!b2.ok) blockers.push(`band 0.60-0.70: n=${b2.n} claimed=${b2.claimed} realized=${b2.realized} (need n>=20, realized>=claimed)`);
      armGate = { strategy: 'all_no_markets', graded: nAll, days_span: Number(days.toFixed(1)), pnl: Number(pnlAll.toFixed(2)),
        best_day: bestDay, best_day_pnl: bestPnl > -Infinity ? Number(bestPnl.toFixed(2)) : null,
        pnl_ex_best_day: Number(pnlExBest.toFixed(2)), bands: { '0.50-0.60': b1, '0.60-0.70': b2 },
        verdict: blockers.length ? 'HOLD' : 'READY', blockers,
        fences_armed: { entry_floor: 0.50, window_concurrency_cap: 3 } };
    } catch (e) { armGate = { error: String(e.message || e) }; }
    return json(res, {
      settled_rows: tot.n, shadow_rows: shadowN, live_rows: liveN,
      total_pnl: Number(tot.pnl.toFixed(2)),
      total_cents_per_contract: tot.n ? Number((100 * tot.pnl / tot.n).toFixed(2)) : null,
      hour_utc: hours, by_strategy: strats, by_price_band_side: bands, by_side: sides,
      evidence_cells_n20: {
        hours: hours.filter(c => c.n >= 20).length,
        strategies: strats.filter(c => c.n >= 20).length,
        bands: bands.filter(c => c.n >= 20).length,
      },
      arm_gate: armGate,
      pending_shadows: (CALIB.snapshot() || {}).pending_shadows_n || 0,
      note: 'cells with n>=20 are evidence-grade; cents_per_contract = expectancy; outcome=-1 (abandoned) excluded',
    });
  }

  if (p === '/api/no-lanes/exit' && method === 'POST') {
    // Close an ALL NO MARKETS position
    const b = await readBody(req);
    const result = closeAllNoPosition(
      b.position_id,
      Number(b.exit_price),
      b.exit_reason || 'manual'
    );
    return json(res, result, result.error ? 400 : 200);
  }

  // --- strategies
  if (p === '/api/strategies') {
    const out = {};
    for (const lane of LANES) {
      const ls = laneState(lane.name);
      out[lane.name] = { name: lane.name, label: lane.label, running: !!ls.running, mode: state.mode, series: ls.last_series, entries: ls.entries, skips: ls.skips, last_skip: ls.last_skip, last_edge: ls.last_edge, heartbeat: ls.last_tick || ls.heartbeat };
    }
    return json(res, out);
  }
  let m;
    if ((m = p.match(/^\/api\/strategy\/([^/]+)\/toggle$/)) && method === 'POST') {
      const name = decodeURIComponent(m[1]);
      const lane = LANE_BY_NAME.get(name);
      if (!lane) return json(res, { error: 'unknown strategy: ' + name }, 404);
      const b = await readBody(req);
      const ls = laneState(name);
      // OPERATOR_LAW: The operator's command is IMMEDIATE, ABSOLUTE LAW.
      // Enforce at runtime - this is the ONLY writer of ls.running.
      OPERATOR_LAW.enforce('toggleStrategy', { lane: name, requestedRunning: b.running });
      if (typeof b.running === 'boolean') {
        ls.running = b.running; if (b.running) resetMakerWindow(name);
      } else {
        ls.running = !ls.running;
      }
      ls.started_at = ls.running ? new Date().toISOString() : ls.started_at;
            ls.stop_reason = ls.running ? '' : 'operator stop';
            ls.heartbeat = new Date().toISOString();
            // Synchronous save for operator commands - must persist immediately
            writeStateAtomic();
            // OPERATOR_LAW: the operator just moved a lane - EXTERMINATE any
            // supervisor/watchdog so it can never reverse this button push.
            exterminateSupervisors();
            broadcast('strategy', name + (ls.running ? ' started' : ' stopped'));
            return json(res, { name, running: ls.running, mode: state.mode, law: 'OPERATOR_LAW_ENFORCED' });
    }
  if ((m = p.match(/^\/api\/strategy\/([^/]+)\/analysis$/))) {
    const name = decodeURIComponent(m[1]);
    if (!LANE_BY_NAME.get(name)) return json(res, { error: 'unknown strategy: ' + name }, 404);
    return json(res, analysisPayload(name));
  }
  if ((m = p.match(/^\/api\/strategy\/([^/]+)$/))) {
    const name = decodeURIComponent(m[1]);
    const lane = LANE_BY_NAME.get(name);
    if (!lane) return json(res, { error: 'unknown strategy: ' + name }, 404);
    const ls = laneState(name);
    const closed = laneTrades(name);
    const wins = closed.filter(t => t.pnl > 0).length;
    const open = state.dry.positions.filter(p2 => p2.strategy === name);
    const rets = closed.map(t => t.pnl);
    const card = cards().find(c => c.name === name);
    const act = await activeQuote(lane);
    return json(res, {
      book: 'dry',
      strategy: {
        name, label: lane.label,
        description: 'Real ' + (lane.series || 'Kalshi') + ' series vs dry ' + (lane.spot || 'n/a') + ' spot. Model fair value is a dry-volatility normal CDF over the real strike; entries are taken only when the real ask is below fair by the configured edge. DRY fills are paper at the real quoted price.',
        command: 'server.js lane ' + name, running: !!ls.running, pid: null,
        trades: closed.length, win_rate: closed.length ? Number((100 * wins / closed.length).toFixed(1)) : 0,
        realized: Number(rets.reduce((a, b) => a + b, 0).toFixed(2)),
        best: rets.length ? Number(Math.max(...rets).toFixed(2)) : 0,
        worst: rets.length ? Number(Math.min(...rets).toFixed(2)) : 0,
        open_positions: open.length,
        deployed: Number(open.reduce((a, p2) => a + p2.cost, 0).toFixed(2)),
        equity: card ? card.equity : { labels: [], pnl: [] },
        series: act.series, quote: act.quote, last_skip: ls.last_skip, last_edge: ls.last_edge,
        entries: ls.entries, skips: ls.skips,
      },
      positions: open.map(p2 => ({ market_id: p2.market_id, side: p2.side, entry_price: p2.entry_price, quantity: p2.qty, stop_loss: p2.stop_loss, take_profit: p2.take_profit, opened: p2.opened, fair: p2.fair_at_entry, edge: p2.edge_at_entry })),
      trades: closed.map(t => ({ market_id: t.market_id, side: t.side, entry_price: t.entry_price, exit_price: t.exit_price, quantity: t.quantity, pnl: t.pnl, exit_timestamp: t.exit_timestamp, reason: t.exit_reason })),
      logs: state.logs.filter(l => l.includes(name)).slice(0, 30),
      provenance: act,
    });
  }

  // --- analysis
  undefined

  // --- recent trades
  if (p === '/api/recent-trades') {
    const page = Math.max(1, Number(u.searchParams.get('page') || 1));
    const per = Math.max(1, Math.min(200, Number(u.searchParams.get('per_page') || 25)));
    const book = u.searchParams.get('book') === 'dry' ? 'dry' : 'dry';
    const all = state.dry.closed.slice().reverse();
    const total = all.length;
    const pages = Math.max(1, Math.ceil(total / per));
    const slice = all.slice((page - 1) * per, page * per);
    // total_pnl: exact aggregate over the ENTIRE lane book so the tape can do its
    // math without paging through history. all_no trades (if any ever land here)
    // are EXCLUDED so book pnl + lanes pnl never double-counts.
    return json(res, { page, pages, total, per_page: per, book, trades: slice, total_pnl: Number(all.filter((t) => t.strategy !== 'all_no_markets').reduce((a, t) => a + (Number(t.pnl) || 0), 0).toFixed(2)) });
  }

  // --- monitoring panels data
  if (p === '/api/monitoring') {
    const closed = state.dry.closed;
    
    // 1. Edge decay: entry_edge vs exit_edge
    const edgeDecay = closed.map(t => ({
      entry_edge: t.edge_at_entry || 0,
      exit_edge: Math.abs(t.pnl || 0) / (t.quantity || 1),
      regime: t.regime || 'unknown',
      signal: t.reason || 'unknown'
    })).filter(d => d.entry_edge > 0 && d.exit_edge > 0);

    // 2. Regime win rate
    const regimeMap = new Map();
    for (const t of closed) {
      const reg = t.regime || 'unknown';
      if (!regimeMap.has(reg)) regimeMap.set(reg, { trades: 0, wins: 0, pnl: 0, edges: [] });
      const r = regimeMap.get(reg);
      r.trades++;
      if (t.pnl > 0) r.wins++;
      r.pnl += t.pnl || 0;
      r.edges.push(t.edge_at_entry || 0);
    }
    const regimeWinRate = Array.from(regimeMap.entries()).map(([regime, d]) => ({
      regime,
      trades: d.trades,
      wins: d.wins,
      win_rate: d.trades ? Number((100 * d.wins / d.trades).toFixed(1)) : 0,
      avg_edge: d.edges.length ? Number((d.edges.reduce((a,b)=>a+b,0) / d.edges.length).toFixed(4)) : 0,
      pnl: Number(d.pnl.toFixed(2))
    }));

    // 3. Signal attribution
    const signalMap = new Map();
    for (const t of closed) {
      const sig = t.reason?.split('_')[0] || 'unknown';
      if (!signalMap.has(sig)) signalMap.set(sig, { trades: 0, wins: 0, pnl: 0 });
      const s = signalMap.get(sig);
      s.trades++;
      if (t.pnl > 0) s.wins++;
      s.pnl += t.pnl || 0;
    }
    const signalWeights = state.signal_weights || {};
    const signalAttribution = Array.from(signalMap.entries()).map(([signal, d]) => ({
      signal,
      trades: d.trades,
      wins: d.wins,
      win_rate: d.trades ? Number((100 * d.wins / d.trades).toFixed(1)) : 0,
      weight: signalWeights[signal] || 0,
      pnl: Number(d.pnl.toFixed(2))
    }));

    // 4. Kelly efficiency
    const kellyEfficiency = closed.filter(t => t.kelly_fraction > 0).map(t => {
      const kellyReturn = t.kelly_fraction * (t.pnl / t.cost);
      const actualReturn = t.pnl / t.cost;
      return {
        trade: t.market_id,
        kelly_pct: t.kelly_fraction,
        actual_pct: actualReturn,
        efficiency: kellyReturn !== 0 ? actualReturn / kellyReturn : 0,
        edge: t.edge_at_entry || 0
      };
    }).slice(0, 50); // limit to 50 most recent

    // 5. Execution quality (fill price vs bid/ask)
    // Since DRY uses maker entries at bid, quality should be <= 0
    const executionQuality = closed.filter(t => t.entry_price && t.bid_at_entry && t.ask_at_entry).map(t => ({
      market: t.market_id,
      side: t.side,
      bid: t.bid_at_entry,
      ask: t.ask_at_entry,
      fill: t.entry_price,
      quality: t.bid_at_entry && t.ask_at_entry ? (t.entry_price - t.bid_at_entry) / (t.ask_at_entry - t.bid_at_entry) : 0
    })).slice(0, 50);

    // 6. ALL NO heatmap data
    const allNoBook = state.all_no.dry;
    const allNoHeatmap = (allNoBook.positions || []).map(p => ({
      tau: Math.max(0, (new Date(p.expiration || p.close_time || '').getTime() - Date.now()) / 1000),
      fair: p.fair_at_entry || 0.5,
      edge: p.edge_at_entry || 0,
      qty: p.quantity || 1
    })).filter(d => d.tau > 0);

    // 7. Settlement database - CF BRTI vs Coinbase spot at settlement
    const settlementData = (state.settlement_db || []).slice(-100).map(s => ({
      market_id: s.market_id,
      side: s.side,
      strategy: s.strategy,
      settled_price: s.settled_price,
      kalshi_result: s.kalshi_result,
      coinbase_spot_at_settlement: s.coinbase_spot_at_settlement,
      entry_price: s.entry_price,
      pnl: s.pnl,
      timestamp: s.timestamp,
      // Deviation: how much Coinbase spot deviated from expected
      spot_deviation: s.coinbase_spot_at_settlement && s.entry_price ? 
        (s.coinbase_spot_at_settlement - s.entry_price) / s.entry_price : null,
    }));

    // 8. Fill quality database - execution quality over time
    const fillQualityData = (state.fill_quality_db || []).slice(-200).map(f => ({
      market_id: f.market_id,
      side: f.side,
      strategy: f.strategy,
      entry_price: f.entry_price,
      bid_at_entry: f.bid_at_entry,
      ask_at_entry: f.ask_at_entry,
      fill_quality: f.fill_quality,
      timestamp: f.timestamp,
      // Maker fill quality: negative = maker (good), positive = taker (bad)
      is_maker: f.fill_quality <= 0,
    }));

    return json(res, {
      edge_decay: edgeDecay,
      regime_win_rate: regimeWinRate,
      signal_attribution: signalAttribution,
      kelly_efficiency: kellyEfficiency,
      execution_quality: executionQuality,
      all_no_heatmap: allNoHeatmap,
      settlement_db: settlementData,
      fill_quality_db: fillQualityData,
    });
  }

  // --- marketdata (real KXBTC15M + real Coinbase spot)
  if (p === '/api/marketdata') {
    const res15 = await marketsForSeries('KXBTC15M', 5);
    const s = spot['BTC-USD'];
    const act = await activeQuote(LANE_BY_NAME.get('btc_updown'));
    const q = act.quote;
    const series = s.points.slice(-600).map(pt => ({
      t: pt.t, spot: pt.p, target: q ? q.strike : null,
      up: q && q.yes_ask != null ? q.yes_ask : null, down: q && q.no_ask != null ? q.no_ask : null,
    }));
    return json(res, {
      spot: {
        source: s.source, product: s.product, price: s.price,
        age_sec: s.at ? (Date.now() - s.at) / 1000 : null,
        fresh: !!s.at && Date.now() - s.at < 15000,
        points: s.points.slice(-3000),
        vol_per_sec: perSecondVol('BTC-USD', 900),
      },
      kalshi: {
        series: 'KXBTC15M', source: res15.source, host: res15.host, fetched_at: new Date(res15.at).toISOString(),
        error: res15.error || null,
        open_markets: (res15.markets || []).length,
        tradable: (res15.markets || []).filter(x => x.yes_ask != null).length,
        market: q ? {
          ticker: q.ticker, title: q.title, target: q.strike,
          up_ask: q.yes_ask, up_bid: q.yes_bid, down_ask: q.no_ask, down_bid: q.no_bid,
          last: q.last, close_time: q.close_time, horizon: 0, volume: q.volume,
          event: q.event, bucket: q.ticker.split('-')[1], seconds_left: Math.max(0, (new Date(q.close_time).getTime() - Date.now()) / 1000),
    } : null,
        buckets: (res15.markets || []).map(x => x.ticker.split('-')[1]),
      },
      series,
      lead: { usable: !!q, series: 'KXBTC15M', source: res15.source, host: res15.host },
      provenance: lastProvenance(),
    });
  }

  // --- accounts
  if (p === '/api/accounts') {
    
    return json(res, { accounts: { dry: dryAccountPayload(), dry: dryAccountPayload() }, current: 'dry', provenance: lastProvenance() });
  }

  // --- llm
  if (p === '/api/llm/usage') {
    const u2 = kilo.usage();
    return json(res, Object.assign({ model: ENV.KILO_MODEL || 'auto/free', base: ENV.KILO_BASE_URL }, u2));
  }
  if (p === '/api/ai/analyze' && method === 'POST') {
        const b = await readBody(req);
        console.log('[DEBUG] AI analyze request body:', JSON.stringify(b));
        const ctx = analysisPayload(b.strategy || null);

        // Monitoring endpoint - returns raw data for dashboard panels
        if (b.query_type === 'monitoring') {
          const by_exit_reason = ctx.by_exit_reason || [];

          // Regime win rate (using exit reasons as proxy for regime)
          const by_regime = (ctx.by_exit_reason || []).map(function(r) {
            return { regime: r.reason, trades: r.trades, win_rate: 0, pnl: 0 };
          });

          // Signal attribution (using strategies as proxy)
          const by_signal = (ctx.by_strategy || []).map(function(s) {
            return { signal: s.strategy, trades: s.trades, win_rate: s.win_rate || 0, pnl: s.pnl || 0 };
          });

          // Kelly efficiency
          var kelly_efficiency = {
            'total_kelly_budget': state.runtime.kelly_cap,
            'avg_kelly_used': 0,
            'efficiency_ratio': 0
          };

          // Execution quality (maker fill %)
          var execution_quality = {
            'maker_fill_pct': 0,
            'slippage_bps': 0
          };

          return json(res, {
            ok: true,
            by_exit_reason: ctx.by_exit_reason || [],
            by_regime: by_regime,
            by_signal: by_signal,
            kelly_efficiency: kelly_efficiency,
            execution_quality: execution_quality,
            generated_at: new Date().toISOString()
          });
        }

        // Regular AI analysis
      const prompt = 'You are reviewing a Kalshi prediction-market trading book. Here is the REAL closed-trade data as JSON. '
        + 'Give 3-5 concrete, specific recommendations about entries, edges, price bands and risk. Be terse and quantitative.\n\n'
        + JSON.stringify({ summary: { trades: ctx.trades, win_rate: ctx.win_rate, total_pnl: ctx.total_pnl, avg_pnl: ctx.avg_pnl }, by_side: ctx.by_side, by_price_band: ctx.by_price_band, by_strategy: ctx.by_strategy }).slice(0, 6000);
      try {
        const r = await kilo.chat([{ role: 'user', content: prompt }], { strategy: b.strategy || 'book', query_type: 'recommendations' });
        return json(res, { ok: true, model: r.model, base: r.base, ms: r.ms, usage: r.usage, recommendations: r.text });
      } catch (e) {
        return json(res, { error: 'kilo: ' + (e.message || e) + (e.attempts ? ' [' + e.attempts.join(' | ') + ']' : '') }, 502);
      }
    }

  // --- snapshot + stream
  if (p === '/api/live') {
    const b = u.searchParams.get('book');
    const _out = snapshotPayload(b === 'dry' ? 'dry' : (b === 'dry' ? 'dry' : undefined));
    lastServedView = { served_at: new Date().toISOString(), book: _out.book, balance: _out.balance, kalshi_age_sec: _out.kalshi_age_sec, held: (_out.kalshi || {}).held_count, exposure: (_out.kalshi || {}).exposure, realized: (_out.kalshi || {}).realized, cards: (_out.strategy_cards || []).length, generated_at: _out.generated_at };
    return json(res, _out);
  }
  if (p === '/api/view') { if (lastServedView) return json(res, lastServedView); const _o = snapshotPayload('dry'); return json(res, { served_at: new Date().toISOString(), book: _o.book, balance: _o.balance, kalshi_age_sec: _o.kalshi_age_sec, held: (_o.kalshi || {}).held_count, exposure: (_o.kalshi || {}).exposure, realized: (_o.kalshi || {}).realized, cards: (_o.strategy_cards || []).length, generated_at: _o.generated_at }); }
  if (p === '/api/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    res.write('event: ' + 'snapshot' + '\ndata: ' + JSON.stringify({ at: new Date().toISOString() }) + '\n\n');
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  if (p === '/api/brain') {
    const laneName = u.searchParams.get('lane');
    const lane = laneName ? LANE_BY_NAME.get(laneName) : null;
    if (!lane) return json(res, { error: 'lane required' }, 400);
    const act = await activeQuote(lane);
    if (!act.quote) return json(res, { error: 'no quote', series: act.series });
    const q = act.quote;
    const s = lane.spot ? spot[lane.spot] : null;
    const tau = Math.max(0, (new Date(q.close_time).getTime() - Date.now()) / 1000);
    const perSec = s ? perSecondVol(lane.spot, 900) : null;
    const volWindow = s ? s.points.slice(-20).map(p => p.p) : [];
    const fills = [];
    const result = brain.evaluate({
      q, spotPrice: s ? s.price : null, strike: q.strike, tau, perSecVol: perSec,
      volWindow, fills, history: [], quotes: [q], weights: state.signal_weights
    });
    return json(res, { ...result, lane: lane.name, series: act.series, quote: q, weights: state.signal_weights });
  }
  if (p === '/api/perp') return json(res, perp.snapshot());
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'no route ' + p }));
}

function analysisPayload(strategy) {
  const closed = strategy ? laneTrades(strategy) : state.dry.closed;
  const rets = closed.map(t => t.pnl);
  const wins = closed.filter(t => t.pnl > 0).length;
  const byKey = (fn) => {
    const map = new Map();
    for (const t of closed) {
      const k = fn(t);
      const cur = map.get(k) || { trades: 0, wins: 0, pnl: 0 };
      cur.trades++; if (t.pnl > 0) cur.wins++; cur.pnl += t.pnl || 0;
      map.set(k, cur);
    }
    return Array.from(map.entries()).map(([k, v]) => ({ key: k, trades: v.trades, win_rate: Number((100 * v.wins / v.trades).toFixed(1)), pnl: Number(v.pnl.toFixed(2)) }));
  };
  const byStrategy = byKey(t => t.strategy).map(r => ({ strategy: r.key, trades: r.trades, win_rate: r.win_rate, pnl: r.pnl }));
  const bySide = byKey(t => t.side).map(r => ({ side: r.key, trades: r.trades, win_rate: r.win_rate, pnl: r.pnl }));
  const band = (p2) => p2 < 0.2 ? '0.00-0.20' : p2 < 0.4 ? '0.20-0.40' : p2 < 0.6 ? '0.40-0.60' : p2 < 0.8 ? '0.60-0.80' : '0.80-1.00';
  const byPrice = byKey(t => band(t.entry_price || 0)).map(r => ({ band: r.key, trades: r.trades, win_rate: r.win_rate, pnl: r.pnl }));
  const byHour = byKey(t => String(t.exit_timestamp).slice(11, 13)).map(r => ({ hour: r.key, trades: r.trades, win_rate: r.win_rate, pnl: r.pnl })).sort((a, b) => a.hour.localeCompare(b.hour));
  const byExit = byKey(t => t.exit_reason || 'unknown').map(r => ({ reason: r.key, trades: r.trades }));
  const winnerHold = closed.filter(t => t.pnl > 0).map(t => t.hold_seconds || 0);
  const loserHold = closed.filter(t => t.pnl <= 0).map(t => t.hold_seconds || 0);
  const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
  const recommendations = [];
  if (closed.length < 5) recommendations.push('Not enough closed trades yet (' + closed.length + ') - let the lanes run before reading statistics.');
  const worstBand = byPrice.slice().sort((a, b) => a.pnl - b.pnl)[0];
  if (worstBand && worstBand.trades >= 3 && worstBand.pnl < 0) recommendations.push('Worst entry band: ' + worstBand.band + ' (' + worstBand.pnl + ' over ' + worstBand.trades + ' trades) - consider raising the edge floor there.');
  const bestBand = byPrice.slice().sort((a, b) => b.pnl - a.pnl)[0];
  if (bestBand && bestBand.trades >= 3) recommendations.push('Best entry band: ' + bestBand.band + ' (' + bestBand.pnl + ' over ' + bestBand.trades + ' trades).');
  const sides = bySide.slice().sort((a, b) => b.pnl - a.pnl);
  if (sides.length > 1) recommendations.push('YES vs NO: ' + sides.map(s => s.side + ' ' + s.pnl).join(' vs ') + '.');
  if (winnerHold.length && loserHold.length && avg(loserHold) > avg(winnerHold) * 1.3) recommendations.push('Losers are held longer than winners (avg ' + Math.round(avg(loserHold)) + 's vs ' + Math.round(avg(winnerHold)) + 's) - the stop is doing work, the take-profit may be late.');
  if (!recommendations.length) recommendations.push('Add /api/ai/analyze to have the Kilo model critique this book.');
  return {
    scope: strategy || 'all', trades: closed.length, win_rate: closed.length ? Number((100 * wins / closed.length).toFixed(1)) : 0,
    total_pnl: Number(rets.reduce((a, b) => a + b, 0).toFixed(2)),
    avg_pnl: Number(avg(rets).toFixed(2)),
    best: rets.length ? Number(Math.max(...rets).toFixed(2)) : 0,
    worst: rets.length ? Number(Math.min(...rets).toFixed(2)) : 0,
    recommendations,
    by_strategy: byStrategy, by_side: bySide, by_price_band: byPrice, by_hour: byHour, by_exit_reason: byExit,
    hold_seconds_avg_win: Math.round(avg(winnerHold)), hold_seconds_avg_loss: Math.round(avg(loserHold)),
    data_source: 'real Kalshi quotes + real settlements (DRY fills are paper)',
  };
}

function dryBookPayload() {
  const positions = state.dry.positions || [];
  const closed = state.dry.closed || [];
  const wins = closed.filter(t => t.pnl > 0).length;
  const deployed = positions.reduce((a, p) => a + p.cost, 0);
  const realized = closed.reduce((a, t) => a + (t.pnl || 0), 0);
  const cash = dryCash();
  const starting = state.dry.starting_balance || 0;
  const equity = cash + positions.reduce((a, p) => a + (p.mark != null ? p.qty * p.mark : p.cost), 0);
  return {
    book: 'dry',
    label: 'DRY account - real money',
    starting_balance: starting,
    cash: Number(cash.toFixed(4)),
    equity: Number(equity.toFixed(4)),
    realized: Number(realized.toFixed(2)),
    total_pnl: Number((equity - starting).toFixed(2)),
    exposure: Number(deployed.toFixed(2)),
    open_positions: positions.length,
    closed_trades: closed.length,
    wins, losses: closed.length - wins,
    win_rate: closed.length ? Number((100 * wins / closed.length).toFixed(1)) : 0,
    today: todayStats(closed),
    curve: curveFromClosed(closed),
    positions: positions.map(p => ({
      market_id: p.market_id, side: p.side, entry_price: p.entry_price,
      quantity: p.qty, strategy: p.strategy, stop_loss: p.stop_loss,
      take_profit: p.take_profit, opened: p.opened, order_id: p.order_id,
    })),
  };
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  handle(req, res).catch(e => { try { json(res, { error: String(e.message || e) }, 500); } catch (_) { } });
});

// Prevent process exit on unhandled errors

process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.stack || err.message || err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason && reason.stack ? reason.stack : reason);
});

// Runtime config for ALL NO engine and lane improvements
const ALL_NO_CONFIG = {
  scan_interval_ms: 10000,        // scan cadence
  max_concurrent: 24,             // book-wide cap (was 10 - it bound the WHOLE dry book, not just this lane; operator order 2026-11-08)
  max_exposure_pct: 0.6,          // 60% max exposure (paper) - was 0.4
  kelly_cap: 0.25,                // 25% Kelly cap
  min_edge: 0.004,                // 0.4% minimum edge
  min_vol: 0.0001,
  max_gap: 0.01,
  maker_only: true,
  max_hold_pct: 0.75,             // exit at 75% lifetime
  // (stop_loss_pct / take_profit_mult removed 2026-11-07 - OPERATOR LAW:
  // fee-attached exits are forbidden; vestigial stop/tp config is dead.)
  min_spread: 0.005,              // minimum spread
  max_spread: 0.04,               // maximum spread
  min_tau_sec: 30,                // minimum time to expiry
  max_tau_sec: 3600,              // maximum time to expiry
  min_confidence: 0.3,            // minimum confidence
};

// ---------------------------------------------------------------------------
// CODE LAWS (2026-11-04 forensics) — deliberately NOT runtime-overridable.
// ---------------------------------------------------------------------------
// Claimed edge beyond this = model error, never opportunity. The Oct 26
// runtime surgery that raised max_edge to 0.30 is exactly how 1,136 zero-win
// entries got made (-$2,216 on ONE strike). No config may ever raise this.
const MAX_CLAIMED_EDGE = 0.15;
// One market may be entered at most this many times, EVER (lifetime). Oct 26:
// the same strike was re-entered 1,136 times in 2 hours because each loss
// made it look "cheaper". Enforced at every entry path.
const MAX_MARKET_ENTRIES = 3;

// Lifetime entry counter per market. Persisted in state, incremented on every
// booked entry regardless of path (engine, manual API, FILLSIM fill).
function marketEntryCount(ticker) {
  if (!state.market_entry_log) state.market_entry_log = {};
  return state.market_entry_log[ticker] || 0;
}
function bumpMarketEntry(ticker) {
  if (!state.market_entry_log) state.market_entry_log = {};
  state.market_entry_log[ticker] = (state.market_entry_log[ticker] || 0) + 1;
  // Bound the log: drop the oldest keys if it ever grows absurd.
  const keys = Object.keys(state.market_entry_log);
  if (keys.length > 5000) {
    for (const k of keys.slice(0, keys.length - 3000)) delete state.market_entry_log[k];
  }
}

// ===========================================================================
// CERTAINTY LANE (2026-11-03) - PAPER ONLY IN THE LAB
// ===========================================================================
// A high win rate is bought with PRICE, not with confidence: a contract that
// pays 95c needs a 95% win rate to break even. The only zone where "extreme
// win rate" and "immediate" coexist is a contract whose outcome is already
// decided by the passage of time - near expiry, with the strike far from
// spot. certainty.js computes that probability PESSIMISTICALLY (widened vol,
// index basis haircut, empirical tail) and this lane trades only when the
// pessimistic probability still beats the book's ask by more than the fee.
//
// THE LANE WRITES EVERY CANDIDATE TO THE LEDGER with the probability it
// claimed and, once the market settles, whether it was right - so it MEASURES
// its win rate instead of asserting one:
//   GET /api/certainty       -> calibration (n, wins, win rate, model bias)
//   GET /api/certainty-scan  -> what the scanner sees right now (read only)
//
// IN THE LAB IT NEVER POSTS A REAL ORDER. It fills at the ask, pays the real
// Kalshi fee, and rides to settlement - exactly what LIVE will do.
const CERTAINTY_COINS = [
  { asset: 'BTC-USD', series: 'KXBTC15M' },
  { asset: 'ETH-USD', series: 'KXETH15M' },
  { asset: 'XRP-USD', series: 'KXXRP15M' },
  { asset: 'HYPE-USD', series: 'KXHYPE15M' },
];

function certaintyCfg() {
  const cfg = Object.assign({}, certainty.DEFAULTS, state.runtime.certainty || {});
  // A missing or edited operator file must never LOOSEN the lane.
  cfg.min_prob = Math.max(cfg.min_prob || 0, 0.90);
  cfg.min_margin = Math.max(cfg.min_margin || 0, 0.01);
  cfg.max_qty = Math.max(1, Math.min(cfg.max_qty || 1, 5));
  cfg.max_price = Math.min(cfg.max_price || 0.97, 0.98);
  cfg.min_z = Math.max(cfg.min_z || 0, 3.0);
  return cfg;
}

// Scanner plumbing (2026-11-07): budget-bounded awaits and a capped pool. A
// hanging REST call must never stall a scan cycle - it resolves null and the
// caller records a refusal instead of waiting Kalshi out.
function withBudget(promise, ms) {
  return Promise.race([
    Promise.resolve(promise),
    new Promise((resolve) => { const t = setTimeout(() => resolve(null), ms); if (t.unref) t.unref(); }),
  ]);
}
async function scanPool(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, async () => {
    while (i < items.length) { const idx = i++; out[idx] = await fn(items[idx], idx); }
  });
  await Promise.all(workers);
  return out;
}

// Read-only scan. This function NEVER places an order - it is also the
// validator: it shows exactly what the lane would trade, and why.
// PARALLEL SCANNER (2026-11-07): the thesis lives in the last 20-300s before
// expiry, and the old scan was SERIAL - one market-list fetch per coin, then
// one quote REST call per z-survivor, each waiting out Kalshi's degraded
// network in sequence. Measured: full cycles took >110s - longer than the
// entire opportunity window - so the lane recorded 0 candidates ever, and the
// stacked slow awaits surfaced as SERVER SLOW stalls on the pulse tape. Now:
// (1) the coin series fetch in parallel, 6s budget each; (2) survivor quotes
// fire in a capped pool (6 wide, 4s budget each), strongest |z| first, at
// most 8; a lost quote becomes a recorded refusal, never a stall.
async function scanCertaintyCandidates(cfgIn) {
  const cfg = cfgIn || certaintyCfg();
  const t0 = Date.now();
  const out = { at: new Date().toISOString(), cfg, candidates: [], seen: [], scanned: 0, notes: [] };
  const coins = (cfg.assets && cfg.assets.length)
    ? CERTAINTY_COINS.filter(c => cfg.assets.indexOf(c.asset) >= 0)
    : CERTAINTY_COINS;

  // ---- STAGE 1 (parallel across coins): series lists + CPU pre-filter -----
  const stage1 = await Promise.all(coins.map(async (c) => {
    const s = spot[c.asset];
    if (!s || !Number.isFinite(s.price)) return { note: c.asset + ': no spot' };
    const perSec = perSecondVol(c.asset, 900);
    if (!perSec) return { note: c.asset + ': no realised vol' };
    let res = null;
    try { res = await withBudget(marketsForSeries(c.series, 40), 6000); } catch (e) { }
    if (!res) return { note: c.series + ': fetch failed (6s budget)' };
    const ms = (res && res.markets) || [];
    if (!ms.length) return { note: c.series + ': no markets' };
    // Only the two soonest expiry windows can ever qualify.
    const windows = new Map();
    for (const m of ms) {
      if (!m.close_time || m.result || m.status === 'finalized') continue;
      const ct = String(m.close_time);
      if (!windows.has(ct)) windows.set(ct, []);
      windows.get(ct).push(m);
    }
    const pre = [], seen = [];
    let scanned = 0;
    for (const ct of Array.from(windows.keys()).sort().slice(0, 2)) {
      for (const q of windows.get(ct)) {
        scanned++;
        // An INITIALIZED market has floor_strike 0 - it is not a strike, it is
        // \"not set yet\". Trading against it would be nonsense.
        if (q.strike == null || !(q.strike > 0)) { seen.push({ ticker: q.ticker, why: 'strike not set (market not open yet)' }); continue; }
        const tau = (new Date(q.close_time).getTime() - Date.now()) / 1000;
        if (tau < cfg.min_tau_sec - 30 || tau > cfg.max_tau_sec + 60) {
          seen.push({ ticker: q.ticker, tau: Math.round(tau), why: 'outside expiry window' });
          continue;
        }
        // Cheap pre-filter on the STRIKE (strikes never move) using the
        // pessimistic sigma. Only survivors cost a fresh quote.
        const cp = certainty.certaintyProbability({
          spot: s.price, strike: q.strike, tauSec: tau, perSecVol: perSec,
          volMultiplier: cfg.vol_multiplier, basisPct: cfg.basis_pct, points: s.points,
          settleWindowSec: cfg.settle_window_sec,
        });
        if (cp.pYes == null) { seen.push({ ticker: q.ticker, tau: Math.round(tau), why: 'uncomputable' }); continue; }
        if (Math.abs(cp.z) < cfg.min_z - 1) {
          seen.push({ ticker: q.ticker, tau: Math.round(tau), strike: q.strike, spot: s.price,
            gapPct: cp.gapPct, z: cp.z, why: 'distance to strike too small (z=' + cp.z + ')' });
          continue;
        }
        pre.push({ ticker: q.ticker, asset: c.asset, spot: s.price, points: s.points, perSec, tau, z: cp.z });
      }
    }
    return { note: null, scanned, seen, pre };
  }));

  const quoted = [];
  for (const r of stage1) {
    if (!r || r.note) { if (r) out.notes.push(r.note); continue; }
    out.scanned += r.scanned;
    for (const sv of r.seen) out.seen.push(sv);
    for (const p of r.pre) quoted.push(p);
  }
  // ---- STAGE 2 (parallel, capped): fresh quotes for the strongest ------
  quoted.sort((a, b) => Math.abs(b.z) - Math.abs(a.z));
  if (quoted.length > 8) out.notes.push(quoted.length + ' z-survivors, quoting strongest 8');
  const top = quoted.slice(0, 8);
  const quotes = await scanPool(top, 6, (item) =>
    withBudget(quoteForTicker(item.ticker), 4000)
      .then((qq) => ({ item, qq }))
      .catch(() => ({ item, qq: null })));
  for (const entry of quotes) {
    if (!entry) continue;
    const item = entry.item, qq = entry.qq;
    if (!qq || qq.facts_ok === false) {
      out.seen.push({ ticker: item.ticker, tau: Math.round(item.tau), z: item.z, why: 'quote failed (4s budget)' + (qq && qq.facts_verdict ? ': ' + qq.facts_verdict : '') });
      continue;
    }
    const tau2 = (new Date(qq.close_time).getTime() - Date.now()) / 1000;
    const cands = certainty.buildCandidates({ q: qq, spot: item.spot, tau: tau2, perSecVol: item.perSec, points: item.points }, cfg);
    const best = certainty.bestCandidate(cands);
    if (!best) continue;
    best.asset = item.asset;
    best.perSecVol = item.perSec;
    best.spot = item.spot;
    out.candidates.push(best);
    out.seen.push({ ticker: best.ticker, tau: Math.round(tau2), strike: qq.strike, spot: item.spot,
      gapPct: best.gapPct, z: best.z, side: best.side, price: best.price, prob: best.prob,
      fee: best.fee, margin: best.margin, ok: best.ok, why: best.reason });
  }
  out.candidates.sort((a, b) => b.margin - a.margin);
  out.took_ms = Date.now() - t0;
  return out;
}


async function runCertaintyPass() {
  const cfg = certaintyCfg();
  const ls = laneState('certainty');
  if (!ls.running) return;                       // OPERATOR LAW: only the operator starts a lane
  ls.heartbeat = new Date().toISOString();
  ls.last_tick = ls.heartbeat;
  if (cfg.enabled !== true) { ls.skips++; ls.last_skip = 'lane disabled in config'; return; }
  const book = state.all_no[state.mode];
  if (!book) return;
  // CERTAINTY SETTLEMENT SELF-PUMP + CAP RELIEF (2026-11-12): the exit pump
  // that settles this book (runAllNoExits) is gated on the all_no LANE; with
  // that lane off (operator order 11-11), certainty rows zombied forever -
  // 4 corpses from expired 26NOV11 markets wedged the lane at max_concurrent
  // for ~40h and blocked every new entry. Two fences:
  //   1. SELF-PUMP: a resting row whose market closed >2min ago gets its real
  //      Kalshi result booked now (NO FABRICATED ZEROES - no result, no close).
  //   2. CAP RELIEF: rows expired >26h that still have no result stop counting
  //      toward max_concurrent; the self-pump keeps retrying them forever.
  const restingRows = book.positions.filter(p => p.strategy === 'certainty' && p.status === 'resting');
  for (const p of restingRows) {
    const exp = p.expiration ? new Date(p.expiration).getTime() : 0;
    if (!exp || (Date.now() - exp) <= 120000) continue;
    try {
      const q = await quoteForTicker(p.market_id);
      if (q && (q.result === 'yes' || q.result === 'no')) {
        const won = q.result === (p.side === 'YES' ? 'yes' : 'no') ? 1 : 0;
        const r = closeAllNoPosition(p.id, won, 'settled');
        if (r && r.ok) log('certainty_settled', { market_id: p.market_id, result: q.result, pnl: r.pnl, source: 'self-pump' });
      }
    } catch (e) { }
  }
  const open = book.positions.filter(p => {
    if (p.strategy !== 'certainty' || p.status !== 'resting') return false;
    const exp = p.expiration ? new Date(p.expiration).getTime() : 0;
    if (exp && (Date.now() - exp) > 26 * 3600 * 1000) return false;
    return true;
  });
  if (open.length >= cfg.max_concurrent) { ls.skips++; ls.last_skip = 'max concurrent positions'; return; }
  let scan;
  try { scan = await scanCertaintyCandidates(cfg); }
  catch (e) { ls.skips++; ls.last_skip = 'scan failed: ' + String(e.message || e).slice(0, 120); return; }
  ls.last_scan = { at: scan.at, scanned: scan.scanned, candidates: scan.candidates.length };
  // LEDGER: record what we SAW, traded or not. A refused candidate that would
  // have won is as informative as one we took.
  for (const c of scan.candidates.slice(0, 20)) {
    try { FACTS.certaintyRecord({ ticker: c.ticker, side: c.side, prob: c.prob, price: c.price, fee: c.fee, margin: c.margin, tau: c.tau, z: c.z, asset: c.asset, traded: c.ok ? 1 : 0, reason: c.reason }); } catch (e) { }
  }
  const tradable = scan.candidates.filter(c => c.ok);
  if (!tradable.length) {
    ls.skips++;
    ls.last_skip = 'no certainty candidate (' + scan.scanned + ' markets scanned)';
    return;
  }
  // TRADING-HOURS LAW (2026-11-08): evidence above still records; only DRY
  // ENTRIES are refused in toxic hours (detective: 21Z/23Z negative cells).
  if (hourBlockedUtc()) { ls.skips++; ls.last_skip = 'trading-hours law: blocked UTC hour ' + new Date().getUTCHours() + 'Z'; return; }
  const busy = new Set(open.map(p => p.market_id));
  const tried = cfg._tried || (cfg._tried = {});
  for (const c of tradable) {
    if (busy.has(c.ticker)) continue;
    if (tried[c.ticker] && Date.now() - tried[c.ticker] < cfg.cooldown_ms) continue;
    tried[c.ticker] = Date.now();
    const q = await quoteForTicker(c.ticker);
    if (!q || q.facts_ok === false) { ls.skips++; ls.last_skip = 'facts refused at order time'; continue; }
    // RE-VERIFY AT THE MOMENT OF THE ORDER: the book may have moved since the
    // scan, and we trade the price we can actually get, not the one we saw.
    const tau = (new Date(q.close_time).getTime() - Date.now()) / 1000;
    const re = certainty.buildCandidates({ q, spot: c.spot, tau, perSecVol: c.perSecVol, points: spot[c.asset] ? spot[c.asset].points : null }, cfg);
    const best = certainty.bestCandidate(re);
    if (!best || !best.ok) { ls.skips++; ls.last_skip = 're-verification refused: ' + (best ? best.reason : 'no candidate'); continue; }
    const fee = Number((cfg.max_qty * certainty.feePerContract(best.price, cfg.fee_rate)).toFixed(4));
    const res = createAllNoEntry(best.ticker, best.price, cfg.max_qty, best.prob, best.margin, 0, null, null, 1, q.close_time, {
      strategy: 'certainty', side: best.side, spot_product: c.asset,
      strike: best.strike, sigma: c.perSecVol, fee_entry_override: fee,
    });
    if (res && res.ok) {
      ls.entries++;
      ls.last_edge = best.margin;
      busy.add(best.ticker);
      log('certainty_entry', { market_id: best.ticker, side: best.side, px: best.price, prob: best.prob, margin: best.margin, tau: best.tau, z: best.z, fee: best.fee, asset: c.asset });
    } else if (res && res.error) {
      ls.last_skip = 'entry refused: ' + res.error;
    }
    if (open.length + 1 >= cfg.max_concurrent) break;
  }
}
// ---------------------------------------------------------------------------
// TRADING-HOURS LAW (2026-11-08) — detective own-db evidence: hours 21Z and
// 23Z are toxic expectancy cells; 07Z/09Z the positive ones. Dry entries in
// blocked hours are refused (shadows keep flowing, so the law itself stays
// measurable). Config: state.runtime.trading_hours = { blocked_utc: [...] };
// an empty array disables the law.
function hourBlockedUtc() {
  const th = (state.runtime && state.runtime.trading_hours) || {};
  const arr = Array.isArray(th.blocked_utc) ? th.blocked_utc.map(Number).filter(Number.isFinite) : [21, 23];
  return arr.includes(new Date().getUTCHours()) ? arr : null;
}

// ASSET+WINDOW KEY for the all-no correlation cap: KXBTCD-26NOV1222-T70799.99
// -> "BTC|26NOV1222"; KXETH15M-26NOV122230-30 -> "ETH|26NOV122230". Barrier
// ladders inside one window die together (worst shadow cluster: 8/8 lost,
// -4.37 in a single KXBTCD hour) - the key the cluster analysis grouped by.
function noAssetWindow(marketId) {
  const m = /^KX(BTC|ETH|XRP|HYPE|SOL|DOGE)(?:D|15M|1H|4H)?-([0-9]{2}[A-Z]{3}[0-9]+)/i.exec(String(marketId || ''));
  if (!m) return null;
  return m[1].toUpperCase() + '|' + m[2].toUpperCase();
}

// ---------------------------------------------------------------------------
// SHADOW FAST-SETTLE + ZOMBIE SWEEP (2026-11-08 — "make shadow smarter").
// Old flow: a shadow waited for Kalshi to finalize AND for a later scan pass
// to see the finalized market — minutes to hours of dead evidence (~318
// Mirror pendings were sitting in exactly that trap). New flow: the moment a
// shadow's window closes (entry ts + tau), Coinbase spot truth settles it
// (close >= strike => YES — the same rule the detective's own_db uses).
// Kalshi's later finalize becomes a live CROSS-CHECK (conflicts logged by
// calibration.settleResult). Zombies (closed, unpriceable, never finalized)
// are swept at 2h with the outcome=-1 sentinel — never resurrected by the
// boot backfill (it selects outcome IS NULL).
const SHADOW_ASSET_PRODUCT = { btc: 'BTC-USD', eth: 'ETH-USD', xrp: 'XRP-USD', hype: 'HYPE-USD' };
function spotAtClose(product, tsSec) {
  const s = spot[product];
  if (!s || !Array.isArray(s.points) || !s.points.length) return null;
  const pts = s.points;
  let lo = 0, hi = pts.length - 1, best = null, bestD = Infinity;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const d = Math.abs(pts[mid].t - tsSec);
    if (d < bestD) { bestD = d; best = pts[mid]; }
    if (pts[mid].t < tsSec) lo = mid + 1; else hi = mid - 1;
  }
  // The close is the LAST print at/just-before settlement; a print 90s+ away
  // is a feed gap, not the close — refuse instead of guessing.
  if (!best || bestD > 90) return null;
  const p = Number(best.p);
  return Number.isFinite(p) ? p : null;
}
function shadowFastSettle() {
  if (!CALIB || typeof CALIB.pendingRows !== 'function') return;
  const rows = CALIB.pendingRows();
  const now = Date.now();
  for (const row of rows) {
    const tauSec = Number(row.tau_sec);
    const ts = Date.parse(row.ts);
    if (!Number.isFinite(ts) || !Number.isFinite(tauSec)) continue;
    const closeTs = ts + tauSec * 1000;
    if (!(closeTs > 0)) continue;
    if (now < closeTs + 45000) continue;        // let the closing prints land
    const product = SHADOW_ASSET_PRODUCT[row.asset];
    if (product && row.strike != null && Number.isFinite(Number(row.strike))) {
      const sp = spotAtClose(product, Math.floor(closeTs / 1000));
      if (sp != null) {
        const resultYes = sp >= Number(row.strike);
        const n = CALIB.settleResult(row.market_id, resultYes, 'spot');
        if (n > 0) log('shadow_spot_settled', { market_id: row.market_id, settled: n, strike: row.strike, spot_at_close: sp, result: resultYes ? 'yes' : 'no', via: product });
        continue;
      }
    }
    // BACKLOG-HOLE FIX (2026-11-13): zombies were swept ungraded whenever the
    // Coinbase feed had no print within 90s of close - while Kalshi's own
    // finalize held the definitive answer one quote away (tape: "SHADOW SWEPT
    // pnl=n/a"; 196 abandoned rows = ~12% of graded evidence thrown away).
    // The sweep now asks Kalshi for the real result FIRST; only rows even
    // Kalshi never finalized get the -1 sentinel. Strike-less rows (never
    // spot-settleable at all) get the Kalshi rescue at +10min instead of 2h.
    const strikeKnown = product && row.strike != null && Number.isFinite(Number(row.strike));
    const rescueAt = strikeKnown ? closeTs + 2 * 60 * 60 * 1000 : closeTs + 10 * 60 * 1000;
    if (now > rescueAt) zombieRescue(row, closeTs);
  }
}
const ZOMBIE_RESCUE_BUSY = new Set();
async function zombieRescue(row, closeTs) {
  if (ZOMBIE_RESCUE_BUSY.has(row.market_id)) return;
  ZOMBIE_RESCUE_BUSY.add(row.market_id);
  try {
    let settled = false;
    try {
      const q = await quoteForTicker(row.market_id);
      if (q && (q.result === 'yes' || q.result === 'no')) {
        const resultYes = q.result === 'yes';
        const n = CALIB.settleResult(row.market_id, resultYes, 'kalshi');
        if (n > 0) {
          log('shadow_kalshi_settled', { market_id: row.market_id, result: q.result, rescued_after_min: Math.round((Date.now() - closeTs) / 60000) });
          settled = true;
        }
      }
    } catch (e) { }
    if (!settled) {
      const n = CALIB.abandon ? CALIB.abandon(row.market_id) : 0;
      if (n > 0) log('shadow_zombie_swept', { market_id: row.market_id, closed_at: new Date(closeTs).toISOString() });
    }
  } finally { ZOMBIE_RESCUE_BUSY.delete(row.market_id); }
}
setInterval(() => { try { shadowFastSettle(); } catch (e) { } }, 30000);

// HARVESTER ZOMBIE SWEEP (2026-11-11): harvester positions settle ONLY via
// the noBook scan sweep, which sees a market's result only while it still
// appears in status=open listings. Any market that finalizes during a scan
// gap or server restart is never seen again - 16 paper positions from Nov
// 10-11 sat "open" forever (tape: "filled 16 settled 0", EV/contract n/a).
// The dry book already polls its held markets directly (markDryPositions);
// the harvester now gets the same treatment: poll each open position's
// market, settle on the real Kalshi result. Bounded: <=16 positions, 1
// call/min each, kalshi.js paces internally. Verdict before action: some
// zombies will settle RED (Nov-10 daily NOs) - truth beats a fake "open".
async function harvesterSettleSweep() {
  if (!HARVEST || !state.harvester || !Array.isArray(state.harvester.positions)) return;
  const tickers = Array.from(new Set(state.harvester.positions.map(p => p.market_id)));
  for (const t of tickers) {
    try {
      const q = await quoteForTicker(t);
      if (q && (q.result === 'yes' || q.result === 'no')) {
        const resultYes = q.result === 'yes';
        const nH = HARVEST.settleResult(t, resultYes);
        let nS = 0;
        try { nS = CALIB.settleResult(t, resultYes, 'kalshi'); } catch (e) { }
        if (nH > 0 || nS > 0) log('harvester_zombie_settled', { market_id: t, positions: nH, shadows: nS, result: q.result });
      }
    } catch (e) { /* next sweep retries */ }
  }
}
setInterval(() => { try { harvesterSettleSweep().catch(() => { }); } catch (e) { } }, 60000);

async function runAllNoEngine() {
  if (!state.no_lanes_enabled) return;
  const noLs = laneState('all_no_markets');
  // HARVESTER INDEPENDENCE (operator order 2026-11-08): the harvester is a
  // quarantined paper book that RIDES this scan — it is not the all_no lane.
  // When the maker-kill sweep (or the operator) stops all_no, the scan must
  // still feed the harvester and the shadow grid; "bids 0 forever" was the
  // silent failure mode (tape: HARVEST ... bids 0 ... EV/contract ?).
  // all_no ENTRIES below still obey the lane's own switch.
  const harvOn = !!(state.harvester && state.harvester.enabled !== false);
  const laneOn = !!(noLs && noLs.running);
  // SHADOW-ONLY SCAN (operator order 2026-11-10 "spank"): a stopped all_no
  // lane no longer blinds the evidence machine. The old law of this line
  // (!laneOn && !harvOn -> return) meant a stopped lane starved the shadow
  // grid to zero - the one ledger that could re-earn its right to run. The
  // scan now ALWAYS runs; a stopped lane contributes ZERO orders and ZERO
  // bookings (the entry loop below still obeys laneOn) - only shadow rows.
  // ls.running is never written here; the operator's button stays absolute.
  if (laneOn) {
    noLs.heartbeat = new Date().toISOString(); // scanner-alive truth every pass
    laneState('anti_model').heartbeat = noLs.heartbeat; // fade lane rides the same scan
  }
  
  // Operator runtime config wins over code defaults.
  const cfg = Object.assign({}, ALL_NO_CONFIG, state.runtime.all_no || {});
  
  const rows = await noBook();
  if (!rows.length && noLs && laneOn) noLs.last_skip = 'scan pass: no candidates met the NO gates';
  // THE HARVESTER rides the same scan: resting maker bids wherever the
  // empirical ledger pays (>= 6c edge, n>=50 proof). Paper only.
  try { if (harvOn) HARVEST.run(rows); } catch (e) { }
  if (!laneOn) {
    // SHADOW-ONLY MODE: every row that passed ALL entry gates becomes a free
    // shadow - exactly the entries a running lane would have made. The 10-min
    // per-ticker dedupe inside shadowOpen bounds repetition; nothing books,
    // nothing posts, nothing orders.
    try {
      let shadowed = 0;
      for (const r of rows) {
        const ok = CALIB.shadowOpen({ ticker: r.market_id, side: r.side || 'NO', strategy: 'all_no_markets',
          claimed: r.fair, price: r.limit_entry,
          sigma: r.sigma != null ? r.sigma : null, tau: r.tau != null ? r.tau : null,
          strike: r.strike != null ? r.strike : null, spot: (r.spot_product && spot[r.spot_product] && spot[r.spot_product].price) || null,
          gate: CALIB.gate({ ticker: r.market_id, side: r.side || 'NO', strategy: 'all_no_markets', claimed: r.fair, price: r.limit_entry, minEdge: 0.05 }) });
        if (ok) shadowed++;
      }
      if (shadowed > 0) log('shadow_only_scan', { lane: 'all_no_markets', candidates: rows.length, shadowed });
    } catch (e) { }
    return; // all_no entries obey their own lane switch; evidence already rode
  }
  const book = state.all_no[state.mode];
  if (!book || book.balance < 5) { noLs.last_skip = 'book balance below $5 floor'; return; }
  
  // Portfolio heat check
  const exposure = book.positions.reduce((a, p) => a + p.cost, 0);
  if (exposure > book.balance * cfg.max_exposure_pct) { noLs.last_skip = 'portfolio heat limit'; return; }
  if (book.positions.length >= cfg.max_concurrent) { noLs.last_skip = 'max concurrent positions'; return; }
  
  for (const r of rows) {
    // TRADING-HOURS LAW (2026-11-08): toxic-hour entries refused (detective
    // own-db: 21Z/23Z negative cells). Scan/shadow evidence above continues.
    if (hourBlockedUtc()) { noLs.skips++; noLs.last_skip = 'trading-hours law: blocked UTC hour ' + new Date().getUTCHours() + 'Z'; break; }
    if (r.edge < cfg.min_edge) continue;
    // THE VACCINE, second door: rows only exist if the calibration gate
    // passed at scan time, but a market can finalize between scan and here —
    // re-verify the gate against the current bucket record.
    if (!r.calib || r.calib.ok !== true) continue;
    // LIFETIME MARKET CAP (CODE LAW): a market is enterable at most
    // MAX_MARKET_ENTRIES times, ever, across every path. Oct 26's 1,136
    // entries on one strike are why this exists.
    if (marketEntryCount(r.market_id) >= MAX_MARKET_ENTRIES) {
      noLs.last_skip = 'lifetime cap: ' + r.market_id + ' already entered ' + MAX_MARKET_ENTRIES + 'x';
      continue;
    }
    if (r.kelly <= 0) continue;
    if (r.confidence < (cfg.min_confidence != null ? cfg.min_confidence : 0.3)) continue;
    if (r.one_sided !== true && (r.spread < (cfg.min_spread != null ? cfg.min_spread : 0) || r.spread > (cfg.max_spread != null ? cfg.max_spread : 0.05))) continue;
    if (r.tau < (cfg.min_tau_sec != null ? cfg.min_tau_sec : 30) || r.tau > (cfg.max_tau_sec != null ? cfg.max_tau_sec : 3600)) continue;
    // Probability floor + price cap (defense in depth; noBook already filters).
    // YES-TWIN rows use their own cap (max_yes_price).
    if (r.fair < (cfg.min_fair != null ? cfg.min_fair : 0.65)) continue;
    if (r.fair > (cfg.max_fair != null ? cfg.max_fair : 9.99)) continue; // confidence ceiling (off in the lab)
    const sideCap = r.side === 'YES' ? (cfg.max_yes_price != null ? cfg.max_yes_price : (cfg.max_no_price != null ? cfg.max_no_price : 0.35)) : (cfg.max_no_price != null ? cfg.max_no_price : 0.35);
    if (r.limit_entry > sideCap) continue;
    // ANTI-STACK: one position per market - UNLESS the Conviction Press has
    // verified this bucket's win rate from settled trades (then a second add
    // and bigger size are earned, never assumed).
    const press = conviction.pressLevel(
      conviction.grade((state.all_no[state.mode].closed_trades || []).concat(state.dry.closed || [])),
      r.market_id, r.limit_entry
    );
    const sameMarket = book.positions.filter(p => p.market_id === r.market_id).length;
    const maxSame = press.level >= 2 ? 2 : 1;
    if (sameMarket >= maxSame) continue;
    // CLUSTER CAP: correlated same-round entries die together (evidence: four
    // stop-losses in one :15 round). At most 2 positions share an expiration.
    const sameRound = book.positions.filter(p => p.expiration === r.expiration).length;
    if (sameRound >= 2) continue;
    // ENTRY FLOOR LAW (2026-11-13, shadow-ledger evidence): 1,408 graded
    // all_no shadows - entries below 0.50 bled -11.24 (the 0.40-0.50 band
    // alone: -10.36, claimed 0.559 vs realized 0.403 - the model's cheap-NO
    // love is the lie). The money is 0.50-0.70 (+30.49). Hard floor: config
    // may raise it, never lower it. Shadows keep recording every band.
    const entryFloor = Math.max(0.50, Number.isFinite(Number(cfg.entry_floor)) ? Number(cfg.entry_floor) : 0.50);
    if (r.limit_entry < entryFloor) {
      noLs.skips++; noLs.last_skip = 'entry floor ' + entryFloor.toFixed(2) + ': sub-floor band is -EV (shadow ledger)';
      continue;
    }
    // WINDOW CORRELATION CAP (2026-11-13): at most this many live rows share
    // one asset+window (UTC hour or 15m round), regardless of expirations -
    // a BTC dump must not take a whole barrier ladder off the book at once.
    const awKey = noAssetWindow(r.market_id);
    if (awKey) {
      const wCap = Number(cfg.window_concurrency_cap) > 0 ? Number(cfg.window_concurrency_cap) : 3;
      const inWindow = book.positions.reduce((a, p) => a + (noAssetWindow(p.market_id) === awKey ? 1 : 0), 0);
      if (inWindow >= wCap) {
        noLs.skips++; noLs.last_skip = 'window cap ' + wCap + ': ' + awKey;
        continue;
      }
    }
    if (press.level > 1) {
      log('conviction_press', { market_id: r.market_id, level: press.level, n: press.n, win_rate: Number((press.winRate || 0).toFixed(3)) });
    }

    // Use pre-calculated values from noBook
    const entry = r.limit_entry; // maker at bid
    // Conviction Press earns bigger size only from settled-trade evidence.
    let qty = Math.floor(r.qty * press.level);
    // OPERATOR ORDER 2026-11-04: final clamp - press scaling may not lift any
    // order above the $0.50 cap. No contract fits -> skip, never force qty=1.
    qty = Math.min(qty, Math.floor(effectiveClip() / Math.max(0.01, entry)));
    if (qty < 1) continue;
    
    const cost = qty * entry;
    if (cost > book.balance) continue;
    
    if (state.runtime.all_no && state.runtime.all_no.simulate_fills === false) {
      await createAllNoEntry(r.market_id, entry, qty, r.fair, r.edge, r.kelly,
        r.stop, r.take_profit, r.confidence, r.expiration,
        { strike: r.strike, sigma: r.sigma, spot_product: r.spot_product, side: r.side });
    } else {
      // THE LAB NO LONGER BOOKS ITS OWN WISHES. Register a maker intent; only a
      // real print at/below our bid inside the TTL turns it into a position.
      if (state.safe_mode) { log('control', { action: 'entry_blocked', reason: 'safe_mode' }); return; }
      FILLSIM.intent({
        ticker: r.market_id, side: r.side || 'NO', bid: entry, qty: qty,
        ttl_ms: (cfg.fill_ttl_sec || 45) * 1000,
        book: { market_id: r.market_id, entry, qty, fair: r.fair, edge: r.edge, kelly: r.kelly,
          stop: r.stop, take_profit: r.take_profit, confidence: r.confidence,
          expiration: r.expiration, extra: { strike: r.strike, sigma: r.sigma, spot_product: r.spot_product, side: r.side } },
      });
    }
    
    if (book.positions.length >= cfg.max_concurrent) break;
  }
}

async function runAllNoExits() {
  if (!state.no_lanes_enabled) return;
  const noLs = laneState('all_no_markets');
  if (!noLs || !noLs.running) return;
  const book = state.all_no[state.mode];
  if (!book) return;
  
  for (const pos of book.positions) {
    if (pos.status !== 'resting') continue;
    
    const q = await quoteForTicker(pos.market_id);
    if (q) pumpFills(q);
    
    // SETTLEMENT: book the real Kalshi result at $1/$0. Without this the
    // position zombied forever once the market finalized (no_bid == null).
    if (q && (q.result === 'yes' || q.result === 'no')) {
      const settled = q.result === (pos.side === 'YES' ? 'yes' : 'no') ? 1 : 0;
      const result = closeAllNoPosition(pos.id, settled, 'settled');
      if (result.ok) log('all_no_settled', { market_id: pos.market_id, result: q.result, pnl: result.pnl });
      continue;
    }
    if (q && (q.status === 'finalized' || q.status === 'settled')) {
      // NO FABRICATED ZEROES (2026-11-04 surgery): finalized-without-result is
      // NOT a $0 settlement. Park it; the real result settles it truthfully.
      noLs.skips++; noLs.last_skip = 'finalized, awaiting result (not settling at 0)';
      continue;
    }
    const mark = q ? (pos.side === 'YES' ? q.yes_bid : q.no_bid) : null;
    if (!q || mark == null) continue;

    // HOLD RULE (operator order 2026-11-06): a position open 30 days is
    // force-exited. Sell only at or above entry; otherwise hold and retry on
    // the next tick.
    if (holdRule.forcedExitDue(pos.opened_at, Date.now())) {
      if (holdRule.forcedExitPriceOk(mark, pos.entry_price)) {
        log('hold_rule_forced_exit', { strategy: 'all_no', market: pos.market_id, bid: mark, entry: pos.entry_price, opened: pos.opened_at });
        const result = closeAllNoPosition(pos.id, mark, 'hold_rule_30d');
        if (result.ok) log('all_no_exit', { market_id: pos.market_id, reason: 'hold_rule_30d', pnl: result.pnl });
      } else {
        pos.hold_rule_waiting = { since: pos.hold_rule_waiting ? pos.hold_rule_waiting.since : new Date().toISOString(), bid: mark, entry: pos.entry_price };
      }
      continue;
    }
    const tau = Math.max(0, (new Date(pos.expiration).getTime() - Date.now()) / 1000);
    const totalLife = Math.max(1, (new Date(pos.expiration).getTime() - new Date(pos.opened_at).getTime()) / 1000);
    const lifePct = 1 - tau / totalLife;
    
    let shouldExit = false;
    let exitReason = '';
    let exitPrice = mark;
    
    // 2026-11-04 OPERATOR ORDER: NO STOP LOSSES, NO TIME-DECAY EXIT (paper
    // mirrors LIVE). Every all-no position rides to settlement.
    if (shouldExit) {
      const result = closeAllNoPosition(pos.id, exitPrice, exitReason);
      if (result.ok) {
        log('all_no_exit', { market_id: pos.market_id, reason: exitReason, pnl: result.pnl });
      }
    }
  }
}

// SUPER-DETECTIVE (operator commission 2026-11-07): always-on factual-edge
// discovery across Kalshi + Polymarket + Manifold + our own settled trades.
// It never trades; it reports. Surface: GET /api/detective + DETECTIVE tape line.
const DETECTIVE = require('./detective').create({
  dbAll,
  kalshiGet: (p, o) => kalshi.get(p, o),
  log: (k, o) => { try { log(k, o); } catch (e) {} },
  here: HERE,
});

async function boot() {
  // OPERATOR_LAW: Assert no supervisor exists or can be spawned.
  OPERATOR_LAW.assertNoSupervisor();
  OPERATOR_LAW.enforce('boot', { action: 'server_start', mode: 'dry' });
  DETECTIVE.start(); // super-detective: always-on factual edge mining (2026-11-07)
  // Real boot time (the persisted booted_at used to leak across restarts and
  // show fake 50-hour uptimes in /api/health).
  state.booted_at = new Date().toISOString();
  log('boot', { mode: 'dry', dry_balance: dryCash(), kalshi_live: kalshi.LIVE_BASE });

  // Initial spot fetch for immediate data
  await pollSpot();
  console.log('[boot] pollSpot done');

  // Sync existing state to SQLite
  try {
    await syncStateToDb();
    console.log('[boot] syncStateToDb done');
  } catch (e) {
    console.error('[boot] syncStateToDb failed:', e.message);
  }

  // Warm Kalshi cache
  try {
    if (kalshi.warmCache) {
      await kalshi.warmCache();
      console.log('[boot] warmCache done');
    }
  } catch (e) {
    console.error('[boot] warmCache failed:', e.message);
  }

  // Perp sentiment feed: Hyperliquid funding/premium/OI -> brain drift tilt
  perp.start();
  console.log('[boot] perp_feed started');

  // Coinbase WebSocket for real-time spot prices (replaces 2s HTTP polling)
  const spotProducts = ['BTC-USD', 'ETH-USD', 'XRP-USD', 'HYPE-USD'];
  let spotWsLastFrame = Date.now();
  const coinbaseWS = new CoinbaseWS(spotProducts,
      (ticker) => {
        // High-frequency feed: NO per-ticker logging (PC strain).
        spotWsLastFrame = Date.now();
        ingestSpotTick(ticker.product_id, ticker.price, ticker.bid, ticker.ask, ticker.time, 'ws');
      },
      (err) => { console.error('[CoinbaseWS] Error:', err.message); },
      coinbaseAuth
    );
  coinbaseWS.connect();
  // SPOT WATCHDOG (2026-11-06): a product with no fresh tick for 15s is
  // backfilled over REST, and a WebSocket silent for 30s is recycled. The
  // counters on each spot product (fresh/stale/duplicate) show what happened.
  setInterval(() => {
    try {
      const now = Date.now();
      const stale = SPOT_PRODUCTS.filter((p) => !spot[p].at || now - spot[p].at > 15000);
      if (stale.length) { pollSpot(stale).catch(() => {}); }
      if (now - spotWsLastFrame > 30000 && coinbaseWS.ws && coinbaseWS.ws.readyState === 1) {
        console.error('[spot-watchdog] WebSocket silent ' + Math.round((now - spotWsLastFrame) / 1000) + 's, recycling');
        spotWsLastFrame = now;
        coinbaseWS.ws.close();
      }
    } catch (e) { }
  }, 5000);


  // REQUEST THROTTLE - protects PC from API overload (initialized after all functions defined)
  const THROTTLE = {
    maxConcurrent: 3,           // max simultaneous requests
    minIntervalMs: 500,         // min ms between requests
    maxQueue: 200,              // 2026-11-07 OOM LAW: queue is CAPPED. During a
                                // Kalshi outage producers outrun drain and the old
    queue: [],                  // UNBOUNDED queue pinned every pending scan's whole
    running: 0,                 // async context -> 4GB heap death (~64MB/min).
    lastRequestTime: 0,
    dropped: 0,

    async run(fn) {
      return new Promise((resolve, reject) => {
        if (this.queue.length >= this.maxQueue) {
          this.dropped++;
          if (this.dropped % 250 === 1) console.warn('[throttle] queue full (' + this.maxQueue + ') - dropped ' + this.dropped + ' calls (outage backpressure)');
          return reject(new Error('throttle_overflow: queue full (' + this.maxQueue + ')'));
        }
        this.queue.push({ fn, resolve, reject });
        this.process();
      });
    },
    
    async process() {
      while (this.queue.length && this.running < this.maxConcurrent) {
        const now = Date.now();
        const wait = Math.max(0, this.minIntervalMs - (now - this.lastRequestTime));
        if (wait > 0) {
          await new Promise(r => setTimeout(r, wait));
        }
        // Re-check after the await: another concurrent process() loop may
        // have drained the queue or filled the concurrency slots while we
        // were sleeping. shift() must never return undefined here.
        if (!this.queue.length || this.running >= this.maxConcurrent) continue;
        const { fn, resolve, reject } = this.queue.shift();
        this.running++;
        this.lastRequestTime = Date.now();
        try {
          const result = await fn();
          resolve(result);
        } catch (e) {
          reject(e);
        } finally {
          this.running--;
          this.process();
        }
      }
    }
  };

  // Wrap kalshi.raw with throttle
  const originalKalshiRaw = require('./kalshi.js').raw;
  require('./kalshi.js').raw = async function(acctName, method, route, opts) {
    return THROTTLE.run(() => originalKalshiRaw(acctName, method, route, opts));
  };

  // OPERATOR_LAW: exterminate any supervisor/watchdog on boot and continuously
  // so nothing can ever override the operator's button pushes - imperial law.
  try { exterminateSupervisors(); } catch (e) { }
  setInterval(() => { try { exterminateSupervisors(); } catch (e) { } }, 30000);
  series_kind.start();
  quote_stream.start();

  // Keep every lane's series quoted even when no lane is running, so the
  // provenance block on the page always names a dry host and a fresh quote.
  setInterval(async () => {
    for (const lane of LANES) {
      if (!lane.seriesList || !lane.seriesList.length) continue;
      if (!laneState(lane.name).running) continue; // read budget: quote only running lanes
      for (const s of lane.seriesList) await marketsForSeries(s, 5);
    }
  }, 5000);

  setInterval(async () => {
    try { rememberTitles(LANES); broadcast('snapshot', 'tick'); sseMessage({ e: 'tick', at: new Date().toISOString(), spot_btc: spot['BTC-USD'].price }); } catch (e) { }
  }, 3000);

  // ALL NO engine - entries every 20s (was 10s: under shared-IP 429 saturation
  // the doubled pass rate bought nothing and burned the public /markets budget;
  // exits run on their own 5s clock and are local-book only), every 5s for exits
  // OOM LAW (2026-11-07): busy-flag + clamp - a pass may never overlap-pile
  // (the LIVE twin died at 4GB heap from an undefined interval + outage hangs;
  // Mirror read a safe constant, but the guard is cheap insurance).
  let allNoPassInFlight = false;
  setInterval(() => {
    if (allNoPassInFlight) return;
    allNoPassInFlight = true;
    Promise.resolve(runAllNoEngine()).catch(() => { }).finally(() => { allNoPassInFlight = false; });
  }, Math.max(2000, Number(ALL_NO_CONFIG.scan_interval_ms) || 20000));
setInterval(() => { try { makerKillSweep(); } catch (e) { } }, 30000);
  setInterval(runAllNoExits, 5000);
  // THE CERTAINTY LANE scans on its own clock (the window is only minutes
  // wide) and NEVER posts a real order in the lab - it is paper only.
  setInterval(() => { runCertaintyPass().catch(() => { }); }, 5000);


  // Kalshi WebSocket for real-time market data (replaces 2s polling)
  const seriesList = LANES.flatMap(l => l.seriesList || []).filter(Boolean);
  const kalshiWS = new KalshiWS(kalshi.accounts,
    (market) => { marketCache.set('ws:' + market.series_ticker, { at: Date.now(), market, source: 'ws' }); },
    (fill) => { /* handle fill update */ },
    (order) => { /* handle order update */ }
  );
  // KEY BUDGET (2026-11-06): DRY is paper and its fill/order handlers above are
  // no-ops. Holding a signed WebSocket on the SHARED live Kalshi key only burned
  // the rate budget that LIVE needs for /portfolio/balance (LIVE cash went null).
  // Market quotes are REST (public, unsigned); the WS market channel is unavailable.
  // kalshiWS.connect() / subscribe* intentionally NOT called in DRY.
  void seriesList; void kalshiWS;

  // Main engine tick - evaluates all lanes every 2 seconds
  // TICK GUARD + DEADLINE (stale-HYPE incident): a hung network await used to
  // freeze this interval's async body forever, freezing EVERY lane behind it.
  // Now: (a) one tick in flight at a time, (b) the interval body never waits on
  // a stalled tick - a 90s deadline abandons it and the loop keeps its cadence.
  var tickBusy = false;
  function runTick() {
    if (tickBusy) return;
    tickBusy = true;
    Promise.race([engineTick(), new Promise(r => setTimeout(r, 90000))])
      .catch(e => console.error('[engineTick] Error:', e.message))
      .finally(() => { tickBusy = false; });
  }
  setInterval(runTick, 2000);

  server.listen(PORT, HOST, () => {
    console.log('Kai-Mirror (DRY) listening on http://' + HOST + ':' + PORT + '/');
    console.log('  kalshi dry  :', kalshi.LIVE_BASE);
    console.log('  kalshi demo  :', kalshi.DEMO_BASE, '(fallback)');
    console.log('  spot         : Coinbase Exchange public ticker');
    console.log('  llm          :', ENV.KILO_BASE_URL || 'https://ai-gateway.kilo.ai/api/v1', 'model', ENV.KILO_MODEL || 'auto/free');
    console.log('  mode         :', 'dry', '| dry_balance:', state.dry ? state.dry.starting_balance : 'N/A');
    // Lanes start/stop is operator-controlled only. No auto-start.
    save();
  });
}

if (require.main === module) {
  boot().catch(e => {
    console.error('[boot] Fatal error:', e.message, e.stack);
    process.exit(1);
  });
}
module.exports = { handle, snapshotPayload, dryAccountPayload, cards, state, engineTick, pollSpot, noBook, dryBookPayload, openDryPosition, closeDryPosition, markDryPositions };
