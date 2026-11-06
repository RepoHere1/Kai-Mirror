#!/usr/bin/env node
'use strict';

/*
 * Kalshi settled-market backtester (strategy-agnostic).
 *
 * Strategies: ./strategies/*.js (files starting with "_" are skipped). Each file exports an
 * array of { name, description, decide(ctx), max_entries_per_market? }.
 *   decide(ctx) -> null | { side: 'YES'|'NO', qty: 1, limit_price }
 *   ctx.quotes.<side>_ask is 1 and <side>_bid is 0 when that side has no offer (never null).
 *
 * Data (public, unsigned GETs only; no /portfolio routes):
 *   Kalshi    https://api.elections.kalshi.com/trade-api/v2
 *             GET /markets?status=settled&series_ticker=S&min_close_ts=A&max_close_ts=B&limit=1000[&cursor=C]
 *             GET /markets/candlesticks?market_tickers=T1,..,Tn&start_ts=A&end_ts=B&period_interval=1
 *               (Kalshi limits: n <= 100 and n * window_minutes <= 10000 per request)
 *   Coinbase  https://api.exchange.coinbase.com/products/{BTC,ETH,XRP,HYPE}-USD/candles?granularity=60&start=&end=
 *               (<= 300 rows per request)
 *
 * Replay (per settled market; decision at every minute T from open+60s to close-60s):
 *   spot(T)     close of the 1m Coinbase candle starting at T-60s (known at T, no look-ahead)
 *   sigma       stdev(last 15 one-minute log returns) / sqrt(60)          [per root-second]
 *   sigma_long  same over last 60 returns (regime reference)
 *   tau         max(20, close - T) seconds
 *   fair_yes    'above' (greater*): normCdf(ln(S/K) / (sigma*sqrt(tau)))
 *               'below' (less*):    1 - above
 *               'between' (bins):   P(lo <= S_T <= hi) = P(S_T>=lo) - P(S_T>hi)
 *   quotes(T)   Kalshi 1m candle ENDING at T: yes_bid/yes_ask close; no_ask = 1 - yes_bid,
 *               no_bid = 1 - yes_ask. quote_source='kalshi_candle'.
 *               If no usable candle at T: MODELED quotes fair +/- 0.02 (quote_source='modeled').
 *   fill        taker at the ask of minute T; qty 1; filled only if ask <= limit_price.
 *               Held to settlement (no exits). Maker orders are NOT simulated.
 *   fee         ceil_to_cent(FEE_RATE * qty * p * (1 - p)),  FEE_RATE = 0.07
 *   pnl         payoff (1 if side == result else 0) - price - fee
 *
 * Outputs: backtest_results.json (see runBacktest), backtest_cache/ (raw pages + per-ticker candles).
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const ROOT = __dirname;
const CACHE_DIR = path.join(ROOT, 'backtest_cache');
const CANDLE_DIR = path.join(CACHE_DIR, 'kalshi_candles');
const RESULTS_PATH = path.join(ROOT, 'backtest_results.json');
const STRATEGIES_DIR = path.join(ROOT, 'strategies');

const KALSHI_BASE = 'https://api.elections.kalshi.com/trade-api/v2';
const COINBASE_BASE = 'https://api.exchange.coinbase.com';
const USER_AGENT = 'kai-mirror-backtest';

const FEE_RATE = 0.07;
const MODEL_HALF_SPREAD = 0.02;
const MIN_TAU_SEC = 20;
const SIGMA_SHORT_MIN = 15;
const SIGMA_LONG_MIN = 60;
const DEFAULT_DAYS = 14;
const DEFAULT_MAX_ENTRIES = 1;
const DEFAULT_CARRY_MAX_MIN = 15; // Kalshi emits no candle for quiet minutes; carry last real quote this long
const PROVEN_MIN_TRADES = 50;

const SERIES = ['KXBTC15M', 'KXETH15M', 'KXXRP15M', 'KXHYPE15M', 'KXBTCD', 'KXETHD', 'KXXRPD', 'KXHYPE'];
const PRODUCT_FOR_ASSET = { BTC: 'BTC-USD', ETH: 'ETH-USD', XRP: 'XRP-USD', HYPE: 'HYPE-USD' };

const MIN_INTERVAL_MS = 400; // per host: request starts >= 400ms apart -> at most 2.5 req/s (limit ~3/s)
const CANDLE_CONCURRENCY = 3; // in-flight Kalshi batch requests (responses overlap; starts still spaced)
const MAX_ATTEMPTS = 8;
const BACKOFF_START_MS = 2000;
const BACKOFF_CAP_MS = 60000;
const LISTING_PAGE_LIMIT = 1000;
const MAX_BATCH_TICKERS = 100;
const BATCH_CANDLE_BUDGET = 9000; // Kalshi hard limit is 10000 (tickers x minutes)
const SPOT_LOOKBACK_SEC = 26 * 3600; // markets live up to ~25h; decisions need spot from before open
const SPOT_MAX_FILL_MIN = 3; // forward-fill at most 3 missing 1m Coinbase candles
const CB_CHUNK_MIN = 300;
const LISTING_TTL_MS = 15 * 60 * 1000; // for day-chunks that may still change
const SPOT_TTL_MS = 10 * 60 * 1000;

// ---------------------------------------------------------------- helpers

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const isNum = v => typeof v === 'number' && Number.isFinite(v);
const r = (x, d = 6) => (isNum(x) ? Number(x.toFixed(d)) : null);
const iso = sec => new Date(sec * 1000).toISOString();
const clamp01 = x => Math.min(1, Math.max(0, x));
const cmpStr = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

// Abramowitz-Stegun 7.1.26 -- identical to normCdf in server.js
function normCdf(x) {
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp(-x * x / 2);
  const p = d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x >= 0 ? 1 - p : p;
}

// Kalshi fee: round UP to the next cent
function ceilCent(x) {
  return Math.ceil(x * 100 - 1e-9) / 100;
}
function feeFor(price, qty) {
  return ceilCent(FEE_RATE * qty * price * (1 - price));
}

// Wilson score interval (z = 1.96 -> 95%)
function wilsonInterval(wins, n, z = 1.96) {
  if (!n) return { lo: null, hi: null };
  const p = wins / n;
  const z2 = z * z;
  const denom = 1 + z2 / n;
  const center = (p + z2 / (2 * n)) / denom;
  const half = (z * Math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))) / denom;
  return { lo: center - half, hi: center + half };
}

function assetOf(series) {
  const m = /^KX(BTC|ETH|XRP|HYPE)/.exec(series);
  return m ? m[1] : null;
}

// ---------------------------------------------------------------- HTTP (rate-limited, cached)

const httpStats = {
  requests: { kalshi: 0, coinbase: 0 },
  rate_limited_429: { kalshi: 0, coinbase: 0 },
  server_errors_5xx: { kalshi: 0, coinbase: 0 },
  network_errors: 0,
  retries: 0,
  cache_hits: 0,
  cache_writes: 0,
  failed_requests: 0,
  batch_missing_entries: 0,
};
const lastRequestAt = { kalshi: 0, coinbase: 0 };
const throttleChain = { kalshi: Promise.resolve(), coinbase: Promise.resolve() };

function resetHttpStats() {
  for (const k of Object.keys(httpStats)) {
    if (typeof httpStats[k] === 'number') httpStats[k] = 0;
    else for (const h of Object.keys(httpStats[k])) httpStats[k][h] = 0;
  }
}

// Serialises request START times per host so at most one request starts every MIN_INTERVAL_MS,
// while up to CONCURRENCY requests may be in flight (their responses overlap).
function throttle(host) {
  const slot = throttleChain[host].then(async () => {
    const wait = lastRequestAt[host] + MIN_INTERVAL_MS - Date.now();
    if (wait > 0) await sleep(wait);
    lastRequestAt[host] = Date.now();
  });
  throttleChain[host] = slot.catch(() => {});
  return slot;
}

async function getJson(host, url) {
  let backoff = BACKOFF_START_MS;
  for (let attempt = 1; ; attempt++) {
    await throttle(host);
    httpStats.requests[host]++;
    let res;
    try {
      res = await fetch(url, { headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' } });
    } catch (err) {
      httpStats.network_errors++;
      if (attempt >= MAX_ATTEMPTS) throw new Error(`network error after ${attempt} attempts: ${err.message}`);
      httpStats.retries++;
      await sleep(backoff);
      backoff = Math.min(backoff * 2, BACKOFF_CAP_MS);
      continue;
    }
    if (res.status === 429 || res.status >= 500) {
      if (res.status === 429) httpStats.rate_limited_429[host]++;
      else httpStats.server_errors_5xx[host]++;
      if (attempt >= MAX_ATTEMPTS) throw new Error(`HTTP ${res.status} after ${attempt} attempts: ${url.slice(0, 120)}`);
      httpStats.retries++;
      const retryAfterMs = Number(res.headers.get('retry-after')) * 1000;
      await sleep(Math.max(backoff, Number.isFinite(retryAfterMs) ? Math.min(retryAfterMs, BACKOFF_CAP_MS) : 0));
      backoff = Math.min(backoff * 2, BACKOFF_CAP_MS);
      continue;
    }
    const text = await res.text();
    if (!res.ok) {
      const err = new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
      err.status = res.status;
      throw err;
    }
    return JSON.parse(text);
  }
}

// Cache every GET response (page) as JSON; ttlMs = Infinity means immutable.
async function cachedGet(host, subdir, url, ttlMs) {
  const key = crypto.createHash('sha1').update(url).digest('hex').slice(0, 24);
  const file = path.join(CACHE_DIR, subdir, key + '.json');
  if (fs.existsSync(file) && Date.now() - fs.statSync(file).mtimeMs < ttlMs) {
    try {
      const cached = JSON.parse(fs.readFileSync(file, 'utf8'));
      httpStats.cache_hits++;
      return cached;
    } catch (e) {
      // corrupt cache entry -> refetch
    }
  }
  const json = await getJson(host, url);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(json));
  httpStats.cache_writes++;
  return json;
}

// ---------------------------------------------------------------- Kalshi markets

function normalizeMarket(raw, series) {
  const openTs = Date.parse(raw.open_time) / 1000;
  const closeTs = Date.parse(raw.close_time) / 1000;
  if (!isNum(openTs) || !isNum(closeTs) || closeTs <= openTs) return null;
  const num = v => (v === null || v === undefined || v === '' ? null : Number(v));
  const floor = num(raw.floor_strike);
  const cap = num(raw.cap_strike);
  let kind = null;
  let strike = null;
  let lo = null;
  let hi = null;
  switch (raw.strike_type) {
    case 'greater':
    case 'greater_or_equal':
      kind = 'above';
      strike = floor;
      break;
    case 'less':
    case 'less_or_equal':
      kind = 'below';
      strike = cap !== null ? cap : floor;
      break;
    case 'between':
      kind = 'between';
      lo = floor;
      hi = cap;
      strike = isNum(lo) && isNum(hi) ? (lo + hi) / 2 : null;
      break;
    default:
      break;
  }
  const result = raw.result === 'yes' || raw.result === 'no' ? raw.result : null;
  const usable = result !== null && kind !== null && isNum(strike) && strike > 0 &&
    (kind !== 'between' || (isNum(lo) && isNum(hi) && lo < hi));
  return {
    ticker: raw.ticker,
    series,
    event_ticker: raw.event_ticker || raw.ticker,
    strike_type: raw.strike_type || null,
    kind,
    strike: usable ? strike : null,
    lo: kind === 'between' ? lo : null,
    hi: kind === 'between' ? hi : null,
    open_ts: openTs,
    close_ts: closeTs,
    result,
    usable,
  };
}

// Settled markets for one series, chunked by UTC day so each day can be cached immutably.
async function listSettled(series, windowStart, windowEnd, nowSec) {
  const byTicker = new Map();
  let raw = 0;
  let pages = 0;
  for (let day = Math.floor(windowStart / 86400) * 86400; day < windowEnd; day += 86400) {
    const complete = day + 86400 <= nowSec - 86400;
    let cursor = '';
    do {
      const url = `${KALSHI_BASE}/markets?status=settled&series_ticker=${series}` +
        `&min_close_ts=${day}&max_close_ts=${day + 86399}&limit=${LISTING_PAGE_LIMIT}` +
        (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
      const page = await cachedGet('kalshi', 'kalshi_markets', url, complete ? Infinity : LISTING_TTL_MS);
      pages++;
      for (const m of page.markets || []) {
        raw++;
        const nm = normalizeMarket(m, series);
        if (nm && nm.close_ts > windowStart && nm.close_ts <= windowEnd) byTicker.set(nm.ticker, nm);
      }
      cursor = page.cursor || '';
    } while (cursor);
  }
  return { markets: [...byTicker.values()], raw, pages };
}

// ---------------------------------------------------------------- Kalshi candles

function candleFile(series, ticker) {
  return path.join(CANDLE_DIR, series, ticker.replace(/[^A-Za-z0-9._-]/g, '_') + '.json');
}

// Compact candle: [end_ts, yes_bid_close, yes_ask_close, price_close, volume]
function compactCandle(c) {
  const dol = v => {
    if (v === null || v === undefined || v === '') return NaN;
    return Number(v);
  };
  const close = o => (o && typeof o === 'object' ? (o.close_dollars !== undefined ? o.close_dollars : o.close) : undefined);
  const t = c.end_period_ts;
  if (!isNum(t)) return null;
  return [t, dol(close(c.yes_bid)), dol(close(c.yes_ask)), dol(close(c.price)), dol(c.volume_fp !== undefined ? c.volume_fp : c.volume)];
}

// Ensure every usable market has a cached candle file covering its full life.
async function ensureCandles(series, markets, log) {
  fs.mkdirSync(path.join(CANDLE_DIR, series), { recursive: true });
  // A candle file is only ever written by a batch whose window covers the market's full life, and
  // writes are atomic (tmp + rename), so existence is sufficient (no need to parse every file).
  const todo = [];
  let alreadyCached = 0;
  for (const m of markets) {
    if (fs.existsSync(candleFile(series, m.ticker))) alreadyCached++;
    else todo.push(m);
  }
  todo.sort((a, b) => (a.open_ts - b.open_ts) || cmpStr(a.ticker, b.ticker));

  // greedy time-contiguous groups: <= 100 tickers and tickers x window_minutes <= budget
  const plan = [];
  let i = 0;
  while (i < todo.length) {
    let gs = todo[i].open_ts;
    let ge = todo[i].close_ts;
    const group = [todo[i]];
    let j = i + 1;
    while (j < todo.length && group.length < MAX_BATCH_TICKERS) {
      const ns = Math.min(gs, todo[j].open_ts);
      const ne = Math.max(ge, todo[j].close_ts);
      const minutes = (ne - ns) / 60 + 1;
      if ((group.length + 1) * minutes > BATCH_CANDLE_BUDGET) break;
      gs = ns;
      ge = ne;
      group.push(todo[j]);
      j++;
    }
    plan.push({ group, gs, ge });
    i = j;
  }

  let next = 0;
  let batches = 0;
  let fetched = 0;
  let failed = 0;
  const worker = async () => {
    while (next < plan.length) {
      const p = plan[next++];
      batches++;
      const ok = await fetchCandleBatch(series, p.group, p.gs, p.ge);
      if (ok) fetched += p.group.length;
      else failed += p.group.length;
      if (batches % 100 === 0) log(`  ${series}: candle batches ${batches}/${plan.length}, markets fetched ${fetched}/${todo.length}`);
    }
  };
  await Promise.all(Array.from({ length: Math.min(CANDLE_CONCURRENCY, plan.length) }, worker));
  if (failed > 0) log(`  ${series}: ${failed} markets failed; last error: ${lastBatchError}`);
  return { usable: markets.length, already_cached: alreadyCached, to_fetch: todo.length, fetched, failed, batches };
}

let lastBatchError = null;

async function fetchCandleBatch(series, group, gs, ge) {
  const tickers = group.map(m => encodeURIComponent(m.ticker)).join(',');
  const url = `${KALSHI_BASE}/markets/candlesticks?market_tickers=${tickers}&start_ts=${gs}&end_ts=${ge}&period_interval=1`;
  let json;
  try {
    json = await getJson('kalshi', url);
  } catch (err) {
    httpStats.failed_requests++;
    lastBatchError = `${series} n=${group.length} window=${(ge - gs) / 60}min: ${String(err.message).slice(0, 220)}`;
    return false;
  }
  const got = new Map();
  for (const entry of (json && json.markets) || []) got.set(entry.market_ticker, entry.candlesticks || []);
  if (got.size < group.length) httpStats.batch_missing_entries += group.length - got.size;
  for (const m of group) {
    const candles = (got.get(m.ticker) || []).map(compactCandle).filter(Boolean);
    const file = candleFile(series, m.ticker);
    const tmp = file + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ ticker: m.ticker, series, start: gs, end: ge, candles }));
    fs.renameSync(tmp, file);
  }
  return true;
}

// Candles for one market, sorted ascending by end_period_ts. Empty if no covering cache file.
function readCandles(m) {
  let doc = null;
  try {
    doc = JSON.parse(fs.readFileSync(candleFile(m.series, m.ticker), 'utf8'));
  } catch (e) {
    doc = null;
  }
  if (doc && isNum(doc.start) && isNum(doc.end) && doc.start <= m.open_ts && doc.end >= m.close_ts && Array.isArray(doc.candles)) {
    return doc.candles.slice().sort((a, b) => a[0] - b[0]);
  }
  return [];
}

// ---------------------------------------------------------------- Coinbase spot

async function loadSpot(asset, fromSec, toSec, nowSec) {
  const product = PRODUCT_FOR_ASSET[asset];
  const chunk = CB_CHUNK_MIN * 60;
  const firstChunk = Math.floor(fromSec / chunk) * chunk;
  const lastChunk = Math.floor(toSec / chunk) * chunk;
  const baseMin = firstChunk / 60;
  const n = (lastChunk - firstChunk) / 60 + CB_CHUNK_MIN;
  const close = new Float64Array(n).fill(NaN);
  let pages = 0;
  for (let cs = firstChunk; cs <= lastChunk; cs += chunk) {
    const ce = cs + (CB_CHUNK_MIN - 1) * 60;
    const url = `${COINBASE_BASE}/products/${PRODUCT_FOR_ASSET[asset]}/candles?granularity=60` +
      `&start=${encodeURIComponent(iso(cs))}&end=${encodeURIComponent(iso(ce))}`;
    const complete = cs + chunk <= nowSec - 3600;
    const page = await cachedGet('coinbase', 'coinbase_pages', url, complete ? Infinity : SPOT_TTL_MS);
    if (!Array.isArray(page)) throw new Error(`coinbase ${product}: unexpected response ${JSON.stringify(page).slice(0, 160)}`);
    pages++;
    for (const row of page) {
      const idx = row[0] / 60 - baseMin;
      if (idx >= 0 && idx < n && isNum(row[4])) close[idx] = row[4];
    }
  }
  // forward-fill short gaps (<= SPOT_MAX_FILL_MIN minutes); longer gaps stay NaN
  const ff = new Float64Array(n);
  let last = NaN;
  let run = 0;
  let realMinutes = 0;
  let filledMinutes = 0;
  for (let i = 0; i < n; i++) {
    const c = close[i];
    if (isNum(c)) {
      ff[i] = c;
      last = c;
      run = 0;
      realMinutes++;
    } else {
      run++;
      if (run <= SPOT_MAX_FILL_MIN && isNum(last)) {
        ff[i] = last;
        filledMinutes++;
      } else {
        ff[i] = NaN;
      }
    }
  }
  // log return ending at minute i: lr[i] = ln(ff[i]/ff[i-1])
  const lr = new Float64Array(n).fill(NaN);
  for (let i = 1; i < n; i++) {
    if (isNum(ff[i]) && isNum(ff[i - 1]) && ff[i] > 0 && ff[i - 1] > 0) lr[i] = Math.log(ff[i] / ff[i - 1]);
  }
  return { asset, product, base: baseMin, n, ff, lr, pages, real_minutes: realMinutes, filled_minutes: filledMinutes };
}

// stdev of the `count` log returns ending at index i (sample stdev, two-pass)
function sdReturns(lr, i, count) {
  let sum = 0;
  for (let j = i - count + 1; j <= i; j++) {
    const v = lr[j];
    if (!isNum(v)) return NaN;
    sum += v;
  }
  const mean = sum / count;
  let ss = 0;
  for (let j = i - count + 1; j <= i; j++) ss += (lr[j] - mean) * (lr[j] - mean);
  return Math.sqrt(ss / (count - 1));
}

// ---------------------------------------------------------------- fair value and quotes

// P(S_T >= x) under zero-drift lognormal; s = sigma * sqrt(tau)
function pGE(S, x, s) {
  if (!(s > 0)) return S >= x ? 1 : 0;
  return normCdf(Math.log(S / x) / s);
}

function fairYesOf(m, S, sigma, tau) {
  const s = sigma * Math.sqrt(tau);
  if (m.kind === 'above') return pGE(S, m.strike, s);
  if (m.kind === 'below') return 1 - pGE(S, m.strike, s);
  return clamp01(pGE(S, m.lo, s) - pGE(S, m.hi, s));
}

// signed standardized distance, positive = YES favoured for 'above'/'below'; 'between' uses bin centre
function zOf(m, S, sigma, tau) {
  const s = sigma * Math.sqrt(tau);
  const d = s > 0 ? Math.log(S / m.strike) / s : (S >= m.strike ? 99 : -99);
  return m.kind === 'below' ? -d : d;
}

// Kalshi candle -> quotes. Returns null if neither side has a real two-sided offer at this minute.
function kalshiQuotes(c) {
  const yb = c[1];
  const ya = c[2];
  const bidOk = isNum(yb) && yb > 0 && yb < 1;
  const askOk = isNum(ya) && ya > 0 && ya < 1;
  if (!bidOk && !askOk) return null;
  return {
    yes_bid: bidOk ? yb : 0,
    yes_ask: askOk ? ya : 1,
    no_bid: askOk ? 1 - ya : 0,
    no_ask: bidOk ? 1 - yb : 1,
  };
}

function modeledQuotes(fairYes, fairNo) {
  const c = x => Math.min(0.99, Math.max(0.01, x));
  return {
    yes_bid: c(fairYes - MODEL_HALF_SPREAD),
    yes_ask: c(fairYes + MODEL_HALF_SPREAD),
    no_bid: c(fairNo - MODEL_HALF_SPREAD),
    no_ask: c(fairNo + MODEL_HALF_SPREAD),
  };
}

// ---------------------------------------------------------------- strategies

function loadStrategies() {
  if (!fs.existsSync(STRATEGIES_DIR)) throw new Error(`strategies folder missing: ${STRATEGIES_DIR}`);
  const files = fs.readdirSync(STRATEGIES_DIR).filter(f => f.endsWith('.js') && !f.startsWith('_')).sort();
  const out = [];
  const seen = new Set();
  for (const f of files) {
    const mod = require(path.join(STRATEGIES_DIR, f));
    const list = Array.isArray(mod) ? mod : (mod && Array.isArray(mod.strategies) ? mod.strategies : null);
    if (!list) throw new Error(`strategies/${f} must export an array of { name, description, decide }`);
    for (const s of list) {
      if (!s || typeof s.name !== 'string' || typeof s.decide !== 'function') {
        throw new Error(`strategies/${f}: entry needs string name and decide(ctx) function`);
      }
      if (seen.has(s.name)) throw new Error(`duplicate strategy name: ${s.name}`);
      seen.add(s.name);
      out.push({
        name: s.name,
        description: typeof s.description === 'string' ? s.description : '',
        decide: s.decide,
        max_entries_per_market: s.max_entries_per_market,
        file: f,
      });
    }
  }
  if (!out.length) throw new Error('no strategies found in strategies/');
  return out;
}

function validateDecision(dec, name) {
  if (dec === null || dec === undefined) return null;
  if (typeof dec !== 'object') throw new Error(`strategy ${name}: decide() must return null or an object`);
  if (dec.side !== 'YES' && dec.side !== 'NO') throw new Error(`strategy ${name}: side must be YES or NO, got ${dec.side}`);
  if (dec.qty !== undefined && dec.qty !== 1) throw new Error(`strategy ${name}: only qty 1 is supported`);
  if (!isNum(dec.limit_price) || dec.limit_price <= 0 || dec.limit_price >= 1) {
    throw new Error(`strategy ${name}: limit_price must be in (0,1), got ${dec.limit_price}`);
  }
  return dec;
}

// ---------------------------------------------------------------- statistics

function verdictFor(n, ev, ciLo, avgPrice) {
  if (n >= PROVEN_MIN_TRADES && ev > 0 && ciLo > avgPrice) return 'PROVEN';
  if (n >= PROVEN_MIN_TRADES && ev < 0) return 'LOSING';
  return 'UNPROVEN';
}

// Streaming accumulator. Trades are recorded in settlement order (markets are replayed by close time,
// then ticker), so the running cumulative PnL gives the same max drawdown as sorting the trade list
// by (settle_ts, entry_ts, ticker). Memory is O(1) per accumulator, not O(trades).
function newAcc() {
  return { n: 0, wins: 0, pnl: 0, risked: 0, priceSum: 0, modeled: 0, carried: 0, cum: 0, peak: 0, maxDd: 0 };
}

function accAdd(a, t) {
  a.n++;
  if (t.won) a.wins++;
  a.pnl += t.pnl;
  a.risked += t.price + t.fee;
  a.priceSum += t.price;
  if (t.quote_source === 'modeled') a.modeled++;
  else if (t.quote_age_min > 0) a.carried++;
  a.cum += t.pnl;
  if (a.cum > a.peak) a.peak = a.cum;
  if (a.peak - a.cum > a.maxDd) a.maxDd = a.peak - a.cum;
}

function statsFromAcc(a) {
  const n = a.n;
  const winRate = n ? a.wins / n : null;
  const ev = n ? a.pnl / n : null;
  const avgPrice = n ? a.priceSum / n : null;
  const wi = wilsonInterval(a.wins, n);
  const verdict = verdictFor(n, ev, wi.lo, avgPrice);
  return {
    n_trades: n,
    wins: a.wins,
    win_rate: r(winRate, 4),
    total_pnl: r(a.pnl, 4),
    ev_per_trade: r(ev, 4),
    avg_price: r(avgPrice, 4),
    total_risked: r(a.risked, 4),
    pnl_per_dollar_risked: r(a.risked > 0 ? a.pnl / a.risked : null, 4),
    max_drawdown: r(a.maxDd, 4),
    ci_lo: r(wi.lo, 4),
    ci_hi: r(wi.hi, 4),
    quote_sources: {
      kalshi_candle: n - a.modeled,
      kalshi_exact_minute: n - a.modeled - a.carried,
      kalshi_carried: a.carried,
      modeled: a.modeled,
      kalshi_share: n ? r((n - a.modeled) / n, 4) : null,
      modeled_share: n ? r(a.modeled / n, 4) : null,
    },
    verdict,
  };
}

function newStrategyAcc() {
  const bySeries = {};
  for (const s of SERIES) bySeries[s] = newAcc();
  return {
    overall: newAcc(),
    bySeries,
    bySource: { kalshi_exact: newAcc(), kalshi_carried: newAcc(), modeled: newAcc() },
    markets: 0, // markets with >= 1 entry
    noMarkets: 0, // of those, markets settling NO
  };
}

function recordTrade(sa, t) {
  accAdd(sa.overall, t);
  accAdd(sa.bySeries[t.series], t);
  const src = t.quote_source === 'modeled' ? 'modeled' : (t.quote_age_min > 0 ? 'kalshi_carried' : 'kalshi_exact');
  accAdd(sa.bySource[src], t);
}

// ---------------------------------------------------------------- engine

async function runBacktest(opts = {}) {
  const days = Number(opts.days) > 0 ? Number(opts.days) : DEFAULT_DAYS;
  const defaultMaxEntries = Number.isInteger(opts.maxEntries) && opts.maxEntries > 0 ? opts.maxEntries : DEFAULT_MAX_ENTRIES;
  const carryMaxMin = Number.isFinite(Number(opts.carryMaxMin)) && Number(opts.carryMaxMin) >= 0
    ? Number(opts.carryMaxMin) : DEFAULT_CARRY_MAX_MIN;
  const carryMaxSec = carryMaxMin * 60;
  const log = typeof opts.log === 'function' ? opts.log : () => {};
  const t0 = Date.now();
  resetHttpStats();

  const nowSec = Math.floor(Date.now() / 1000);
  // window end: hour-aligned so cache keys stay stable; opts.endSec pins the window for reproducible re-runs
  const endSec = Number.isFinite(Number(opts.endSec)) && Number(opts.endSec) > 0 ? Number(opts.endSec) : nowSec;
  const windowEnd = Math.floor(endSec / 3600) * 3600;
  const windowStart = windowEnd - Math.round(days * 86400);

  const strategies = loadStrategies();
  log(`strategies: ${strategies.map(s => s.name).join(', ')}`);
  log(`window ${iso(windowStart)} -> ${iso(windowEnd)} (${days} days)`);
  const timings = {};
  let phaseStart = Date.now();
  const phaseDone = name => {
    timings[name] = r((Date.now() - phaseStart) / 1000, 1);
    phaseStart = Date.now();
  };

  // 1. settled markets per series
  const perSeries = {};
  const allMarkets = [];
  for (const series of SERIES) {
    const lst = await listSettled(series, windowStart, windowEnd, nowSec);
    const usable = lst.markets.filter(m => m.usable).sort((a, b) => (a.close_ts - b.close_ts) || cmpStr(a.ticker, b.ticker));
    perSeries[series] = {
      settled_listed_raw: lst.raw,
      listed_in_window: lst.markets.length,
      usable: usable.length,
      outcome_yes: usable.filter(m => m.result === 'yes').length,
      outcome_no: usable.filter(m => m.result === 'no').length,
      markets_with_kalshi_quotes: 0,
      candle_fetch: null,
      listing_pages: lst.pages,
    };
    allMarkets.push(...usable);
    log(`${series}: settled listed=${lst.raw} usable=${usable.length} pages=${lst.pages}`);
  }
  // global settlement order: replay (and therefore drawdown accumulation) runs in (close_ts, ticker) order
  allMarkets.sort((a, b) => (a.close_ts - b.close_ts) || cmpStr(a.ticker, b.ticker));
  phaseDone('listing');

  // 2. Kalshi candles (batched, cached per ticker)
  for (const series of SERIES) {
    const ms = allMarkets.filter(m => m.series === series);
    perSeries[series].candle_fetch = await ensureCandles(series, ms, log);
    const cf = perSeries[series].candle_fetch;
    log(`${series}: candles cached=${cf.already_cached} fetched=${cf.fetched} failed=${cf.failed} batches=${cf.batches}`);
  }
  phaseDone('kalshi_candles');

  // 3. Coinbase 1m spot per asset
  const assets = {};
  for (const series of SERIES) {
    const a = assetOf(series);
    if (a && !assets[a]) {
      assets[a] = await loadSpot(a, windowStart - SPOT_LOOKBACK_SEC - 300, windowEnd, nowSec);
      log(`${a}: coinbase pages=${assets[a].pages} real 1m=${assets[a].real_minutes} forward-filled=${assets[a].filled_minutes}`);
    }
  }
  phaseDone('coinbase_spot');

  // 4. ladder groups (markets sharing an event)
  const groups = new Map();
  for (const m of allMarkets) {
    m.group_key = `${m.series}|${m.event_ticker}`;
    if (!groups.has(m.group_key)) groups.set(m.group_key, []);
    groups.get(m.group_key).push(m);
  }
  const ladderCaches = new Map(); // group_key -> Map(T -> nearest-strike result), released after last sibling
  const ladderLeft = new Map(); // group_key -> siblings not yet replayed

  // 5. minute-by-minute replay
  const nStrat = strategies.length;
  const perStrat = strategies.map(s => ({
    s,
    maxEntries: Number.isInteger(s.max_entries_per_market) && s.max_entries_per_market > 0 ? s.max_entries_per_market : defaultMaxEntries,
    acc: newStrategyAcc(),
  }));
  const minutes = { kalshi_exact: 0, kalshi_carried: 0, modeled: 0 };
  const skipped = { spot_missing: 0 };
  let candlePointsLoaded = 0;
  let marketsReplayed = 0;
  let marketsWithExact = 0;

  for (const m of allMarkets) {
    const sa = assets[assetOf(m.series)];
    const candles = readCandles(m);
    candlePointsLoaded += candles.length;
    let cp = 0; // pointer: candles[0..cp) have end_ts <= T
    let last = null; // most recent candle with end_ts <= T
    const grp = groups.get(m.group_key);
    const sibs = grp && grp.length > 1 ? grp : null;
    if (sibs && !ladderLeft.has(m.group_key)) ladderLeft.set(m.group_key, sibs.length);
    const openIso = iso(m.open_ts);
    const closeIso = iso(m.close_ts);
    const entries = new Int32Array(nStrat);
    let kalshiMinutes = 0;
    marketsReplayed++;

    const firstT = (Math.floor(m.open_ts / 60) + 1) * 60;
    for (let T = firstT; T <= m.close_ts - 60; T += 60) {
      while (cp < candles.length && candles[cp][0] <= T) last = candles[cp++];
      const i = T / 60 - 1 - sa.base; // index of the 1m candle starting at T-60s (spot as of T)
      if (i < SIGMA_LONG_MIN || i >= sa.n) {
        skipped.spot_missing++;
        continue;
      }
      const S = sa.ff[i];
      const sd15 = sdReturns(sa.lr, i, SIGMA_SHORT_MIN);
      const sd60 = sdReturns(sa.lr, i, SIGMA_LONG_MIN);
      const move5 = isNum(sa.ff[i - 5]) && isNum(S) ? Math.log(S / sa.ff[i - 5]) : NaN;
      if (!isNum(S) || !isNum(sd15) || !isNum(sd60) || !isNum(move5)) {
        skipped.spot_missing++;
        continue;
      }
      const sigma = sd15 / Math.sqrt(60);
      const sigmaLong = sd60 / Math.sqrt(60);
      const tau = Math.max(MIN_TAU_SEC, m.close_ts - T);
      const fairYes = fairYesOf(m, S, sigma, tau);
      const fairNo = 1 - fairYes;

      // Kalshi quote: the latest candle at or before T (exact if it ends at T; carried if older
      // but within carryMaxSec). Candles with no two-sided/one-sided offer fall through to modeled.
      let quotes = null;
      let quoteAgeMin = 0;
      if (last !== null && T - last[0] <= carryMaxSec) {
        quotes = kalshiQuotes(last);
        quoteAgeMin = (T - last[0]) / 60;
      }
      let quoteSource;
      if (quotes) {
        quoteSource = 'kalshi_candle';
        if (quoteAgeMin === 0) {
          minutes.kalshi_exact++;
          kalshiMinutes++;
        } else {
          minutes.kalshi_carried++;
        }
      } else {
        quotes = modeledQuotes(fairYes, fairNo);
        quoteSource = 'modeled';
        quoteAgeMin = null;
        minutes.modeled++;
      }

      let ladderSize = 1;
      let nearestStrike = m.strike;
      let isNearest = true;
      if (sibs) {
        // nearest-to-spot strike among the event's live markets at minute T (cached per event-minute;
        // all siblings share the same spot, so the result is identical for each sibling)
        let ladderCache = ladderCaches.get(m.group_key);
        if (!ladderCache) {
          ladderCache = new Map();
          ladderCaches.set(m.group_key, ladderCache);
        }
        let hit = ladderCache.get(T);
        if (!hit) {
          let best = null;
          let bestD = Infinity;
          let alive = 0;
          for (const x of sibs) {
            if (x.open_ts > T || x.close_ts <= T) continue;
            alive++;
            const d = Math.abs(S - x.strike);
            if (d < bestD || (d === bestD && best && cmpStr(x.ticker, best.ticker) < 0)) {
              best = x;
              bestD = d;
            }
          }
          hit = { alive, nearestStrike: best ? best.strike : null, nearestTicker: best ? best.ticker : null };
          ladderCache.set(T, hit);
        }
        ladderSize = hit.alive;
        nearestStrike = hit.nearestStrike;
        isNearest = hit.nearestTicker === m.ticker;
      }

      const ctx = {
        series: m.series,
        asset: assetOf(m.series),
        ticker: m.ticker,
        event_ticker: m.event_ticker,
        strike_type: m.strike_type,
        kind: m.kind,
        yes_direction: m.kind === 'above' ? 1 : m.kind === 'below' ? -1 : 0,
        strike: m.strike,
        strike_lo: m.lo,
        strike_hi: m.hi,
        open_time: openIso,
        close_time: closeIso,
        ts: T,
        minute_index: (T - m.open_ts) / 60,
        tau_sec: tau,
        spot: S,
        sigma_per_sec: sigma,
        sigma_per_sec_60m: sigmaLong,
        sigma_5m: sigma * Math.sqrt(300),
        spot_move_5m: move5,
        z: zOf(m, S, sigma, tau),
        fair_yes: fairYes,
        fair_no: fairNo,
        quotes,
        quote_source: quoteSource,
        quote_age_min: quoteAgeMin,
        spot_history_1m: sa.ff.subarray(i - SIGMA_LONG_MIN + 1, i + 1),
        ladder_size: ladderSize,
        nearest_strike: nearestStrike,
        is_nearest_strike: isNearest,
        close_ts: m.close_ts,
      };

      for (let k = 0; k < nStrat; k++) {
        const ps = perStrat[k];
        if (entries[k] >= ps.maxEntries) continue;
        const dec = validateDecision(ps.s.decide(ctx), ps.s.name);
        if (!dec) continue;
        const ask = dec.side === 'YES' ? quotes.yes_ask : quotes.no_ask;
        if (!(ask > 0 && ask < 1)) continue; // no offer on that side
        if (ask > dec.limit_price + 1e-12) continue; // taker fill only within limit
        const price = ask;
        const fee = feeFor(price, 1);
        const won = dec.side.toLowerCase() === m.result; // m.result is 'yes'|'no'
        const payoff = won ? 1 : 0;
        recordTrade(ps.acc, {
          series: m.series,
          price,
          fee,
          pnl: payoff - price - fee,
          won,
          quote_source: quoteSource,
          quote_age_min: quoteAgeMin,
        });
        entries[k]++;
      }
    }
    // per-market bookkeeping (after all minutes): markets traded, NO settlements among them
    for (let k = 0; k < nStrat; k++) {
      if (entries[k] > 0) {
        perStrat[k].acc.markets++;
        if (m.result === 'no') perStrat[k].acc.noMarkets++;
      }
    }
    if (sibs) {
      const left = ladderLeft.get(m.group_key) - 1;
      if (left <= 0) {
        ladderLeft.delete(m.group_key);
        ladderCaches.delete(m.group_key);
      } else {
        ladderLeft.set(m.group_key, left);
      }
    }
    if (kalshiMinutes > 0) {
      marketsWithExact++;
      perSeries[m.series].markets_with_kalshi_quotes++;
    }
  }

  phaseDone('replay');

  // 6. statistics
  const seriesList = SERIES.filter(s => perSeries[s].usable > 0);
  const strategyResults = perStrat.map(ps => {
    const a = ps.acc;
    const bySeries = {};
    for (const s of SERIES) bySeries[s] = statsFromAcc(a.bySeries[s]);
    return {
      name: ps.s.name,
      description: ps.s.description,
      file: ps.s.file,
      max_entries_per_market: ps.maxEntries,
      markets_traded: a.markets,
      ...statsFromAcc(a.overall),
      by_series: bySeries,
      by_quote_source: {
        kalshi_exact: statsFromAcc(a.bySource.kalshi_exact),
        kalshi_carried: statsFromAcc(a.bySource.kalshi_carried),
        modeled: statsFromAcc(a.bySource.modeled),
      },
    };
  });

  // sanity block: always-NO strategy vs settled NO rate
  const sanityStrat = strategyResults.find(s => s.name === 'sanity_always_no') || null;
  let sanity = null;
  if (sanityStrat) {
    const sacc = perStrat.find(p => p.s.name === 'sanity_always_no').acc;
    const traded = sacc.markets;
    const noTraded = sacc.noMarkets;
    const noAll = allMarkets.filter(m => m.result === 'no').length;
    sanity = {
      strategy: 'sanity_always_no',
      n_trades: sanityStrat.n_trades,
      win_rate: sanityStrat.win_rate,
      markets_traded: traded,
      no_rate_of_traded_markets: traded ? r(noTraded / traded, 4) : null,
      no_rate_all_usable_markets: allMarkets.length ? r(noAll / allMarkets.length, 4) : null,
      match: sanityStrat.n_trades === traded && traded > 0 &&
        Math.abs(sanityStrat.win_rate - noTraded / traded) < 5e-5, // win_rate is rounded to 4dp
    };
  }

  const kalshiDecisionMinutes = minutes.kalshi_exact + minutes.kalshi_carried;
  const totalMinutes = kalshiDecisionMinutes + minutes.modeled;
  const outcomeYes = allMarkets.filter(m => m.result === 'yes').length;
  const outcomeNo = allMarkets.filter(m => m.result === 'no').length;
  let coinbaseReal = 0;
  let coinbaseFilled = 0;
  const coinbaseByAsset = {};
  for (const a of Object.keys(assets)) {
    coinbaseReal += assets[a].real_minutes;
    coinbaseFilled += assets[a].filled_minutes;
    coinbaseByAsset[a] = assets[a].real_minutes;
  }

  const results = {
    generated_at: new Date().toISOString(),
    window: { from: iso(windowStart), to: iso(windowEnd), days },
    data: {
      markets: marketsReplayed,
      markets_with_kalshi_quotes: marketsWithExact,
      markets_with_kalshi_quotes_def: 'markets with >=1 exact-minute Kalshi candle quote used in replay',
      markets_by_series: perSeries,
      candles: {
        kalshi_1m_points_loaded: candlePointsLoaded,
        coinbase_1m_real: coinbaseReal,
        coinbase_1m_real_by_asset: coinbaseByAsset,
        coinbase_1m_forward_filled: coinbaseFilled,
        coinbase_pages: Object.values(assets).reduce((a, x) => a + x.pages, 0),
      },
      quote_sources: {
        kalshi_candle: {
          decision_minutes: kalshiDecisionMinutes,
          share_of_decisions: totalMinutes ? r(kalshiDecisionMinutes / totalMinutes, 4) : null,
          exact_minute: minutes.kalshi_exact,
          carried_from_last_candle: minutes.kalshi_carried,
        },
        modeled: {
          decision_minutes: minutes.modeled,
          share_of_decisions: totalMinutes ? r(minutes.modeled / totalMinutes, 4) : null,
        },
      },
      skipped_minutes: skipped,
      outcomes: { yes: outcomeYes, no: outcomeNo, no_share: allMarkets.length ? r(outcomeNo / allMarkets.length, 4) : null },
      series_with_markets: seriesList,
      http: JSON.parse(JSON.stringify(httpStats)),
      timings_sec: timings,
      runtime_sec: r((Date.now() - t0) / 1000, 1),
    },
    fee_rate: FEE_RATE,
    quote_model: {
      kalshi_candle: `yes_bid/yes_ask close of the latest 1m candle ending at or before the decision minute, if no older than ${carryMaxMin} min (quote_age_min recorded; 0 = exact minute). no_ask = 1 - yes_bid, no_bid = 1 - yes_ask`,
      modeled: `fair_yes/fair_no +/- ${MODEL_HALF_SPREAD} (half-spread), clamped to [0.01, 0.99]; used only when no Kalshi candle at that minute`,
      fill: 'taker at the ask of the decision minute; qty 1; filled only if ask <= limit_price; held to settlement',
    },
    engine: {
      carry_max_min: carryMaxMin,
      decision_grid: 'every minute T from open+60s to close-60s',
      spot: 'close of the 1m Coinbase candle starting at T-60s',
      sigma: 'stdev(last 15 one-minute log returns)/sqrt(60); sigma_long over last 60 returns',
      tau_floor_sec: MIN_TAU_SEC,
      verdict_rule: `PROVEN: n>=${PROVEN_MIN_TRADES} && ev>0 && wilson_lo>avg_price; LOSING: n>=${PROVEN_MIN_TRADES} && ev<0; else UNPROVEN`,
      pnl_definition: 'payoff - price - fee; total_risked = sum(price + fee)',
      max_drawdown: 'peak-to-trough of cumulative pnl ordered by settlement time (positive number)',
    },
    sanity,
    strategies: strategyResults,
  };

  fs.writeFileSync(RESULTS_PATH, JSON.stringify(results, null, 2));
  log(`wrote ${RESULTS_PATH} in ${results.data.runtime_sec}s`);
  return results;
}

function loadResults() {
  try {
    return JSON.parse(fs.readFileSync(RESULTS_PATH, 'utf8'));
  } catch (e) {
    return null;
  }
}

function summary() {
  const res = loadResults();
  if (!res) return { generated_at: null, age_min: null, markets: 0, strategies: [], proven: [], losing: [] };
  const ageMin = Math.round((Date.now() - Date.parse(res.generated_at)) / 6000) / 10;
  const strategies = res.strategies.map(s => ({
    name: s.name,
    n: s.n_trades,
    win_rate: s.win_rate,
    ev_per_trade: s.ev_per_trade,
    total_pnl: s.total_pnl,
    ci_lo: s.ci_lo,
    verdict: s.verdict,
    quote_source_share: { kalshi_candle: s.quote_sources.kalshi_share, modeled: s.quote_sources.modeled_share },
  }));
  return {
    generated_at: res.generated_at,
    age_min: ageMin,
    markets: res.data.markets,
    strategies,
    proven: strategies.filter(s => s.verdict === 'PROVEN').map(s => s.name),
    losing: strategies.filter(s => s.verdict === 'LOSING').map(s => s.name),
  };
}

// ---------------------------------------------------------------- CLI

function parseArgs(argv) {
  const args = {
    cmd: argv[0] || 'run',
    days: DEFAULT_DAYS,
    maxEntries: DEFAULT_MAX_ENTRIES,
    carryMaxMin: DEFAULT_CARRY_MAX_MIN,
    endSec: null,
    bySeries: false,
  };
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i];
    const eq = a.indexOf('=');
    const key = eq >= 0 ? a.slice(0, eq) : a;
    const val = () => (eq >= 0 ? a.slice(eq + 1) : argv[++i]);
    if (key === '--days') args.days = Number(val());
    else if (key === '--max-entries') args.maxEntries = Number(val());
    else if (key === '--carry-max-min') args.carryMaxMin = Number(val());
    else if (key === '--end') args.endSec = Math.floor(Date.parse(val()) / 1000);
    else if (key === '--by-series') args.bySeries = true;
  }
  return args;
}

const fmtPct = x => (isNum(x) ? (x * 100).toFixed(1) + '%' : '-');
const fmtNum = (x, d) => (isNum(x) ? x.toFixed(d) : '-');

function printTable(res, bySeries) {
  const header = ['strategy', 'n', 'win%', 'EV/trade', 'total PnL', 'Wilson lo', 'verdict', 'modeled share'];
  const rows = res.strategies.map(s => [
    s.name,
    String(s.n_trades),
    fmtPct(s.win_rate),
    fmtNum(s.ev_per_trade, 4),
    fmtNum(s.total_pnl, 2),
    fmtNum(s.ci_lo, 3),
    s.verdict,
    fmtPct(s.quote_sources.modeled_share),
  ]);
  const widths = header.map((h, i) => Math.max(h.length, ...rows.map(row => row[i].length)));
  const line = cells => cells.map((c, i) => c.padEnd(widths[i])).join('  ');
  console.log(`window ${res.window.from} -> ${res.window.to} (${res.window.days}d)  fee_rate=${res.fee_rate}`);
  console.log(line(header));
  console.log(widths.map(w => '-'.repeat(w)).join('  '));
  for (const row of rows) console.log(line(row));
  if (bySeries) {
    console.log('');
    for (const s of res.strategies) {
      console.log(`${s.name} by series:`);
      for (const [series, st] of Object.entries(s.by_series)) {
        if (!st.n_trades) continue;
        console.log(`  ${series.padEnd(11)} n=${String(st.n_trades).padEnd(6)} win=${fmtPct(st.win_rate).padEnd(7)} ev=${fmtNum(st.ev_per_trade, 4).padEnd(8)} pnl=${fmtNum(st.total_pnl, 2).padEnd(9)} lo=${fmtNum(st.ci_lo, 3)} ${st.verdict}`);
      }
    }
  }
  console.log('');
  console.log('by quote source (n / win% / EV per trade):');
  const sHeader = ['strategy', 'exact n', 'exact win', 'exact EV', 'carried n', 'carried EV', 'modeled n', 'modeled win', 'modeled EV'];
  const sRows = res.strategies.map(s => {
    const e = s.by_quote_source.kalshi_exact;
    const c = s.by_quote_source.kalshi_carried;
    const mo = s.by_quote_source.modeled;
    return [s.name, String(e.n_trades), fmtPct(e.win_rate), fmtNum(e.ev_per_trade, 4), String(c.n_trades), fmtNum(c.ev_per_trade, 4), String(mo.n_trades), fmtPct(mo.win_rate), fmtNum(mo.ev_per_trade, 4)];
  });
  const sW = sHeader.map((h, i) => Math.max(h.length, ...sRows.map(row => row[i].length)));
  const sLine = cells => cells.map((c, i) => c.padEnd(sW[i])).join('  ');
  console.log(sLine(sHeader));
  for (const row of sRows) console.log(sLine(row));

  const d = res.data;
  console.log('');
  console.log(`markets=${d.markets} with_exact_kalshi_quote=${d.markets_with_kalshi_quotes} outcomes yes=${d.outcomes.yes} no=${d.outcomes.no} no_share=${d.outcomes.no_share}`);
  for (const s of Object.keys(d.markets_by_series)) {
    const p = d.markets_by_series[s];
    console.log(`  ${s.padEnd(11)} listed=${p.settled_listed_raw} usable=${p.usable} kalshi_quoted=${p.markets_with_kalshi_quotes} yes=${p.outcome_yes} no=${p.outcome_no}`);
  }
  const qs = d.quote_sources;
  console.log(`decision minutes: kalshi_candle=${qs.kalshi_candle.decision_minutes} (exact ${qs.kalshi_candle.exact_minute}, carried<=${res.engine.carry_max_min}m ${qs.kalshi_candle.carried_from_last_candle}) modeled=${qs.modeled.decision_minutes}`);
  console.log(`candles: kalshi_1m=${d.candles.kalshi_1m_points_loaded} coinbase_1m=${d.candles.coinbase_1m_real} (forward-filled ${d.candles.coinbase_1m_forward_filled})`);
  console.log(`http: requests kalshi=${d.http.requests.kalshi} coinbase=${d.http.requests.coinbase} 429s kalshi=${d.http.rate_limited_429.kalshi} coinbase=${d.http.rate_limited_429.coinbase} retries=${d.http.retries} cache_hits=${d.http.cache_hits} failed_batches=${d.http.failed_requests}`);
  if (res.sanity) {
    const s = res.sanity;
    console.log(`sanity always-NO: n=${s.n_trades} win=${fmtPct(s.win_rate)} vs NO-rate of traded markets=${fmtPct(s.no_rate_of_traded_markets)} (all usable=${fmtPct(s.no_rate_all_usable_markets)}) match=${s.match}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.cmd === 'run') {
    const res = await runBacktest({
      days: args.days,
      maxEntries: args.maxEntries,
      carryMaxMin: args.carryMaxMin,
      endSec: args.endSec,
      log: msg => process.stderr.write(msg + '\n'),
    });
    printTable(res, args.bySeries);
    return;
  }
  if (args.cmd === 'summary') {
    console.log(JSON.stringify(summary(), null, 2));
    return;
  }
  console.error('usage: node backtest.js run [--days 14] [--max-entries 1] [--carry-max-min 15] [--end ISO] [--by-series]\n       node backtest.js summary');
  process.exitCode = 2;
}

if (require.main === module) {
  main().catch(err => {
    console.error(err && err.stack ? err.stack : err);
    process.exitCode = 1;
  });
}

module.exports = {
  runBacktest,
  loadResults,
  summary,
  loadStrategies,
  normCdf,
  FEE_RATE,
  RESULTS_PATH,
  _test: { newAcc, accAdd, statsFromAcc }, // internals exposed for unit checks
};
