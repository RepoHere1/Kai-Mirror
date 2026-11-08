'use strict';
// Kalshi REST client: RSA-PSS request signing + dry-first / demo-fallback.
// Live ("hot") API is the primary source for BOTH dry and dry books; the demo
// API is only touched when the dry call fails (rate limit / outage).

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const HERE = __dirname;
// Use api.kalshi.com for LIVE (main Kalshi platform), elections.kalshi.com for elections-only
// MASTER ENV CHAIN (2026-11-07, new hardware): D:\master.env is the single
// source of truth for credentials on this PC. Resolution order:
//   1. an explicitly passed file (tools that ask for one specific .env)
//   2. D:\master.env (or $KAI_MASTER_ENV) — ALWAYS overrides when present
//   3. <repo>/.env — fills gaps only (may be absent entirely)
// Name adapters translate master.env conventions (KALSHI_API_KEY_ID +
// KALSHI_PRIVATE_KEY_PATH, path-based) onto this codebase's names
// (KALSHI_LIVE_KEY_ID + KALSHI_LIVE_PRIVATE_KEY, inline PEM).
const MASTER_ENV_PATH = process.env.KAI_MASTER_ENV || 'D:\\master.env';
const ENV_SOURCES = {};
function parseEnvFile(p) {
  const out = {};
  let text = '';
  try { text = fs.readFileSync(p, 'utf8'); } catch (e) { return out; }
  const lines = text.split(/\r?\n/);
  let key = null, buf = [];
  const flush = () => {
    if (key) out[key] = buf.join('\n').trim();
    key = null; buf = [];
  };
  for (const line of lines) {
    // Skip empty lines and comments
    if (!line.trim() || line.trim().startsWith('#')) {
      if (key && buf.length > 0) {
        // If we were building a value and hit a comment/empty line, flush
        flush();
      }
      continue;
    }
    if (/^[A-Z0-9_]+\s*=/.test(line)) {
      flush();
      const i = line.indexOf('=');
      key = line.slice(0, i).trim();
      buf.push(line.slice(i + 1));
    } else if (key) {
      buf.push(line);
    }
  }
  flush();
  return out;
}
function readPemFile(v, baseDir) {
  if (!v) return '';
  const t = String(v).trim().replace(/^["']|["']$/g, '');
  if (!t) return '';
  let p = t;
  try {
    if (!path.isAbsolute(p)) p = path.join(baseDir || process.cwd(), p);
    return fs.readFileSync(p, 'utf8').trim();
  } catch (e) { return ''; }
}
function loadEnv(file) {
  // Repo .env first (gap-fill), then master.env overlays — master wins.
  const out = parseEnvFile(file || path.join(HERE, '.env'));
  if (!file) {
    const master = parseEnvFile(MASTER_ENV_PATH);
    const masterDir = path.dirname(MASTER_ENV_PATH);
    for (const k of Object.keys(master)) {
      if (String(master[k] || '').trim() !== '') {
        out[k] = master[k];
        ENV_SOURCES[k] = MASTER_ENV_PATH;
      }
    }
    // ---- adapters: fill gaps only; native names always win ----
    if (!out.KALSHI_LIVE_KEY_ID && out.KALSHI_API_KEY_ID) out.KALSHI_LIVE_KEY_ID = out.KALSHI_API_KEY_ID;
    if (!out.KALSHI_LIVE_PRIVATE_KEY && out.KALSHI_PRIVATE_KEY_PATH) out.KALSHI_LIVE_PRIVATE_KEY = readPemFile(out.KALSHI_PRIVATE_KEY_PATH, masterDir);
    if (!out.KALSHI_DEMO_KEY_ID && out.KALSHI_DEMO_API_KEY_ID) out.KALSHI_DEMO_KEY_ID = out.KALSHI_DEMO_API_KEY_ID;
    if (!out.KALSHI_DEMO_PRIVATE_KEY && out.KALSHI_DEMO_PRIVATE_KEY_PATH) out.KALSHI_DEMO_PRIVATE_KEY = readPemFile(out.KALSHI_DEMO_PRIVATE_KEY_PATH, masterDir);
    if (!out.KALSHI_LIVE_BASE && out.KALSHI_BASE_URL) out.KALSHI_LIVE_BASE = out.KALSHI_BASE_URL;
  }
  return out;
}

const ENV = loadEnv();
const LIVE_BASE = ENV.KALSHI_LIVE_BASE || process.env.KALSHI_LIVE_BASE || 'https://api.kalshi.com/trade-api/v2';
const DEMO_BASE = 'https://demo-api.kalshi.co/trade-api/v2';

const accounts = {
  // DRY mode: uses live API credentials but paper trading (simulated fills)
  dry: { 
    keyId: (ENV.KALSHI_LIVE_KEY_ID || '').trim(), 
    pem: (ENV.KALSHI_LIVE_PRIVATE_KEY || '').trim(), 
    base: LIVE_BASE, 
    name: 'dry' 
  },
  // LIVE mode: real Kalshi account with shard support
  // Shard accounts are separate exchange_index for different trading segments
  // (crypto, politics, sports, etc.) that don't appear on web/app UI
  live: { 
    keyId: (ENV.KALSHI_LIVE_KEY_ID || '').trim(), 
    pem: (ENV.KALSHI_LIVE_PRIVATE_KEY || '').trim(), 
    base: LIVE_BASE, 
    name: 'live',
    // Shard exchange_index values - user must provide these
    // exchange_index 0 = primary, 1+ = category-specific shards
    shards: {
      crypto: parseInt(ENV.KALSHI_SHARD_CRYPTO) || 0,
      politics: parseInt(ENV.KALSHI_SHARD_POLITICS) || 0,
      sports: parseInt(ENV.KALSHI_SHARD_SPORTS) || 0,
      finance: parseInt(ENV.KALSHI_SHARD_FINANCE) || 0,
    },
    subaccount: parseInt(ENV.KALSHI_SUBACCOUNT) || 0,
  },
  demo: { 
    keyId: (ENV.KALSHI_DEMO_KEY_ID || '').trim(), 
    pem: (ENV.KALSHI_DEMO_PRIVATE_KEY || '').trim(), 
    base: DEMO_BASE, 
    name: 'demo' 
  },
};

function sign(acct, method, fullPath, ts, scheme) {
  const msg = ts + method.toUpperCase() + fullPath;
  const opts = scheme === 'pkcs1'
    ? { key: acct.pem, padding: crypto.constants.RSA_PKCS1_PADDING }
    : { key: acct.pem, padding: crypto.constants.RSA_PKCS1_PSS_PADDING, saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST };
  return crypto.sign('sha256', Buffer.from(msg, 'utf8'), opts).toString('base64');
}

function buildUrl(acct, route, query) {
  const qs = query ? '?' + new URLSearchParams(query).toString() : '';
  const fullPath = '/trade-api/v2' + route + qs;
  return { url: acct.base + route + qs, fullPath };
}

// Signature strategies, in the order they are tried. Kalshi's documented rule
// ("sign the path with the query string") is only true for some endpoints:
// /portfolio/positions and /portfolio/fills reject a signature that includes
// the query string (401 INCORRECT_API_KEY_SIGNATURE) while /markets accepts it.
// So each request tries the variants and keeps the one that worked.
// CRITICAL: Cache strategy PER ROUTE because different endpoints need different signing
const ALL_STRATEGIES = [
  { scheme: 'pss', withQuery: true },    // markets needs query string
  { scheme: 'pss', withQuery: false },   // portfolio needs NO query string
  { scheme: 'pkcs1', withQuery: true },
  { scheme: 'pkcs1', withQuery: false },
];
// For portfolio endpoints with shard params, we NEED query string in signature
const PORTFOLIO_STRATEGIES = [
  { scheme: 'pss', withQuery: true },   // portfolio WITH query string (for shard params)
  { scheme: 'pss', withQuery: false },
  { scheme: 'pkcs1', withQuery: true },
  { scheme: 'pkcs1', withQuery: false },
];
const strategyCache = {}; // key: "acctName:route"

async function raw(acctName, method, route, opts) {
  opts = opts || {};
  const acct = accounts[acctName];
  if (!acct || !acct.keyId || !acct.pem) throw new Error('no credentials for ' + acctName);
  
  // Build query params with shard support for live account
  const query = Object.assign({}, opts.query || {});
  if (acctName === 'live' && acct.shards) {
    // For portfolio endpoints, add exchange_index and subaccount if not already provided
    const portfolioRoutes = ['/portfolio/balance', '/portfolio/positions', '/portfolio/fills', '/portfolio/orders'];
    if (portfolioRoutes.some(r => route.startsWith(r))) {
      if (query.exchange_index == null) query.exchange_index = acct.shards.crypto || 0;
      if (query.subaccount == null) query.subaccount = acct.subaccount || 0;
    }
  }
  
  const { url, fullPath } = buildUrl(acct, route, query);
  const pathNoQuery = '/trade-api/v2' + route;
  
  // Strategy cache keyed by account + route (different endpoints need different signing)
  const cacheKey = acctName + ':' + route;
  
  // Use portfolio-specific strategy order for portfolio endpoints
  const portfolioRoutes = ['/portfolio/balance', '/portfolio/positions', '/portfolio/fills', '/portfolio/orders'];
  const isPortfolioRoute = portfolioRoutes.some(r => route.startsWith(r));
  const strategiesToTry = strategyCache[cacheKey]
    ? [strategyCache[cacheKey]].concat((isPortfolioRoute ? PORTFOLIO_STRATEGIES : ALL_STRATEGIES).filter(s => s !== strategyCache[cacheKey]))
    : (isPortfolioRoute ? PORTFOLIO_STRATEGIES : ALL_STRATEGIES);
  
  let lastErr = null;
  for (const st of strategiesToTry) {
    const ts = Math.floor(Date.now() / 1000).toString();
    // 2026-11-04: Kalshi market data (/markets, /series, /events...) is PUBLIC -
    // unsigned GETs return 200 (verified live). Signing public reads ties every
    // quote to the signing key's 429 penalty (LIVE starved all lanes this way);
    // public reads stay decoupled from any key throttle. /portfolio/* stays signed.
    const isPublicRead = method === 'GET' && /^(\/markets|\/series|\/events|\/calendar|\/exchange|\/tags)/.test(route);
    const headers = isPublicRead ? {
      'Accept': 'application/json',
    } : {
      'KALSHI-ACCESS-KEY': acct.keyId,
      'KALSHI-ACCESS-SIGNATURE': sign(acct, method, st.withQuery ? fullPath : pathNoQuery, ts, st.scheme),
      'KALSHI-ACCESS-TIMESTAMP': ts,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    };
    const init = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
    let r;
    try {
      // TIMEOUT GUARD: a stalled TCP connection used to hang this promise
      // FOREVER (undici has no default body/head timeout). One hung call froze
      // the whole lane tick chain - the 'stale HYPE heartbeat' incident.
      r = await fetch(url, Object.assign({}, init, { signal: AbortSignal.timeout(15000) }));
    } catch (e) {
      lastErr = new Error('network: ' + e.message);
      break;
    }
    const text = await r.text();
    let parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch (e) { parsed = { raw: text }; }
    if (r.ok) {
      strategyCache[cacheKey] = st;
      return parsed;
    }
    lastErr = new Error('HTTP ' + r.status + ' ' + method + ' ' + fullPath + ' :: ' + text.slice(0, 300));
    lastErr.status = r.status;
    // Rate limit - retry after delay
    if (r.status === 429) {
      const retryAfter = parseInt(r.headers.get('retry-after') || '2') * 1000;
      await new Promise(resolve => setTimeout(resolve, retryAfter));
      continue;
    }
    if (r.status === 401) continue;
    break;
  }
  throw lastErr;
}

const cache = new Map();
const CACHE_MAX_SIZE = 1000; // LRU eviction limit
const NEG_TTL_MS = 5000;     // fast-fail window for failed keys (outage storm bound)
const cacheAccessOrder = new Set(); // For LRU tracking

function cached(key, ttlMs, fn) {
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && !hit.err && now - hit.at < ttlMs) {
    // Update LRU order
    cacheAccessOrder.delete(key);
    cacheAccessOrder.add(key);
    return Promise.resolve(hit.val);
  }
  // NEGATIVE CACHE (2026-11-07 OOM LAW): during an outage every producer poll
  // used to hit raw() directly (measured 60+ calls/s -> the unbounded throttle
  // queue pinned ~64MB/min of async contexts -> 4GB heap death). A failed key
  // now fast-fails for NEG_TTL_MS without touching the network, so any number
  // of callers self-bound to ~one real attempt per key per window.
  if (hit && hit.err && now - hit.at < NEG_TTL_MS) {
    return Promise.reject(hit.err);
  }
  return Promise.resolve(fn()).then(val => {
    // Evict oldest if at capacity
    if (cache.size >= CACHE_MAX_SIZE && !cache.has(key)) {
      const oldestKey = cacheAccessOrder.values().next().value;
      if (oldestKey) {
        cacheAccessOrder.delete(oldestKey);
        cache.delete(oldestKey);
      }
    }
    cache.set(key, { at: now, val });
    cacheAccessOrder.delete(key);
    cacheAccessOrder.add(key);
    return val;
  }, err => {
    if (hit && !hit.err) return hit.val; // serve the last GOOD value on error
    if (hit && hit.err) { hit.at = Date.now(); throw err; } // already negative: refresh window
    // record a negative entry (same LRU bookkeeping)
    if (cache.size >= CACHE_MAX_SIZE && !cache.has(key)) {
      const oldestKey = cacheAccessOrder.values().next().value;
      if (oldestKey) {
        cacheAccessOrder.delete(oldestKey);
        cache.delete(oldestKey);
      }
    }
    cache.set(key, { at: Date.now(), err });
    cacheAccessOrder.delete(key);
    cacheAccessOrder.add(key);
    throw err;
  });
}

// Cache statistics
function getCacheStats() {
  return {
    size: cache.size,
    maxSize: CACHE_MAX_SIZE,
    keys: Array.from(cache.keys()).slice(0, 20)
  };
}

// Clear cache
function clearCache() {
  cache.clear();
  cacheAccessOrder.clear();
}

// Warm cache for critical endpoints
async function warmCache() {
  const criticalEndpoints = [
    // 2026-11-04: /portfolio/balance removed from warmup. The warm was a signed
    // live-key read at every boot - one more drain on the shared key's tight
    // per-key budget. The mirror never needed it (realBalanceCash is inert).
    { route: '/markets', opts: { query: { limit: 5, status: 'open', series_ticker: 'KXBTC15M' } } },
    { route: '/markets', opts: { query: { limit: 5, status: 'open', series_ticker: 'KXETH15M' } } },
  ];
  
  let warmed = 0;
  for (const { route, opts } of criticalEndpoints) {
    const key = 'GET ' + route + ' ' + JSON.stringify(opts.query || {});
    if (!cache.has(key)) {
      try {
        await cached(key, 1500, async () => {
          const data = await raw('dry', 'GET', route, opts);
          return { data, source: 'dry', host: LIVE_BASE, ms: 0, at: new Date().toISOString() };
        });
        warmed++;
      } catch (e) {
        console.warn('[cache] Warm failed for', route, e.message);
      }
    }
  }
  console.log('[cache] Warmed', warmed, 'endpoints');
}

// Periodic cache cleanup
setInterval(() => {
  const now = Date.now();
  for (const [key, val] of cache.entries()) {
    if (now - val.at > 300000) { // 5 minutes
      cache.delete(key);
      cacheAccessOrder.delete(key);
    }
  }
}, 60000);

// PUBLIC MARKET DATA (2026-11-04 shared-key fix): /markets, /events, /series
// are unauthenticated on the live exchange. Unsigned requests are rate-limited
// by IP, not by the API key, so the dry mirror no longer eats the LIVE
// account's per-key request budget (the sustained /portfolio/balance 429s).
async function publicGet(route, query) {
  const qs = query && Object.keys(query).length ? '?' + new URLSearchParams(query).toString() : '';
  const url = LIVE_BASE + route + qs;
  let r;
  try {
    r = await fetch(url, { headers: { 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) });
  } catch (e) {
    throw new Error('public network: ' + (e.message || e));
  }
  const text = await r.text();
  let parsed = null;
  try { parsed = text ? JSON.parse(text) : null; } catch (e) { parsed = { raw: text }; }
  if (!r.ok) throw new Error('HTTP ' + r.status + ' GET ' + route + qs + ' :: ' + text.slice(0, 300));
  return parsed;
}

// Live-first with demo fallback. Every return is stamped with provenance so the
// dashboard (and the operator) can see exactly which host answered.
async function get(route, opts) {
  opts = opts || {};
  const key = 'GET ' + route + ' ' + JSON.stringify(opts.query || {});
  return cached(key, opts.ttlMs || 1500, async () => {
    const started = Date.now();
    // Non-portfolio GETs are PUBLIC market data: go unsigned first. Only
    // portfolio routes (and a public failure) ever touch a signed key.
    if (!route.startsWith('/portfolio')) {
      try {
        const data = await publicGet(route, opts.query);
        return { data, source: 'public', host: LIVE_BASE, ms: Date.now() - started, at: new Date().toISOString() };
      } catch (e) { /* fall through to the signed path */ }
    }
    try {
      const data = await raw('dry', 'GET', route, opts);
      return { data, source: 'dry', host: LIVE_BASE, ms: Date.now() - started, at: new Date().toISOString() };
    } catch (liveErr) {
      try {
        const data = await raw('demo', 'GET', route, opts);
        return { data, source: 'demo', host: DEMO_BASE, ms: Date.now() - started, at: new Date().toISOString(), live_error: String(liveErr.message || liveErr) };
      } catch (demoErr) {
        const e = new Error('dry: ' + (liveErr.message || liveErr) + ' | demo: ' + (demoErr.message || demoErr));
        e.live_error = String(liveErr.message || liveErr);
        e.demo_error = String(demoErr.message || demoErr);
        throw e;
      }
    }
  });
}

async function post(route, body) {
  try {
    const data = await raw('dry', 'POST', route, { body });
    return { data, source: 'dry', host: LIVE_BASE };
  } catch (liveErr) {
    const data = await raw('demo', 'POST', route, { body });
    return { data, source: 'demo', host: DEMO_BASE, live_error: String(liveErr.message || liveErr) };
  }
}

// Convenience methods for common queries
async function portfolio() {
  // Fetch from all shards and aggregate using live account (which has shard config)
  const shards = ['crypto', 'politics', 'sports', 'finance'];
  let totalBalance = 0;
  let allPositions = [];
  let allFills = [];
  let lastSource = 'demo';
  let lastHost = DEMO_BASE;
  
  for (const shardName of shards) {
    try {
      const exchangeIndex = accounts.live.shards[shardName] || 0;
      const subaccount = accounts.live.subaccount || 0;
      
      // Use raw('live', ...) to get shard-specific data
      const bal = await raw('live', 'GET', '/portfolio/balance', { 
        query: { exchange_index: exchangeIndex, subaccount: subaccount }, 
        ttlMs: 4000 
      });
      const pos = await raw('live', 'GET', '/portfolio/positions', { 
        query: { limit: 200, exchange_index: exchangeIndex, subaccount: subaccount }, 
        ttlMs: 8000 
      });
      
      if (bal && bal.balance_available != null) {
        totalBalance += Number(bal.balance_available);
      }
      if (pos) {
        allPositions = allPositions.concat(pos);
      }
      
      lastSource = 'live';
      lastHost = LIVE_BASE;
    } catch (e) {
      console.error(`Portfolio fetch failed for shard ${shardName}:`, e.message);
    }
  }
  
  // Also fetch fills from primary shard (crypto)
  let fills = null;
  try { 
    fills = await raw('live', 'GET', '/portfolio/fills', { 
      query: { limit: 200, exchange_index: accounts.live.shards.crypto || 0, subaccount: accounts.live.subaccount || 0 }, 
      ttlMs: 15000 
    }); 
  } catch (e) { }
  
  return {
    balance: { balance_available: totalBalance, balance_total: totalBalance },
    positions: allPositions,
    fills: fills || null,
    source: lastSource,
    host: lastHost,
  };
}

async function markets(series, limit) {
  return get('/markets', { query: { limit: limit || 5, status: 'open', series_ticker: series }, ttlMs: 1500 });
}

module.exports = { loadEnv, ENV, ENV_SOURCES, accounts, sign, raw, get, post, LIVE_BASE, DEMO_BASE, cache, portfolio, markets, getCacheStats, clearCache, warmCache };
