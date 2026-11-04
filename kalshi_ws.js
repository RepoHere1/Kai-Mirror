'use strict';
// Kalshi WebSocket client - real-time quotes, fills, market data
// Replaces 2s HTTP polling with sub-second WebSocket updates

const crypto = require('crypto');

class KalshiWS {
  constructor(accounts, onMarketUpdate, onFillUpdate, onOrderUpdate) {
    this.accounts = accounts;
    this.onMarketUpdate = onMarketUpdate;
    this.onFillUpdate = onFillUpdate;
    this.onOrderUpdate = onOrderUpdate;
    this.ws = null;
    this.reconnectTimer = null;
    this.subscriptions = new Set();
    this.connected = false;
    this.authenticated = false;
    this.pendingSubscriptions = new Map();
    this.cmdId = 1;
    this.heartbeatTimer = null;
  }

  connect() {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    try {
      // Same host the REST client uses. Kalshi authenticates the WS at the
      // HTTP upgrade via KALSHI-ACCESS-* headers (same scheme as REST);
      // a bare handshake is rejected with 401 and used to 1006-loop forever.
      const wsUrl = 'wss://api.elections.kalshi.com/trade-api/ws/v2';
      const acct = this.accounts.live;
      const ts = Math.floor(Date.now() / 1000).toString();
      const sig = require('crypto').sign('sha256',
        Buffer.from(ts + 'GET/trade-api/ws/v2'),
        { key: acct.pem, padding: require('crypto').constants.RSA_PKCS1_PSS_PADDING, saltLength: require('crypto').constants.RSA_PSS_SALTLEN_DIGEST }
      ).toString('base64');
      this.ws = new WebSocket(wsUrl, {
        headers: {
          'KALSHI-ACCESS-KEY': acct.keyId,
          'KALSHI-ACCESS-SIGNATURE': sig,
          'KALSHI-ACCESS-TIMESTAMP': ts
        }
      });

      this.ws.onopen = () => {
        console.log('[KalshiWS] Connected (header-authenticated)');
        this.connected = true;
        this.authenticated = true; // auth rode the handshake
        this.startHeartbeat();
        this.resubscribe();
      };
    } catch (e) {
      console.error('[KalshiWS] Connect error:', e.message);
      this.scheduleReconnect();
    }

    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        this.handleMessage(msg);
      } catch (e) {
        console.error('[KalshiWS] Parse error:', e.message);
      }
    };

    this.ws.onclose = (event) => {
      console.log('[KalshiWS] Disconnected:', event.code, event.reason, 'wasClean:', event.wasClean);
      this.connected = false;
      this.authenticated = false;
      this.stopHeartbeat();
      this.scheduleReconnect();
    };

    this.ws.onerror = (err) => {
      // Rate-limited: WS error storms (code 1006 reconnect loops) were
      // flooding the log and straining the PC. One line per 30s max.
      const now = Date.now();
      if (this._lastErrLog && now - this._lastErrLog < 30000) return;
      this._lastErrLog = now;
      try {
        const msg = err?.message || err?.code || (err && typeof err === 'object' ? JSON.stringify(err) : String(err)) || 'unknown error';
        console.error('[KalshiWS] Error:', msg);
      } catch (e) {
        console.error('[KalshiWS] Error (failed to format):', String(e));
      }
      // Prevent any error in error handler from propagating
    };
  }

  authenticate() {
    try {
      const method = 'GET';
      const path = '/trade-api/ws/v2';
      const timestamp = Math.floor(Date.now() / 1000);
      const msg = timestamp + method + path;
      const signature = this.sign(this.accounts.live, method, path, timestamp);

      this.ws.send(JSON.stringify({
        type: 'auth',
        key_id: this.accounts.live.keyId,
        timestamp: timestamp.toString(),
        signature: signature
      }));
    } catch (e) {
      console.error('[KalshiWS] Authenticate error:', e.message);
    }
  }

  sign(account, method, path, timestamp) {
    const msg = timestamp + method + path;
    const sign = crypto.createSign('RSA-SHA256');
    sign.update(msg);
    sign.end();
    // Account object uses 'pem' property for private key
    const privateKey = account.pem || account.privateKey;
    try {
      return sign.sign(privateKey, 'base64');
    } catch (e) {
      console.error('[KalshiWS] Sign error:', e.message, 'key length:', privateKey?.length);
      throw e;
    }
  }

  startHeartbeat() {
    // 2026-11-04: Kalshi WS v2 has NO client heartbeat command - sending
    // {type:'heartbeat'} every 30s produced the endless "Unknown command"
    // errors (one per boot every 30s). The server pings; we answer at the
    // protocol layer automatically. Keep-alive sending is DELETED.
    this.heartbeatTimer = null;
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimer) return;
    // Exponential backoff: 5s doubling to a 60s cap, reset after a clean auth.
    const delay = this.reconnectDelayMs || 5000;
    this.reconnectDelayMs = Math.min(60000, delay * 2);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      console.log('[KalshiWS] Reconnecting...');
      this.connect();
    }, delay);
  }

  resubscribe() {
    for (const sub of this.pendingSubscriptions.values()) {
      this.send(sub);
    }
    this.pendingSubscriptions.clear();
  }

  subscribeMarkets(seriesTickers) {
    // 2026-11-04 VERIFIED LIVE: Kalshi WS v2 has NO market-data channel.
    //   {channels:['market']}          -> {"code":8,"msg":"Unknown channel name"}
    //   {channels:[{name:'market',...}]} -> {"code":1,"msg":"Unable to process message"}
    // The only push channels are 'fill', 'user_orders', 'market_positions'.
    // Quotes therefore come from REST (marketsForSeries + quote_stream), which
    // is what has been carrying the book all along. This no-op keeps the
    // call-site contract and logs the reality once.
    if (!this._marketSubNoticeLogged) {
      this._marketSubNoticeLogged = true;
      console.log('[KalshiWS] market-data channel unavailable on WS v2 (verified live); REST quotes remain the source of truth');
    }
    for (const series of seriesTickers) {
      this.pendingSubscriptions.delete(series);
    }
  }

  subscribeFills() {
    // Proven working shape (verified live 2026-11-04): cmd/params, string channels.
    const msg = { id: this.cmdId++, cmd: 'subscribe', params: { channels: ['fill'] } };
    this.pendingSubscriptions.set('fills', msg);
    if (this.connected && this.authenticated) {
      this.send(msg);
    }
  }

  subscribeOrders() {
    const msg = { id: this.cmdId++, cmd: 'subscribe', params: { channels: ['user_orders'] } };
    this.pendingSubscriptions.set('orders', msg);
    if (this.connected && this.authenticated) {
      this.send(msg);
    }
  }

  send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  handleMessage(msg) {
    switch (msg.type) {
      case 'auth':
        if (msg.success) {
          this.authenticated = true;
          this.reconnectDelayMs = 5000; // healthy session: reset backoff
          console.log('[KalshiWS] Authenticated');
          this.resubscribe();
        } else {
          console.error('[KalshiWS] Auth failed:', msg.error);
        }
        break;

      case 'market':
        if (this.onMarketUpdate) {
          this.onMarketUpdate(msg.data);
        }
        break;

      case 'fill':
        if (this.onFillUpdate) {
          this.onFillUpdate(msg.data);
        }
        break;

      case 'order':
        if (this.onOrderUpdate) {
          this.onOrderUpdate(msg.data);
        }
        break;

      case 'heartbeat':
        break;

      case 'subscribed':
        // 2026-11-04: ack for the fill/user_orders subscriptions. Kalshi v2 has
        // no market-data channel, so this is the expected terminal state - one
        // log line, never per-ack spam.
        if (!this._subAckLogged) {
          this._subAckLogged = true;
          console.log('[KalshiWS] subscriptions acked (fill, user_orders)');
        }
        break;

      case 'error':
        // Log the WHOLE message: the server's error text is not always in
        // msg.error (it printed "undefined" for every error). The raw payload
        // is the only reliable record of what Kalshi actually said.
        console.error('[KalshiWS] Server error:', JSON.stringify(msg).slice(0, 400));
        break;

      default:
        console.log('[KalshiWS] Unknown message type:', msg.type);
    }
  }

  close() {
    this.stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.connected = false;
    this.authenticated = false;
  }
}

module.exports = { KalshiWS };