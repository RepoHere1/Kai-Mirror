#!/usr/bin/env python3
"""
Apply all server.js changes cleanly:
1. Remove throttle from top level
2. Add global.perSecondVol = perSecondVol
3. Move fireWebhooks to top level
4. Add throttle setup inside boot()
5. Add engineTick interval in boot()
"""

import re

with open('server.js', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove throttle from top level (between WebSocket require and first function)
# Find the throttle block and remove it
throttle_pattern = r'// ===========================================================================\n// REQUEST THROTTLE - protects PC from API overload\n// ===========================================================================\nconst THROTTLE = \{[\s\S]*?\n\};[\s\S]*?require\(\'./kalshi\.js\'\)\.raw = async function\(acctName, method, route, opts\) \{[\s\S]*?return THROTTLE\.run\(\(\) => originalKalshiRaw\(acctName, method, route, opts\)\);[\s\S]*?\};'
content = re.sub(throttle_pattern, '', content)

# 2. Add global.perSecondVol = perSecondVol after the function
persec_pattern = r'(function perSecondVol\(product, windowSec\) \{[\s\S]*?return perBar / Math\.sqrt\(avgDt\); // per root-second\n\})'
content = re.sub(persec_pattern, r'\1\nglobal.perSecondVol = perSecondVol;', content)

# 3. Move fireWebhooks to top level (before handle)
# Find the fireWebhooks inside handle and move it before handle
firewebhooks_pattern = r'(  // --- fire webhook helper\n  async function fireWebhooks\(event, data\) \{[\s\S]*?\n  \}\n)'
firewebhooks_match = re.search(firewebhooks_pattern, content)
if firewebhooks_match:
    firewebhooks_code = firewebhooks_match.group(1)
    # Remove from inside handle
    content = content.replace(firewebhooks_match.group(0), '')
    # Insert before handle function
    content = content.replace('async function handle(req, res) {',
        'async function fireWebhooks(event, data) {\n' + firewebhooks_code.split('\n', 1)[1] +
        '\nasync function handle(req, res) {')

# 4. Add throttle setup inside boot() after coinbaseWS.connect()
boot_throttle = '''
  // REQUEST THROTTLE - protects PC from API overload (initialized after all functions defined)
  const THROTTLE = {
    maxConcurrent: 3,           // max simultaneous requests
    minIntervalMs: 500,         // min ms between requests
    queue: [],
    running: 0,
    lastRequestTime: 0,

    async run(fn) {
      return new Promise((resolve, reject) => {
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

'''
content = content.replace('  coinbaseWS.connect();\n\n  // OPERATOR_LAW: exterminate',
    '  coinbaseWS.connect();\n\n' + boot_throttle + '  // OPERATOR_LAW: exterminate')

# 5. Add engineTick interval in boot() after kalshiWS.subscribeOrders()
content = content.replace(
    '  kalshiWS.subscribeOrders();\n\n  server.listen(PORT, HOST, () => {',
    '  kalshiWS.subscribeOrders();\n\n  // Main engine tick - evaluates all lanes every 2 seconds\n  setInterval(async () => {\n    try { await engineTick(); } catch (e) { console.error(\'[engineTick] Error:\', e.message); }\n  }, 2000);\n\n  server.listen(PORT, HOST, () => {')

# Write the modified content
with open('server.js', 'w', encoding='utf-8') as f:
    f.write(content)

print("Done!")