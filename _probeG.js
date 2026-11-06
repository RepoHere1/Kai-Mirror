const sqlite3 = require('sqlite3').verbose();
const fs = require('fs'), path = require('path'), os = require('os');
const say = (s) => { try { fs.writeSync(1, s + '\n'); } catch (e) { } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const TMP = path.join(os.tmpdir(), 'kai_gc.db');
const useGc = process.argv[2] === 'gc';
for (const f of [TMP, TMP+'-journal', TMP+'-wal', TMP+'-shm']) { try { fs.unlinkSync(f); } catch (e) { } }
process.on('uncaughtException', (e) => { say('UNCAUGHT: ' + (e && e.message)); });
let B = new sqlite3.Database(TMP);
(async () => {
  await new Promise((res) => B.run('CREATE TABLE IF NOT EXISTS t (a INT)', res));
  await new Promise((res) => B.run('BEGIN EXCLUSIVE', res));
  await sleep(300);
  for (let i = 0; i < 5; i++) {
    const raw = new sqlite3.Database(TMP);
    raw.on('error', (e) => say('DB_LEVEL ' + e.code));
    raw.run('INSERT INTO t (a) VALUES (1)');
    if (useGc && global.gc) { global.gc(); say("gc-done"); }
    await sleep(300);
  }
  await sleep(500);
  say('DONE no-crash');
  process.exit(0);
})();
