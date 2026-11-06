const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');
const os = require('os');
const TMP = path.join(os.tmpdir(), 'kai_probe5.db');
const say = (s) => { try { fs.writeSync(1, s + '\n'); } catch (e) { } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
for (const f of [TMP, TMP+'-journal', TMP+'-wal', TMP+'-shm']) { try { fs.unlinkSync(f); } catch (e) { } }
let B = new sqlite3.Database(TMP);
const tryWrite = (label) => new Promise((res) => {
  const raw = new sqlite3.Database(TMP);
  raw.on('error', (e) => say(label + ' DB_LEVEL ' + e.code));
  raw.run('INSERT INTO t (a) VALUES (1)', (e) => { say(label + ' write err=' + (e && e.code)); raw.close(()=>res()); });
});
(async () => {
  await new Promise((res) => B.run('CREATE TABLE IF NOT EXISTS t (a INT)', res));
  const r = await new Promise((res) => B.run('BEGIN EXCLUSIVE', (e) => res(e || null)));
  say('B begin err=' + (r && r.code));
  await sleep(300);
  await tryWrite('B');
  process.exit(0);
})();
