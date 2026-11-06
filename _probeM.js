const sqlite3 = require('sqlite3').verbose();
const fs = require('fs'), path = require('path'), os = require('os');
const say = (s) => { try { fs.writeSync(1, s + '\n'); } catch (e) { } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const mode = process.argv[2];
const TMP = path.join(os.tmpdir(), 'kai_m_' + mode + '.db');
for (const f of [TMP, TMP+'-journal', TMP+'-wal', TMP+'-shm']) { try { fs.unlinkSync(f); } catch (e) { } }
process.on('uncaughtException', (e) => { say('UNCAUGHT: ' + (e && e.message)); process.exit(9); });
let B = new sqlite3.Database(TMP);
async function writeNoCb() {
  const raw = new sqlite3.Database(TMP);
  raw.on('error', (e) => say('DB_LEVEL ' + e.code));
  raw.run('INSERT INTO t (a) VALUES (1)');
  await sleep(1200);
  say('SURVIVED');
}
(async () => {
  await new Promise((res) => B.run('CREATE TABLE IF NOT EXISTS t (a INT)', res));
  if (mode === 'await') {
    await new Promise((res) => B.run('BEGIN EXCLUSIVE', res));
    await sleep(300);
    await writeNoCb();
  } else {
    B.run('BEGIN EXCLUSIVE', (e) => {
      say('begin err=' + (e && e.code));
      writeNoCb();
    });
  }
  process.exit(0);
})();
