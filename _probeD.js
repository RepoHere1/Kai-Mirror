'use strict';
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs'), path = require('path'), os = require('os');
const say = (s) => { try { fs.writeSync(1, s + '\n'); } catch (e) { } };
const mode = process.argv[2] || 'run';
const TMP = path.join(os.tmpdir(), 'kai_d_' + mode + '.db');
for (const f of [TMP, TMP + '-journal', TMP + '-wal', TMP + '-shm']) { try { fs.unlinkSync(f); } catch (e) { } }
let uncaught = 0, dblevel = 0;
process.on('uncaughtException', (e) => { uncaught++; say('UNCAUGHT[' + uncaught + ']: ' + (e && e.message)); });
const B = new sqlite3.Database(TMP);
B.run('CREATE TABLE IF NOT EXISTS t (a INT)', () => {
  B.run('BEGIN EXCLUSIVE', () => {
    say('LOCK HELD');
    const raw = new sqlite3.Database(TMP);
    raw.on('error', (e) => { dblevel++; say('DB_LEVEL: ' + e.code); });
    if (mode === 'prep') {
      const st = raw.prepare('INSERT INTO t (a) VALUES (1)');
      st.run();
    } else if (mode === 'prep_lit') {
      const st = raw.prepare('INSERT INTO t (a) VALUES (1)');
      st.on('error', (e) => say('STMT_LISTENER: ' + e.code));
      st.run();
    } else {
      raw.run('INSERT INTO t (a) VALUES (1)');
    }
    setTimeout(() => {
      B.run('COMMIT', () => {
        const c = new sqlite3.Database(TMP);
        c.get('SELECT count(*) AS n FROM t', (e, r) => {
          say('ROWS=' + (r ? r.n : 'err:' + (e && e.code)));
          say('SUMMARY mode=' + mode + ' uncaught=' + uncaught + ' dblevel=' + dblevel);
          process.exit(0);
        });
      });
    }, 1500);
  });
});
