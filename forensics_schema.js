const Database=require('better-sqlite3');
const fs=require('fs');
const db=new Database('trading.db',{readonly:true});
const tabs=db.prepare("select name from sqlite_master where type='table'").all().map(t=>t.name);
const out=[];
out.push('TABLES: '+tabs.join(','));
for(const t of tabs){
  const cols=db.prepare(`pragma table_info("${t}")`).all();
  out.push(`-- ${t} :: `+cols.map(c=>`${c.name}:${c.type}`).join(', '));
  const texts=cols.filter(c=>/TEXT/i.test(c.type)||!c.type);
  for(const c of texts){
    try{
      const rows=db.prepare(`select * from "${t}" where "${c.name}" like ? limit 25`).all('%26NOV031330%');
      if(rows.length) out.push(`HIT ${t}.${c.name} -> ${rows.length}\n`+rows.map(r=>JSON.stringify(r)).join('\n'));
    }catch(e){}
  }
}
fs.writeFileSync('forensics_schema.txt',out.join('\n'));
console.log('wrote',out.length,'lines');
