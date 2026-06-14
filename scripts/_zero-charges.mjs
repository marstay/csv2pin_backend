/** Read-only: list all $0 (and near-$0) payment records, newest first, to see
 *  which customers got zero-amount payment events during the migration testing. */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
const __dirname = dirname(fileURLToPath(import.meta.url));
const rawl = readFileSync(resolve(__dirname, '../.env'), 'utf8').split(/\r?\n/);
let section=null,key=null,base=null;
for (const line of rawl){const t=line.trim();if(/^#\s*live\b/i.test(t)){section='live';continue;}if(/^#\s*test\b/i.test(t)){section='test';continue;}const c=t.replace(/^#\s*/,'');const mk=c.match(/^DODO_API_KEY=(.+)$/);const mb=c.match(/^DODO_BASE_URL=(.+)$/);if(section==='live'){if(mk)key=mk[1].trim();if(mb)base=mb[1].trim();}}
const H={Authorization:`Bearer ${key}`};
async function listAll(p){const out=[];for(let i=0;i<300;i++){const qs=new URLSearchParams({page_size:'100',page_number:String(i)});const r=await fetch(`${base}${p}?${qs}`,{headers:H});if(!r.ok)break;const j=await r.json();const it=Array.isArray(j)?j:j.items||j.data||[];out.push(...it);if(it.length<100)break;}return out;}
const eo=(o)=>(o.customer?.email||o.customer_email||'?').toLowerCase();
const pays=await listAll('/payments');
const zero=pays.filter(p=>Number(p.total_amount??p.amount??0)===0).sort((a,b)=>String(a.created_at)<String(b.created_at)?1:-1);
console.log(`Total payments: ${pays.length}   Zero-amount payments: ${zero.length}\n`);
for(const p of zero) console.log(`${String(p.created_at).slice(0,19)}  ${String(p.status).padEnd(12)}  ${p.currency} 0  ${eo(p)}  ${p.payment_id||p.id||''}`);
