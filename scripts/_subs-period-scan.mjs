/** Read-only: list every subscription's billing PERIOD vs payment frequency,
 *  to find subs configured to expire after 1 period instead of renewing. */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
const __dirname = dirname(fileURLToPath(import.meta.url));
const raw = readFileSync(resolve(__dirname, '../.env'), 'utf8').split(/\r?\n/);
let section=null,key=null,base=null;
for (const line of raw){const t=line.trim();if(/^#\s*live\b/i.test(t)){section='live';continue;}if(/^#\s*test\b/i.test(t)){section='test';continue;}const c=t.replace(/^#\s*/,'');const mk=c.match(/^DODO_API_KEY=(.+)$/);const mb=c.match(/^DODO_BASE_URL=(.+)$/);if(section==='live'){if(mk)key=mk[1].trim();if(mb)base=mb[1].trim();}}
const H={Authorization:`Bearer ${key}`};
async function listAll(path){const out=[];for(let p=0;p<300;p++){const qs=new URLSearchParams({page_size:'100',page_number:String(p)});const r=await fetch(`${base}${path}?${qs}`,{headers:H});if(!r.ok)break;const j=await r.json();const items=Array.isArray(j)?j:j.items||j.data||[];out.push(...items);if(items.length<100)break;}return out;}
const emailOf=(o)=>(o.customer?.email||o.customer_email||'?').toLowerCase();
const subs=await listAll('/subscriptions');
async function getSub(id){try{const r=await fetch(`${base}/subscriptions/${id}`,{headers:H});if(r.ok)return await r.json();}catch{}return null;}
const rows=[];
for(const s of subs){
  const id=s.subscription_id||s.id;
  const f=(await getSub(id))||s;
  rows.push({
    id,
    created:String(f.created_at||'').slice(0,10),
    status:String(f.status||'?').toLowerCase(),
    period:`${f.subscription_period_interval||'?'}x${f.subscription_period_count??'?'}`,
    freq:`${f.payment_frequency_interval||'?'}x${f.payment_frequency_count??'?'}`,
    expires:String(f.expires_at||'').slice(0,10),
    email:emailOf(f),
  });
}
rows.sort((a,b)=>a.created<b.created?1:-1);
console.log('created     status     period      freq        expires     email');
for(const r of rows){
  const flag = (r.period.startsWith('Month')||r.period.startsWith('Week')||r.period.startsWith('Day')) ? '  <-- expires after 1 period (no renewal)' : '';
  console.log(`${r.created}  ${r.status.padEnd(9)}  ${r.period.padEnd(10)}  ${r.freq.padEnd(10)}  ${r.expires.padEnd(10)}  ${r.email}${flag}`);
}
const broken = rows.filter(r=>/^(Month|Week|Day)/.test(r.period));
const brokenActive = broken.filter(r=>r.status==='active');
console.log(`\nTotal subs: ${rows.length}`);
console.log(`Subs with short period (will/did expire after 1 cycle): ${broken.length}`);
console.log(`  ...currently ACTIVE (time bomb — will expire at next_billing): ${brokenActive.length}`);
brokenActive.forEach(r=>console.log(`     ${r.id}  ${r.created}  ${r.email}  expires ${r.expires}`));
