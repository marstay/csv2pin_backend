/** Read-only: detail on expired subs (involuntary churn candidates). */
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
async function getSub(id){const r=await fetch(`${base}/subscriptions/${id}`,{headers:H});return r.ok?r.json():null;}
const subs=await listAll('/subscriptions');
const expired=subs.filter(s=>String(s.status).toLowerCase()==='expired');
for(const s of expired){
  const f=await getSub(s.subscription_id||s.id)||s;
  console.log(`${eo(f)}`);
  console.log(`   plan=${f.metadata?.app_plan_type||'?'}  amount=${f.currency} ${(f.recurring_pre_tax_amount/100).toFixed(2)}  period=${f.subscription_period_interval}x${f.subscription_period_count}`);
  console.log(`   created=${String(f.created_at).slice(0,10)}  expired/next=${String(f.next_billing_date).slice(0,10)}  cancel_at_next=${f.cancel_at_next_billing_date}  cancelled_at=${f.cancelled_at||'null'}`);
  console.log(`   referral=${f.metadata?.affonso_referral||f.metadata?.referral_key||'none'}  sub=${f.subscription_id}\n`);
}
console.log(`Total expired: ${expired.length}`);
