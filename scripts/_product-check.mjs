/** Read-only: inspect live product billing config (subscription period vs payment
 *  frequency) to confirm new signups won't expire after 1 cycle. */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
const __dirname = dirname(fileURLToPath(import.meta.url));
const rawl = readFileSync(resolve(__dirname, '../.env'), 'utf8').split(/\r?\n/);
let section=null,key=null,base=null;
for (const line of rawl){const t=line.trim();if(/^#\s*live\b/i.test(t)){section='live';continue;}if(/^#\s*test\b/i.test(t)){section='test';continue;}const c=t.replace(/^#\s*/,'');const mk=c.match(/^DODO_API_KEY=(.+)$/);const mb=c.match(/^DODO_BASE_URL=(.+)$/);if(section==='live'){if(mk)key=mk[1].trim();if(mb)base=mb[1].trim();}}
const H={Authorization:`Bearer ${key}`};
console.log(`base=${base}\n`);

const PRODUCTS = {
  'STARTER (monthly, original)': 'pdt_0Ndb9g0FwykY0t1ocF3gX',
  'CREATOR (monthly, original)': 'pdt_0NaGpo1nQkFHLbNshs5tU',
  'PRO (monthly, original)':     'pdt_0NaGpmzdu7ElnfyOVYodG',
  'AGENCY (monthly, original)':  'pdt_0NaGpnVlIo4Dk6faELq4y',
  'STARTER clone':               'pdt_0NgyuAv3JfgARMJn6y565',
  'CREATOR clone':               'pdt_0NgyuB52osrrcb4fOL1Qd',
  'PRO clone':                   'pdt_0NgyuAyAk0xMFXszQkaN0',
  'AGENCY clone':                'pdt_0NgyuB1IsOWFmqSyThoKD',
  'STARTER annual':              'pdt_0NeJwvMqM8GY3oFrG3csl',
  'CREATOR annual':              'pdt_0NeJxO7k4xZwgytYkYC38',
  'PRO annual':                  'pdt_0NeJxeohWyVvjtIXrwCkz',
  'AGENCY annual':               'pdt_0NeJxqAJQgsWAucXhgVQN',
};

for (const [label, id] of Object.entries(PRODUCTS)) {
  const r = await fetch(`${base}/products/${id}`, { headers: H });
  if (!r.ok) { console.log(`${label.padEnd(28)} ${id}  ->  HTTP ${r.status} (not on live)`); continue; }
  const p = await r.json();
  const price = p.price || {};
  const pf = `${price.payment_frequency_interval ?? '?'}x${price.payment_frequency_count ?? '?'}`;
  const sp = `${price.subscription_period_interval ?? '?'}x${price.subscription_period_count ?? '?'}`;
  const amt = price.price != null ? (price.price/100).toFixed(2) : '?';
  const flag = /^(Month|Week|Day)/.test(sp) ? '  <-- STILL SHORT PERIOD (will expire after 1 cycle!)' : '  <-- ok (long/ongoing term)';
  console.log(`${label.padEnd(28)} ${id}`);
  console.log(`   type=${price.type||'?'}  price=${price.currency||''} ${amt}  pay_every=${pf}  subscription_period=${sp}  trial=${price.trial_period_days??0}${flag}\n`);
}
