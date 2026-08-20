import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const { data: accts } = await sb.from('pinterest_accounts').select('user_id,account_name,created_at');
console.log('DISCONNECT FINGERPRINT — pinterest account row created AFTER pin generation started\n');
console.log('email                                 acct created  1st gen     gap(d)  gens  posted');
const out=[];
for (const a of accts||[]) {
  const { data: first } = await sb.from('urltopin_history').select('created_at').eq('user_id',a.user_id).order('created_at',{ascending:true}).limit(1);
  if (!first?.length) continue;
  const gap = (new Date(a.created_at) - new Date(first[0].created_at))/86400000;
  const { count: gens } = await sb.from('urltopin_history').select('id',{count:'exact',head:true}).eq('user_id',a.user_id);
  const { count: posted } = await sb.from('scheduled_pins').select('id',{count:'exact',head:true}).eq('user_id',a.user_id).not('pinterest_pin_id','is',null);
  out.push({ uid:a.user_id, name:a.account_name, gap, gens, posted, acct:a.created_at, first:first[0].created_at });
}
out.sort((x,y)=>y.gap-x.gap);
for (const r of out.slice(0,14)) {
  const { data:u } = await sb.auth.admin.getUserById(r.uid);
  const flag = r.gap > 1 && r.posted === 0 ? '  <-- LOST HISTORY?' : (r.gap > 1 ? '  <-- reconnected' : '');
  console.log(`${String(u?.user?.email||r.uid).padEnd(36)} ${String(r.acct).slice(0,10)}   ${String(r.first).slice(0,10)}  ${String(r.gap.toFixed(0)).padStart(5)}  ${String(r.gens).padStart(5)} ${String(r.posted).padStart(6)}${flag}`);
}
const suspicious = out.filter(r=>r.gap>1 && r.posted===0);
console.log(`\naccounts whose Pinterest row postdates their first generation AND have zero posted pins: ${suspicious.length} of ${out.length}`);
