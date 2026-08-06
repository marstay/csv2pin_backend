/**
 * Reports whether the profiles.attribution_* columns exist and how much of the
 * user base is attributed yet. Run after applying supabase/profiles_attribution.sql.
 *
 *   node backend/scripts/_attribution-check.mjs
 */
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

dotenv.config({ path: join(dirname(fileURLToPath(import.meta.url)), '..', '.env') });

const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const { count: total } = await db.from('profiles').select('id', { count: 'exact', head: true });
console.log(`profiles rows: ${total}`);

const { error } = await db.from('profiles').select('attribution_channel').limit(1);
if (error) {
  console.log(`attribution columns: NOT PRESENT (${error.message})`);
  console.log('\n-> paste supabase/profiles_attribution.sql into the Supabase SQL editor and run it');
  process.exit(0);
}

console.log('attribution columns: PRESENT');

const { data: rows } = await db
  .from('profiles')
  .select('attribution_channel, attribution_landing, plan_type')
  .not('attribution_channel', 'is', null);

console.log(`attributed profiles: ${rows.length} / ${total}`);
if (!rows.length) {
  console.log('\nNo attributed profiles yet — expected until the frontend deploy lands and someone signs up.');
  process.exit(0);
}

const byChannel = new Map();
for (const r of rows) {
  const c = r.attribution_channel;
  if (!byChannel.has(c)) byChannel.set(c, { signups: 0, paid: 0 });
  const b = byChannel.get(c);
  b.signups += 1;
  if (r.plan_type && r.plan_type !== 'free') b.paid += 1;
}

console.log('\nchannel    | signups | paid | conv');
for (const [channel, b] of [...byChannel].sort((a, b2) => b2[1].paid - a[1].paid)) {
  const conv = b.signups ? ((b.paid / b.signups) * 100).toFixed(1) + '%' : '—';
  console.log(`${channel.padEnd(10)} | ${String(b.signups).padStart(7)} | ${String(b.paid).padStart(4)} | ${conv.padStart(5)}`);
}
