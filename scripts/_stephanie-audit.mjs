/**
 * Read-only: audit every account/subscription tied to a customer email.
 * Usage: node backend/scripts/_stephanie-audit.mjs mywinefund@gmail.com
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');
const TARGET = (process.argv[2] || '').toLowerCase().trim();

function readEnv() {
  const out = {};
  for (const line of readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const t = line.trim().replace(/^#\s*/, '');
    const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
    if (m && !out[m[1]]) out[m[1]] = m[2].trim();
  }
  return out;
}

const env = readEnv();
const db = createClient(
  env.SUPABASE_URL,
  env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_SERVICE_KEY || env.SUPABASE_KEY,
  { auth: { persistSession: false } }
);

// 1) auth users matching the email (catches +aliases and duplicates)
const { data: authList } = await db.auth.admin.listUsers({ page: 1, perPage: 1000 });
const users = (authList?.users || []).filter((u) =>
  String(u.email || '').toLowerCase().includes(TARGET.split('@')[0])
);
console.log(`=== AUTH USERS matching "${TARGET.split('@')[0]}": ${users.length}`);
for (const u of users) {
  console.log(`  ${u.id} | ${u.email} | created ${String(u.created_at).slice(0, 19)} | last_sign_in ${String(u.last_sign_in_at || '-').slice(0, 19)}`);
}

const ids = users.map((u) => u.id);
if (!ids.length) {
  console.log('No auth users found.');
  process.exit(0);
}

// 2) profiles
const { data: profiles } = await db
  .from('profiles')
  .select('id, email, plan_type, pins_used, pins_limit, updated_at')
  .in('id', ids);
console.log(`\n=== PROFILES: ${(profiles || []).length}`);
for (const p of profiles || []) {
  console.log(`  ${p.id} | ${p.email} | plan=${p.plan_type} | used=${p.pins_used ?? '-'} | limit=${p.pins_limit ?? '-'} | updated ${String(p.updated_at || '').slice(0, 19)}`);
}

// 3) billing_subscriptions — the table known to carry duplicate rows
const { data: subs } = await db
  .from('billing_subscriptions')
  .select('id, user_id, plan_type, status, billing_interval, dodo_subscription_id, current_period_end, created_at')
  .in('user_id', ids)
  .order('created_at', { ascending: false });
console.log(`\n=== BILLING_SUBSCRIPTIONS: ${(subs || []).length}`);
for (const s of subs || []) {
  console.log(
    `  ${String(s.status).padEnd(9)} | ${String(s.plan_type).padEnd(8)} | ${String(s.billing_interval || '-').padEnd(7)} | user=${s.user_id.slice(0, 8)} | sub=${String(s.dodo_subscription_id || '-').slice(0, 22)} | period_end=${String(s.current_period_end || '-').slice(0, 10)} | created=${String(s.created_at).slice(0, 19)}`
  );
}

// 4) what the app would resolve as the active plan, per user
console.log('\n=== ACTIVE-SUB RESOLUTION (mirrors getActiveSubscriptionForUser ordering) ===');
for (const id of ids) {
  const mine = (subs || []).filter((s) => s.user_id === id);
  const active = mine.filter((s) => ['active', 'trialing', 'past_due'].includes(String(s.status)));
  const profile = (profiles || []).find((p) => p.id === id);
  console.log(`  user ${id.slice(0, 8)} | profile.plan_type=${profile?.plan_type} | active rows=${active.length} -> [${active.map((a) => a.plan_type).join(', ')}]`);
}
