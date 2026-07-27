/**
 * Read-only: where did one customer come from?
 * Pulls the Dodo subscription metadata (referral_key), the Supabase profile
 * referral slug, signup->paid timing, and any affiliate commission.
 * Usage: node backend/scripts/_attribution-lookup.mjs sales@el-linc.com
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');
dotenv.config({ path: envPath });

const TARGET = (process.argv[2] || '').toLowerCase().trim();
if (!TARGET) { console.error('Pass an email.'); process.exit(1); }

// Live Dodo creds live in a commented "# live" section of .env.
function extractLiveCreds() {
  const raw = readFileSync(envPath, 'utf8').split(/\r?\n/);
  let section = null, key = null, base = null;
  for (const line of raw) {
    const t = line.trim();
    if (/^#\s*live\b/i.test(t)) { section = 'live'; continue; }
    if (/^#\s*test\b/i.test(t)) { section = 'test'; continue; }
    const c = t.replace(/^#\s*/, '');
    const mk = c.match(/^DODO_API_KEY=(.+)$/);
    const mb = c.match(/^DODO_BASE_URL=(.+)$/);
    if (section === 'live') { if (mk) key = mk[1].trim(); if (mb) base = mb[1].trim(); }
  }
  return { key, base };
}

const emailOf = (o) => (o.customer?.email || o.customer_email || '').toLowerCase();

async function dodoAttribution() {
  const { key, base } = extractLiveCreds();
  if (!key || !base) { console.log('(no live Dodo creds — skipping Dodo)'); return; }
  const listAll = async (path) => {
    const out = [];
    for (let page = 0; page < 200; page++) {
      const qs = new URLSearchParams({ page_size: '100', page_number: String(page) });
      const resp = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: `Bearer ${key}` } });
      if (!resp.ok) break;
      const json = await resp.json();
      const items = Array.isArray(json) ? json : json.items || json.data || [];
      out.push(...items);
      if (items.length < 100) break;
    }
    return out;
  };
  const subs = (await listAll('/subscriptions')).filter((s) => emailOf(s) === TARGET);
  console.log(`\n=== DODO SUBSCRIPTIONS (${subs.length}) ===`);
  for (const s of subs) {
    console.log(JSON.stringify({
      id: s.subscription_id || s.id,
      status: s.status,
      created_at: s.created_at,
      amount: s.recurring_pre_tax_amount ?? s.amount,
      currency: s.currency,
      product_id: s.product_id,
      metadata: s.metadata || {},
      discount_code: s.discount_code || s.discount_id || null,
    }, null, 2));
  }
  const pays = (await listAll('/payments')).filter((p) => emailOf(p) === TARGET);
  console.log(`\n=== DODO PAYMENTS (${pays.length}) ===`);
  for (const p of pays.sort((a, b) => (String(a.created_at) < String(b.created_at) ? 1 : -1))) {
    console.log(`${String(p.created_at).slice(0, 19)}  ${String(p.status).padEnd(10)}  ${p.currency} ${p.total_amount ?? p.amount}  meta=${JSON.stringify(p.metadata || {})}`);
  }
}

async function supabaseAttribution() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) { console.log('\n(no Supabase creds — skipping Supabase)'); return; }
  const sb = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

  // Resolve email -> auth user (paginate).
  let user = null;
  for (let page = 1; page <= 40 && !user; page++) {
    const { data, error } = await sb.auth.admin.listUsers({ page, perPage: 200 });
    if (error) { console.log('auth listUsers error:', error.message); break; }
    const users = data?.users || [];
    user = users.find((u) => String(u.email || '').toLowerCase() === TARGET) || null;
    if (users.length < 200) break;
  }
  console.log('\n=== SUPABASE ===');
  if (!user) { console.log('No auth user found for that email.'); return; }
  console.log(`auth user id: ${user.id}`);
  console.log(`signed up:    ${user.created_at}`);

  const { data: profile } = await sb
    .from('profiles')
    .select('plan_type, referred_by_affiliate_slug, created_at')
    .eq('id', user.id)
    .maybeSingle();
  console.log(`profile:      ${JSON.stringify(profile || {})}`);

  const { data: subs } = await sb
    .from('billing_subscriptions')
    .select('plan_type, status, billing_interval, created_at, current_period_start, dodo_subscription_id')
    .eq('user_id', user.id)
    .order('created_at', { ascending: false });
  console.log(`billing subs: ${JSON.stringify(subs || [], null, 2)}`);

  const { data: comms } = await sb
    .from('affiliate_commissions')
    .select('affiliate_id, plan_type, amount_cents, commission_cents, status, commission_kind, created_at')
    .eq('referred_user_id', user.id);
  console.log(`commissions:  ${JSON.stringify(comms || [])}`);

  // Signup -> paid timing.
  if (subs && subs[0] && user.created_at) {
    const days = (new Date(subs[0].created_at) - new Date(user.created_at)) / 86400000;
    console.log(`signup → paid: ${days.toFixed(1)} days`);
  }
}

await dodoAttribution();
await supabaseAttribution();
console.log('\nNote: UTM params are not stored server-side (they live in the visitor\'s Google Analytics only).');
