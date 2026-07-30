/**
 * Read-only: find customers whose PAID plan (Dodo product) disagrees with what the app
 * thinks they're on.
 *
 * Root cause this detects: the change-plan flow updates Dodo's product/price and the local
 * billing_subscriptions row, but NOT the Dodo subscription's metadata.app_plan_type.
 * On renewal, the webhook trusts metadata first, so the customer silently reverts to the
 * plan they ORIGINALLY checked out with — while still being charged the new price.
 *
 * Usage: node backend/scripts/_plan-drift-audit.mjs
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');
const envRaw = readFileSync(envPath, 'utf8');

function extractLiveCreds() {
  let section = null, key = null, base = null;
  for (const line of envRaw.split(/\r?\n/)) {
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

function plainEnv() {
  const out = {};
  for (const l of envRaw.split(/\r?\n/)) {
    const t = l.trim().replace(/^#\s*/, '');
    const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
    if (m && !out[m[1]]) out[m[1]] = m[2].trim();
  }
  return out;
}

const env = plainEnv();
const { key, base } = extractLiveCreds();
const db = createClient(
  env.SUPABASE_URL,
  env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_SERVICE_KEY || env.SUPABASE_KEY,
  { auth: { persistSession: false } }
);

// product_id -> plan name, from the .env product map
const productToPlan = {};
for (const m of envRaw.matchAll(/DODO_PRODUCT_([A-Z]+?)(?:_ANNUAL)?_ID\s*=\s*(\S+)/g)) {
  productToPlan[m[2].trim()] = m[1].toLowerCase();
}

async function listAll(path) {
  const out = [];
  for (let page = 0; page < 100; page += 1) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(page) });
    const r = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: `Bearer ${key}` } });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

const subs = (await listAll('/subscriptions')).filter((s) => String(s.status) === 'active');
console.log(`active Dodo subscriptions: ${subs.length}\n`);

const rows = [];
for (const s of subs) {
  const subId = s.subscription_id || s.id;
  // Fetch detail for metadata (list endpoint often omits it)
  const r = await fetch(`${base}/subscriptions/${subId}`, { headers: { Authorization: `Bearer ${key}` } });
  const d = r.ok ? await r.json() : s;
  const truePlan = productToPlan[d.product_id] || `?(${d.product_id})`;
  const metaPlan = String(d.metadata?.app_plan_type || d.metadata?.plan_type || '(none)').toLowerCase();
  const uid = d.metadata?.supabase_user_id || null;

  let localPlan = '(no row)';
  if (uid) {
    const { data } = await db
      .from('billing_subscriptions')
      .select('plan_type')
      .eq('user_id', uid)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1);
    localPlan = data?.[0]?.plan_type || '(no active row)';
  }

  rows.push({
    email: d.customer?.email || '-',
    amount: d.recurring_pre_tax_amount ?? d.amount ?? 0,
    truePlan,
    metaPlan,
    localPlan,
    subId,
    drift: truePlan !== metaPlan || truePlan !== localPlan,
  });
}

console.log('email                          | paid  | true plan | metadata  | app sees  | DRIFT');
for (const r of rows.sort((a, b) => Number(b.drift) - Number(a.drift))) {
  console.log(
    [
      String(r.email).slice(0, 30).padEnd(30),
      String(r.amount).padStart(5),
      String(r.truePlan).padEnd(9),
      String(r.metaPlan).padEnd(9),
      String(r.localPlan).padEnd(9),
      r.drift ? '*** YES ***' : 'ok',
    ].join(' | ')
  );
}

const bad = rows.filter((r) => r.drift);
console.log(`\nAFFECTED: ${bad.length} of ${rows.length} active subscriptions`);
const under = bad.filter((r) => r.localPlan !== r.truePlan);
console.log(`Being under-served (app plan < paid plan): ${under.length}`);
for (const r of under) console.log(`  ${r.email} — paying for ${r.truePlan}, app gives ${r.localPlan} (${r.subId})`);
