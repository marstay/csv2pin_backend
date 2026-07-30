/**
 * Repair plan drift: subscriptions whose Dodo metadata.app_plan_type disagrees with the product
 * they are actually billed for, and any local rows/profile that followed the stale metadata.
 *
 * Dry run (default):
 *   node backend/scripts/fix-plan-drift.mjs
 * Apply:
 *   node backend/scripts/fix-plan-drift.mjs --apply
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const APPLY = process.argv.includes('--apply');
const __dirname = dirname(fileURLToPath(import.meta.url));
const envRaw = readFileSync(resolve(__dirname, '../.env'), 'utf8');

function liveCreds() {
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
const { key, base } = liveCreds();
const db = createClient(
  env.SUPABASE_URL,
  env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_SERVICE_KEY || env.SUPABASE_KEY,
  { auth: { persistSession: false } }
);

const PLAN_LIMITS = { free: 10, starter: 60, creator: 150, pro: 450, agency: 1000 };
const productToPlan = {};
const productToInterval = {};
for (const m of envRaw.matchAll(/DODO_PRODUCT_([A-Z]+?)(_ANNUAL)?_ID\s*=\s*(\S+)/g)) {
  productToPlan[m[3].trim()] = m[1].toLowerCase();
  productToInterval[m[3].trim()] = m[2] ? 'year' : 'month';
}

async function listAll(path) {
  const out = [];
  for (let p = 0; p < 100; p += 1) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(p) });
    const r = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: `Bearer ${key}` } });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

console.log(APPLY ? '*** APPLYING CHANGES ***\n' : '--- DRY RUN (pass --apply to write) ---\n');

const subs = (await listAll('/subscriptions')).filter((s) => String(s.status) === 'active');
let fixedMeta = 0;
let fixedLocal = 0;

for (const s of subs) {
  const subId = s.subscription_id || s.id;
  const r = await fetch(`${base}/subscriptions/${subId}`, { headers: { Authorization: `Bearer ${key}` } });
  if (!r.ok) continue;
  const d = await r.json();

  const truePlan = productToPlan[d.product_id];
  const trueInterval = productToInterval[d.product_id] || 'month';
  if (!truePlan) continue;
  const metaPlan = String(d.metadata?.app_plan_type || '').toLowerCase();
  const uid = d.metadata?.supabase_user_id;
  const email = d.customer?.email || '-';

  // 1) Dodo metadata repair
  if (metaPlan !== truePlan) {
    console.log(`[metadata] ${email}: "${metaPlan || '(none)'}" -> "${truePlan}" (${subId})`);
    if (APPLY) {
      const patch = await fetch(`${base}/subscriptions/${subId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
        body: JSON.stringify({
          metadata: { ...(d.metadata || {}), app_plan_type: truePlan, app_billing_interval: trueInterval },
        }),
      });
      console.log(`           PATCH -> HTTP ${patch.status}${patch.ok ? '' : ' ' + (await patch.text()).slice(0, 160)}`);
      if (patch.ok) fixedMeta += 1;
    }
  }

  if (!uid) continue;

  // 2) Local active rows must match the paid plan
  const { data: rows } = await db
    .from('billing_subscriptions')
    .select('id, plan_type, pins_limit_per_month, status, created_at')
    .eq('user_id', uid)
    .eq('status', 'active')
    .order('created_at', { ascending: false });

  for (const row of rows || []) {
    if (row.plan_type === truePlan && row.pins_limit_per_month === PLAN_LIMITS[truePlan]) continue;
    console.log(`[local] ${email}: row ${row.id.slice(0, 8)} ${row.plan_type}/${row.pins_limit_per_month} -> ${truePlan}/${PLAN_LIMITS[truePlan]}`);
    if (APPLY) {
      const { error } = await db
        .from('billing_subscriptions')
        .update({ plan_type: truePlan, pins_limit_per_month: PLAN_LIMITS[truePlan], billing_interval: trueInterval })
        .eq('id', row.id);
      if (error) console.log('        ERROR:', error.message);
      else fixedLocal += 1;
    }
  }

  // 3) profiles.plan_type must match
  const { data: prof } = await db.from('profiles').select('id, plan_type').eq('id', uid).maybeSingle();
  if (prof && prof.plan_type !== truePlan) {
    console.log(`[profile] ${email}: ${prof.plan_type} -> ${truePlan}`);
    if (APPLY) {
      const { error } = await db.from('profiles').update({ plan_type: truePlan }).eq('id', uid);
      if (error) console.log('        ERROR:', error.message);
    }
  }
}

console.log(`\n${APPLY ? 'applied' : 'would apply'} — metadata fixes: ${fixedMeta}, local row fixes: ${fixedLocal}`);
