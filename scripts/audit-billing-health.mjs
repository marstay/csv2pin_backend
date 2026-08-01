/**
 * Full billing-health audit: Supabase internal consistency + Dodo cross-consistency,
 * with specific attention to the two flows that have bitten us — RENEWAL (second payment)
 * and PLAN CHANGE (upgrade/downgrade).
 *
 * Read-only. Usage: node backend/scripts/audit-billing-health.mjs
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

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
const ACTIVE_STATES = new Set(['active', 'trialing', 'past_due']);
const productToPlan = {};
const productToInterval = {};
// _LEGACYn ids map grandfathered customers on superseded pricing; without them every customer
// still on old products reads as an unknown plan.
for (const m of envRaw.matchAll(/DODO_PRODUCT_([A-Z]+?)(_ANNUAL)?(_LEGACY\d*)?_ID\s*=\s*(\S+)/g)) {
  productToPlan[m[4].trim()] = m[1].toLowerCase();
  productToInterval[m[4].trim()] = m[2] ? 'year' : 'month';
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

const findings = [];
const add = (sev, area, msg, detail) => findings.push({ sev, area, msg, detail });

// ---------------------------------------------------------------- load data
const { data: subs } = await db.from('billing_subscriptions').select('*');
const { data: profiles } = await db.from('profiles').select('id, plan_type, created_at');
const { data: usage } = await db.from('pin_usage').select('user_id, year_month, pins_used, user_photo_pins_used');
const dodoSubs = await listAll('/subscriptions');
const dodoPayments = await listAll('/payments');
const dodoRefunds = await listAll('/refunds');

const profileById = new Map((profiles || []).map((p) => [p.id, p]));
const subsByUser = new Map();
for (const s of subs || []) {
  if (!subsByUser.has(s.user_id)) subsByUser.set(s.user_id, []);
  subsByUser.get(s.user_id).push(s);
}

console.log(`loaded: ${subs?.length} local subs · ${profiles?.length} profiles · ${usage?.length} usage rows`);
console.log(`dodo:   ${dodoSubs.length} subs · ${dodoPayments.length} payments · ${dodoRefunds.length} refunds\n`);

// ------------------------------------------------- A. Supabase consistency
let dupUsers = 0;
for (const [uid, rows] of subsByUser) {
  const active = rows.filter((r) => ACTIVE_STATES.has(String(r.status)));
  if (active.length > 1) {
    dupUsers += 1;
    const plans = [...new Set(active.map((a) => a.plan_type))];
    if (plans.length > 1) {
      add('HIGH', 'duplicates', `user ${uid.slice(0, 8)} has ${active.length} active rows with DIFFERENT plans: ${plans.join(', ')}`, uid);
    }
  }
  // limit must match plan
  for (const r of active) {
    const expect = PLAN_LIMITS[r.plan_type];
    if (expect && r.pins_limit_per_month !== expect) {
      add('MED', 'limits', `user ${uid.slice(0, 8)} active ${r.plan_type} row has limit ${r.pins_limit_per_month}, expected ${expect}`, uid);
    }
  }
  // expired but still active
  for (const r of active) {
    if (r.current_period_end && new Date(r.current_period_end).getTime() < Date.now()) {
      add('HIGH', 'expired', `user ${uid.slice(0, 8)} ${r.plan_type} row still 'active' but period ended ${String(r.current_period_end).slice(0, 10)}`, uid);
    }
  }
  // profile vs active sub
  const newestActive = active.sort((a, b) => String(b.created_at) < String(a.created_at) ? -1 : 1)[0];
  const prof = profileById.get(uid);
  if (newestActive && prof && prof.plan_type !== newestActive.plan_type) {
    add('HIGH', 'profile-drift', `user ${uid.slice(0, 8)} profile=${prof.plan_type} but active sub=${newestActive.plan_type}`, uid);
  }
  if (newestActive && !prof) {
    add('MED', 'missing-profile', `user ${uid.slice(0, 8)} has active ${newestActive.plan_type} sub but NO profile row`, uid);
  }
}
console.log(`A. duplicate active rows: ${dupUsers} users affected (same-plan dupes are cosmetic; different-plan dupes are flagged)`);

// paid profile with no active sub = free access
let ghostPaid = 0;
for (const p of profiles || []) {
  const pt = String(p.plan_type || 'free').toLowerCase();
  if (pt === 'free' || !PLAN_LIMITS[pt]) continue;
  const rows = subsByUser.get(p.id) || [];
  if (!rows.some((r) => ACTIVE_STATES.has(String(r.status)))) {
    ghostPaid += 1;
    add('HIGH', 'ghost-paid', `profile ${p.id.slice(0, 8)} says plan=${pt} but has NO active subscription (free paid access)`, p.id);
  }
}
console.log(`A. profiles on a paid plan with no active subscription: ${ghostPaid}`);

// ------------------------------------------------- B. Dodo cross-consistency
const dodoActive = dodoSubs.filter((s) => String(s.status) === 'active');
const localActiveByDodoId = new Map();
for (const s of subs || []) {
  if (ACTIVE_STATES.has(String(s.status)) && s.dodo_subscription_id) {
    localActiveByDodoId.set(s.dodo_subscription_id, s);
  }
}

for (const d of dodoActive) {
  const id = d.subscription_id || d.id;
  const local = localActiveByDodoId.get(id);
  const truePlan = productToPlan[d.product_id];
  if (!local) {
    add('CRITICAL', 'paying-no-access', `Dodo sub ${id} (${d.customer?.email}) is ACTIVE but has no active local row — customer paying with no access`, id);
    continue;
  }
  if (truePlan && local.plan_type !== truePlan) {
    add('CRITICAL', 'plan-drift', `${d.customer?.email}: paying for ${truePlan}, app serves ${local.plan_type}`, id);
  }
  const trueInterval = productToInterval[d.product_id];
  if (trueInterval && local.billing_interval && local.billing_interval !== trueInterval) {
    add('MED', 'interval-drift', `${d.customer?.email}: Dodo interval=${trueInterval}, local=${local.billing_interval}`, id);
  }
}

// local active rows whose Dodo sub is NOT active = free access
const dodoActiveIds = new Set(dodoActive.map((d) => d.subscription_id || d.id));
const dodoAllById = new Map(dodoSubs.map((d) => [d.subscription_id || d.id, d]));
let freeloaders = 0;
for (const [dodoId, local] of localActiveByDodoId) {
  if (String(dodoId).startsWith('comp:')) continue; // intentional comped access
  if (dodoActiveIds.has(dodoId)) continue;
  const d = dodoAllById.get(dodoId);
  freeloaders += 1;
  add('HIGH', 'access-no-payment', `local active ${local.plan_type} row for user ${local.user_id.slice(0, 8)} but Dodo sub ${dodoId} is ${d ? d.status : 'MISSING'}`, dodoId);
}
console.log(`B. Dodo active w/o local access: ${findings.filter((f) => f.area === 'paying-no-access').length}`);
console.log(`B. local access w/o active Dodo sub: ${freeloaders}`);

// ------------------------------------------------------------- C. payments
const succeeded = dodoPayments.filter((p) => String(p.status) === 'succeeded');
const zero = succeeded.filter((p) => Number(p.total_amount ?? 0) === 0);
if (zero.length) add('LOW', 'zero-payments', `${zero.length} succeeded payments with total_amount = 0`, null);

const refundedPayments = new Set(dodoRefunds.map((r) => r.payment_id));
console.log(`C. succeeded payments: ${succeeded.length} · zero-amount: ${zero.length} · refunds: ${dodoRefunds.length}`);

// refunded customers who still have active access
for (const r of dodoRefunds) {
  const pay = dodoPayments.find((p) => (p.payment_id || p.id) === r.payment_id);
  const email = pay?.customer?.email;
  if (!email) continue;
  const stillActive = dodoActive.find((d) => d.customer?.email === email);
  if (stillActive) {
    add('LOW', 'refund-active', `${email} has a refund but an active subscription (may be legitimate re-subscribe)`, r.payment_id);
  }
}

// --------------------------------------------- D. renewal / upgrade risk
console.log('\nD. RENEWAL & UPGRADE RISK');
const soon = dodoActive
  .map((d) => ({ d, when: new Date(d.next_billing_date).getTime() }))
  .filter((x) => Number.isFinite(x.when))
  .sort((a, b) => a.when - b.when);

for (const { d, when } of soon.slice(0, 8)) {
  const id = d.subscription_id || d.id;
  const truePlan = productToPlan[d.product_id] || '?';
  const days = Math.round((when - Date.now()) / 86400000);
  console.log(`   ${String(d.customer?.email || '-').slice(0, 30).padEnd(30)} renews in ${String(days).padStart(3)}d as ${truePlan}`);
}

// annual subs use a calendar-month usage bucket; monthly use billing-period
const annual = dodoActive.filter((d) => productToInterval[d.product_id] === 'year');
if (annual.length) {
  console.log(`   annual subscriptions: ${annual.length} (usage resets on calendar month, not billing period — by design)`);
}

// usage buckets that are non-zero at the very start of a period
const nowMs = Date.now();
for (const [uid, rows] of subsByUser) {
  const active = rows.filter((r) => ACTIVE_STATES.has(String(r.status)) && r.current_period_start);
  if (!active.length) continue;
  const r = active[0];
  const ageH = (nowMs - new Date(r.current_period_start).getTime()) / 3600000;
  if (ageH > 24 || ageH < 0) continue; // only look at freshly-started periods
  const bucket = String(r.current_period_start).slice(0, 10);
  const u = (usage || []).find((x) => x.user_id === uid && x.year_month === bucket);
  if (u && Number(u.pins_used) > 0) {
    add('HIGH', 'usage-carryover', `user ${uid.slice(0, 8)} period started ${ageH.toFixed(1)}h ago but bucket already shows ${u.pins_used} pins used`, uid);
  }
}

// ---------------------------------------------------------------- report
console.log('\n================ FINDINGS ================');
const order = { CRITICAL: 0, HIGH: 1, MED: 2, LOW: 3 };
findings.sort((a, b) => order[a.sev] - order[b.sev]);
if (!findings.length) console.log('No issues found.');
for (const f of findings) console.log(`[${f.sev.padEnd(8)}] ${f.area.padEnd(18)} ${f.msg}`);
console.log(`\ntotal findings: ${findings.length}`);
for (const sev of ['CRITICAL', 'HIGH', 'MED', 'LOW']) {
  const n = findings.filter((f) => f.sev === sev).length;
  if (n) console.log(`  ${sev}: ${n}`);
}
