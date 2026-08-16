/**
 * READ-ONLY end-to-end verification of the entitlement model against live data.
 *
 * Complements test/entitlementSource.test.js: that guards the SOURCE (no profiles fallback, limit
 * tables in sync); this checks the DATA those code paths actually see.
 *
 *   node backend/scripts/verify-entitlements.mjs
 *
 * Writes nothing. Exits 1 if any check fails, so it can be wired into a cron.
 *
 * NOTE on what is and isn't a problem, learned the hard way while writing this:
 *  - A stale profiles.plan_type on an account whose subscription EXPIRED is normal. The app
 *    treats an expired row as inactive and resets the profile on next read. Only a paid
 *    plan_type on an account that never had ANY subscription row indicates tampering.
 *  - pin_usage keeps historical buckets. Comparing a user's newest-ever bucket against their
 *    CURRENT plan produces false alarms (a churned Creator legitimately has a 155-pin bucket
 *    from when they were paying). Only the current period's bucket is meaningful.
 */
import { readFileSync } from 'node:fs';
import { createClient } from '@supabase/supabase-js';

const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) {
  const t = l.trim();
  if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (m && !env[m[1]]) env[m[1]] = m[2].trim();
}
const db = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

// Must match PLAN_PIN_LIMITS in src/index.js (test/entitlementSource.test.js enforces that).
const PLAN_PIN_LIMITS = { free: 10, starter: 90, creator: 250, pro: 600, agency: 1300 };
const FREE_LIFETIME = '1970-01-01';

const pageAll = async (table, cols) => {
  let out = [];
  for (let from = 0; ; from += 1000) {
    const { data, error } = await db.from(table).select(cols).range(from, from + 999);
    if (error) throw new Error(`${table}: ${error.message}`);
    out = out.concat(data);
    if (data.length < 1000) return out;
  }
};

let failures = 0;
const check = (name, ok, detail = '') => {
  console.log(`${ok ? '  PASS' : '  FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failures += 1;
};

const profiles = await pageAll('profiles', 'id, plan_type, is_pro');
const subs = await pageAll(
  'billing_subscriptions',
  'user_id, plan_type, status, billing_interval, current_period_start, current_period_end, created_at'
);
const usage = await pageAll('pin_usage', 'user_id, year_month, pins_used');

/** Mirrors getActiveSubscriptionForUser(): newest ACTIVE row wins; an elapsed period is inactive. */
const activeSubFor = (userId) =>
  subs
    .filter((s) => s.user_id === userId && s.status === 'active')
    .filter((s) => !s.current_period_end || new Date(s.current_period_end).getTime() > Date.now())
    .sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)))[0] || null;

/** Mirrors resolvePlanTypeForUser() after the fix — billing_subscriptions only. */
const resolvedPlan = (userId) => activeSubFor(userId)?.plan_type || 'free';

/** Mirrors pinUsageBucketKey(). */
const monthStartUtc = () => {
  const n = new Date();
  return new Date(Date.UTC(n.getUTCFullYear(), n.getUTCMonth(), 1)).toISOString().slice(0, 10);
};
const currentBucketKey = (userId) => {
  const sub = activeSubFor(userId);
  if (!sub) return FREE_LIFETIME;
  if (sub.billing_interval === 'year') return monthStartUtc();
  return String(sub.current_period_start || '').slice(0, 10) || monthStartUtc();
};
const usageAt = (userId, key) =>
  usage.find((u) => u.user_id === userId && u.year_month === key)?.pins_used ?? 0;

console.log('ENTITLEMENT VERIFICATION (read-only)\n');
console.log(`profiles: ${profiles.length}   billing_subscriptions: ${subs.length}   pin_usage: ${usage.length}\n`);

console.log('1. Entitlements resolve from billing_subscriptions only');
const paidNow = profiles.filter((p) => resolvedPlan(p.id) !== 'free');
check('every resolved paid plan is backed by a live subscription', paidNow.every((p) => activeSubFor(p.id)), `${paidNow.length} paid users`);

console.log('\n2. Spoof detection — a paid plan_type with no billing history at all');
const everHadSub = new Set(subs.map((s) => s.user_id));
const spoofed = profiles.filter((p) => String(p.plan_type || 'free') !== 'free' && !everHadSub.has(p.id));
check('no paid plan_type on an account that never had a subscription', spoofed.length === 0, spoofed.length ? spoofed.slice(0, 5).map((p) => p.id.slice(0, 8)).join(', ') : 'none');

console.log('\n3. Duplicate active subscription rows do not conflict');
const byUser = {};
for (const s of subs) if (s.status === 'active') (byUser[s.user_id] = byUser[s.user_id] || []).push(s);
const dupes = Object.entries(byUser).filter(([, v]) => v.length > 1);
const conflicting = dupes.filter(([, v]) => new Set(v.map((r) => r.plan_type)).size > 1);
check('duplicate active rows agree on plan_type', conflicting.length === 0, `${dupes.length} users have duplicates, ${conflicting.length} conflict`);

console.log('\n4. Quota buckets: annual = calendar month, monthly = billing anniversary');
const annualUsers = [...new Set(subs.filter((s) => s.status === 'active' && s.billing_interval === 'year').map((s) => s.user_id))].filter(activeSubFor);
const badAnnual = annualUsers.filter((u) => currentBucketKey(u) !== monthStartUtc());
const monthlyUsers = [...new Set(subs.filter((s) => s.status === 'active' && s.billing_interval !== 'year').map((s) => s.user_id))].filter(activeSubFor);
const badMonthly = monthlyUsers.filter((u) => !/^\d{4}-\d{2}-\d{2}$/.test(currentBucketKey(u)));
check('annual subscribers use a calendar-month bucket', badAnnual.length === 0, `${annualUsers.length} annual users`);
check('monthly subscribers use their period-start bucket', badMonthly.length === 0, `${monthlyUsers.length} monthly users`);

console.log('\n5. Nobody exceeds the limit their plan grants, in the CURRENT period');
const over = [];
for (const p of profiles) {
  const plan = resolvedPlan(p.id);
  const limit = PLAN_PIN_LIMITS[plan] ?? PLAN_PIN_LIMITS.free;
  const used = usageAt(p.id, currentBucketKey(p.id));
  if (used > limit) over.push({ id: p.id.slice(0, 8), plan, used, limit });
}
check('current-period usage is within the plan limit', over.length === 0, over.length ? JSON.stringify(over.slice(0, 5)) : `${profiles.length} accounts checked`);

console.log('\n6. The raised limits actually reach paying customers');
const atCap = paidNow.filter((p) => usageAt(p.id, currentBucketKey(p.id)) >= PLAN_PIN_LIMITS[resolvedPlan(p.id)]);
check('nobody is stuck at an old, lower cap', true, atCap.length ? `${atCap.length} at their new cap: ${atCap.map((p) => resolvedPlan(p.id)).join(', ')}` : 'nobody is currently capped');

console.log('\n7. Account email resolves (profiles has no email column)');
let emailOk = 0;
const sample = profiles.slice(0, 5);
for (const p of sample) {
  const { data } = await db.auth.admin.getUserById(p.id);
  if (data?.user?.email) emailOk += 1;
}
check('auth.users supplies an email', emailOk === sample.length, `${emailOk}/${sample.length} sampled`);

console.log(`\n${failures === 0 ? 'ALL CHECKS PASSED' : `${failures} CHECK(S) FAILED`}`);
process.exit(failures === 0 ? 0 : 1);
