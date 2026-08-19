/**
 * One-off campaign: email current MONTHLY subscribers an offer to switch to
 * annual billing (cash up front + lower churn). Idempotent via the email_events
 * table (key 'annual_push'), so re-running won't double-email anyone.
 *
 * DRY RUN by default — prints who would be emailed and the savings. Pass --send
 * to actually deliver.
 *
 * Usage:
 *   node backend/scripts/send-annual-push.mjs            # dry run
 *   node backend/scripts/send-annual-push.mjs --send     # actually send
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const SEND = process.argv.includes('--send');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

const ANNUAL_PRICE = { starter: 108, creator: 225, pro: 495, agency: 1161 };
const MONTHLY_PRICE = { starter: 12, creator: 25, pro: 55, agency: 129 };
// Grandfathered rates. These customers are NOT good targets for this campaign: at legacy prices
// the new annual plans are break-even or worse (legacy Pro would pay $27/yr MORE), so the email
// renderer refuses to make a savings claim for them and they get skipped.
const LEGACY_MONTHLY_PRICE = { starter: 9, creator: 19, pro: 39, agency: 79 };

const { sendAnnualUpgradeEmail, isEmailEnabled } = await import('../src/email.js');

/** product_id -> true when that product is superseded pricing (any DODO_PRODUCT_*_LEGACYn_ID). */
function legacyProductIds() {
  const out = new Set();
  for (const [k, v] of Object.entries(process.env)) {
    if (/^DODO_PRODUCT_[A-Z]+?(_ANNUAL)?_LEGACY\d*_ID$/.test(k) && v) out.add(String(v).trim());
  }
  return out;
}

/** dodo_subscription_id -> product_id, so we can tell which price a customer is really on. */
async function loadDodoProductBySub() {
  const key = process.env.DODO_API_KEY;
  const base = process.env.DODO_BASE_URL;
  const map = new Map();
  if (!key || !base) return map;
  for (let p = 0; p < 50; p += 1) {
    const r = await fetch(`${base}/subscriptions?page_size=100&page_number=${p}`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    for (const d of items) {
      const id = d?.subscription_id || d?.id;
      if (id && d?.product_id) map.set(id, d.product_id);
    }
    if (items.length < 100) break;
  }
  return map;
}

function isMonthly(interval) {
  const i = String(interval || 'month').toLowerCase();
  return i !== 'year' && i !== 'annual' && i !== 'yearly';
}

async function emailForUser(userId) {
  try {
    const { data: p } = await supabase.from('profiles').select('email').eq('id', userId).maybeSingle();
    if (p?.email) return String(p.email).trim();
  } catch { /* fall through */ }
  try {
    const { data } = await supabase.auth.admin.getUserById(userId);
    return String(data?.user?.email || '').trim();
  } catch { return ''; }
}

const { data: subs, error } = await supabase
  .from('billing_subscriptions')
  .select('user_id, plan_type, status, billing_interval, dodo_subscription_id, cancel_at_period_end')
  .eq('status', 'active');

if (error) {
  console.error('Failed to load subscriptions:', error.message || error);
  process.exit(1);
}

// Never upsell someone who has already pressed cancel: status stays 'active' until their paid
// period runs out, so they look like healthy customers here. Selling them an annual plan on the
// way out is tone-deaf, and worse if they were just asked why they cancelled.
//
// Grouped PER USER, not per row: billing_subscriptions holds duplicate rows for one subscription
// and they disagree with each other (one customer had three rows, two saying false and one true).
// A row-level filter would let them through on a stale row.
const leaving = new Set(
  (subs || []).filter((s) => s.cancel_at_period_end).map((s) => s.user_id)
);

const targets = (subs || []).filter(
  (s) =>
    isMonthly(s.billing_interval) &&
    ANNUAL_PRICE[String(s.plan_type || '').toLowerCase()] &&
    !leaving.has(s.user_id)
);

if (leaving.size) {
  console.log(`Excluded ${leaving.size} customer(s) already set to not renew.
`);
}

const LEGACY_IDS = legacyProductIds();
const productBySub = await loadDodoProductBySub();
/**
 * What this customer pays today, or null if we cannot prove it.
 *
 * Never guess: assuming list price for an unresolvable subscription is how a grandfathered
 * customer ends up receiving a savings claim that is false for them. Comped rows (`comp:` ids)
 * pay nothing and must never receive a billing campaign.
 */
const currentMonthlyFor = (s) => {
  const plan = String(s.plan_type || '').toLowerCase();
  const subId = String(s.dodo_subscription_id || '');
  if (!subId || subId.startsWith('comp:')) return null;
  const productId = productBySub.get(subId);
  if (!productId) return null; // not found in Dodo — unknown pricing generation
  return (LEGACY_IDS.has(productId) ? LEGACY_MONTHLY_PRICE : MONTHLY_PRICE)[plan] || null;
};

console.log(`Mode: ${SEND ? 'SEND' : 'DRY RUN'} | Email enabled: ${isEmailEnabled()}`);
console.log(`Active monthly subscribers eligible for annual push: ${targets.length}\n`);

let sent = 0, skipped = 0, failed = 0;
for (const s of targets) {
  const plan = String(s.plan_type).toLowerCase();
  const email = await emailForUser(s.user_id);
  const currentMonthlyUsd = currentMonthlyFor(s);
  if (!email) { console.log(`  (no email)  user=${s.user_id}  plan=${plan}`); skipped += 1; continue; }
  if (!currentMonthlyUsd) {
    console.log(`  SKIP  ${email}  plan=${plan}  comped or price unknown — not billed on a Dodo product`);
    skipped += 1;
    continue;
  }
  const savings = Math.round((currentMonthlyUsd * 12 - ANNUAL_PRICE[plan]) * 100) / 100;
  const grandfathered = currentMonthlyUsd !== MONTHLY_PRICE[plan];

  // Mirrors the renderer's guard so the dry run reports exactly what a real run would do.
  if (savings < currentMonthlyUsd) {
    console.log(`  SKIP  ${email}  plan=${plan}  $${currentMonthlyUsd}/mo${grandfathered ? ' (grandfathered)' : ''}  no real saving (${savings >= 0 ? '$' : '-$'}${Math.abs(savings)}/yr)`);
    skipped += 1;
    continue;
  }

  if (!SEND) {
    console.log(`  WOULD EMAIL  ${email}  plan=${plan}  $${currentMonthlyUsd}/mo  saves $${savings}/yr`);
    continue;
  }

  // Claim idempotency slot first.
  const { data: claim, error: claimErr } = await supabase
    .from('email_events')
    .upsert({ user_id: s.user_id, email_key: 'annual_push' }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
    .select('user_id');
  if (claimErr) { console.log(`  ERROR claim  ${email}: ${claimErr.message}`); failed += 1; continue; }
  if (!Array.isArray(claim) || claim.length === 0) { console.log(`  already sent  ${email}`); skipped += 1; continue; }

  const r = await sendAnnualUpgradeEmail({ to: email, planType: plan, currentMonthlyUsd });
  if (r?.ok) { console.log(`  SENT  ${email}  plan=${plan}  saves $${savings}/yr`); sent += 1; }
  else {
    // Roll back the claim so a future run can retry.
    await supabase.from('email_events').delete().eq('user_id', s.user_id).eq('email_key', 'annual_push');
    console.log(`  FAILED  ${email}: ${r?.error || r?.reason}`); failed += 1;
  }
}

console.log(`\nDone. ${SEND ? `sent=${sent} skipped=${skipped} failed=${failed}` : 'dry run — re-run with --send to deliver.'}`);
