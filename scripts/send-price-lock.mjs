/**
 * One-off campaign: tell existing subscribers that 2026 pricing does NOT apply to them.
 *
 * Each customer's real rate is resolved from the Dodo PRODUCT their subscription sits on, not
 * from a price table — a grandfathered customer must never be quoted the list price. Anyone
 * already on current pricing has nothing to be reassured about and is skipped.
 *
 * DRY RUN by default — prints the exact recipient list and each person's numbers.
 * Idempotent via email_events (key 'price_lock_2026'), so re-running never double-emails.
 *
 * Usage:
 *   node backend/scripts/send-price-lock.mjs            # dry run
 *   node backend/scripts/send-price-lock.mjs --send     # actually deliver
 *   node backend/scripts/send-price-lock.mjs --send --only you@example.com   # send one test
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const SEND = process.argv.includes('--send');
const onlyIdx = process.argv.indexOf('--only');
const ONLY = onlyIdx > -1 ? String(process.argv[onlyIdx + 1] || '').toLowerCase().trim() : null;
const EMAIL_KEY = 'price_lock_2026';

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const { sendPriceLockEmail, isEmailEnabled } = await import('../src/email.js');

// Legacy = what grandfathered customers still pay. Current = the 2026 list price.
const LEGACY_MONTHLY = { starter: 9, creator: 19, pro: 39, agency: 79 };
const LEGACY_ANNUAL = { starter: 84, creator: 180, pro: 384, agency: 780 };
const NEW_MONTHLY = { starter: 12, creator: 25, pro: 55, agency: 129 };
const NEW_ANNUAL = { starter: 108, creator: 225, pro: 495, agency: 1161 };

const legacyProductIds = new Set();
for (const [k, v] of Object.entries(process.env)) {
  if (/^DODO_PRODUCT_[A-Z]+?(_ANNUAL)?_LEGACY\d*_ID$/.test(k) && v) legacyProductIds.add(String(v).trim());
}
if (!legacyProductIds.size) {
  console.error('FATAL: no DODO_PRODUCT_*_LEGACY*_ID vars set — cannot tell who is grandfathered.');
  process.exit(1);
}

/** dodo_subscription_id -> product_id. The product is the only proof of which price applies. */
async function loadDodoProducts() {
  const map = new Map();
  const base = process.env.DODO_BASE_URL;
  const key = process.env.DODO_API_KEY;
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

async function emailForUser(userId) {
  try {
    const { data: p } = await supabase.from('profiles').select('email').eq('id', userId).maybeSingle();
    if (p?.email) return String(p.email).trim();
  } catch { /* fall through to auth */ }
  try {
    const { data } = await supabase.auth.admin.getUserById(userId);
    return String(data?.user?.email || '').trim();
  } catch { return ''; }
}

const productBySub = await loadDodoProducts();
const { data: subs, error } = await supabase
  .from('billing_subscriptions')
  .select('user_id, plan_type, status, billing_interval, dodo_subscription_id')
  .in('status', ['active', 'trialing', 'past_due']);

if (error) {
  console.error('Failed to load subscriptions:', error.message || error);
  process.exit(1);
}

// billing_subscriptions has known duplicate rows — one email per human, not per row.
const byUser = new Map();
for (const s of subs || []) {
  const subId = String(s.dodo_subscription_id || '');
  if (!subId || subId.startsWith('comp:')) continue; // comped users pay nothing
  if (byUser.has(s.user_id)) continue;
  const productId = productBySub.get(subId);
  if (!productId) continue; // cannot prove their price — never guess
  const plan = String(s.plan_type || '').toLowerCase();
  const yearly = String(s.billing_interval || 'month').toLowerCase() === 'year';
  const legacy = legacyProductIds.has(productId);
  const currentUsd = (legacy ? (yearly ? LEGACY_ANNUAL : LEGACY_MONTHLY) : (yearly ? NEW_ANNUAL : NEW_MONTHLY))[plan];
  const newUsd = (yearly ? NEW_ANNUAL : NEW_MONTHLY)[plan];
  if (!currentUsd || !newUsd) continue;
  byUser.set(s.user_id, { plan, yearly, legacy, currentUsd, newUsd });
}

console.log(`Mode: ${SEND ? 'SEND' : 'DRY RUN'} | Email enabled: ${isEmailEnabled()}${ONLY ? ` | ONLY ${ONLY}` : ''}`);
console.log(`Paying customers found: ${byUser.size}\n`);

let sent = 0, skipped = 0, failed = 0;
for (const [userId, v] of byUser) {
  const email = await emailForUser(userId);
  const tag = `${v.plan}${v.yearly ? '/yr' : '/mo'}`;

  if (!email) { console.log(`  (no email)   user=${userId.slice(0, 8)} ${tag}`); skipped += 1; continue; }
  if (ONLY && email.toLowerCase() !== ONLY) { skipped += 1; continue; }
  if (!v.legacy) {
    console.log(`  SKIP         ${email}  ${tag}  already on current pricing`);
    skipped += 1;
    continue;
  }

  const line = `${email.padEnd(38)} ${tag.padEnd(12)} pays $${v.currentUsd} · new rate $${v.newUsd}`;
  if (!SEND) { console.log(`  WOULD EMAIL  ${line}`); continue; }

  // Claim the idempotency slot BEFORE sending so a crash mid-run cannot double-send.
  const { data: claim, error: claimErr } = await supabase
    .from('email_events')
    .upsert({ user_id: userId, email_key: EMAIL_KEY }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
    .select('user_id');
  if (claimErr) { console.log(`  ERROR claim  ${email}: ${claimErr.message}`); failed += 1; continue; }
  if (!Array.isArray(claim) || claim.length === 0) { console.log(`  already sent ${email}`); skipped += 1; continue; }

  const r = await sendPriceLockEmail({
    to: email,
    planType: v.plan,
    currentUsd: v.currentUsd,
    newUsd: v.newUsd,
    yearly: v.yearly,
  });
  if (r?.ok) { console.log(`  SENT         ${line}`); sent += 1; }
  else {
    // Roll the claim back so a later run can retry this person.
    await supabase.from('email_events').delete().eq('user_id', userId).eq('email_key', EMAIL_KEY);
    console.log(`  FAILED       ${email}: ${r?.error || r?.reason}`);
    failed += 1;
  }
}

console.log(
  `\nDone. ${SEND ? `sent=${sent} skipped=${skipped} failed=${failed}` : 'dry run — add --send to deliver.'}`
);
