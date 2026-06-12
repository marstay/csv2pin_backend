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

const ANNUAL_PRICE = { starter: 84, creator: 180, pro: 384, agency: 780 };
const MONTHLY_PRICE = { starter: 9, creator: 19, pro: 39, agency: 79 };

const { sendAnnualUpgradeEmail, isEmailEnabled } = await import('../src/email.js');

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
  .select('user_id, plan_type, status, billing_interval')
  .eq('status', 'active');

if (error) {
  console.error('Failed to load subscriptions:', error.message || error);
  process.exit(1);
}

const targets = (subs || []).filter(
  (s) => isMonthly(s.billing_interval) && ANNUAL_PRICE[String(s.plan_type || '').toLowerCase()]
);

console.log(`Mode: ${SEND ? 'SEND' : 'DRY RUN'} | Email enabled: ${isEmailEnabled()}`);
console.log(`Active monthly subscribers eligible for annual push: ${targets.length}\n`);

let sent = 0, skipped = 0, failed = 0;
for (const s of targets) {
  const plan = String(s.plan_type).toLowerCase();
  const email = await emailForUser(s.user_id);
  const savings = MONTHLY_PRICE[plan] * 12 - ANNUAL_PRICE[plan];
  if (!email) { console.log(`  (no email)  user=${s.user_id}  plan=${plan}`); skipped += 1; continue; }

  if (!SEND) {
    console.log(`  WOULD EMAIL  ${email}  plan=${plan}  saves $${savings}/yr`);
    continue;
  }

  // Claim idempotency slot first.
  const { data: claim, error: claimErr } = await supabase
    .from('email_events')
    .upsert({ user_id: s.user_id, email_key: 'annual_push' }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
    .select('user_id');
  if (claimErr) { console.log(`  ERROR claim  ${email}: ${claimErr.message}`); failed += 1; continue; }
  if (!Array.isArray(claim) || claim.length === 0) { console.log(`  already sent  ${email}`); skipped += 1; continue; }

  const r = await sendAnnualUpgradeEmail({ to: email, planType: plan });
  if (r?.ok) { console.log(`  SENT  ${email}  plan=${plan}  saves $${savings}/yr`); sent += 1; }
  else {
    // Roll back the claim so a future run can retry.
    await supabase.from('email_events').delete().eq('user_id', s.user_id).eq('email_key', 'annual_push');
    console.log(`  FAILED  ${email}: ${r?.error || r?.reason}`); failed += 1;
  }
}

console.log(`\nDone. ${SEND ? `sent=${sent} skipped=${skipped} failed=${failed}` : 'dry run — re-run with --send to deliver.'}`);
