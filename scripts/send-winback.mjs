/**
 * Win-back email for customers who have scheduled a cancellation but still have paid time left.
 *
 * Three segments, because one message would be wrong for two thirds of them:
 *   UNPUBLISHED — generated pins but barely posted any. The pins exist and are doing nothing;
 *                 this is fixable and they may not know. Highest-value segment.
 *   WORKING     — already has real impressions. Show the number; they usually have not seen it.
 *   EARLY       — published, but Pinterest has not given distribution yet. Be honest about
 *                 timelines rather than promising a turnaround.
 *
 * DRY RUN by default. Idempotent via email_events (key 'winback_2026_08').
 *   node backend/scripts/send-winback.mjs
 *   node backend/scripts/send-winback.mjs --send
 *   node backend/scripts/send-winback.mjs --send --only firence@gmail.com
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
const EMAIL_KEY = 'winback_2026_08';

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const { sendEmail, isEmailEnabled, emailConfig } = await import('../src/email.js');

const pageAll = async (table, cols) => {
  const out = [];
  for (let i = 0; i < 40; i += 1) {
    const { data, error } = await supabase.from(table).select(cols).range(i * 1000, i * 1000 + 999);
    if (error || !data?.length) break;
    out.push(...data);
    if (data.length < 1000) break;
  }
  return out;
};

const APP = emailConfig?.APP_URL || 'https://url2pin.com/app';
const fmt = (n) => Number(n || 0).toLocaleString('en-US');

/** Copy per segment. Kept here so the whole message is reviewable in one place. */
function buildEmail(seg, s) {
  const unposted = Math.max(0, s.created - s.posted);
  if (seg === 'UNPUBLISHED') {
    return {
      subject: `You have ${unposted} pins that were never posted`,
      html: `<p>Hi,</p>
<p>I noticed you created <strong>${fmt(s.created)} pins</strong> but only <strong>${fmt(s.posted)}</strong> made it to Pinterest — so ${fmt(unposted)} are sitting in your account doing nothing.</p>
<p>That's almost certainly why you saw no results. Pins only earn impressions once they're published, and Pinterest rewards posting steadily rather than all at once.</p>
<p>Your account stays active until <strong>${s.endsOn}</strong>, so those pins are still there and still yours. If you'd like, reply and I'll schedule them out for you properly — a few a day across your boards. Takes me five minutes and costs you nothing.</p>
<p>And if the tool just wasn't what you expected, I'd genuinely like to know why. One line helps me more than you'd think.</p>`,
    };
  }
  if (seg === 'WORKING') {
    return {
      subject: `Your pins earned ${fmt(s.imp)} impressions`,
      html: `<p>Hi,</p>
<p>Before your plan ends on <strong>${s.endsOn}</strong> — your pins have earned <strong>${fmt(s.imp)} impressions</strong>${s.clk ? ` and <strong>${fmt(s.clk)} outbound clicks</strong>` : ''} on Pinterest so far.</p>
<p>I'm mentioning it because that number isn't obvious inside the app, and it's genuinely working. Pinterest compounds slowly — accounts usually see clicks pick up around month two or three, well after the impressions start.</p>
<p>No pitch. If it's not for you that's completely fine. But if you cancelled because you weren't sure it was doing anything, it was.</p>
<p>Happy to look at your account and suggest what to post next — just reply.</p>`,
    };
  }
  return {
    subject: 'Honest note about your Pinterest results',
    html: `<p>Hi,</p>
<p>You published <strong>${fmt(s.posted)} pins</strong> and they've earned very little so far. I want to be straight with you about why.</p>
<p>Pinterest gives new or low-activity accounts almost no distribution for the first 4–8 weeks, regardless of how good the pins are. It's the single most frustrating thing about the platform and it's not something any tool can shortcut.</p>
<p>Your plan runs until <strong>${s.endsOn}</strong>. If you want to give it a proper test, the thing that works is posting steadily — a few pins a day for a few weeks — rather than in bursts. Reply and I'll set that up with you.</p>
<p>If you'd rather leave it, no hard feelings at all. But I'd really value one line on what you expected that didn't happen.</p>`,
  };
}

const { data: subs } = await supabase
  .from('billing_subscriptions')
  .select('user_id,plan_type,status,cancel_at_period_end,current_period_end,dodo_subscription_id');

const targets = new Map();
for (const s of subs || []) {
  if (String(s.dodo_subscription_id || '').startsWith('comp:')) continue;
  if (!['active', 'trialing', 'past_due'].includes(String(s.status))) continue;
  if (!s.cancel_at_period_end) continue;
  if (!targets.has(s.user_id)) targets.set(s.user_id, s);
}

const pins = await pageAll('scheduled_pins', 'user_id,status,impressions,outbound_clicks');
const usage = await pageAll('pin_usage', 'user_id,pins_used');

console.log(`Mode: ${SEND ? 'SEND' : 'DRY RUN'} | Email enabled: ${isEmailEnabled()}`);
console.log(`Scheduled to cancel: ${targets.size}\n`);

let sent = 0, skipped = 0, failed = 0;
for (const [uid, sub] of targets) {
  const { data: au } = await supabase.auth.admin.getUserById(uid);
  const email = String(au?.user?.email || '').trim();
  let created = 0, posted = 0, imp = 0, clk = 0;
  for (const p of pins) {
    if (p.user_id !== uid) continue;
    created += 1;
    if (p.status === 'posted') { posted += 1; imp += Number(p.impressions || 0); clk += Number(p.outbound_clicks || 0); }
  }
  const used = usage.filter((u) => u.user_id === uid).reduce((a, b) => a + Number(b.pins_used || 0), 0);
  const stats = { created, posted, imp, clk, used, endsOn: new Date(sub.current_period_end).toLocaleDateString('en-GB', { day: 'numeric', month: 'long' }) };

  // Publishing gap first: an unposted library is fixable, low impressions often are not.
  const seg = posted <= 5 && created >= 10 ? 'UNPUBLISHED' : imp >= 100 ? 'WORKING' : 'EARLY';
  const { subject, html } = buildEmail(seg, stats);

  if (!email) { console.log(`  (no email) user=${uid.slice(0, 8)}`); skipped += 1; continue; }
  if (ONLY && email.toLowerCase() !== ONLY) { skipped += 1; continue; }

  if (!SEND) {
    console.log(`  WOULD EMAIL  ${email.padEnd(32)} [${seg}]  ${subject}`);
    continue;
  }

  const { data: claim, error: claimErr } = await supabase
    .from('email_events')
    .upsert({ user_id: uid, email_key: EMAIL_KEY }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
    .select('user_id');
  if (claimErr) { console.log(`  ERROR claim ${email}: ${claimErr.message}`); failed += 1; continue; }
  if (!Array.isArray(claim) || claim.length === 0) { console.log(`  already sent ${email}`); skipped += 1; continue; }

  const r = await sendEmail({ to: email, subject, html, replyTo: emailConfig?.SUPPORT_EMAIL });
  if (r?.ok) { console.log(`  SENT  ${email.padEnd(32)} [${seg}]`); sent += 1; }
  else {
    await supabase.from('email_events').delete().eq('user_id', uid).eq('email_key', EMAIL_KEY);
    console.log(`  FAILED ${email}: ${r?.error || r?.reason}`);
    failed += 1;
  }
}

console.log(`\nDone. ${SEND ? `sent=${sent} skipped=${skipped} failed=${failed}` : 'dry run — add --send to deliver.'}`);
