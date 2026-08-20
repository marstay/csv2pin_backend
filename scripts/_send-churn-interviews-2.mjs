/**
 * One-off: the three cancellers not covered by the first churn-interview batch.
 *
 * Unlike that batch, this one CLAIMS an email_events key before sending, so the send is auditable
 * and a re-run cannot double-email anyone. Ad-hoc scripts that go straight to Resend leave no
 * trace, which is how it became unclear whether these three had already been contacted.
 *
 * Three different failures, three different questions:
 *   repti3dexotics  published 24 of 27 pins, then vanished after 2.6 days
 *   orellana1809    switched off renewal within minutes of paying, then used it anyway
 *   lgfy1984        paid for 3 weeks, never connected Pinterest, published nothing
 *
 *   node scripts/_send-churn-interviews-2.mjs            dry run
 *   node scripts/_send-churn-interviews-2.mjs --send     actually send
 */
import { readFileSync } from 'node:fs';
import { createClient } from '@supabase/supabase-js';

const SEND = process.argv.includes('--send');
const EVENT_KEY = 'churn_interview_2026_08b';

const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) {
  const t = l.trim();
  if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (m && !env[m[1]]) env[m[1]] = m[2].trim();
}
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const p = (s) => `<p style="margin:0 0 14px">${s}</p>`;
const wrap = (b) =>
  `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${b}<p style="margin:16px 0 0">Aris<br>URL2Pin</p></div>`;

const EMAILS = [
  {
    to: 'repti3dexotics@gmail.com',
    note: 'published 24 of 27 · cancelled after 2.6d · access ends 23 Aug',
    subject: 'You published 24 pins and then stopped — can I ask why?',
    body:
      p('Hi,') +
      p('You joined in July, published <strong>24 pins</strong> to Pinterest within the first couple of days, and then did not come back.') +
      p('That is an unusual pattern &mdash; most people who leave never get that far. You clearly had it working, which makes me more curious rather than less.') +
      p('Was it that you had a set of products to get through and the job was simply done? Or did something about the pins, the results, or the price not hold up?') +
      p('Your access runs to 23 August either way, and there is no offer attached to this email. I would just like to know.'),
  },
  {
    to: 'orellana1809@gmail.com',
    note: 'cancelled 6.6 min after paying · 21 generated, 10 posted · access ends 27 Aug',
    subject: 'You turned off renewal the same day — can I ask why?',
    body:
      p('Hi,') +
      p('You subscribed in July and switched off auto-renewal a few minutes later &mdash; then went on to generate 21 pins and publish 10 of them.') +
      p('So the product did something useful for you. It was the monthly commitment that was not the right shape, and I would rather understand that than guess at it.') +
      p('Was it a particular batch of products you needed to get through, rather than something ongoing? If that turns out to be common, I would rather sell credits than force everything into a subscription &mdash; and hearing it from you would genuinely shape what I build next.') +
      p('Access runs to 27 August regardless, and there is no discount attached here. I would just like the answer.'),
  },
  {
    to: 'lgfy1984@gmail.com',
    note: 'paid 3 weeks · never connected Pinterest · 0 published · access ends 27 Aug',
    subject: 'Your pins never went out — what stopped you?',
    body:
      p('Hi,') +
      p('You paid for three weeks in July but never connected a Pinterest account, so none of your pins were ever published. That means you paid and got nothing back, and I would rather not let that pass without asking about it.') +
      p('Was connecting Pinterest more hassle than it was worth, or just something you never got round to? Or had you already decided the pins were not what you wanted before it got that far?') +
      p('You still have access until 27 August. If it would help, reply and I will walk you through connecting it &mdash; no charge, and no attempt to talk you into staying subscribed. Otherwise I would simply value knowing what stopped you.'),
  },
];

async function claim(userId) {
  const { data, error } = await sb
    .from('email_events')
    .upsert({ user_id: userId, email_key: EVENT_KEY }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
    .select('user_id');
  if (error) { console.log(`  claim error (skipping): ${error.message}`); return false; }
  return Array.isArray(data) && data.length > 0;
}

// resolve emails -> user ids so the claim can be recorded
const idByEmail = new Map();
for (let page = 1; page <= 20; page += 1) {
  const { data } = await sb.auth.admin.listUsers({ page, perPage: 200 });
  const us = data?.users || [];
  for (const u of us) idByEmail.set(String(u.email || '').toLowerCase(), u.id);
  if (us.length < 200) break;
}

console.log(`${SEND ? 'SENDING' : 'DRY RUN'}  key=${EVENT_KEY}\n`);
let sent = 0, skipped = 0, failed = 0;
for (const e of EMAILS) {
  const uid = idByEmail.get(e.to.toLowerCase());
  if (!uid) { console.log(`  NO ACCOUNT  ${e.to}`); failed += 1; continue; }
  if (!SEND) { console.log(`  [dry] ${e.to.padEnd(28)} ${e.note}\n        ${e.subject}`); continue; }
  if (!(await claim(uid))) { console.log(`  already sent  ${e.to}`); skipped += 1; continue; }
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: env.EMAIL_FROM || 'URL2Pin <hello@url2pin.com>',
        to: e.to,
        reply_to: env.SUPPORT_EMAIL || 'hello@url2pin.com',
        subject: e.subject,
        html: wrap(e.body),
      }),
    });
    const j = await r.json().catch(() => ({}));
    if (r.ok && j?.id) { sent += 1; console.log(`  SENT  ${e.to.padEnd(28)} id=${j.id}`); }
    else {
      failed += 1;
      console.log(`  FAIL  ${e.to.padEnd(28)} ${r.status} ${JSON.stringify(j).slice(0, 150)}`);
      // release the claim so a retry is possible
      await sb.from('email_events').delete().eq('user_id', uid).eq('email_key', EVENT_KEY);
    }
  } catch (err) {
    failed += 1;
    console.log(`  FAIL  ${e.to.padEnd(28)} ${err?.message || err}`);
    await sb.from('email_events').delete().eq('user_id', uid).eq('email_key', EVENT_KEY);
  }
  await new Promise((r) => setTimeout(r, 900));
}
if (SEND) console.log(`\ndone — sent=${sent} skipped=${skipped} failed=${failed}`);
else console.log('\nNothing sent. Re-run with --send.');
