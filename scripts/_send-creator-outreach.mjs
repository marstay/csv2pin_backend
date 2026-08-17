/**
 * One-off: send the TikTok creator affiliate-programme outreach via Resend.
 *
 * Sent as plain, personal-looking HTML rather than the branded transactional template — cold
 * outreach that looks like a marketing blast draws spam complaints, and complaints against
 * hello@url2pin.com would degrade delivery of billing/activation mail for 555 users.
 *
 *   node scripts/_send-creator-outreach.mjs            dry run
 *   node scripts/_send-creator-outreach.mjs --send     actually send
 */
import { readFileSync } from 'node:fs';

const SEND = process.argv.includes('--send');
const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) {
  const t = l.trim();
  if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (m && !env[m[1]]) env[m[1]] = m[2].trim();
}
const KEY = env.RESEND_API_KEY;
const FROM = env.EMAIL_FROM || 'URL2Pin <hello@url2pin.com>';
const REPLY_TO = env.SUPPORT_EMAIL || 'hello@url2pin.com';

const SIG = '<p style="margin:16px 0 0">Aris<br>URL2Pin — <a href="https://url2pin.com">url2pin.com</a></p>';
const p = (s) => `<p style="margin:0 0 14px">${s}</p>`;
const wrap = (body) =>
  `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${body}${SIG}</div>`;

const EMAILS = [
  {
    to: 'socialcheatsheet2.0@gmail.com',
    handle: '@socialcheatsheet2.0',
    subject: 'Affiliate programme — Pinterest tool for the make-money-online crowd',
    body:
      p('Hi,') +
      p('You give people the actual tactics for making money online, so this is aimed at your audience more than at you.') +
      p('URL2Pin turns an Amazon or Etsy product link into a batch of Pinterest pins — image, title and description written per pin — then schedules them. Pinterest affiliate income is one of the few side hustles that keeps working months later, and the bottleneck is always making enough pins.') +
      p('We pay <strong>30% recurring for 12 months</strong> on referrals, plans $12–$129/month.') +
      p('Happy to set you up with a free account first so you can judge it honestly. No obligation to post about it.'),
  },
  {
    to: 'Contact@aylakurdoglu.com',
    handle: '@aylakurdoglu',
    subject: 'Something to pair with your Pinterest guide',
    body:
      p('Hi Ayla,') +
      p("You have a Pinterest guide, so you'll know the gap: people learn the strategy and then stall on producing enough pins to make it work.") +
      p("That's what URL2Pin does — paste a product link, get a batch of genuinely different pins with copy written for each, scheduled out over days. It teaches nothing; it does the part people quit at.") +
      p('Our affiliate programme is <strong>30% recurring for 12 months</strong> on plans from $12/month, so it could sit alongside your guide as the natural next recommendation.') +
      p('Happy to give you a free account to judge it first.'),
  },
  {
    to: 'Christina@christinalazaro.com',
    handle: '@christinamlazaro',
    subject: 'Affiliate programme — tool for digital product creators',
    body:
      p('Hi Christina,') +
      p("You teach people to create and sell digital products, and most of them hit the same wall: they've made the thing, now they need traffic.") +
      p('URL2Pin turns a product or page URL into a batch of Pinterest pins, copy included, scheduled automatically. Pinterest is where digital products keep selling long after a launch, and pin volume is the work nobody wants to do.') +
      p('<strong>30% recurring for 12 months</strong> on referrals, plans $12–$129/month. Happy to set you up free first.'),
  },
  {
    to: 'Thehonesthustlemama@gmail.com',
    handle: '@thehonesthustlema',
    subject: 'Amazon affiliate tips — tool that might speed up the $2k goal',
    body:
      p('Hi,') +
      p("You're testing side hustles until you hit $2k/month and already share Amazon affiliate tips, so you'll recognise the bottleneck: the strategy is simple, making the pins is the grind.") +
      p('URL2Pin takes an Amazon link and gives you a batch of Pinterest pins with copy written per pin, scheduled out. I built it for exactly the person you describe in your bio.') +
      p('If it earns a mention, our affiliate programme pays <strong>30% recurring for 12 months</strong> from $12/month plans.') +
      p("Happy to give you a free account either way — and if it doesn't help, saying so to your audience is more useful than a recommendation."),
  },
  {
    to: 'Janet.socials@gmail.com',
    handle: '@janet.socials',
    subject: 'Creator tool — Pinterest pins from product links',
    body:
      p('Hi Janet,') +
      p('You cover creator tips and personal finance, so this sits in both: Pinterest affiliate income is one of the more durable creator income streams, and the work is making enough pins.') +
      p('URL2Pin turns a product link into a batch of pins with titles and descriptions written per pin, scheduled out. <strong>30% recurring for 12 months</strong> on referrals if it&rsquo;s worth covering.') +
      p('Free account to test first, no strings.'),
  },
];

console.log(`${SEND ? 'SENDING' : 'DRY RUN'}  from: ${FROM}   reply-to: ${REPLY_TO}\n`);
if (!KEY) {
  console.error('RESEND_API_KEY missing — nothing sent.');
  process.exit(1);
}

let sent = 0;
let failed = 0;
for (const e of EMAILS) {
  if (!SEND) {
    console.log(`  [dry] ${e.handle.padEnd(22)} -> ${e.to}`);
    console.log(`        subject: ${e.subject}`);
    continue;
  }
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: FROM,
        to: e.to,
        reply_to: REPLY_TO,
        subject: e.subject,
        html: wrap(e.body),
      }),
    });
    const j = await r.json().catch(() => ({}));
    if (r.ok && j?.id) {
      sent += 1;
      console.log(`  SENT  ${e.handle.padEnd(22)} -> ${e.to}   id=${j.id}`);
    } else {
      failed += 1;
      console.log(`  FAIL  ${e.handle.padEnd(22)} -> ${e.to}   ${r.status} ${JSON.stringify(j).slice(0, 160)}`);
    }
  } catch (err) {
    failed += 1;
    console.log(`  FAIL  ${e.handle.padEnd(22)} -> ${e.to}   ${err?.message || err}`);
  }
  await new Promise((r) => setTimeout(r, 700)); // gentle pacing
}

if (SEND) console.log(`\ndone — sent=${sent} failed=${failed}`);
else console.log('\nNothing sent. Re-run with --send.');
