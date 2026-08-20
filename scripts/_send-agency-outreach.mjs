/**
 * One-off: outreach to Pinterest management AGENCIES.
 *
 * Different pitch from creators and from solo affiliates: agencies do not want a commission, they
 * want their per-client pin production cost to fall. Pin design is their labour cost, and every
 * one of these three says on their own site that they produce pin graphics by hand.
 *
 * Multi-account already works and is ungated — one account in production has 18 Pinterest accounts
 * connected — so nothing needs building to serve this segment.
 *
 *   node scripts/_send-agency-outreach.mjs            dry run
 *   node scripts/_send-agency-outreach.mjs --send     actually send
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
const wrap = (b) =>
  `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${b}${SIG}</div>`;

const EMAILS = [
  {
    to: 'hello@studiosocials.com',
    who: 'Studio Socials · Hanna · Pinterest Manager Academy',
    subject: 'The "new pin images daily" problem',
    body:
      p('Hi Hanna,') +
      p('Your services page describes monthly management as needing <em>new pin images on a daily basis</em>. That line is the reason I built URL2Pin, so I&rsquo;ll keep this short.') +
      p('You paste a product URL, a listing, or a blog post, and get back a batch of genuinely different pins &mdash; image, title and description written per pin &mdash; then schedule them. You can connect as many client accounts as you need; there is no cap on that.') +
      p('For an agency the point is not the software, it is that the daily design work stops being the thing that limits how many clients you can carry.') +
      p('Happy to set you up with a free account to run against one client and judge it properly. And if it holds up, it may be more useful to your Pinterest Manager Academy students than to you &mdash; they are the ones learning the job and hitting the volume wall for the first time.'),
  },
  {
    to: 'info@truebluecreatives.com',
    who: 'True Blue Creatives · $500/mo management, custom graphics',
    subject: 'Cutting the pin-design hours per client',
    body:
      p('Hi,') +
      p('You offer monthly Pinterest management with custom graphics for every client. That design time is almost certainly the part that decides how many accounts one person can hold.') +
      p('URL2Pin turns a product page, Etsy listing or blog post into a batch of distinct pins &mdash; image, title and description written per pin &mdash; and schedules them out. It sits alongside what you already use for engagement rather than replacing your strategy work.') +
      p('There is no limit on connected client accounts, so it scales with your roster rather than per seat.') +
      p('Happy to give you a free account to test on a single client first. If it does not save real hours, it is not worth your time and I would rather hear that.'),
  },
  {
    to: 'jabed@growpinterest.com',
    who: 'Grow Pinterest · Jabed · 500+ projects, Shopify/ecommerce',
    subject: 'Pins straight from Shopify product URLs',
    body:
      p('Hi Jabed,') +
      p('With 500+ Pinterest projects behind you, mostly ecommerce and Shopify stores, you will have felt the bottleneck: strategy is quick, producing enough pins per product is not.') +
      p('URL2Pin takes a product URL and returns a batch of pins with the copy written for each, then schedules them. Shopify works natively &mdash; both myshopify.com and custom domains &mdash; along with Amazon, Etsy, eBay and Walmart, so a client&rsquo;s catalogue becomes pins without anyone opening a design tool.') +
      p('Client accounts are unlimited, so it fits an agency roster rather than a single brand.') +
      p('Happy to set you up free to run against one store and see whether it stands up to real client work.'),
  },
];

console.log(`${SEND ? 'SENDING' : 'DRY RUN'}  from: ${FROM}  (${EMAILS.length} emails)\n`);
if (!KEY) { console.error('RESEND_API_KEY missing — nothing sent.'); process.exit(1); }

let sent = 0, failed = 0;
for (const e of EMAILS) {
  if (!SEND) { console.log(`  [dry] ${e.who}\n        -> ${e.to}  |  ${e.subject}`); continue; }
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: FROM, to: e.to, reply_to: REPLY_TO, subject: e.subject, html: wrap(e.body) }),
    });
    const j = await r.json().catch(() => ({}));
    if (r.ok && j?.id) { sent += 1; console.log(`  SENT  ${e.to.padEnd(30)} id=${j.id}`); }
    else { failed += 1; console.log(`  FAIL  ${e.to.padEnd(30)} ${r.status} ${JSON.stringify(j).slice(0, 160)}`); }
  } catch (err) {
    failed += 1;
    console.log(`  FAIL  ${e.to.padEnd(30)} ${err?.message || err}`);
  }
  await new Promise((r) => setTimeout(r, 900));
}
if (SEND) console.log(`\ndone — sent=${sent} failed=${failed}`);
else console.log('\nNothing sent. Re-run with --send.');
