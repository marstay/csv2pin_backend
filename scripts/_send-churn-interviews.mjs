/**
 * One-off: ask the heavy-use early cancellers why they left.
 *
 * All three used the product hard and then switched off renewal well before their period ended,
 * which does not fit a "didn't like it" story. The goal is ONE answer: was the need a one-off
 * batch (a pricing-model problem) or was it price / invisible results (a product problem)? Those
 * imply opposite fixes and the database cannot tell them apart.
 *
 * Deliberately no discount offer — a retention bribe converts an honest question into a haggle
 * and buys a month rather than an explanation. Paul is excluded; he was emailed separately.
 *
 *   node scripts/_send-churn-interviews.mjs            dry run
 *   node scripts/_send-churn-interviews.mjs --send     actually send
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

const SIG = '<p style="margin:16px 0 0">Aris<br>URL2Pin</p>';
const p = (s) => `<p style="margin:0 0 14px">${s}</p>`;
const wrap = (body) =>
  `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${body}${SIG}</div>`;

const EMAILS = [
  {
    to: 'mwramsey482@gmail.com',
    who: 'mwramsey482 · cancelled after 0.2d, 38 pins',
    subject: 'You cancelled the same day — can I ask why?',
    body:
      p('Hi,') +
      p('You switched off renewal within a few hours of subscribing, and then went on to generate 38 pins. So the tool did something useful for you — it was the monthly commitment that was not the right shape.') +
      p("I'd rather understand that than guess at it. Was it that you had a particular batch of products to get through and simply didn't need it month after month? Or was it something else — the price, the results, something in the product itself?") +
      p("If it's the first, I'm weighing up one-off credit packs instead of forcing everything into a subscription, and knowing that would genuinely shape what I build next.") +
      p('Your access runs to 2 September either way, and there is no discount attached to this email. I would just like the answer.'),
  },
  {
    to: 'firence@gmail.com',
    who: 'firence · cancelled after 7d, 62 pins',
    subject: '62 pins in a week, then cancelled — what happened?',
    body:
      p('Hi,') +
      p('You made 62 pins in your first week and then turned off renewal. That is a lot of use from someone who decided not to continue, so I suspect something more specific was going on than simply not liking it.') +
      p('My guess is you had a set of products to work through and the job was finished — but I could easily be wrong, and it could just as well be the price, or that Pinterest had not shown you anything yet.') +
      p("If it turns out people mostly arrive with one batch of work, I'd rather sell credits than a subscription, and hearing that from you would help me decide.") +
      p('Access runs to 24 August regardless, and there is no offer attached here. I would just like to know.'),
  },
  {
    to: 'caitlin-e@hotmail.com',
    who: 'caitlin-e · cancelled after 12d, 60 pins',
    subject: 'What made you cancel?',
    body:
      p('Hi Caitlin,') +
      p('You made 60 pins over about two weeks and then switched off renewal. People who dislike a product do not usually use it that much first, so I think something else was going on and I would rather ask than assume.') +
      p('The two possibilities I cannot tell apart from my side: whether you had a one-off batch of products and the job was simply done, or whether it came down to the price, or to Pinterest not having produced visible results yet.') +
      p("If it is the first, I'm considering one-off credit packs rather than a monthly plan, and your answer would count for a lot in that decision.") +
      p('Your access runs to 29 August either way, and there is no discount attached to this. I would just value the honest answer.'),
  },
];

console.log(`${SEND ? 'SENDING' : 'DRY RUN'}  from: ${FROM}   reply-to: ${REPLY_TO}   (${EMAILS.length} emails)\n`);
if (!KEY) {
  console.error('RESEND_API_KEY missing — nothing sent.');
  process.exit(1);
}

let sent = 0;
let failed = 0;
for (const e of EMAILS) {
  if (!SEND) {
    console.log(`  [dry] ${e.who}`);
    console.log(`        -> ${e.to}   subject: ${e.subject}`);
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
      console.log(`  SENT  ${e.to.padEnd(28)} id=${j.id}`);
    } else {
      failed += 1;
      console.log(`  FAIL  ${e.to.padEnd(28)} ${r.status} ${JSON.stringify(j).slice(0, 160)}`);
    }
  } catch (err) {
    failed += 1;
    console.log(`  FAIL  ${e.to.padEnd(28)} ${err?.message || err}`);
  }
  await new Promise((r) => setTimeout(r, 900));
}

if (SEND) console.log(`\ndone — sent=${sent} failed=${failed}`);
else console.log('\nNothing sent. Re-run with --send.');
