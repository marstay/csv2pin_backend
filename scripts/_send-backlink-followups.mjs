/**
 * One-off: follow-ups to the backlink outreach sent 2026-07-25 / 07-28.
 *
 * One nudge only, each naming the specific page originally pitched, each giving an easy way to
 * say no. Roundup owners get a lot of these; a short follow-up that does not guilt-trip is the
 * version that gets a reply.
 *
 * NOT included:
 *   - nichepursuits, kontentsia, craftybase, finsavvypanda  -> contact-form only, no address
 *   - bootstrapped.ventures, thecommamamaco.com             -> batch 7, unclear if ever sent;
 *     a "follow-up" to someone who never got a first email reads badly. Check sent folder first.
 *
 *   node scripts/_send-backlink-followups.mjs            dry run
 *   node scripts/_send-backlink-followups.mjs --send     actually send
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
    to: 'contact@bloggingwizard.com',
    who: 'Adam Connell · bloggingwizard.com · "13 Best Pinterest Tools"',
    subject: 'Following up — Pinterest tools roundup',
    body:
      p('Hi Adam,') +
      p('Following up on a note I sent in late July about your <em>13 Best Pinterest Tools</em> roundup. I suggested URL2Pin as an addition &mdash; it turns a product or blog URL into a batch of Pinterest pins with the copy written per pin.') +
      p('Completely fine if it is not a fit; I know those roundups get a lot of requests. Happy to set you up with a free account if you would rather judge it yourself than take my word for it.'),
  },
  {
    to: 'care@designlumo.com',
    who: 'designlumo.com · "Pinterest Power Tools for Etsy Sellers"',
    subject: 'Following up — Pinterest tools for Etsy sellers',
    body:
      p('Hi,') +
      p('Following up on a message from late July about your <em>Pinterest Power Tools for Etsy Sellers</em> page. I suggested adding URL2Pin, which turns an Etsy listing URL into a batch of pins with images and copy written for each.') +
      p('No problem at all if it does not suit the page. Happy to give you a free account to try against a real listing either way.'),
  },
  {
    to: 'kyla@dishitoutsocial.com',
    who: 'Kyla Dinielle · dishitoutsocial.com · "10 Pinterest Marketing Tools I Actually Use"',
    subject: 'Following up on your Pinterest tools post',
    body:
      p('Hi Kyla,') +
      p('Following up on a note from late July about your <em>10 Pinterest Marketing Tools I Actually Use</em> post. The honest framing of that piece is why I wrote rather than sending a generic pitch.') +
      p('URL2Pin turns a product or blog URL into a batch of distinct pins, copy included. If you would actually use it, I would rather you tried it first &mdash; happy to set up a free account, no expectation of a mention.'),
  },
  {
    to: 'allaboutthehouseetsy@gmail.com',
    who: 'Rachael · allaboutplanners.com.au · Etsy/blogger tools post',
    subject: 'Following up — the tools list on your blog',
    body:
      p('Hi Rachael,') +
      p('Following up on a message from late July. I had flagged that BoardBooster is still listed on your Etsy/blogger tools post despite having shut down, and suggested URL2Pin as something current &mdash; it turns a listing or blog URL into a batch of Pinterest pins.') +
      p('The dead-link flag is worth acting on regardless of whether you add us. Happy to give you a free account if you want to look properly.'),
  },
  {
    to: 'Info@InfluencerSEO.com',
    who: 'influencerseo.com · Amazon-affiliate-on-Pinterest guide',
    subject: 'Following up — your Amazon affiliate on Pinterest guide',
    body:
      p('Hi,') +
      p('Following up on a note from late July about your guide to Amazon affiliate links on Pinterest. I suggested URL2Pin as a tool mention &mdash; paste an Amazon URL, get a batch of pins with titles and descriptions written per pin.') +
      p('No worries if it does not fit the piece. Free account available if you would like to test it before deciding.'),
  },
  {
    to: 'millie@bymilliepham.com',
    who: 'Millie Pham · bymilliepham.com · "7 Best Pinterest Tools"',
    subject: 'Following up — your best Pinterest tools roundup',
    body:
      p('Hi Millie,') +
      p('Following up on a note from late July about your <em>Best Pinterest Tools</em> roundup, where I offered a free Pro account for a proper review of URL2Pin.') +
      p('That offer stands, and there is no obligation to write anything positive &mdash; a review that says where it falls short would be more useful to your readers than one that does not. Happy to set it up whenever suits.'),
  },
  {
    to: 'tara@marketingartfully.com',
    who: 'Tara Jacobsen · marketingartfully.com · /doing-pinterest-for-etsy-sellers/',
    subject: 'Following up — Pinterest for Etsy sellers',
    body:
      p('Hi Tara,') +
      p('Following up on a message from late July about your <em>Doing Pinterest for Etsy Sellers</em> post. The tools you list are all design or scheduling; URL2Pin sits before both &mdash; it takes an Etsy listing URL and produces the pins themselves, copy included.') +
      p('Entirely your call whether that earns a line in the post. Happy to give you a free account to test it on a real shop first.'),
  },
];

console.log(`${SEND ? 'SENDING' : 'DRY RUN'}  from: ${FROM}   (${EMAILS.length} follow-ups)\n`);
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
    if (r.ok && j?.id) { sent += 1; console.log(`  SENT  ${e.to.padEnd(32)} id=${j.id}`); }
    else { failed += 1; console.log(`  FAIL  ${e.to.padEnd(32)} ${r.status} ${JSON.stringify(j).slice(0, 150)}`); }
  } catch (err) {
    failed += 1;
    console.log(`  FAIL  ${e.to.padEnd(32)} ${err?.message || err}`);
  }
  await new Promise((r) => setTimeout(r, 900));
}
if (SEND) console.log(`\ndone — sent=${sent} failed=${failed}`);
else console.log('\nNothing sent. Re-run with --send.');
