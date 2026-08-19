/**
 * One-off: outreach to the Pinterest-services / Etsy population found by
 * `tiktok-creator-discovery.mjs --set services`.
 *
 * Unlike the creators batch, most of these people would BUY the tool to run client work rather
 * than promote it for commission, so the affiliate offer is secondary or absent. Sent as plain,
 * personal-looking HTML rather than the branded transactional template.
 *
 *   node scripts/_send-pinterest-services-outreach.mjs            dry run
 *   node scripts/_send-pinterest-services-outreach.mjs --send     actually send
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
    to: 'Kristin@PinterestVA.com',
    handle: '@thekristinlarsen_',
    subject: 'For your Pinterest VA students — the part they get stuck on',
    body:
      p('Hi Kristin,') +
      p('You train people to become Pinterest VAs, so you know the wall they hit better than I do: they land the client, and then discover the job is producing pins at volume, week after week, for accounts that all need different creative.') +
      p("I build URL2Pin. You paste a product or blog URL and get back a batch of genuinely different pins — image, title and description written for each one — then schedule them out. It doesn't teach anyone strategy; it removes the part that makes VA work feel like a treadmill.") +
      p("That makes it complementary to what you sell rather than competing with it. If it's useful to your students, our affiliate programme pays <strong>30% recurring for 12 months</strong> on plans from $12 to $129/month, so it compounds as they grow rather than paying once.") +
      p("Before any of that I'd rather you used it properly — happy to set you up with a free account, no obligation to mention it anywhere. And if it doesn't hold up for client work, I'd genuinely rather hear that from someone who does this every day."),
  },
  {
    to: 'jess@itsjessicahawks.com',
    handle: '@itsjessicahawks',
    subject: 'Pinterest affiliate income — the bottleneck nobody covers',
    body:
      p('Hi Jessica,') +
      p("You teach people the actual skills for making money online, so this is aimed at your audience more than at you.") +
      p('Pinterest affiliate income is one of the few online income streams that keeps working months after the work is done — a pin published today still sends traffic next year. The reason most people abandon it is that it needs volume, and making a hundred distinct pins by hand is miserable.') +
      p('URL2Pin turns a product or blog URL into a batch of pins with copy written per pin, scheduled across days. That is the entire product.') +
      p('If it earns a mention, the affiliate programme is <strong>30% recurring for 12 months</strong> on plans $12–$129/month. Happy to give you a free account first so you can judge it honestly — and telling your audience it fell short would be more useful to them than a recommendation.'),
  },
  {
    to: 'Katelyn@katelynhunter.co',
    handle: '@itskatelynhunter',
    subject: 'Etsy → Pinterest, without making every pin by hand',
    body:
      p('Hi Katelyn,') +
      p("$300k+ on Etsy means you already know where the traffic comes from, and that Pinterest keeps sending it long after a listing stops being new.") +
      p('URL2Pin takes an Etsy or Amazon product URL and returns a batch of pins — image, title and description written for each — then schedules them. Etsy links work natively, which matters because most pin tools are built around blogs and treat product listings as an afterthought.') +
      p("Given you're teaching people to get paid to create, it may be more useful to them than to you — the affiliate programme is <strong>30% recurring for 12 months</strong>, plans $12–$129/month.") +
      p('Happy to set you up with a free account to test it against your own listings first, no strings attached.'),
  },
  {
    to: 'iamracquelj@gmail.com',
    handle: '@iamracquelj',
    subject: 'For the Etsy audits — when the problem turns out to be traffic',
    body:
      p('Hi Racquel,') +
      p("You audit Etsy shops that aren't making sales, so you'll have seen how often the listing is fine and the real answer is that nobody is finding it.") +
      p('URL2Pin is for that half of the problem. Paste an Etsy listing URL, get a batch of Pinterest pins with copy written per pin, scheduled out over days — which is the traffic advice most sellers receive and then never act on, because doing it by hand is the actual work.') +
      p("It could sit at the end of an audit as the concrete next step. If you'd rather it earn you something, the affiliate programme pays <strong>30% recurring for 12 months</strong> on plans from $12/month.") +
      p("Happy to give you a free account to try against a client shop first — and if it doesn't survive contact with real listings, I'd like to know that."),
  },
  {
    to: 'contentbyramlah@gmail.com',
    handle: '@contentbyramlah',
    subject: 'Tool for the people you teach — Pinterest pins from a URL',
    body:
      p('Hi Ramlah,') +
      p("Four years in social media marketing and now teaching what you know — so you'll recognise the gap between people understanding a channel and actually keeping up with it.") +
      p('URL2Pin closes that gap for Pinterest specifically. A product or blog URL becomes a batch of pins with image, title and description written per pin, scheduled automatically. It teaches nothing; it does the part people quit at after your guide has done its job.') +
      p('If it fits alongside what you already give away, our affiliate programme is <strong>30% recurring for 12 months</strong> on plans $12–$129/month.') +
      p("Happy to set you up free to form your own view first. No expectation either way, and if it's not worth your audience's attention I'd rather you said so."),
  },
  {
    to: 'Dontghostyourdreams@gmail.com',
    handle: '@dontquit__doit',
    subject: 'Pinterest traffic for the shop, without the pin-making grind',
    body:
      p('Hi,') +
      p("Bringing a print-on-demand shop back from the dead while parenting is not a small project, so I'll keep this short and there's nothing to buy.") +
      p('Pinterest is still the cheapest traffic for POD and Etsy — pins keep working months later, and it costs nothing but time. The time is the problem: it needs a lot of pins, and making them one by one in Canva is where most people stop.') +
      p('URL2Pin takes a product URL and gives you a batch of pins with the copy written for each, scheduled out over days.') +
      p("Happy to set you up with a free account to use on your own shop — genuinely no strings, and no need to post about it. If it helps your shop come back, that's a better outcome for me than anything else."),
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
  await new Promise((r) => setTimeout(r, 900)); // gentle pacing
}

if (SEND) console.log(`\ndone — sent=${sent} failed=${failed}`);
else console.log('\nNothing sent. Re-run with --send.');
