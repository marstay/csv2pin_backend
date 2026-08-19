/** One-off: reply to Damjan (firence@gmail.com) — churned for external reasons, not product. */
import { readFileSync } from 'node:fs';
const SEND = process.argv.includes('--send');
const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) {
  const t = l.trim(); if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/); if (m && !env[m[1]]) env[m[1]] = m[2].trim();
}
const p = (s) => `<p style="margin:0 0 14px">${s}</p>`;
const html = `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${
  p('Hi Damjan,') +
  p('Thanks for coming back to me — and sorry, being locked out of your own admin area is a miserable position to be in.') +
  p('One useful thing for the migration: URL2Pin handles Shopify product URLs natively, both myshopify.com and custom domains, so nothing changes on your side once the new store is live. Changing the domain will not cause you any problems either.') +
  p('Nothing to decide now. When you are back up and running, just email me and I will get you set up again.') +
  p('Good luck with the move.')
}<p style="margin:16px 0 0">Aris<br>URL2Pin</p></div>`;

const body = {
  from: env.EMAIL_FROM || 'URL2Pin <hello@url2pin.com>',
  to: 'firence@gmail.com',
  reply_to: env.SUPPORT_EMAIL || 'hello@url2pin.com',
  subject: 'Re: 62 pins in a week, then cancelled — what happened?',
  html,
};
if (!SEND) { console.log('DRY RUN\n', JSON.stringify({ ...body, html: '(html)' }, null, 1)); process.exit(0); }
const r = await fetch('https://api.resend.com/emails', {
  method: 'POST',
  headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});
const j = await r.json().catch(() => ({}));
console.log(r.ok && j?.id ? `SENT id=${j.id}` : `FAIL ${r.status} ${JSON.stringify(j).slice(0, 200)}`);
