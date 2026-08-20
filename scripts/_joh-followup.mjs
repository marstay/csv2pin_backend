/** Follow-up to Johannes Deltl (deltl.de) — reviewer/affiliate, comp Creator until 16 Sept, no usage yet. */
import { readFileSync } from 'node:fs';
import { createClient } from '@supabase/supabase-js';
const SEND = process.argv.includes('--send');
const KEY_EVENT = 'johannes_followup_2026_08';
const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) { const t=l.trim(); if(!t||t.startsWith('#'))continue;
  const m=t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/); if(m&&!env[m[1]]) env[m[1]]=m[2].trim(); }
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const p = (s) => `<p style="margin:0 0 14px">${s}</p>`;
const html = `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${
  p('Hallo Johannes,') +
  p('Ihr Testzugang ist aktiv &mdash; ich wollte nur zwei Dinge nachreichen, die den Einstieg erleichtern.') +
  p('<strong>Erstens:</strong> Um zu sehen, was das Tool produziert, m&uuml;ssen Sie kein Pinterest-Konto verbinden. Es reicht, eine Produkt-URL einzuf&uuml;gen &mdash; Amazon, Etsy, Shopify oder auch ein Blogartikel &mdash; und zu generieren. Die fertigen Pins lassen sich direkt herunterladen. Das dauert etwa zwei Minuten.') +
  p('<strong>Zweitens, und das ist mir wichtiger:</strong> eine bekannte Schw&auml;che, bevor Sie selbst darauf sto&szlig;en. Bei Produkten, deren Verpackung haupts&auml;chlich aus Text besteht &mdash; B&uuml;cher vor allem &mdash; zeichnet das Modell den Titel manchmal falsch nach, obwohl das Originalbild als Referenz mitgegeben wird. Ich l&ouml;se das gerade &uuml;ber Compositing statt &uuml;ber den Prompt. Bei physischen Produkten mit klarer Form und Farbe tritt das Problem nicht auf.') +
  p('Ihr Zugang l&auml;uft bis zum 16. September. Falls Sie f&uuml;r einen Bericht mehr Zeit brauchen oder etwas Bestimmtes ben&ouml;tigen &mdash; Screenshots, Details zu den Limits, Preisstruktur &mdash; sagen Sie einfach Bescheid, ich stelle das gerne zusammen.') +
  p('Viele Gr&uuml;&szlig;e')
}<p style="margin:16px 0 0">Aris<br>URL2Pin &mdash; <a href="https://url2pin.com">url2pin.com</a></p></div>`;
const body = {
  from: env.EMAIL_FROM || 'URL2Pin <hello@url2pin.com>',
  to: 'johannes@deltl.de',
  reply_to: env.SUPPORT_EMAIL || 'hello@url2pin.com',
  subject: 'Kurz zu URL2Pin \u2013 und eine Schw\u00e4che, die Sie kennen sollten',
  html,
};
if (!SEND) { console.log('DRY RUN ->', body.to, '\n ', body.subject); process.exit(0); }
let u=null;
for(let pg=1;pg<=20&&!u;pg++){const {data}=await sb.auth.admin.listUsers({page:pg,perPage:200});const us=data?.users||[];u=us.find(x=>String(x.email||'').toLowerCase()==='johannes@deltl.de');if(us.length<200)break;}
const { data: claim } = await sb.from('email_events')
  .upsert({ user_id: u.id, email_key: KEY_EVENT }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
  .select('user_id');
if (!Array.isArray(claim) || claim.length === 0) { console.log('already sent — nothing to do'); process.exit(0); }
const r = await fetch('https://api.resend.com/emails', {
  method: 'POST',
  headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});
const j = await r.json().catch(() => ({}));
if (r.ok && j?.id) console.log(`SENT id=${j.id}`);
else { console.log(`FAIL ${r.status} ${JSON.stringify(j).slice(0,200)}`); await sb.from('email_events').delete().eq('user_id',u.id).eq('email_key',KEY_EVENT); }
