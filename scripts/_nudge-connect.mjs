/** One-off: paid today, 24 pins generated, Pinterest never connected — nothing can publish. */
import { readFileSync } from 'node:fs';
import { createClient } from '@supabase/supabase-js';
const SEND = process.argv.includes('--send');
const KEY_EVENT = 'connect_pinterest_nudge_2026_08';
const TO = 'pujaagrawal635@gmail.com';
const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) { const t=l.trim(); if(!t||t.startsWith('#'))continue;
  const m=t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/); if(m&&!env[m[1]]) env[m[1]]=m[2].trim(); }
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const p = (s) => `<p style="margin:0 0 14px">${s}</p>`;
const html = `<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.6;color:#222">${
  p('Hi,') +
  p("I noticed you've generated 24 pins today \u2014 good start. One thing that isn't obvious from the app: they won't go out to Pinterest until you connect your Pinterest account. It's the <strong>Connect Pinterest</strong> button in the header and takes about a minute.") +
  p('Until then nothing is lost \u2014 every pin you have made is saved under Scheduled Pins, and they will publish once the account is linked.') +
  p("If the connection gives you any trouble, just reply and I'll sort it out. I'd rather fix it than have you paying for pins that never reach Pinterest.")
}<p style="margin:16px 0 0">Aris<br>URL2Pin</p></div>`;
const body = { from: env.EMAIL_FROM || 'URL2Pin <hello@url2pin.com>', to: TO,
  reply_to: env.SUPPORT_EMAIL || 'hello@url2pin.com',
  subject: 'Your 24 pins need one more step', html };
if (!SEND) { console.log('DRY RUN ->', TO, '\n ', body.subject); process.exit(0); }
let u=null;
for(let pg=1;pg<=20&&!u;pg++){const {data}=await sb.auth.admin.listUsers({page:pg,perPage:200});const us=data?.users||[];
  u=us.find(x=>String(x.email||'').toLowerCase()===TO); if(us.length<200)break;}
const { data: claim } = await sb.from('email_events')
  .upsert({ user_id: u.id, email_key: KEY_EVENT }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
  .select('user_id');
if (!Array.isArray(claim) || claim.length === 0) { console.log('already sent — nothing to do'); process.exit(0); }
const r = await fetch('https://api.resend.com/emails', { method:'POST',
  headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type':'application/json' },
  body: JSON.stringify(body) });
const j = await r.json().catch(()=>({}));
if (r.ok && j?.id) console.log(`SENT id=${j.id}`);
else { console.log(`FAIL ${r.status} ${JSON.stringify(j).slice(0,200)}`);
  await sb.from('email_events').delete().eq('user_id',u.id).eq('email_key',KEY_EVENT); }
