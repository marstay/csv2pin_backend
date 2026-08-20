/** One-off: ask the Brazilian agency (18 Pinterest accounts, churned after 7 days) what happened. */
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
  p('Olá,') +
  p('Em junho vocês assinaram o plano Agency do URL2Pin, conectaram <strong>18 contas do Pinterest</strong> e geraram mais de 400 pins na primeira semana. Depois disso, pararam.') +
  p('Não estou escrevendo para vender nada nem para oferecer desconto. Queria apenas entender o que aconteceu &mdash; vocês são a única agência que usou o produto nessa escala, e essa informação vale mais para mim do que a assinatura.') +
  p('Uma coisa em particular me chamou a atenção: foram mais de 400 pins gerados, mas <strong>apenas 33 chegaram a ser publicados</strong> no Pinterest. Isso foi uma decisão de vocês, ou algo travou na hora de publicar?') +
  p('Qualquer resposta, mesmo curta, ajuda muito. Se preferir responder em inglês, sem problema nenhum.') +
  p('Obrigado,')
}<p style="margin:16px 0 0">Aris<br>URL2Pin</p></div>`;
const body = {
  from: env.EMAIL_FROM || 'URL2Pin <hello@url2pin.com>',
  to: 'agencia@automatise.com.br',
  reply_to: env.SUPPORT_EMAIL || 'hello@url2pin.com',
  subject: 'Vocês conectaram 18 contas do Pinterest e depois pararam — posso perguntar por quê?',
  html,
};
if (!SEND) { console.log('DRY RUN ->', body.to, '\n', body.subject); process.exit(0); }
const r = await fetch('https://api.resend.com/emails', {
  method: 'POST',
  headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});
const j = await r.json().catch(() => ({}));
console.log(r.ok && j?.id ? `SENT id=${j.id}` : `FAIL ${r.status} ${JSON.stringify(j).slice(0,200)}`);
