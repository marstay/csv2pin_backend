/**
 * Read-only: look up one customer's subscriptions + payments in live Dodo.
 * Usage: node backend/scripts/_lookup-user.mjs harpreet.soori@gmail.com
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');
const TARGET = (process.argv[2] || '').toLowerCase().trim();

function extractLiveCreds() {
  const raw = readFileSync(envPath, 'utf8').split(/\r?\n/);
  let section = null, key = null, base = null;
  for (const line of raw) {
    const t = line.trim();
    if (/^#\s*live\b/i.test(t)) { section = 'live'; continue; }
    if (/^#\s*test\b/i.test(t)) { section = 'test'; continue; }
    const content = t.replace(/^#\s*/, '');
    const mk = content.match(/^DODO_API_KEY=(.+)$/);
    const mb = content.match(/^DODO_BASE_URL=(.+)$/);
    if (section === 'live') { if (mk) key = mk[1].trim(); if (mb) base = mb[1].trim(); }
  }
  return { key, base };
}

const { key, base } = extractLiveCreds();
if (!key || !base) { console.error('No live DODO creds'); process.exit(1); }
console.log(`base=${base} key=${key.slice(0,4)}…${key.slice(-2)} target=${TARGET}\n`);

async function listAll(path) {
  const out = [];
  for (let page = 0; page < 200; page++) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(page) });
    const resp = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: `Bearer ${key}` } });
    if (!resp.ok) { console.error(`${path} HTTP ${resp.status}`); break; }
    const json = await resp.json();
    const items = Array.isArray(json) ? json : json.items || json.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

const emailOf = (o) => (o.customer?.email || o.customer_email || '').toLowerCase();

const subs = (await listAll('/subscriptions')).filter((s) => emailOf(s) === TARGET);
console.log(`=== SUBSCRIPTIONS (${subs.length}) ===`);
for (const s of subs) {
  console.log(JSON.stringify({
    id: s.subscription_id || s.id,
    status: s.status,
    created_at: s.created_at,
    next_billing_date: s.next_billing_date || s.current_period_end,
    cancelled_at: s.cancelled_at ?? s.canceled_at ?? null,
    cancel_at_next_billing_date: s.cancel_at_next_billing_date ?? s.cancel_at_period_end ?? null,
    amount: s.recurring_pre_tax_amount ?? s.amount,
    currency: s.currency,
    product_id: s.product_id,
  }, null, 2));
}

const payments = (await listAll('/payments')).filter((p) => emailOf(p) === TARGET);
console.log(`\n=== PAYMENTS (${payments.length}) ===`);
for (const p of payments.sort((a,b)=> String(a.created_at) < String(b.created_at) ? 1 : -1)) {
  console.log(`${String(p.created_at).slice(0,19)}  ${String(p.status).padEnd(10)}  ${p.currency} ${p.total_amount ?? p.amount}  ${p.payment_id || p.id || ''}  ${p.error_message || p.error_code || ''}`);
}
