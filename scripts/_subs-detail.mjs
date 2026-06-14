/**
 * Read-only: dump full subscription objects + linked payments for key accounts,
 * to compare renewed vs expired vs on_hold and find why renewals don't happen.
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');

function extractLiveCreds() {
  const raw = readFileSync(envPath, 'utf8').split(/\r?\n/);
  let section = null, key = null, base = null;
  for (const line of raw) {
    const t = line.trim();
    if (/^#\s*live\b/i.test(t)) { section = 'live'; continue; }
    if (/^#\s*test\b/i.test(t)) { section = 'test'; continue; }
    const c = t.replace(/^#\s*/, '');
    const mk = c.match(/^DODO_API_KEY=(.+)$/);
    const mb = c.match(/^DODO_BASE_URL=(.+)$/);
    if (section === 'live') { if (mk) key = mk[1].trim(); if (mb) base = mb[1].trim(); }
  }
  return { key, base };
}
const { key, base } = extractLiveCreds();
const H = { Authorization: `Bearer ${key}` };

async function listAll(path) {
  const out = [];
  for (let page = 0; page < 300; page++) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(page) });
    const resp = await fetch(`${base}${path}?${qs}`, { headers: H });
    if (!resp.ok) break;
    const json = await resp.json();
    const items = Array.isArray(json) ? json : json.items || json.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

const emailOf = (o) => (o.customer?.email || o.customer_email || '').toLowerCase();
const TARGETS = new Set([
  'ricardojoaomail@gmail.com', // expired after 1 payment
  'harpreet.soori@gmail.com',  // expired after 1 payment
  'kickplug0@gmail.com',       // on_hold (renewal attempted + failed)
  'fresnosteph@gmail.com',     // active, renewed (paid=2)
  'dennis.gallon67@gmail.com', // active, renewed (paid=2)
]);

const subs = (await listAll('/subscriptions')).filter((s) => TARGETS.has(emailOf(s)));
const payments = await listAll('/payments');

// Try to fetch a single subscription by id for richer fields (mandate, etc.)
async function getSub(id) {
  try {
    const r = await fetch(`${base}/subscriptions/${id}`, { headers: H });
    if (r.ok) return await r.json();
  } catch {}
  return null;
}

for (const s of subs) {
  const id = s.subscription_id || s.id;
  const full = (await getSub(id)) || s;
  console.log('\n==================================================');
  console.log(`${emailOf(s)}  status=${s.status}`);
  // Print a curated set of fields likely relevant to renewal/mandate config
  const keys = [
    'subscription_id','status','created_at','next_billing_date','previous_billing_date',
    'current_period_end','payment_frequency_interval','payment_frequency_count',
    'subscription_period_interval','subscription_period_count','recurring_pre_tax_amount',
    'currency','on_demand','cancel_at_next_billing_date','cancelled_at','expires_at',
    'trial_period_days','payment_method_id','mandate_id','billing','metadata','product_id',
  ];
  const view = {};
  for (const k of keys) if (full[k] !== undefined) view[k] = full[k];
  console.log(JSON.stringify(view, null, 2));
  const mine = payments
    .filter((p) => (p.subscription_id || p.subscription?.subscription_id) === id)
    .sort((a, b) => String(a.created_at) < String(b.created_at) ? 1 : -1);
  console.log(`payments (${mine.length}):`);
  for (const p of mine) {
    console.log(`   ${String(p.created_at).slice(0,19)}  ${String(p.status).padEnd(22)}  ${p.currency} ${p.total_amount ?? p.amount}  ${p.error_code||p.error_message||''}`);
  }
}
