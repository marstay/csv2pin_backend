/**
 * Read-only: dump a live Dodo subscription's plan/product/metadata.
 * Usage: node backend/scripts/_dodo-sub-metadata.mjs sub_0NeG3ilgErb8avjMT0
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');
const SUB = (process.argv[2] || '').trim();

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

const resp = await fetch(`${base}/subscriptions/${SUB}`, {
  headers: { Authorization: `Bearer ${key}` },
});
if (!resp.ok) {
  console.error(`HTTP ${resp.status}`, await resp.text());
  process.exit(1);
}
const s = await resp.json();

console.log('subscription_id   :', s.subscription_id || s.id);
console.log('status            :', s.status);
console.log('product_id        :', s.product_id);
console.log('recurring amount  :', s.recurring_pre_tax_amount ?? s.amount ?? '-', s.currency || '');
console.log('next billing      :', s.next_billing_date);
console.log('previous billing  :', s.previous_billing_date);
console.log('customer          :', s.customer?.email || '-');
console.log('\nMETADATA:');
console.log(JSON.stringify(s.metadata || {}, null, 2));

// Map product_id back to the configured plan so we can see the true current plan.
const env = readFileSync(envPath, 'utf8');
const products = {};
for (const m of env.matchAll(/DODO_PRODUCT_([A-Z_]+)_ID\s*=\s*(\S+)/g)) {
  products[m[2].trim()] = m[1].toLowerCase();
}
console.log('\nproduct_id maps to plan:', products[s.product_id] || '(not found in .env product map)');
