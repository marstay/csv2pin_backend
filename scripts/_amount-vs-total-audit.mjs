/**
 * Read-only: compare `amount` vs `total_amount` on every live Dodo payment, so we can decide
 * which field affiliate commission should be based on (tax-exclusive vs tax-inclusive).
 *
 * Usage: node backend/scripts/_amount-vs-total-audit.mjs
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
    const content = t.replace(/^#\s*/, '');
    const mk = content.match(/^DODO_API_KEY=(.+)$/);
    const mb = content.match(/^DODO_BASE_URL=(.+)$/);
    if (section === 'live') { if (mk) key = mk[1].trim(); if (mb) base = mb[1].trim(); }
  }
  return { key, base };
}

const { key, base } = extractLiveCreds();
if (!key || !base) { console.error('No live DODO creds'); process.exit(1); }

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

const payments = (await listAll('/payments')).filter((p) => String(p.status) === 'succeeded');
console.log(`succeeded payments: ${payments.length}\n`);

if (!payments.length) process.exit(0);

// Which money-ish fields does Dodo actually return?
const fieldNames = new Set();
for (const p of payments) {
  for (const k of Object.keys(p)) {
    if (/amount|tax|currency|settle/i.test(k)) fieldNames.add(k);
  }
}
console.log('money-related fields present:', [...fieldNames].sort().join(', '), '\n');

console.log('cur | amount | total_amount | tax | diff | date       | email');
let differ = 0;
let sumAmount = 0;
let sumTotal = 0;

for (const p of payments.slice().sort((a, b) => String(a.created_at) < String(b.created_at) ? 1 : -1)) {
  const amount = Number(p.amount ?? 0);
  const total = Number(p.total_amount ?? 0);
  const tax = Number(p.tax ?? 0);
  const diff = total - amount;
  if (diff !== 0) differ += 1;
  sumAmount += amount;
  sumTotal += total;
  const email = (p.customer?.email || p.customer_email || '').slice(0, 28);
  console.log(
    [
      String(p.currency || '?').toLowerCase().padEnd(3),
      String(amount).padEnd(6),
      String(total).padEnd(12),
      String(tax).padEnd(4),
      String(diff).padEnd(4),
      String(p.created_at || '').slice(0, 10),
      email,
    ].join(' | ')
  );
}

console.log(`\npayments where total_amount !== amount: ${differ} / ${payments.length}`);
console.log(`sum amount       = ${sumAmount} (${(sumAmount / 100).toFixed(2)} in minor units/100)`);
console.log(`sum total_amount = ${sumTotal} (${(sumTotal / 100).toFixed(2)})`);
const delta = sumTotal - sumAmount;
console.log(`delta            = ${delta} minor units`);
console.log(`\n30% commission on amount       = ${(sumAmount * 0.3 / 100).toFixed(2)}`);
console.log(`30% commission on total_amount = ${(sumTotal * 0.3 / 100).toFixed(2)}`);
console.log(`overpayment if tax included    = ${(delta * 0.3 / 100).toFixed(2)}`);
