/**
 * Read-only audit: independently tally Dodo payments/refunds to cross-check the
 * founder dashboard. Reads the LIVE credentials from the `#live` block in backend/.env
 * (even if commented out) so the secret is never passed on the command line / printed.
 *
 * Usage: node backend/scripts/dodo-audit.mjs   (run from repo root or backend/)
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');

function extractLiveCreds() {
  const raw = readFileSync(envPath, 'utf8').split(/\r?\n/);
  let section = null;
  let key = null;
  let base = null;
  for (const line of raw) {
    const t = line.trim();
    if (/^#\s*live\b/i.test(t)) { section = 'live'; continue; }
    if (/^#\s*test\b/i.test(t)) { section = 'test'; continue; }
    const content = t.replace(/^#\s*/, '');
    const mk = content.match(/^DODO_API_KEY=(.+)$/);
    const mb = content.match(/^DODO_BASE_URL=(.+)$/);
    if (section === 'live') {
      if (mk) key = mk[1].trim();
      if (mb) base = mb[1].trim();
    }
  }
  return { key, base };
}

const { key, base } = extractLiveCreds();
if (!key || !base) {
  console.error('Could not find live DODO creds under the #live block in backend/.env');
  process.exit(1);
}
console.log(`Using base: ${base}  key: ${key.slice(0, 4)}…${key.slice(-2)} (masked)`);

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

// Same default rates as the dashboard (USD value of 1 unit of currency).
const RATES = { USD: 1, EUR: 1.08, CAD: 0.73, AUD: 0.66, BRL: 0.18 };
const usd = (v, currency) => {
  const code = String(currency || 'USD').toUpperCase();
  const rate = Number.isFinite(RATES[code]) ? RATES[code] : 1;
  return ((Number(v) || 0) / 100) * rate;
};
const cents = (v) => (Number(v) || 0) / 100;
const monthKey = (d) => { const dt = new Date(d); return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, '0')}`; };

const payments = await listAll('/payments');
const refunds = await listAll('/refunds');

const byStatus = {};
let gross = 0; const byMonth = {};
const payById = new Map();
for (const p of payments) {
  const st = String(p.status || '').toLowerCase() || '(none)';
  byStatus[st] = (byStatus[st] || 0) + 1;
  const id = p.payment_id || p.id;
  if (id) payById.set(id, p);
  if (['succeeded', 'completed', 'paid', ''].includes(String(p.status || '').toLowerCase())) {
    const g = usd(p.total_amount ?? p.amount, p.currency);
    gross += g;
    const mk = monthKey(p.created_at);
    byMonth[mk] = (byMonth[mk] || 0) + g;
  }
}

let refundTotal = 0; let within24 = 0;
const voidedPaymentIds = new Set();
const refundDetails = [];
for (const r of refunds) {
  const amt = usd(r.amount, r.currency);
  refundTotal += amt;
  const pid = r.payment_id || r.payment?.payment_id;
  const pay = pid ? payById.get(pid) : null;
  let hrs = null;
  if (pay && pay.created_at && r.created_at) {
    const delta = new Date(r.created_at) - new Date(pay.created_at);
    hrs = delta / 3600000;
    if (delta >= 0 && delta <= 24 * 3600 * 1000) { within24 += 1; if (pid) voidedPaymentIds.add(pid); }
  }
  refundDetails.push({
    email: (pay?.customer?.email || pay?.customer_email || '(unknown)').toLowerCase(),
    amount: amt,
    hoursAfterPayment: hrs == null ? 'n/a' : hrs.toFixed(1),
    reason: r.reason || '',
  });
}

// Gross excluding the immediate-refunded (voided) payments, plus per-customer breakdown.
let grossExclVoided = 0;
const byCustomer = new Map();
for (const p of payments) {
  if (!['succeeded', 'completed', 'paid', ''].includes(String(p.status || '').toLowerCase())) continue;
  const id = p.payment_id || p.id;
  const g = usd(p.total_amount ?? p.amount, p.currency);
  if (!voidedPaymentIds.has(id)) grossExclVoided += g;
  const email = (p.customer?.email || p.customer_email || '(unknown)').toLowerCase();
  byCustomer.set(email, (byCustomer.get(email) || 0) + g);
}

console.log('\n=== DODO AUDIT (converted to USD) ===');
console.log('Rates used:', JSON.stringify(RATES));
console.log('Total payments fetched:', payments.length);
console.log('Payments by status:', byStatus);
console.log('Gross (succeeded, USD-converted):', gross.toFixed(2));
console.log('Refunds:', refunds.length, 'total USD:', refundTotal.toFixed(2), '| within 24h of payment:', within24);
console.log('Gross excluding voided (immediate-refund) payments:', grossExclVoided.toFixed(2));
console.log('\nGross by month (succeeded, USD-converted):');
Object.keys(byMonth).sort().forEach((m) => console.log(`  ${m}: $${byMonth[m].toFixed(2)}`));
console.log('\nTop customers by gross (succeeded, USD-converted):');
[...byCustomer.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10).forEach(([e, v]) => console.log(`  ${e}: $${v.toFixed(2)}`));
console.log('\nRefund details (customer · amount · hrs after payment · reason):');
refundDetails.forEach((r) => console.log(`  ${r.email} · $${r.amount.toFixed(2)} · ${r.hoursAfterPayment}h · ${r.reason}`));

// --- Currency inspection ---
console.log('\n=== CURRENCY INSPECTION ===');
const byCurrency = {};
for (const p of payments) {
  if (!['succeeded', 'completed', 'paid', ''].includes(String(p.status || '').toLowerCase())) continue;
  const cur = String(p.currency || '?').toUpperCase();
  byCurrency[cur] = byCurrency[cur] || { count: 0, totalAmountSum: 0 };
  byCurrency[cur].count += 1;
  byCurrency[cur].totalAmountSum += Number(p.total_amount || 0);
}
console.log('Succeeded payments grouped by currency (raw total_amount, minor units):');
Object.entries(byCurrency).forEach(([c, v]) => console.log(`  ${c}: ${v.count} payments, sum total_amount=${v.totalAmountSum}`));

console.log('\nRaw payment fields for jonnatanm + dennis (currency / total_amount / settlement_currency / settlement_amount):');
for (const p of payments) {
  const email = (p.customer?.email || p.customer_email || '').toLowerCase();
  if (!email.includes('jonnatanm') && !email.includes('dennis.gallon')) continue;
  console.log(`  ${email} | status=${p.status} | currency=${p.currency} | total_amount=${p.total_amount} | settlement_currency=${p.settlement_currency} | settlement_amount=${p.settlement_amount} | created=${p.created_at}`);
}
