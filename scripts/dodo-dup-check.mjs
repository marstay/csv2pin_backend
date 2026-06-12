/**
 * Read-only check: detect DUPLICATE charges in live Dodo data.
 *
 * A "duplicate" = two or more succeeded payments for the SAME customer that are
 * close together in time (default < 3 days apart) — i.e. NOT a normal monthly
 * renewal (~30 days). Prints each cluster with dates/amounts and whether each
 * charge was refunded, plus the most-recent duplicate date so we can tell if the
 * fix is holding.
 *
 * Usage: node backend/scripts/dodo-dup-check.mjs
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

const SUCCESS = ['succeeded', 'completed', 'paid', ''];
const isSuccess = (p) => SUCCESS.includes(String(p.status || '').toLowerCase());
const cents = (v) => (Number(v) || 0) / 100;
const DUP_WINDOW_MS = 3 * 24 * 3600 * 1000; // charges within 3 days = not a monthly renewal

const payments = await listAll('/payments');
const refunds = await listAll('/refunds');

const refundedPaymentIds = new Set();
for (const r of refunds) {
  const pid = r.payment_id || r.payment?.payment_id;
  if (pid) refundedPaymentIds.add(pid);
}

// Group succeeded payments by customer email.
const byCustomer = new Map();
for (const p of payments) {
  if (!isSuccess(p)) continue;
  const email = (p.customer?.email || p.customer_email || '(unknown)').toLowerCase();
  if (!byCustomer.has(email)) byCustomer.set(email, []);
  byCustomer.get(email).push({
    id: p.payment_id || p.id,
    at: new Date(p.created_at),
    amount: cents(p.total_amount ?? p.amount),
    currency: String(p.currency || 'USD').toUpperCase(),
    subId: p.subscription_id || p.subscription?.id || '(none)',
  });
}

const clusters = [];
for (const [email, list] of byCustomer.entries()) {
  list.sort((a, b) => a.at - b.at);
  let group = [];
  const flush = () => {
    if (group.length >= 2) clusters.push({ email, group: [...group] });
    group = [];
  };
  for (let i = 0; i < list.length; i++) {
    if (group.length === 0) { group.push(list[i]); continue; }
    const prev = group[group.length - 1];
    if (list[i].at - prev.at <= DUP_WINDOW_MS) group.push(list[i]);
    else { flush(); group.push(list[i]); }
  }
  flush();
}

clusters.sort((a, b) => b.group[b.group.length - 1].at - a.group[a.group.length - 1].at);

console.log('\n=== DUPLICATE CHARGE CLUSTERS (same customer, charges < 3 days apart) ===');
if (clusters.length === 0) {
  console.log('None found. No same-customer charges within 3 days of each other.');
} else {
  let mostRecent = new Date(0);
  for (const c of clusters) {
    console.log(`\n${c.email}  (${c.group.length} charges in cluster)`);
    for (const g of c.group) {
      const refunded = refundedPaymentIds.has(g.id) ? 'REFUNDED' : 'NOT refunded';
      console.log(`   ${g.at.toISOString().slice(0, 16)}  ${g.currency} ${g.amount.toFixed(2)}  sub=${g.subId}  [${refunded}]  ${g.id}`);
      if (g.at > mostRecent) mostRecent = g.at;
    }
  }
  console.log(`\nMost recent duplicate charge: ${mostRecent.toISOString().slice(0, 10)}`);
  console.log(`Total customers affected by duplicates: ${clusters.length}`);
}
