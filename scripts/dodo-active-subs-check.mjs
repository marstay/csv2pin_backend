/**
 * Read-only check: list ACTIVE subscriptions per customer in live Dodo.
 * Flags any customer with more than one active subscription — those are the
 * live duplicates that would re-bill at the next renewal.
 *
 * Usage: node backend/scripts/dodo-active-subs-check.mjs
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

const subs = await listAll('/subscriptions');
console.log(`\nTotal subscriptions fetched: ${subs.length}`);

const statusCounts = {};
for (const s of subs) {
  const st = String(s.status || '(none)').toLowerCase();
  statusCounts[st] = (statusCounts[st] || 0) + 1;
}
console.log('Subscriptions by status:', statusCounts);

const ACTIVE = ['active', 'on_trial', 'trialing'];
const byCustomer = new Map();
for (const s of subs) {
  if (!ACTIVE.includes(String(s.status || '').toLowerCase())) continue;
  const email = (s.customer?.email || s.customer_email || '(unknown)').toLowerCase();
  if (!byCustomer.has(email)) byCustomer.set(email, []);
  byCustomer.get(email).push({
    id: s.subscription_id || s.id,
    status: s.status,
    nextBill: s.next_billing_date || s.current_period_end || '?',
    amount: s.recurring_pre_tax_amount ?? s.amount ?? '?',
    currency: s.currency || '?',
    cancelAtNext: s.cancel_at_next_billing_date ?? s.cancel_at_period_end ?? false,
  });
}

const RECOVERABLE = ['failed', 'on_hold', 'past_due'];
const recoverable = subs
  .filter((s) => RECOVERABLE.includes(String(s.status || '').toLowerCase()))
  .map((s) => ({
    email: (s.customer?.email || s.customer_email || '(unknown)').toLowerCase(),
    status: String(s.status || '').toLowerCase(),
    created: String(s.created_at || '?').slice(0, 10),
    nextBill: String(s.next_billing_date || s.current_period_end || '?').slice(0, 10),
    amount: s.recurring_pre_tax_amount ?? s.amount ?? '?',
    currency: s.currency || '?',
  }))
  .sort((a, b) => (a.created < b.created ? 1 : -1));

console.log('\n=== NON-ACTIVE SUBS (failed / on_hold / past_due) — recoverable involuntary churn ===');
recoverable.forEach((s) =>
  console.log(`   ${s.created}  ${s.status.padEnd(8)}  ${s.email}  ${s.currency} ${s.amount}  next=${s.nextBill}`)
);

const dupes = [...byCustomer.entries()].filter(([, list]) => list.length > 1);
console.log(`\n=== CUSTOMERS WITH >1 ACTIVE SUBSCRIPTION (live duplicates) ===`);
if (dupes.length === 0) {
  console.log('None. Every customer has at most one active subscription.');
} else {
  for (const [email, list] of dupes) {
    console.log(`\n${email}  (${list.length} active subs)`);
    for (const s of list) {
      console.log(`   ${s.id}  status=${s.status}  next=${String(s.nextBill).slice(0, 10)}  ${s.currency} ${s.amount}  cancelAtNext=${s.cancelAtNext}`);
    }
  }
}
