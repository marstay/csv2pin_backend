/**
 * Read-only renewal audit: does the 2nd-month payment happen, or do subs expire
 * after a single charge? Fetches all live Dodo subscriptions + payments, links
 * payments to subscriptions, and reports how many subs ever reach a renewal.
 *
 * Usage: node backend/scripts/_subs-renewal-audit.mjs
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
console.log(`base=${base} key=${key.slice(0,4)}…${key.slice(-2)}\n`);

async function listAll(path) {
  const out = [];
  for (let page = 0; page < 300; page++) {
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

const emailOf = (o) => (o.customer?.email || o.customer_email || '(unknown)').toLowerCase();
const day = (d) => String(d || '').slice(0, 10);

const subs = await listAll('/subscriptions');
const payments = await listAll('/payments');

console.log(`Subscriptions: ${subs.length}   Payments: ${payments.length}\n`);

// Status distributions
const subStatus = {}, payStatus = {};
for (const s of subs) { const k = String(s.status||'?').toLowerCase(); subStatus[k]=(subStatus[k]||0)+1; }
for (const p of payments) { const k = String(p.status||'?').toLowerCase(); payStatus[k]=(payStatus[k]||0)+1; }
console.log('Subscriptions by status:', subStatus);
console.log('Payments by status:', payStatus, '\n');

// Map payments -> subscription_id (if present) and count succeeded per sub
const succeededBySub = new Map();
const failedBySub = new Map();
let paymentsWithSubId = 0;
for (const p of payments) {
  const subId = p.subscription_id || p.subscription?.subscription_id || null;
  if (subId) paymentsWithSubId++;
  const st = String(p.status || '').toLowerCase();
  const map = st === 'succeeded' ? succeededBySub : (['failed','declined'].includes(st) ? failedBySub : null);
  if (!map || !subId) continue;
  map.set(subId, (map.get(subId) || 0) + 1);
}
console.log(`Payments carrying a subscription_id: ${paymentsWithSubId}/${payments.length}`);
console.log('(If 0, Dodo payments are not linked to subs and we infer renewals by customer + count.)\n');

// Per-subscription view
const rows = subs.map((s) => {
  const id = s.subscription_id || s.id;
  const created = new Date(s.created_at || 0);
  const nextBill = s.next_billing_date || s.current_period_end || null;
  const ageDays = Math.floor((Date.now() - created.getTime()) / 86400000);
  return {
    id,
    email: emailOf(s),
    status: String(s.status || '?').toLowerCase(),
    created: day(s.created_at),
    nextBill: day(nextBill),
    nextBillPast: nextBill ? new Date(nextBill).getTime() < Date.now() : false,
    ageDays,
    succeeded: succeededBySub.get(id) || 0,
    failed: failedBySub.get(id) || 0,
    cancelledAt: s.cancelled_at || s.canceled_at || null,
  };
});

console.log('=== PER-SUBSCRIPTION ===');
rows.sort((a,b)=> a.created < b.created ? 1 : -1).forEach((r) => {
  console.log(
    `${r.created}  ${r.status.padEnd(9)}  age=${String(r.ageDays).padStart(3)}d  paid=${r.succeeded} failed=${r.failed}  next=${r.nextBill}${r.nextBillPast?'(past)':''}  ${r.cancelledAt?'cancelled':''}  ${r.email}`
  );
});

// Renewal signal: subs older than ~33 days that should have renewed at least once
const RENEW_WINDOW = 33;
const matured = rows.filter((r) => r.ageDays >= RENEW_WINDOW);
const maturedRenewed = matured.filter((r) => r.succeeded >= 2);
const maturedSinglePayment = matured.filter((r) => r.succeeded <= 1);
const expiredNoCancel = rows.filter((r) => r.status === 'expired' && !r.cancelledAt);

console.log('\n=== RENEWAL SUMMARY ===');
console.log(`Subs old enough to have renewed (>= ${RENEW_WINDOW}d): ${matured.length}`);
console.log(`  ...that reached a 2nd+ payment (renewed):        ${maturedRenewed.length}`);
console.log(`  ...still on a single payment (NO renewal):       ${maturedSinglePayment.length}`);
console.log(`Subs in status 'expired' that were NOT cancelled:  ${expiredNoCancel.length}`);
if (paymentsWithSubId === 0) {
  console.log('\nNOTE: payments are not linked to subscription_id, so paid/failed counts above are 0.');
  console.log('Falling back to customer-level renewal inference:');
  const byEmail = new Map();
  for (const p of payments) {
    if (String(p.status||'').toLowerCase() !== 'succeeded') continue;
    const e = emailOf(p);
    if (!byEmail.has(e)) byEmail.set(e, []);
    byEmail.get(e).push(day(p.created_at));
  }
  let custWithRenewal = 0, custSingle = 0;
  for (const [, dates] of byEmail) {
    const uniqMonths = new Set(dates.map((d) => d.slice(0,7)));
    if (uniqMonths.size >= 2) custWithRenewal++; else custSingle++;
  }
  console.log(`  Paying customers with payments in >=2 distinct months: ${custWithRenewal}`);
  console.log(`  Paying customers with payments in only 1 month:        ${custSingle}`);
}
