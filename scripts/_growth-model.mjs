/**
 * Read-only: compute the real unit economics needed to model a path to a revenue target.
 * MRR, ARPU, plan mix, cohort retention, churn, refund rate, and new-customer velocity.
 *
 * Usage: node backend/scripts/_growth-model.mjs
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envRaw = readFileSync(resolve(__dirname, '../.env'), 'utf8');

function liveCreds() {
  let section = null, key = null, base = null;
  for (const line of envRaw.split(/\r?\n/)) {
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
const { key, base } = liveCreds();

const productToPlan = {};
const productToInterval = {};
for (const m of envRaw.matchAll(/DODO_PRODUCT_([A-Z]+?)(_ANNUAL)?_ID\s*=\s*(\S+)/g)) {
  productToPlan[m[3].trim()] = m[1].toLowerCase();
  productToInterval[m[3].trim()] = m[2] ? 'year' : 'month';
}

async function listAll(path) {
  const out = [];
  for (let p = 0; p < 100; p += 1) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(p) });
    const r = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: `Bearer ${key}` } });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

// FX to USD — rough, only used so non-USD subs are not counted at face value.
const FX = { USD: 1, EUR: 1.08, CAD: 0.73, AUD: 0.66, BRL: 0.18, NGN: 0.00065, ZMW: 0.037, CNY: 0.14, INR: 0.012, GBP: 1.27 };
const usd = (minor, cur) => ((Number(minor) || 0) / 100) * (FX[String(cur || 'USD').toUpperCase()] ?? 1);

const subs = await listAll('/subscriptions');
const payments = await listAll('/payments');
const refunds = await listAll('/refunds');

const active = subs.filter((s) => String(s.status) === 'active');

// ---------------------------------------------------------------- MRR
let mrr = 0;
const mix = {};
for (const s of active) {
  const plan = productToPlan[s.product_id] || '?';
  const interval = productToInterval[s.product_id] || 'month';
  const amt = usd(s.recurring_pre_tax_amount ?? s.amount, s.currency);
  const monthly = interval === 'year' ? amt / 12 : amt;
  mrr += monthly;
  mix[plan] = mix[plan] || { n: 0, mrr: 0 };
  mix[plan].n += 1;
  mix[plan].mrr += monthly;
}

console.log('=== CURRENT STATE ===');
console.log(`active subscriptions : ${active.length}`);
console.log(`MRR                  : $${mrr.toFixed(2)}`);
console.log(`ARPU                 : $${(mrr / Math.max(1, active.length)).toFixed(2)}`);
console.log('\nplan mix:');
for (const [p, v] of Object.entries(mix).sort((a, b) => b[1].mrr - a[1].mrr)) {
  console.log(`  ${p.padEnd(8)} ${String(v.n).padStart(3)} subs  $${v.mrr.toFixed(2)}/mo  (${((v.mrr / mrr) * 100).toFixed(0)}% of MRR)`);
}

// ------------------------------------------------- acquisition & churn
const byMonth = {};
for (const s of subs) {
  const m = String(s.created_at).slice(0, 7);
  byMonth[m] = byMonth[m] || { started: 0, stillActive: 0, churned: 0 };
  byMonth[m].started += 1;
  if (String(s.status) === 'active') byMonth[m].stillActive += 1;
  else byMonth[m].churned += 1;
}
console.log('\n=== ACQUISITION & RETENTION BY SIGNUP MONTH ===');
console.log('month   | started | still active | churned | retention');
for (const [m, v] of Object.entries(byMonth).sort()) {
  const ret = v.started ? ((v.stillActive / v.started) * 100).toFixed(0) + '%' : '-';
  console.log(`${m} | ${String(v.started).padStart(7)} | ${String(v.stillActive).padStart(12)} | ${String(v.churned).padStart(7)} | ${ret.padStart(9)}`);
}

const totalStarted = subs.length;
const totalActive = active.length;
console.log(`\nlifetime: ${totalStarted} subscriptions ever started, ${totalActive} still active (${((totalActive / totalStarted) * 100).toFixed(0)}% survive)`);

// ------------------------------------------------------------ payments
const succeeded = payments.filter((p) => String(p.status) === 'succeeded');
const real = succeeded.filter((p) => Number(p.total_amount ?? 0) > 0);
const gross = real.reduce((s, p) => s + usd(p.total_amount, p.currency), 0);
const refundTotal = refunds.reduce((s, r) => s + usd(r.amount, r.currency), 0);
console.log('\n=== PAYMENTS (lifetime) ===');
console.log(`succeeded payments (non-zero) : ${real.length}`);
console.log(`gross collected               : $${gross.toFixed(2)}`);
console.log(`refunds                       : ${refunds.length} ($${refundTotal.toFixed(2)}, ${((refundTotal / Math.max(1, gross)) * 100).toFixed(1)}% of gross)`);

// repeat payers = renewals actually happening
const byCustomer = {};
for (const p of real) {
  const e = p.customer?.email || '?';
  byCustomer[e] = (byCustomer[e] || 0) + 1;
}
const payers = Object.keys(byCustomer).length;
const repeat = Object.values(byCustomer).filter((n) => n > 1).length;
console.log(`unique paying customers       : ${payers}`);
console.log(`paid more than once (renewed) : ${repeat} (${((repeat / Math.max(1, payers)) * 100).toFixed(0)}%)`);

// ---------------------------------------------------------- modelling
console.log('\n=== WHAT $10K MRR REQUIRES ===');
const arpu = mrr / Math.max(1, active.length);
console.log(`at current ARPU $${arpu.toFixed(2)} -> ${Math.ceil(10000 / arpu)} paying customers (${(Math.ceil(10000 / arpu) / Math.max(1, active.length)).toFixed(0)}x today)`);
for (const a of [30, 40, 50]) {
  console.log(`at ARPU $${a} -> ${Math.ceil(10000 / a)} paying customers`);
}

// net growth rate over the last 3 months of signups
const months = Object.keys(byMonth).sort();
const recent = months.slice(-3);
const recentStarted = recent.reduce((s, m) => s + byMonth[m].started, 0);
console.log(`\nnew subscriptions in last 3 signup-months (${recent.join(', ')}): ${recentStarted} (~${(recentStarted / 3).toFixed(1)}/mo)`);
