/**
 * Create the 2026 pricing products in LIVE Dodo.
 *
 * Prices only — plan pin limits are unchanged, so no code or DB migration is needed. Existing
 * subscriptions stay attached to the OLD products and keep their price indefinitely; only new
 * checkouts use these once DODO_PRODUCT_*_ID env vars are repointed.
 *
 * Dry run (default):  node backend/scripts/create-dodo-2026-pricing.mjs
 * Create for real:    node backend/scripts/create-dodo-2026-pricing.mjs --apply
 *
 * Products are additive and can be archived in the Dodo dashboard if anything is wrong.
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const APPLY = process.argv.includes('--apply');
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
if (!key || !base) { console.error('No live Dodo credentials found.'); process.exit(2); }

/** Mirrors the existing products exactly; only name and price differ. */
const priceBlock = (cents, interval) => ({
  type: 'recurring_price',
  price: cents,
  currency: 'USD',
  tax_inclusive: false,
  discount: 0,
  purchasing_power_parity: false,
  payment_frequency_count: 1,
  payment_frequency_interval: interval, // 'Month' | 'Year'
  subscription_period_count: 20,
  subscription_period_interval: 'Year',
  trial_period_days: 0,
});

// Annual ≈ 10x monthly (~2 months free), matching the existing ladder.
const PLANS = [
  { envKey: 'STARTER', label: 'Starter', monthly: 1200, annual: 12000 },
  { envKey: 'CREATOR', label: 'Creator', monthly: 2500, annual: 25000 },
  { envKey: 'PRO', label: 'Pro', monthly: 5500, annual: 55000 },
  { envKey: 'AGENCY', label: 'Agency', monthly: 12900, annual: 129000 },
];

const jobs = [];
for (const p of PLANS) {
  jobs.push({
    env: `DODO_PRODUCT_${p.envKey}_ID`,
    body: { name: `URL2Pin - ${p.label}`, description: '', is_recurring: true, tax_category: 'saas', price: priceBlock(p.monthly, 'Month') },
  });
  jobs.push({
    env: `DODO_PRODUCT_${p.envKey}_ANNUAL_ID`,
    body: { name: `URL2Pin - ${p.label} Annual`, description: '', is_recurring: true, tax_category: 'saas', price: priceBlock(p.annual, 'Year') },
  });
}

console.log(APPLY ? '*** CREATING LIVE PRODUCTS ***\n' : '--- DRY RUN (pass --apply to create) ---\n');
console.log('env var                              name                            price');
for (const j of jobs) {
  const cents = j.body.price.price;
  const per = j.body.price.payment_frequency_interval === 'Year' ? '/yr' : '/mo';
  console.log(`${j.env.padEnd(36)} ${j.body.name.padEnd(31)} $${(cents / 100).toFixed(2)}${per}`);
}

if (!APPLY) {
  console.log('\nNothing created. Re-run with --apply once the prices above are correct.');
  process.exit(0);
}

console.log('\ncreating...\n');
const results = [];
for (const j of jobs) {
  const r = await fetch(`${base}/products`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
    body: JSON.stringify(j.body),
  });
  const text = await r.text();
  if (!r.ok) {
    console.log(`FAILED  ${j.env}  HTTP ${r.status}  ${text.slice(0, 300)}`);
    results.push({ env: j.env, ok: false });
    continue;
  }
  let id = '';
  try { id = JSON.parse(text).product_id || ''; } catch { /* ignore */ }
  console.log(`OK      ${j.env.padEnd(36)} ${id}`);
  results.push({ env: j.env, id, ok: true });
}

const ok = results.filter((r) => r.ok);
console.log(`\ncreated ${ok.length} of ${jobs.length}`);
if (ok.length) {
  console.log('\n--- paste into Render env (do NOT change until you have tested a checkout) ---');
  for (const r of ok) console.log(`${r.env}=${r.id}`);
}
