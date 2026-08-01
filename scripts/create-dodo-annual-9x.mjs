/**
 * Replace the annual products with a sharper "3 months free" (9x monthly) discount.
 *
 * The 10x annual products created earlier were never pointed at by any env var, so nothing is
 * live on them — archive those in the Dodo dashboard once these are verified.
 *
 * Dry run: node backend/scripts/create-dodo-annual-9x.mjs
 * Create:  node backend/scripts/create-dodo-annual-9x.mjs --apply
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

const priceBlock = (cents) => ({
  type: 'recurring_price',
  price: cents,
  currency: 'USD',
  tax_inclusive: false,
  discount: 0,
  purchasing_power_parity: false,
  payment_frequency_count: 1,
  payment_frequency_interval: 'Year',
  subscription_period_count: 20,
  subscription_period_interval: 'Year',
  trial_period_days: 0,
});

// 9x monthly = 3 months free (25% off).
const PLANS = [
  { envKey: 'STARTER', label: 'Starter', monthly: 12, annual: 10800 },
  { envKey: 'CREATOR', label: 'Creator', monthly: 25, annual: 22500 },
  { envKey: 'PRO', label: 'Pro', monthly: 55, annual: 49500 },
  { envKey: 'AGENCY', label: 'Agency', monthly: 129, annual: 116100 },
];

console.log(APPLY ? '*** CREATING LIVE ANNUAL PRODUCTS (9x) ***\n' : '--- DRY RUN (pass --apply) ---\n');
console.log('plan      monthly   annual     vs 12x monthly   saving');
for (const p of PLANS) {
  const full = p.monthly * 12;
  const save = full - p.annual / 100;
  console.log(
    `${p.label.padEnd(9)} $${String(p.monthly).padEnd(8)} $${String(p.annual / 100).padEnd(9)} $${String(full).padEnd(15)} $${save} (${Math.round((save / full) * 100)}%)`
  );
}

if (!APPLY) {
  console.log('\nNothing created.');
  process.exit(0);
}

console.log('\ncreating...\n');
const out = [];
for (const p of PLANS) {
  const body = {
    name: `URL2Pin - ${p.label} Annual`,
    description: '',
    is_recurring: true,
    tax_category: 'saas',
    price: priceBlock(p.annual),
  };
  const r = await fetch(`${base}/products`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  if (!r.ok) {
    console.log(`FAILED  ${p.label}  HTTP ${r.status}  ${text.slice(0, 300)}`);
    continue;
  }
  const id = JSON.parse(text).product_id;
  console.log(`OK      ${p.label.padEnd(9)} ${id}`);
  out.push([`DODO_PRODUCT_${p.envKey}_ANNUAL_ID`, id]);
}

if (out.length) {
  console.log('\n--- annual env vars (replaces the 10x ones) ---');
  for (const [k, v] of out) console.log(`${k}=${v}`);
}
