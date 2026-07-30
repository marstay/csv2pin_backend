/**
 * Read-only: audit affiliate_commissions for mixed currencies.
 *
 * The dashboard sums commission_cents across all rows and formats the total as USD, without
 * grouping by currency. This checks whether any non-USD rows exist, which would make those
 * totals wrong.
 *
 * Usage: node backend/scripts/_affiliate-currency-audit.mjs
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env');

function readEnv() {
  const out = {};
  for (const line of readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const t = line.trim().replace(/^#\s*/, '');
    const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
    if (m && !out[m[1]]) out[m[1]] = m[2].trim();
  }
  return out;
}

const env = readEnv();
const url = env.SUPABASE_URL;
const key = env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_SERVICE_KEY || env.SUPABASE_KEY;
if (!url || !key) {
  console.error('Missing SUPABASE_URL or service key in backend/.env');
  process.exit(1);
}

const db = createClient(url, key, { auth: { persistSession: false } });

const { data, error } = await db
  .from('affiliate_commissions')
  .select('currency, status, amount_cents, commission_cents');

if (error) {
  console.error('Query failed:', error.message);
  process.exit(1);
}

const rows = data || [];
console.log('total commission rows:', rows.length);

if (!rows.length) {
  console.log('No commission rows yet — nothing to audit.');
  process.exit(0);
}

const byCurrency = new Map();
for (const r of rows) {
  const c = String(r.currency || '(null)').toLowerCase();
  if (!byCurrency.has(c)) byCurrency.set(c, { rows: 0, amount: 0, commission: 0 });
  const g = byCurrency.get(c);
  g.rows += 1;
  g.amount += Number(r.amount_cents) || 0;
  g.commission += Number(r.commission_cents) || 0;
}

console.log('\ncurrency | rows | gross_cents | commission_cents');
for (const [c, g] of byCurrency) {
  console.log(`${c.padEnd(8)} | ${String(g.rows).padEnd(4)} | ${String(g.amount).padEnd(11)} | ${g.commission}`);
}

console.log('\ndistinct currencies:', byCurrency.size);
console.log(byCurrency.size > 1
  ? '⚠️  MIXED CURRENCIES — dashboard USD totals are incorrect.'
  : '✅ Single currency — dashboard totals are safe today.');

// Verify the rate arithmetic on every row.
const rate = Number(env.AFFILIATE_COMMISSION_RATE || '0.30') || 0.3;
const bad = rows.filter((r) => {
  const a = Number(r.amount_cents) || 0;
  const c = Number(r.commission_cents) || 0;
  return a > 0 && Math.abs(c - Math.round(a * rate)) > 1;
});
console.log(`\nrows where commission != round(amount * ${rate}):`, bad.length);
if (bad.length) console.log(bad.slice(0, 5));
