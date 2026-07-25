/**
 * READ-ONLY: dump the affiliate attribution state so we can see, for every
 * partner, whether their slug is active, whether it grants a discount, and
 * how many conversions/commissions actually attributed to them.
 *
 * Usage: node backend/scripts/_affiliate-state.mjs
 */
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in backend/.env');
  process.exit(1);
}
const sb = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

// Parse the customer-facing discount map (slug -> discount code).
let discountMap = {};
try {
  discountMap = JSON.parse(process.env.DODO_PARTNER_DISCOUNT_MAP || '{}');
} catch {
  console.warn('DODO_PARTNER_DISCOUNT_MAP is not valid JSON');
}

const { data: affiliates, error: aErr } = await sb
  .from('affiliates')
  .select('id, slug, email, display_name, status, commission_rate, recurring_months, created_at')
  .order('created_at', { ascending: true });
if (aErr) {
  console.error('affiliates query failed:', aErr.message);
  process.exit(1);
}

const { data: commissions, error: cErr } = await sb
  .from('affiliate_commissions')
  .select('affiliate_id, plan_type, amount_cents, commission_cents, status, commission_kind, created_at');
if (cErr) {
  console.error('affiliate_commissions query failed:', cErr.message);
}

const byAff = new Map();
for (const c of commissions || []) {
  const arr = byAff.get(c.affiliate_id) || [];
  arr.push(c);
  byAff.set(c.affiliate_id, arr);
}

console.log(`\n=== AFFILIATES (${affiliates.length}) ===`);
for (const a of affiliates) {
  const rows = byAff.get(a.id) || [];
  const live = rows.filter((r) => r.status !== 'void');
  const commTotal = live.reduce((s, r) => s + (Number(r.commission_cents) || 0), 0);
  const grossTotal = live.reduce((s, r) => s + (Number(r.amount_cents) || 0), 0);
  const hasDiscount = Object.prototype.hasOwnProperty.call(discountMap, a.slug);
  console.log(`\n  slug=${a.slug}  [${a.status}]  ${a.display_name || ''} <${a.email || ''}>`);
  console.log(`    rate=${a.commission_rate ?? 'default'}  recurring_months=${a.recurring_months ?? 'default'}  joined=${String(a.created_at).slice(0, 10)}`);
  console.log(`    discount code on ?ref=${a.slug}: ${hasDiscount ? discountMap[a.slug] : 'NONE (no customer incentive)'}`);
  console.log(`    conversions=${live.length}  gross=$${(grossTotal / 100).toFixed(2)}  commission owed=$${(commTotal / 100).toFixed(2)}`);
  if (live.length) {
    const byStatus = live.reduce((m, r) => ((m[r.status] = (m[r.status] || 0) + 1), m), {});
    console.log(`    by status: ${JSON.stringify(byStatus)}`);
  }
}

// Discount-map slugs that do NOT match an affiliate row (orphans).
const affSlugs = new Set(affiliates.map((a) => a.slug));
const orphanCodes = Object.keys(discountMap).filter((s) => !affSlugs.has(s));
console.log(`\n=== DISCOUNT MAP (${Object.keys(discountMap).length} slugs) ===`);
console.log(`  slugs: ${Object.keys(discountMap).join(', ') || '(empty)'}`);
if (orphanCodes.length) {
  console.log(`  WARNING: discount slugs with no affiliate row: ${orphanCodes.join(', ')}`);
}

const orphanCommissions = (commissions || []).filter((c) => !affiliates.some((a) => a.id === c.affiliate_id));
console.log(`\n=== TOTALS ===`);
console.log(`  total commission rows: ${(commissions || []).length}`);
console.log(`  active affiliates: ${affiliates.filter((a) => a.status === 'active').length}`);
console.log(`  affiliates with 0 conversions: ${affiliates.filter((a) => !(byAff.get(a.id) || []).some((r) => r.status !== 'void')).length}`);
if (orphanCommissions.length) console.log(`  orphan commission rows (no affiliate): ${orphanCommissions.length}`);
