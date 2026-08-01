/**
 * Show what a partner is owed, and record a payout once you have actually sent the money.
 *
 * Commissions are grouped by SETTLEMENT currency and never summed across currencies — Dodo
 * settles EUR sales in EUR and USD sales in USD, so a single total would be meaningless.
 *
 *   node backend/scripts/affiliate-payout.mjs                          # owed, all partners
 *   node backend/scripts/affiliate-payout.mjs --slug george            # owed, one partner
 *   node backend/scripts/affiliate-payout.mjs --slug george --detail   # every commission row
 *   node backend/scripts/affiliate-payout.mjs --slug george --currency usd --pay --reference "wise-1234"
 *
 * --pay marks pending/approved rows as paid. It never touches `held` (still inside the refund
 * window) or `void` (refunded/charged back), and re-running is a no-op.
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const arg = (name) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > -1 ? String(process.argv[i + 1] || '').trim() : null;
};
const SLUG = (arg('slug') || '').toLowerCase() || null;
const CURRENCY = (arg('currency') || '').toLowerCase() || null;
const REFERENCE = arg('reference');
const PAY = process.argv.includes('--pay');
const DETAIL = process.argv.includes('--detail');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const money = (cents, ccy) => `${(cents / 100).toFixed(2)} ${String(ccy).toUpperCase()}`;

const { data: affiliates } = await supabase.from('affiliates').select('id, slug, display_name, payout_email, commission_rate, status');
const bySlug = new Map((affiliates || []).map((a) => [a.slug, a]));
if (SLUG && !bySlug.has(SLUG)) {
  console.error(`No affiliate with slug "${SLUG}". Known: ${[...bySlug.keys()].join(', ') || '(none)'}`);
  process.exit(2);
}

let q = supabase.from('affiliate_commissions').select('*');
if (SLUG) q = q.eq('affiliate_id', bySlug.get(SLUG).id);
const { data: rows, error } = await q;
if (error) { console.error('Failed to load commissions:', error.message); process.exit(1); }

const byId = new Map((affiliates || []).map((a) => [a.id, a]));
const groups = new Map(); // affiliateId -> { status -> { currency -> cents } }
for (const r of rows || []) {
  const g = groups.get(r.affiliate_id) || {};
  const ccy = String(r.currency || 'usd').toLowerCase();
  g[r.status] = g[r.status] || {};
  g[r.status][ccy] = (g[r.status][ccy] || 0) + (Number(r.commission_cents) || 0);
  groups.set(r.affiliate_id, g);
}

if (!rows?.length) {
  console.log(SLUG ? `No commissions recorded for "${SLUG}" yet.` : 'No commissions recorded yet.');
}

for (const [affId, g] of groups) {
  const a = byId.get(affId);
  console.log(`\n=== ${a?.slug || affId}  (${a?.payout_email || 'no payout email'}, rate ${a?.commission_rate}) ===`);
  for (const status of ['held', 'pending', 'approved', 'paid', 'void']) {
    if (!g[status]) continue;
    const parts = Object.entries(g[status]).map(([c, cents]) => money(cents, c)).join(' · ');
    const note =
      status === 'held' ? '  (inside refund hold — not yet payable)'
      : status === 'void' ? '  (refunded or charged back)'
      : '';
    console.log(`  ${status.padEnd(9)} ${parts}${note}`);
  }
  const payable = { ...(g.pending || {}) };
  for (const [c, v] of Object.entries(g.approved || {})) payable[c] = (payable[c] || 0) + v;
  const owed = Object.entries(payable).filter(([, v]) => v > 0);
  console.log(`  ${'OWED NOW'.padEnd(9)} ${owed.length ? owed.map(([c, v]) => money(v, c)).join(' · ') : '(nothing)'}`);
}

if (DETAIL) {
  console.log('\n--- commission rows ---');
  console.log('created            kind          status    gross      tax    fee    net       commission');
  for (const r of (rows || []).sort((x, y) => String(x.created_at).localeCompare(String(y.created_at)))) {
    const c = String(r.currency || 'usd');
    console.log(
      `${String(r.created_at).slice(0, 16).padEnd(18)} ${String(r.commission_kind).padEnd(13)} ${String(r.status).padEnd(9)} ` +
        `${money(r.amount_cents || 0, c).padEnd(10)} ${String(r.tax_cents ?? '-').padEnd(6)} ${String(r.fee_cents ?? '-').padEnd(6)} ` +
        `${String(r.net_amount_cents ?? '-').padEnd(9)} ${money(r.commission_cents || 0, c)}`
    );
  }
}

if (!PAY) {
  console.log('\n(read-only — add --currency <ccy> --pay --reference "<transfer id>" to record a payout)');
  process.exit(0);
}

if (!SLUG || !CURRENCY || !REFERENCE) {
  console.error('\n--pay requires --slug, --currency and --reference (the transfer id, for your records).');
  process.exit(2);
}

const affiliate = bySlug.get(SLUG);
const patch = { status: 'paid' };
const { error: colErr } = await supabase.from('affiliate_commissions').select('paid_at, payout_reference').limit(1);
if (!colErr) { patch.paid_at = new Date().toISOString(); patch.payout_reference = REFERENCE; }
else console.warn('NOTE: paid_at/payout_reference columns missing — marking paid without the reference.');

const { data: marked, error: payErr } = await supabase
  .from('affiliate_commissions')
  .update(patch)
  .eq('affiliate_id', affiliate.id)
  .eq('currency', CURRENCY)
  .in('status', ['pending', 'approved'])
  .select('id, commission_cents');
if (payErr) { console.error('Payout failed:', payErr.message); process.exit(1); }

const total = (marked || []).reduce((s, r) => s + (Number(r.commission_cents) || 0), 0);
console.log(`\nMarked ${marked?.length || 0} commission(s) as paid — ${money(total, CURRENCY)} (ref: ${REFERENCE})`);
