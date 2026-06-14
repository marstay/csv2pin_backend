/**
 * Fix a subscription stuck on a 1-month "Subscription Period" (expires after 1 charge)
 * by re-migrating it onto its OWN (now-corrected) product via change-plan with
 * proration_billing_mode=do_not_bill (NO charge, billing cycle unchanged).
 *
 * Usage:
 *   node scripts/_fix-sub-period.mjs <email>            # dry run (no API write)
 *   node scripts/_fix-sub-period.mjs <email> --apply    # perform the change
 *
 * Verifies by printing subscription_period + expires_at BEFORE and AFTER.
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const raw = readFileSync(resolve(__dirname, '../.env'), 'utf8').split(/\r?\n/);
let section = null, key = null, base = null;
for (const line of raw) {
  const t = line.trim();
  if (/^#\s*live\b/i.test(t)) { section = 'live'; continue; }
  if (/^#\s*test\b/i.test(t)) { section = 'test'; continue; }
  const c = t.replace(/^#\s*/, '');
  const mk = c.match(/^DODO_API_KEY=(.+)$/);
  const mb = c.match(/^DODO_BASE_URL=(.+)$/);
  if (section === 'live') { if (mk) key = mk[1].trim(); if (mb) base = mb[1].trim(); }
}
const H = { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' };

const TARGET_EMAIL = (process.argv[2] || '').toLowerCase().trim();
const APPLY = process.argv.includes('--apply');
if (!TARGET_EMAIL) { console.error('Pass an email as the first argument.'); process.exit(1); }
console.log(`base=${base}  target=${TARGET_EMAIL}  mode=${APPLY ? 'APPLY' : 'DRY-RUN'}\n`);

const emailOf = (o) => (o.customer?.email || o.customer_email || '').toLowerCase();

async function listAll(path) {
  const out = [];
  for (let p = 0; p < 300; p++) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(p) });
    const r = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: H.Authorization } });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}
async function getSub(id) {
  const r = await fetch(`${base}/subscriptions/${id}`, { headers: { Authorization: H.Authorization } });
  return r.ok ? r.json() : null;
}
const summarize = (s) => ({
  id: s.subscription_id || s.id,
  status: s.status,
  product_id: s.product_id,
  quantity: s.quantity ?? 1,
  period: `${s.subscription_period_interval}x${s.subscription_period_count}`,
  freq: `${s.payment_frequency_interval}x${s.payment_frequency_count}`,
  next_billing_date: s.next_billing_date,
  expires_at: s.expires_at,
});

const subs = (await listAll('/subscriptions')).filter(
  (s) => emailOf(s) === TARGET_EMAIL && String(s.status).toLowerCase() === 'active'
);
if (subs.length === 0) { console.error('No ACTIVE subscription found for that email.'); process.exit(1); }
if (subs.length > 1) { console.warn(`WARNING: ${subs.length} active subs found; aborting for safety.`); process.exit(1); }

const sub = subs[0];
const id = sub.subscription_id || sub.id;
const before = summarize(await getSub(id) || sub);
console.log('BEFORE:'); console.log(JSON.stringify(before, null, 2), '\n');

const body = {
  product_id: before.product_id,        // same product, now corrected (period cap removed)
  quantity: before.quantity || 1,
  proration_billing_mode: 'do_not_bill', // NO charge, billing cycle unchanged
  effective_at: 'immediately',
};
console.log('change-plan body:', JSON.stringify(body), '\n');

if (!APPLY) {
  console.log('DRY-RUN: no API write performed. Re-run with --apply to execute.');
  process.exit(0);
}

const resp = await fetch(`${base}/subscriptions/${id}/change-plan`, {
  method: 'POST', headers: H, body: JSON.stringify(body),
});
const txt = await resp.text();
console.log(`change-plan -> HTTP ${resp.status}`);
if (txt) console.log(txt, '\n');
if (!resp.ok) { console.error('change-plan FAILED. No further action.'); process.exit(1); }

await new Promise((r) => setTimeout(r, 2500));
const after = summarize(await getSub(id));
console.log('AFTER:'); console.log(JSON.stringify(after, null, 2), '\n');

const fixed = !/^Month|^Week|^Day/.test(after.period) || after.expires_at !== before.expires_at;
console.log(fixed
  ? '✅ Period appears changed — verify expires_at moved far out / period no longer Month×1.'
  : '⚠️ Period still looks like Month×1. change-plan did NOT reset it — fall back to Dodo support.');
