/**
 * Backfill billing_subscriptions.dodo_product_id from Dodo.
 *
 * The product a subscription sits on is the only durable record of WHICH PRICE a customer pays.
 * Without it the app cannot tell a grandfathered customer from one on current pricing, which is
 * how the My Account annual banner ended up promising savings that did not exist.
 *
 * Requires the column:
 *   ALTER TABLE billing_subscriptions ADD COLUMN dodo_product_id text;
 *
 * DRY RUN by default.
 *   node backend/scripts/backfill-product-ids.mjs           # show what would change
 *   node backend/scripts/backfill-product-ids.mjs --apply   # write
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const APPLY = process.argv.includes('--apply');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

const { error: colErr } = await supabase.from('billing_subscriptions').select('dodo_product_id').limit(1);
if (colErr) {
  console.error('FATAL: billing_subscriptions.dodo_product_id does not exist yet.');
  console.error('Run this in the Supabase SQL editor first:');
  console.error('  ALTER TABLE billing_subscriptions ADD COLUMN dodo_product_id text;');
  process.exit(2);
}

async function listDodo(path) {
  const out = [];
  const base = process.env.DODO_BASE_URL;
  const key = process.env.DODO_API_KEY;
  for (let p = 0; p < 100; p += 1) {
    const r = await fetch(`${base}${path}?page_size=100&page_number=${p}`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

const dodoSubs = await listDodo('/subscriptions');
const productBySub = new Map();
for (const d of dodoSubs) {
  const id = d?.subscription_id || d?.id;
  if (id && d?.product_id) productBySub.set(id, d.product_id);
}
console.log(`Dodo subscriptions fetched: ${productBySub.size}`);

const { data: rows, error } = await supabase
  .from('billing_subscriptions')
  .select('id, user_id, plan_type, status, dodo_subscription_id, dodo_product_id');
if (error) {
  console.error('Failed to load subscriptions:', error.message);
  process.exit(1);
}

let toSet = 0, toFix = 0, alreadyOk = 0, comped = 0, unresolved = 0, failed = 0;
for (const r of rows || []) {
  const subId = String(r.dodo_subscription_id || '');
  if (!subId || subId.startsWith('comp:')) { comped += 1; continue; }
  const truth = productBySub.get(subId);
  if (!truth) { unresolved += 1; continue; }
  if (r.dodo_product_id === truth) { alreadyOk += 1; continue; }

  const kind = r.dodo_product_id ? 'FIX ' : 'SET ';
  if (r.dodo_product_id) toFix += 1; else toSet += 1;
  console.log(`  ${kind} ${String(r.plan_type).padEnd(8)} ${r.status.padEnd(9)} ${subId}  ${r.dodo_product_id || '(null)'} -> ${truth}`);

  if (APPLY) {
    const { error: upErr } = await supabase
      .from('billing_subscriptions')
      .update({ dodo_product_id: truth, updated_at: new Date().toISOString() })
      .eq('id', r.id);
    if (upErr) { console.log(`       ERROR ${upErr.message}`); failed += 1; }
  }
}

console.log(
  `\n${APPLY ? 'APPLIED' : 'DRY RUN'} — ${toSet} to set, ${toFix} to correct, ${alreadyOk} already correct, ` +
    `${comped} comped/skipped, ${unresolved} not found in Dodo${failed ? `, ${failed} FAILED` : ''}`
);
if (!APPLY) console.log('Re-run with --apply to write.');
