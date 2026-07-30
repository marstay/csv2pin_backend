/**
 * CLI wrapper around src/billingReconcile.js.
 *
 * The same reconciliation also runs nightly inside the backend process
 * (startBillingReconciliation in src/index.js) — this script exists for manual runs,
 * one-off repairs, and external cron. Both share one implementation so they cannot drift.
 *
 * Dry run (default):  node backend/scripts/reconcile-billing.mjs
 * Apply:              node backend/scripts/reconcile-billing.mjs --apply
 * Exits 1 if a CRITICAL issue needs a human (customer paying with no access), 2 on bad config.
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';
import { runBillingReconciliation, buildProductMaps } from '../src/billingReconcile.js';

const APPLY = process.argv.includes('--apply');
const __dirname = dirname(fileURLToPath(import.meta.url));

// Locally the credentials live in backend/.env; in production they are real environment
// variables. Support both, preferring process.env.
let envRaw = '';
try {
  envRaw = readFileSync(resolve(__dirname, '../.env'), 'utf8');
} catch {
  envRaw = '';
}

function plainEnv() {
  const out = {};
  for (const l of envRaw.split(/\r?\n/)) {
    const t = l.trim().replace(/^#\s*/, '');
    const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
    if (m && !out[m[1]]) out[m[1]] = m[2].trim();
  }
  return { ...out, ...Object.fromEntries(Object.entries(process.env).filter(([, v]) => v != null)) };
}

/** The .env file keeps live and test Dodo keys in commented sections; prefer the live block. */
function liveCreds(env) {
  if (process.env.DODO_API_KEY) {
    return {
      key: process.env.DODO_API_KEY.trim(),
      base: (process.env.DODO_BASE_URL || 'https://live.dodopayments.com').trim(),
    };
  }
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
  return { key: key || env.DODO_API_KEY, base: base || env.DODO_BASE_URL };
}

const env = plainEnv();
const { key, base } = liveCreds(env);
const SUPA_URL = env.SUPABASE_URL;
const SUPA_KEY = env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_SERVICE_KEY || env.SUPABASE_KEY;

for (const [name, val] of [
  ['SUPABASE_URL', SUPA_URL],
  ['SUPABASE_SERVICE_ROLE_KEY', SUPA_KEY],
  ['DODO_API_KEY', key],
  ['DODO_BASE_URL', base],
]) {
  if (!val) {
    console.error(`FATAL: missing ${name} (set it as an environment variable).`);
    process.exit(2);
  }
}

const { productToPlan, productToInterval } = buildProductMaps(env);
if (!Object.keys(productToPlan).length) {
  console.error('FATAL: no DODO_PRODUCT_*_ID values found — cannot verify plans.');
  process.exit(2);
}

const db = createClient(SUPA_URL, SUPA_KEY, { auth: { persistSession: false } });

console.log(APPLY ? '*** APPLY MODE ***' : '--- dry run (pass --apply) ---');

const { actions, applied, failed, critical } = await runBillingReconciliation({
  db,
  dodoKey: key,
  dodoBase: base,
  productToPlan,
  productToInterval,
  apply: APPLY,
});

for (const a of actions) {
  console.log(`[${a.sev.padEnd(8)}] ${a.kind.padEnd(18)} ${a.msg}${a.error ? ` (ERROR ${a.error})` : ''}`);
}
console.log(`\n${actions.length} issue(s) found`);
console.log(APPLY ? `applied ${applied}, failed ${failed}` : '(dry run — nothing written)');
if (critical) console.log(`\n!! ${critical} CRITICAL issue(s) need manual attention (customer paying with no access)`);

process.exit(critical ? 1 : 0);
