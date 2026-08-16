import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Source-level guards for the 2026-08-16 entitlement fix.
 *
 * public.profiles is writable by the end user: its RLS policy ("profiles_update_own") is
 * row-scoped (auth.uid() = id) with NO column restriction, and the anon key ships in the browser
 * bundle. So any signed-in user can set their own profiles.plan_type / is_pro / credits_remaining.
 *
 * Three code paths used to trust that column and were fixed:
 *   - resolvePlanTypeForUser()        -> the scheduling paywall
 *   - affiliatePageHourlyLimitForUser() -> bridge-page hourly rate limit
 *   - the Pinterest "connect account" plan check -> how many accounts you may link
 *
 * These tests read the source as text rather than importing it, because importing index.js starts
 * the HTTP server. They exist to fail loudly if someone reintroduces a profiles fallback, and to
 * stop the five duplicated pin-limit tables drifting apart again (they already did once, which
 * would have quoted 60 pins in email while granting 90).
 */

const BACKEND = join(dirname(fileURLToPath(import.meta.url)), '..');
const indexSrc = readFileSync(join(BACKEND, 'src/index.js'), 'utf8');
const emailSrc = readFileSync(join(BACKEND, 'src/email.js'), 'utf8');
const reconcileSrc = readFileSync(join(BACKEND, 'src/billingReconcile.js'), 'utf8');

/** Body of a top-level `async function NAME(...)`, matched to its closing brace at column 0. */
function functionBody(src, name) {
  const start = src.indexOf(`async function ${name}(`);
  assert.notEqual(start, -1, `could not find async function ${name} in source`);
  const end = src.indexOf('\n}', start);
  assert.notEqual(end, -1, `could not find end of ${name}`);
  return src.slice(start, end);
}

/** Parse an object literal like `{ free: 10, starter: 90, ... }` into a plain object. */
function parseLimitTable(src, declaration) {
  const i = src.indexOf(declaration);
  assert.notEqual(i, -1, `could not find ${declaration}`);
  const open = src.indexOf('{', i);
  const close = src.indexOf('}', open);
  const body = src.slice(open + 1, close);
  const out = {};
  for (const m of body.matchAll(/([a-z_]+)\s*:\s*(\d+)/gi)) out[m[1]] = Number(m[2]);
  return out;
}

// --- entitlement source ------------------------------------------------------

test('resolvePlanTypeForUser never reads the user-writable profiles table', () => {
  const body = functionBody(indexSrc, 'resolvePlanTypeForUser');
  const code = body.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, ''); // strip comments
  assert.ok(
    !/profiles/.test(code),
    'resolvePlanTypeForUser references profiles — the paywall bypass has been reintroduced'
  );
  assert.ok(
    /getActiveSubscriptionForUser/.test(code),
    'resolvePlanTypeForUser must derive the plan from billing_subscriptions'
  );
});

test('the bridge-page rate limiter does not read profiles for the plan', () => {
  const body = functionBody(indexSrc, 'affiliatePageHourlyLimitForUser');
  const code = body.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, '');
  assert.ok(!/from\('profiles'\)/.test(code), 'rate limiter reads profiles again');
  assert.ok(/resolvePlanTypeForUser/.test(code), 'rate limiter must use resolvePlanTypeForUser');
});

test('no entitlement path selects plan_type or is_pro from profiles', () => {
  // Whitelist: requirePro is dead code (never attached to a route) and the reconciliation /
  // webhook paths legitimately WRITE profiles to keep it in sync for display.
  const reads = [...indexSrc.matchAll(/\.from\('profiles'\)\s*\n\s*\.select\('([^']+)'\)/g)]
    .map((m) => m[1])
    .filter((cols) => /plan_type|is_pro/.test(cols));
  // 'is_pro' (requirePro, unused) and 'id, email, plan_type' must NOT reappear.
  assert.ok(
    !reads.some((c) => /email/.test(c)),
    `profiles.email does not exist as a column; found select('${reads.find((c) => /email/.test(c))}')`
  );
});

test('the account snapshot takes the email from auth.users, not profiles', () => {
  const body = functionBody(indexSrc, 'getCurrentUsageSnapshot');
  assert.ok(
    /auth\.admin\.getUserById/.test(body),
    'account snapshot must resolve the email via auth.admin.getUserById'
  );
  const code = body.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, '');
  assert.ok(
    !/\.select\('[^']*email[^']*'\)/.test(code),
    'account snapshot selects an email column from a table that has none'
  );
});

// --- the five duplicated limit tables ---------------------------------------

test('every copy of the AI pin limits agrees with the enforced table', () => {
  const canonical = parseLimitTable(indexSrc, 'const PLAN_PIN_LIMITS = ');
  assert.deepEqual(
    canonical,
    { free: 10, starter: 90, creator: 250, pro: 600, agency: 1300 },
    'PLAN_PIN_LIMITS changed — update every copy below and this expectation together'
  );

  const grantSrc = readFileSync(join(BACKEND, 'scripts/grant-influencer-trial.mjs'), 'utf8');
  const copies = {
    'email.js PLAN_AI_PIN_LIMITS': parseLimitTable(emailSrc, 'const PLAN_AI_PIN_LIMITS = '),
    'billingReconcile.js PLAN_LIMITS': parseLimitTable(reconcileSrc, 'export const PLAN_LIMITS = '),
    'index.js FOUNDER_PLAN_AI_PINS': parseLimitTable(indexSrc, 'const FOUNDER_PLAN_AI_PINS = '),
    // Writes pins_limit_per_month into billing_subscriptions for comped accounts.
    'grant-influencer-trial.mjs PLAN_PIN_LIMITS': parseLimitTable(grantSrc, 'const PLAN_PIN_LIMITS = '),
  };
  for (const [where, table] of Object.entries(copies)) {
    for (const plan of ['starter', 'creator', 'pro', 'agency']) {
      assert.equal(
        table[plan],
        canonical[plan],
        `${where} has ${plan}=${table[plan]} but the enforced limit is ${canonical[plan]}`
      );
    }
  }
});

test('the in-app upgrade prompt advertises the limits actually granted', () => {
  const src = readFileSync(join(BACKEND, '../frontend/src/UrlToPinPage.js'), 'utf8');
  const canonical = parseLimitTable(indexSrc, 'const PLAN_PIN_LIMITS = ');
  const ladder = src.slice(src.indexOf('const UPGRADE_LADDER'), src.indexOf('const UPGRADE_LADDER') + 700);
  for (const [plan, expected] of Object.entries(canonical)) {
    if (plan === 'free') continue;
    const m = new RegExp(`next:\\s*"${plan}"[^}]*pins:\\s*(\\d+)`).exec(ladder);
    if (!m) continue; // agency is a terminal tier and has no "next"
    assert.equal(
      Number(m[1]),
      expected,
      `UPGRADE_LADDER promises ${m[1]} pins for ${plan} but the plan grants ${expected}`
    );
  }
});

// --- pricing --------------------------------------------------------------

test('list prices are identical in index.js and email.js', () => {
  const a = parseLimitTable(indexSrc, 'const PLAN_PRICE_USD = ');
  const b = parseLimitTable(emailSrc, 'const PLAN_PRICES_USD = ');
  for (const plan of ['starter', 'creator', 'pro', 'agency']) {
    assert.equal(a[plan], b[plan], `price mismatch for ${plan}: index.js $${a[plan]} vs email.js $${b[plan]}`);
  }
  assert.deepEqual(
    { starter: a.starter, creator: a.creator, pro: a.pro, agency: a.agency },
    { starter: 12, creator: 25, pro: 55, agency: 129 },
    'list prices changed — they must also be changed in Dodo and on Pricing.jsx, in that order'
  );
});
