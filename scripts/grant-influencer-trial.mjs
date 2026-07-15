/**
 * Grant comp Creator (or other paid) access for influencer / partner trials.
 * Creator plan = 150 AI pins per billing period (no Dodo charge).
 *
 * Usage (dry run — default):
 *   node scripts/grant-influencer-trial.mjs --email alice@example.com --email bob@example.com
 *
 * Apply changes:
 *   node scripts/grant-influencer-trial.mjs --apply --email alice@example.com
 *
 * Create auth users if missing (prints generated passwords unless --password is set):
 *   node scripts/grant-influencer-trial.mjs --apply --create --email alice@example.com --email bob@example.com
 *
 * Use the same password for every new account:
 *   node scripts/grant-influencer-trial.mjs --apply --create --password "TrialPass123!" --email test1@test.com
 *
 * Options:
 *   --plan creator          Plan tier (default: creator → 150 AI pins / period)
 *   --days 30               Trial length in days (default: 30 = one Creator billing period)
 *   --interval month        Billing interval stored in DB (month | year)
 *   --label influencer      Stored in dodo_subscription_id as comp:<label> for tracking
 */
import { createClient } from '@supabase/supabase-js';
import { randomBytes } from 'node:crypto';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const PLAN_PIN_LIMITS = {
  free: 10,
  starter: 60,
  creator: 150,
  pro: 450,
  agency: 1000,
};

const FREE_LIFETIME_YEAR_MONTH = '1970-01-01';

function parseArgs(argv) {
  const out = {
    apply: false,
    create: false,
    emails: [],
    plan: 'creator',
    days: 30,
    interval: 'month',
    label: 'influencer-trial',
    password: null,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--apply') out.apply = true;
    else if (a === '--create') out.create = true;
    else if (a === '--email') {
      const email = String(argv[++i] || '').trim().toLowerCase();
      if (email) out.emails.push(email);
    } else if (a === '--plan') out.plan = String(argv[++i] || 'creator').trim().toLowerCase();
    else if (a === '--days') out.days = Math.max(1, Number(argv[++i]) || 30);
    else if (a === '--interval') out.interval = String(argv[++i] || 'month').trim().toLowerCase();
    else if (a === '--label') out.label = String(argv[++i] || 'influencer-trial').trim();
    else if (a === '--password') out.password = String(argv[++i] || '');
  }
  return out;
}

function currentYearMonthDate() {
  const now = new Date();
  const monthStartUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  return monthStartUtc.toISOString().slice(0, 10);
}

function isoDateToBucketKey(isoOrDate) {
  if (!isoOrDate) return null;
  const s = String(isoOrDate).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function normalizeBillingInterval(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'year' || v === 'annual' || v === 'annually') return 'year';
  return 'month';
}

function pinUsageBucketKey(sub, planType) {
  const pt = String(planType || sub?.plan_type || 'free').trim().toLowerCase();
  if (pt === 'free') return FREE_LIFETIME_YEAR_MONTH;
  if (!sub) return currentYearMonthDate();
  const interval = normalizeBillingInterval(sub.billing_interval);
  if (interval === 'year') return currentYearMonthDate();
  return isoDateToBucketKey(sub.current_period_start) || currentYearMonthDate();
}

function randomPassword() {
  return `Url2pin-${randomBytes(5).toString('base64url')}!`;
}

async function findUserByEmail(supabase, email) {
  const target = email.toLowerCase();
  let page = 1;
  const perPage = 200;
  while (page <= 50) {
    const { data, error } = await supabase.auth.admin.listUsers({ page, perPage });
    if (error) throw error;
    const users = data?.users || [];
    const hit = users.find((u) => String(u.email || '').toLowerCase() === target);
    if (hit) return hit;
    if (users.length < perPage) break;
    page += 1;
  }
  return null;
}

async function grantPlan(supabase, userId, opts) {
  const { plan, days, interval, label, apply } = opts;
  const pinsLimit = PLAN_PIN_LIMITS[plan];
  if (!pinsLimit) throw new Error(`Unknown plan: ${plan}`);

  const now = new Date();
  const periodStart = now.toISOString();
  const periodEnd = new Date(now.getTime() + days * 24 * 60 * 60 * 1000).toISOString();
  const billingInterval = normalizeBillingInterval(interval);
  const compId = `comp:${label}`;

  const newBucketStub = {
    billing_interval: billingInterval,
    current_period_start: periodStart,
    status: 'active',
    plan_type: plan,
  };
  const bucket = pinUsageBucketKey(newBucketStub, plan);

  const preview = {
    userId,
    plan,
    pinsLimit,
    periodStart,
    periodEnd,
    billingInterval,
    compId,
    pinUsageBucket: bucket,
  };

  if (!apply) return { preview, applied: false };

  await supabase
    .from('billing_subscriptions')
    .update({ status: 'cancelled', updated_at: now.toISOString() })
    .eq('user_id', userId)
    .eq('status', 'active');

  const { error: insertError } = await supabase.from('billing_subscriptions').insert({
    user_id: userId,
    plan_type: plan,
    pins_limit_per_month: pinsLimit,
    status: 'active',
    billing_interval: billingInterval,
    current_period_start: periodStart,
    current_period_end: periodEnd,
    cancel_at_period_end: true,
    cancelled_at: null,
    dodo_subscription_id: compId,
    usage_baseline_pins_used: 0,
    usage_baseline_user_photo_pins_used: 0,
  });
  if (insertError) throw insertError;

  await supabase.from('pin_usage').upsert(
    {
      user_id: userId,
      year_month: bucket,
      pins_used: 0,
      user_photo_pins_used: 0,
      updated_at: now.toISOString(),
    },
    { onConflict: 'user_id,year_month' }
  );

  const { data: authUser } = await supabase.auth.admin.getUserById(userId);
  await supabase.from('profiles').upsert(
    {
      id: userId,
      email: authUser?.user?.email || null,
      plan_type: plan,
      is_pro: plan !== 'free',
      updated_at: now.toISOString(),
    },
    { onConflict: 'id' }
  );

  return { preview, applied: true };
}

async function main() {
  const args = parseArgs(process.argv);
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env');
    process.exit(1);
  }
  if (!args.emails.length) {
    console.error('Provide at least one --email address.');
    process.exit(1);
  }
  if (!PLAN_PIN_LIMITS[args.plan]) {
    console.error(`Invalid --plan ${args.plan}. Use: ${Object.keys(PLAN_PIN_LIMITS).join(', ')}`);
    process.exit(1);
  }

  const supabase = createClient(url, key, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  console.log(`Mode: ${args.apply ? 'APPLY' : 'DRY RUN'}`);
  console.log(`Plan: ${args.plan} (${PLAN_PIN_LIMITS[args.plan]} AI pins per billing period)`);
  console.log(`Trial: ${args.days} days | interval: ${args.interval} | label: ${args.label}\n`);

  const results = [];

  for (const email of args.emails) {
    console.log(`--- ${email} ---`);
    let user = await findUserByEmail(supabase, email);
    let createdPassword = null;

    if (!user) {
      if (!args.create) {
        console.log('  User not found. Re-run with --create to make an account, or ask them to sign up first.');
        results.push({ email, status: 'missing_user' });
        continue;
      }
      if (!args.apply) {
        console.log('  User not found → would create account + grant plan (--apply --create).');
        results.push({ email, status: 'would_create' });
        continue;
      }
      createdPassword = args.password?.trim() || randomPassword();
      const { data, error } = await supabase.auth.admin.createUser({
        email,
        password: createdPassword,
        email_confirm: true,
      });
      if (error) {
        console.error(`  Create failed: ${error.message}`);
        results.push({ email, status: 'create_failed', error: error.message });
        continue;
      }
      user = data.user;
      console.log('  Created auth user.');
    } else {
      console.log(`  Found user ${user.id}`);
    }

    try {
      const { preview, applied } = await grantPlan(supabase, user.id, {
        plan: args.plan,
        days: args.days,
        interval: args.interval,
        label: args.label,
        apply: args.apply,
      });
      if (applied) {
        console.log(`  Granted ${preview.plan} until ${preview.periodEnd}`);
        if (createdPassword) {
          console.log(`  Temp password: ${createdPassword}`);
        }
        results.push({
          email,
          status: 'granted',
          userId: user.id,
          password: createdPassword,
          ...preview,
        });
      } else {
        console.log(`  Would grant ${preview.plan} until ${preview.periodEnd}`);
        results.push({ email, status: 'would_grant', userId: user.id, ...preview });
      }
    } catch (err) {
      console.error(`  Grant failed: ${err.message || err}`);
      results.push({ email, status: 'grant_failed', error: String(err.message || err) });
    }
    console.log('');
  }

  console.log('Summary:');
  for (const row of results) {
    if (row.password) {
      console.log(`  ${row.email} → ${row.status} (password: ${row.password})`);
    } else {
      console.log(`  ${row.email} → ${row.status}`);
    }
  }

  if (!args.apply) {
    console.log('\nNo changes made. Re-run with --apply to execute.');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
