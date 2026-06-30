/**
 * One-time migration: move paid monthly subscribers from calendar-month pin buckets
 * to billing-period buckets while preserving effective (remaining) quota.
 *
 * Usage:
 *   cd backend && node scripts/migrate-pin-usage-billing-buckets.mjs          # dry run
 *   cd backend && node scripts/migrate-pin-usage-billing-buckets.mjs --apply
 */
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const APPLY = process.argv.includes('--apply');
const FREE_LIFETIME = '1970-01-01';

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(url, key, {
  auth: { autoRefreshToken: false, persistSession: false },
});

function currentYearMonthDate() {
  const now = new Date();
  const monthStartUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  return monthStartUtc.toISOString().slice(0, 10);
}

function normalizeBillingInterval(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'year' || v === 'annual' || v === 'annually') return 'year';
  return 'month';
}

function isoDateToBucketKey(isoOrDate) {
  if (!isoOrDate) return null;
  const s = String(isoOrDate).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function pinUsageBucketKey(sub) {
  const interval = normalizeBillingInterval(sub.billing_interval);
  if (interval === 'year') return currentYearMonthDate();
  return isoDateToBucketKey(sub.current_period_start) || currentYearMonthDate();
}

async function fetchPinRow(userId, bucket) {
  const { data } = await supabase
    .from('pin_usage')
    .select('pins_used, user_photo_pins_used')
    .eq('user_id', userId)
    .eq('year_month', bucket)
    .maybeSingle();
  return data;
}

async function main() {
  console.log(`Mode: ${APPLY ? 'APPLY' : 'DRY RUN'}\n`);

  const { data: subs, error } = await supabase
    .from('billing_subscriptions')
    .select(
      'id, user_id, plan_type, billing_interval, current_period_start, current_period_end, usage_baseline_pins_used, usage_baseline_user_photo_pins_used'
    )
    .eq('status', 'active');

  if (error) {
    console.error('Failed to load subscriptions:', error.message || error);
    process.exit(1);
  }

  const legacyBucket = currentYearMonthDate();
  let scanned = 0;
  let migrated = 0;
  let skipped = 0;

  for (const sub of subs || []) {
    scanned += 1;
    const newBucket = pinUsageBucketKey(sub);
    if (newBucket === legacyBucket) {
      skipped += 1;
      continue;
    }

    const baselineAi = Math.max(0, Number(sub.usage_baseline_pins_used ?? 0) || 0);
    const baselinePhoto = Math.max(0, Number(sub.usage_baseline_user_photo_pins_used ?? 0) || 0);
    const legacyRow = await fetchPinRow(sub.user_id, legacyBucket);
    const legacyAi = legacyRow?.pins_used ?? 0;
    const legacyPhoto = legacyRow?.user_photo_pins_used ?? 0;
    const effectiveAi = Math.max(0, legacyAi - baselineAi);
    const effectivePhoto = Math.max(0, legacyPhoto - baselinePhoto);

    const newRow = await fetchPinRow(sub.user_id, newBucket);
    const newAi = newRow?.pins_used ?? 0;
    const newPhoto = newRow?.user_photo_pins_used ?? 0;
    const seedAi = Math.max(newAi, effectiveAi);
    const seedPhoto = Math.max(newPhoto, effectivePhoto);
    const needsSeed = seedAi > newAi || seedPhoto > newPhoto;
    const needsBaselineReset = baselineAi > 0 || baselinePhoto > 0;

    if (!needsSeed && !needsBaselineReset) {
      skipped += 1;
      continue;
    }

    migrated += 1;
    console.log(
      JSON.stringify({
        user_id: sub.user_id,
        plan: sub.plan_type,
        legacyBucket,
        newBucket,
        effectiveAi,
        effectivePhoto,
        seedAi,
        seedPhoto,
        needsBaselineReset,
      })
    );

    if (!APPLY) continue;

    const usageNowIso = new Date().toISOString();
    if (needsSeed) {
      const { error: upsertError } = await supabase.from('pin_usage').upsert(
        {
          user_id: sub.user_id,
          year_month: newBucket,
          pins_used: seedAi,
          user_photo_pins_used: seedPhoto,
          updated_at: usageNowIso,
        },
        { onConflict: 'user_id,year_month' }
      );
      if (upsertError) {
        console.error('  upsert failed:', upsertError.message || upsertError);
        continue;
      }
    }
    if (needsBaselineReset) {
      await supabase
        .from('billing_subscriptions')
        .update({
          usage_baseline_pins_used: 0,
          usage_baseline_user_photo_pins_used: 0,
          updated_at: usageNowIso,
        })
        .eq('id', sub.id)
        .eq('user_id', sub.user_id);
    }
  }

  console.log(`\nScanned ${scanned} active subscriptions.`);
  console.log(`${migrated} would migrate / migrated. ${skipped} skipped (annual or already aligned).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
