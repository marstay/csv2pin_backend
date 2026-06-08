/**
 * Export emails for free-tier users only (no active or past_due subscription).
 *
 * Usage:
 *   cd backend && node scripts/export-free-users-csv.mjs
 *
 * Requires in backend/.env:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *
 * Output (in backend/):
 *   free-users-YYYY-MM-DDTHHmmss.csv
 */

import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!url || !key) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env');
  process.exit(1);
}

const supabase = createClient(url, key, {
  auth: { autoRefreshToken: false, persistSession: false },
});

function csvEscape(val) {
  if (val == null || val === undefined) return '';
  const s = String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function rowToLine(cols) {
  return cols.map(csvEscape).join(',') + '\n';
}

async function fetchAllAuthUsers() {
  const perPage = 1000;
  let page = 1;
  const all = [];
  for (;;) {
    const { data, error } = await supabase.auth.admin.listUsers({ page, perPage });
    if (error) throw new Error(`auth.admin.listUsers: ${error.message}`);
    const batch = data?.users ?? [];
    all.push(...batch);
    if (batch.length < perPage) break;
    page += 1;
  }
  return all;
}

async function fetchAllProfiles() {
  const pageSize = 1000;
  let from = 0;
  const map = new Map();
  for (;;) {
    const { data, error } = await supabase
      .from('profiles')
      .select('id, plan_type, is_pro, credits_remaining, created_at, updated_at')
      .order('created_at', { ascending: true })
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`profiles: ${error.message}`);
    const rows = data ?? [];
    for (const r of rows) map.set(r.id, r);
    if (rows.length < pageSize) break;
    from += pageSize;
  }
  return map;
}

/** Users with an active or past_due paid subscription — excluded from free export. */
async function fetchPaidUserIds() {
  const pageSize = 1000;
  let from = 0;
  const ids = new Set();
  for (;;) {
    const { data, error } = await supabase
      .from('billing_subscriptions')
      .select('user_id')
      .in('status', ['active', 'past_due'])
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`billing_subscriptions: ${error.message}`);
    const rows = data ?? [];
    for (const r of rows) {
      if (r.user_id) ids.add(r.user_id);
    }
    if (rows.length < pageSize) break;
    from += pageSize;
  }
  return ids;
}

function isFreeProfile(profile) {
  const plan = String(profile?.plan_type || 'free').trim().toLowerCase();
  return plan === 'free' || plan === '';
}

async function main() {
  console.log('Fetching auth.users…');
  const users = await fetchAllAuthUsers();
  console.log(`Found ${users.length} auth users.`);

  console.log('Fetching profiles…');
  const profileById = await fetchAllProfiles();

  console.log('Fetching active/past_due subscriptions…');
  const paidUserIds = await fetchPaidUserIds();
  console.log(`Excluding ${paidUserIds.size} users with active/past_due subscriptions.`);

  const headers = [
    'email',
    'user_id',
    'email_confirmed_at',
    'user_created_at',
    'last_sign_in_at',
    'plan_type',
    'credits_remaining',
  ];

  const lines = [rowToLine(headers)];
  let skippedPaid = 0;
  let skippedNoEmail = 0;
  let skippedNotFree = 0;

  for (const u of users) {
    if (paidUserIds.has(u.id)) {
      skippedPaid += 1;
      continue;
    }

    const p = profileById.get(u.id) || {};
    if (!isFreeProfile(p)) {
      skippedNotFree += 1;
      continue;
    }

    const email = String(u.email || '').trim();
    if (!email) {
      skippedNoEmail += 1;
      continue;
    }

    lines.push(
      rowToLine([
        email,
        u.id,
        u.email_confirmed_at ?? u.confirmed_at ?? '',
        u.created_at ?? '',
        u.last_sign_in_at ?? '',
        p.plan_type ?? 'free',
        p.credits_remaining ?? '',
      ])
    );
  }

  const rowCount = lines.length - 1;
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outPath = resolve(__dirname, `../free-users-${stamp}.csv`);
  const bom = '\uFEFF';
  writeFileSync(outPath, bom + lines.join(''), 'utf8');

  console.log(`Wrote ${outPath}`);
  console.log(`Free users exported: ${rowCount}`);
  console.log(`Skipped (active/paid sub): ${skippedPaid}`);
  console.log(`Skipped (non-free plan_type): ${skippedNotFree}`);
  console.log(`Skipped (no email): ${skippedNoEmail}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
