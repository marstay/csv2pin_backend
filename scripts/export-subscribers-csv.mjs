/**
 * Export every registered user (auth.users), not only paid plans.
 *
 * Usage (from repo root or backend folder):
 *   cd backend && node scripts/export-subscribers-csv.mjs
 *   node backend/scripts/export-subscribers-csv.mjs   # from repo root if node resolves imports
 *
 * Requires in backend/.env:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *
 * Output in the current working directory:
 *   subscribers-YYYY-MM-DDTHHmmss.csv  (full rows)
 *   subscribers-emails-YYYY-MM-DDTHHmmss.txt  (one email per line, non-empty only)
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
    for (const r of rows) {
      map.set(r.id, r);
    }
    if (rows.length < pageSize) break;
    from += pageSize;
  }
  return map;
}

async function main() {
  console.log('Fetching auth.users…');
  const users = await fetchAllAuthUsers();
  console.log(`Found ${users.length} auth users.`);

  console.log('Fetching profiles…');
  const profileById = await fetchAllProfiles();
  console.log(`Found ${profileById.size} profile rows.`);

  const headers = [
    'user_id',
    'email',
    'email_confirmed_at',
    'phone',
    'user_created_at',
    'last_sign_in_at',
    'plan_type',
    'is_pro',
    'credits_remaining',
    'profile_created_at',
    'profile_updated_at',
  ];

  const lines = [rowToLine(headers)];
  const emails = [];

  for (const u of users) {
    const p = profileById.get(u.id) || {};
    const email = String(u.email || '').trim();
    if (email) emails.push(email);
    lines.push(
      rowToLine([
        u.id,
        email,
        u.email_confirmed_at ?? u.confirmed_at ?? '',
        u.phone ?? '',
        u.created_at ?? '',
        u.last_sign_in_at ?? '',
        p.plan_type ?? 'free',
        p.is_pro === true ? 'true' : p.is_pro === false ? 'false' : '',
        p.credits_remaining ?? '',
        p.created_at ?? '',
        p.updated_at ?? '',
      ])
    );
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outName = `subscribers-${stamp}.csv`;
  const bom = '\uFEFF';
  writeFileSync(outName, bom + lines.join(''), 'utf8');
  console.log(`Wrote ${outName} (${users.length} rows + header).`);

  const emailOut = `subscribers-emails-${stamp}.txt`;
  const uniqueEmails = [...new Set(emails)];
  writeFileSync(emailOut, uniqueEmails.join('\n') + (uniqueEmails.length ? '\n' : ''), 'utf8');
  console.log(`Wrote ${emailOut} (${uniqueEmails.length} emails).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
