/**
 * READ-ONLY: segment non-paying users for win-back / conversion outreach.
 * Segments: currently paying (exclude), churned (had paid, now not), free-activated
 * (never paid but generated pins), free-dormant (never paid, never made a pin), comps.
 * Usage: node backend/scripts/_lifecycle-segments.mjs
 */
import dotenv from 'dotenv';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@supabase/supabase-js';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const now = Date.now();
const PAID = new Set(['starter', 'creator', 'pro', 'agency']);

// All signups
const users = [];
for (let page = 1; page <= 60; page++) {
  const { data, error } = await sb.auth.admin.listUsers({ page, perPage: 200 });
  if (error) { console.error('listUsers', error.message); break; }
  const u = data?.users || [];
  users.push(...u);
  if (u.length < 200) break;
}

const { data: subs } = await sb
  .from('billing_subscriptions')
  .select('user_id, plan_type, status, current_period_end, dodo_subscription_id');
const { data: usage } = await sb.from('pin_usage').select('user_id, pins_used, user_photo_pins_used');

const isComp = (s) => String(s.dodo_subscription_id || '').startsWith('comp:');
const everPaid = new Set();
const currentlyActive = new Set();
const compUsers = new Set();
for (const s of subs || []) {
  const p = String(s.plan_type || '').toLowerCase();
  if (!PAID.has(p)) continue;
  if (isComp(s)) { compUsers.add(s.user_id); continue; }
  everPaid.add(s.user_id);
  const notExpired = !s.current_period_end || new Date(s.current_period_end).getTime() > now;
  if (s.status === 'active' && notExpired) currentlyActive.add(s.user_id);
}
const activated = new Set(
  (usage || []).filter((r) => (Number(r.pins_used) || 0) + (Number(r.user_photo_pins_used) || 0) > 0).map((r) => r.user_id)
);

const churned = [];
const freeActivated = [];
const freeDormant = [];
for (const u of users) {
  if (currentlyActive.has(u.id)) continue; // paying now
  if (compUsers.has(u.id) && !everPaid.has(u.id)) continue; // comp only
  if (everPaid.has(u.id)) { churned.push(u); continue; }
  if (activated.has(u.id)) freeActivated.push(u);
  else freeDormant.push(u);
}

const pct = (n) => ((n / Math.max(1, users.length)) * 100).toFixed(0);
console.log('=== LIFECYCLE SEGMENTS ===');
console.log('Total signups:            ', users.length);
console.log('Currently paying:         ', currentlyActive.size);
console.log('Comped (exclude):         ', [...compUsers].filter((id) => !everPaid.has(id)).length);
console.log('---- outreach targets ----');
console.log(`Churned (had paid, left): ${churned.length}  (${pct(churned.length)}%)  -> win-back`);
console.log(`Free, ACTIVATED (made pins): ${freeActivated.length}  (${pct(freeActivated.length)}%)  -> warm upgrade`);
console.log(`Free, DORMANT (never made a pin): ${freeDormant.length}  (${pct(freeDormant.length)}%)  -> activation`);
console.log('\nSample churned emails:', churned.slice(0, 5).map((u) => u.email).join(', ') || '(none)');

// ---- Export CSVs for Resend Audiences (email, first_name) ----
function firstName(u) {
  const raw = String(u?.user_metadata?.full_name || u?.user_metadata?.name || '').trim();
  const token = raw.split(/\s+/)[0] || '';
  // Only use it if it looks like a real name (letters), else leave blank so the
  // Resend merge fallback ("there") is used instead of a mangled email handle.
  return /^[A-Za-zÀ-ÿ'’-]{2,}$/.test(token) ? token : '';
}
function csvCell(s) {
  const v = String(s ?? '');
  return /[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
}
function toCsv(list) {
  const rows = ['email,first_name'];
  for (const u of list) {
    if (!u.email) continue;
    rows.push(`${csvCell(u.email)},${csvCell(firstName(u))}`);
  }
  return rows.join('\n') + '\n';
}

const outDir = resolve(__dirname, '../lifecycle-emails');
await mkdir(outDir, { recursive: true });
await writeFile(resolve(outDir, 'churned.csv'), toCsv(churned), 'utf8');
await writeFile(resolve(outDir, 'free-activated.csv'), toCsv(freeActivated), 'utf8');
await writeFile(resolve(outDir, 'free-dormant.csv'), toCsv(freeDormant), 'utf8');
console.log('\nWrote CSVs to backend/lifecycle-emails/ (churned, free-activated, free-dormant).');
console.log('These contain user emails (PII) — do not commit them; delete after uploading to Resend.');
