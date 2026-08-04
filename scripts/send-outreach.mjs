/**
 * One-off outreach for the 2026-08 activation review. READ-ONLY until you pass --send.
 *
 *   node scripts/send-outreach.mjs --list A            # dry run, prints recipients
 *   node scripts/send-outreach.mjs --list A --send     # actually sends
 *   node scripts/send-outreach.mjs --list B --limit 5 --send
 *
 * List A — paying customers who have NEVER scheduled a pin. They are billed for the one feature
 *          they've never used. Retention save; expect a handful of people.
 * List B — free users who used their whole free allowance AND connected Pinterest, then hit the
 *          paywall (scheduling and post-now are both paid-only). Highest-intent upgrade list.
 *
 * Sends are deduped through `email_events` exactly like the lifecycle emails, so re-running is
 * safe: anyone already emailed for a given key is skipped.
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// .env must land in process.env BEFORE email.js is imported — it reads config at module load.
for (const line of readFileSync(resolve(__dirname, '../.env'), 'utf8').split(/\r?\n/)) {
  const t = line.trim();
  if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
  if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2].trim();
}

const { sendPaidNotStartedEmail, sendConnectedPaywallEmail, isEmailEnabled } = await import('../src/email.js');

// node-fetch, not the global (undici) fetch: on some Windows setups undici cannot reach the
// Supabase host at all (ConnectTimeout) while node-fetch connects fine. email.js already uses it.
const { default: fetch } = await import('node-fetch');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const val = (f, d = null) => { const i = args.indexOf(f); return i >= 0 && args[i + 1] ? args[i + 1] : d; };
const LIST = String(val('--list', 'A')).toUpperCase();
const SEND = has('--send');
const LIMIT = Number(val('--limit', '0')) || 0;

const SUPABASE_URL = process.env.SUPABASE_URL;
const KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !KEY) { console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY'); process.exit(1); }
const H = { apikey: KEY, Authorization: `Bearer ${KEY}` };

/** Never email internal or seeded accounts — the paid list is mostly these. */
const EXCLUDE_RE = /@(test\.com|example\.com|url2pin\.com)$|\+test@/i;

const EMAIL_KEYS = { A: 'outreach_paid_not_started_2026_08', B: 'outreach_connected_paywall_2026_08' };
/** Gate went live ~2026-05-16: no unpaid user has posted since. Before that free users could post. */
const GATE_ISO = '2026-05-16T00:00:00Z';

async function pageAll(table, select) {
  const out = [];
  for (let p = 0; p < 200; p++) {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?select=${select}&limit=1000&offset=${p * 1000}`, { headers: H });
    if (!r.ok) throw new Error(`${table}: ${r.status} ${await r.text()}`);
    const rows = await r.json();
    out.push(...rows);
    if (rows.length < 1000) break;
  }
  return out;
}

async function authUsers() {
  const out = [];
  for (let p = 1; p <= 50; p++) {
    const r = await fetch(`${SUPABASE_URL}/auth/v1/admin/users?page=${p}&per_page=200`, { headers: H });
    if (!r.ok) throw new Error(`auth users: ${r.status}`);
    const j = await r.json();
    const batch = j.users || [];
    out.push(...batch);
    if (batch.length < 200) break;
  }
  return out;
}

/** Same idempotent claim the lifecycle emails use: returns false if already sent. */
async function claimEmailEvent(userId, emailKey) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/email_events?on_conflict=user_id,email_key`, {
    method: 'POST',
    headers: { ...H, 'Content-Type': 'application/json', Prefer: 'resolution=ignore-duplicates,return=representation' },
    body: JSON.stringify({ user_id: userId, email_key: emailKey }),
  });
  if (!r.ok) { console.warn(`  claim failed (${r.status}) — skipping to be safe`); return false; }
  const rows = await r.json().catch(() => []);
  return Array.isArray(rows) && rows.length > 0;
}

const [pins, subs, profiles, accounts, users] = await Promise.all([
  pageAll('scheduled_pins', 'user_id,status'),
  pageAll('billing_subscriptions', 'user_id,plan_type'),
  pageAll('profiles', 'id,plan_type'),
  pageAll('pinterest_accounts', 'user_id,created_at'),
  authUsers(),
]);

const emailOf = new Map(users.map((u) => [u.id, String(u.email || '').trim()]));
const planNow = new Map(profiles.map((p) => [p.id, String(p.plan_type || 'free').toLowerCase()]));
const everPaid = new Set(subs.filter((s) => s.user_id && String(s.plan_type || '').toLowerCase() !== 'free').map((s) => s.user_id));
const gen = new Map(), scheduled = new Set();
for (const p of pins) {
  if (!p.user_id) continue;
  gen.set(p.user_id, (gen.get(p.user_id) || 0) + 1);
  if (['scheduled', 'posted', 'failed'].includes(p.status)) scheduled.add(p.user_id);
}
const firstConnect = new Map();
for (const a of accounts) {
  if (!a.user_id || !a.created_at) continue;
  const t = new Date(a.created_at).getTime();
  if (!firstConnect.has(a.user_id) || t < firstConnect.get(a.user_id)) firstConnect.set(a.user_id, t);
}

let recipients;
if (LIST === 'A') {
  recipients = [...everPaid]
    .filter((u) => !scheduled.has(u) && (planNow.get(u) || 'free') !== 'free')
    .map((u) => ({ user_id: u, email: emailOf.get(u) || '', plan: planNow.get(u), generated: gen.get(u) || 0 }));
} else if (LIST === 'B') {
  const gate = Date.parse(GATE_ISO);
  recipients = [...firstConnect.entries()]
    .filter(([u, t]) => t >= gate && !everPaid.has(u) && (gen.get(u) || 0) > 0)
    .map(([u]) => ({ user_id: u, email: emailOf.get(u) || '', generated: gen.get(u) || 0 }))
    .sort((a, b) => b.generated - a.generated);
} else {
  console.error('--list must be A or B'); process.exit(1);
}

const excluded = recipients.filter((r) => !r.email || EXCLUDE_RE.test(r.email));
recipients = recipients.filter((r) => r.email && !EXCLUDE_RE.test(r.email));
// --tail takes the LOWEST-intent recipients. Use it for test batches: a send is claimed in
// email_events and cannot be repeated, so a rendering mistake would otherwise burn the best leads.
if (LIMIT > 0) recipients = has('--tail') ? recipients.slice(-LIMIT) : recipients.slice(0, LIMIT);

console.log(`list ${LIST}: ${recipients.length} recipients (${excluded.length} excluded as test/internal/no-email)`);
for (const r of excluded) console.log(`  EXCLUDED ${r.email || '(no email)'}`);
console.log(`mode: ${SEND ? 'SEND (live)' : 'DRY RUN'}   email configured: ${isEmailEnabled()}`);
console.log('');
for (const r of recipients) {
  console.log(`  ${r.email}  generated=${r.generated}${r.plan ? `  plan=${r.plan}` : ''}`);
}

// Pre-flight. Resend's shared sender (onboarding@resend.dev) can ONLY deliver to the address
// that owns the Resend account, so a real campaign sent from it silently fails for everyone
// else. Catch that before burning the list rather than after.
const from = process.env.EMAIL_FROM || '';
console.log(`\nfrom: ${from || '(unset — would default to onboarding@resend.dev)'}`);
const senderUnsafe = !from || /resend\.dev/i.test(from);

if (!SEND) {
  if (senderUnsafe) {
    console.log('\n⚠️  EMAIL_FROM is unset or uses Resend\'s shared test sender.');
    console.log('    Set EMAIL_FROM to a verified domain address before a live send, e.g.');
    console.log('    EMAIL_FROM="Aristomenis from URL2Pin <hello@url2pin.com>"');
  }
  console.log('\nDry run — nothing sent. Re-run with --send to deliver.');
  process.exit(0);
}
if (!isEmailEnabled()) { console.error('\nRESEND_API_KEY not set — refusing to run a live send.'); process.exit(1); }
if (senderUnsafe) {
  console.error('\nRefusing to send: EMAIL_FROM is unset or points at resend.dev (test sender).');
  console.error('Delivery would fail for every recipient except your own Resend account address.');
  console.error('Set EMAIL_FROM to a verified domain and re-run.');
  process.exit(1);
}

let sent = 0, skipped = 0, failed = 0;
for (const r of recipients) {
  const claimed = await claimEmailEvent(r.user_id, EMAIL_KEYS[LIST]);
  if (!claimed) { console.log(`  SKIP (already emailed) ${r.email}`); skipped++; continue; }
  const res = LIST === 'A'
    ? await sendPaidNotStartedEmail({ to: r.email, planType: r.plan, generated: r.generated })
    : await sendConnectedPaywallEmail({ to: r.email, generated: r.generated });
  if (res?.ok) { console.log(`  SENT ${r.email}`); sent++; }
  else { console.warn(`  FAILED ${r.email}: ${res?.error || res?.reason}`); failed++; }
  await new Promise((s) => setTimeout(s, 600)); // stay under Resend's rate limit
}
console.log(`\ndone — sent=${sent} skipped=${skipped} failed=${failed}`);
