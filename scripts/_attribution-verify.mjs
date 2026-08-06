/**
 * Pre-deploy verification for the attribution feature. Read-only except for one
 * deliberately-invalid write that the CHECK constraint is expected to reject
 * (a rejected statement changes nothing).
 *
 *   node backend/scripts/_attribution-verify.mjs
 */
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

dotenv.config({ path: join(dirname(fileURLToPath(import.meta.url)), '..', '.env') });

const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

let failed = 0;
const check = (label, ok, detail = '') => {
  if (!ok) failed += 1;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${detail ? ` — ${detail}` : ''}`);
};

// 1. The exact select GET /api/admin/metrics/attribution runs.
const metricsCols = 'id, plan_type, attribution_channel, attribution_landing, attribution_first_seen';
const r1 = await db.from('profiles').select(metricsCols).limit(1);
check('metrics endpoint select', !r1.error, r1.error?.message);

// 2. The exact select /api/admin/metrics/acquisition now runs.
const acqCols =
  'id, plan_type, referred_by_affiliate_slug, attribution_channel, attribution_source, attribution_landing';
const r2 = await db.from('profiles').select(acqCols).limit(1);
check('acquisition endpoint select', !r2.error, r2.error?.message);

// 3. The select POST /api/attribution runs before writing.
const r3 = await db.from('profiles').select('attribution_channel, created_at').limit(1);
check('write-path preflight select', !r3.error, r3.error?.message);

// 3b. The founder aggregator select, which had the same missing-column bug.
const r3b = await db
  .from('profiles')
  .select('id, plan_type, referred_by_affiliate_slug, created_at')
  .limit(1);
check('founder aggregator select', !r3b.error, r3b.error?.message);

// 4. CHECK constraint must reject a channel outside the founder vocabulary.
//    Must target a REAL row: Postgres does not evaluate CHECK constraints when an
//    UPDATE matches zero rows, so a synthetic id proves nothing. The statement is
//    rejected before it writes, so this mutates nothing.
const { data: sample } = await db.from('profiles').select('id').limit(1);
const realId = sample?.[0]?.id;
if (!realId) {
  check('CHECK constraint rejects bad channel', false, 'no profile rows to test against');
} else {
  const r4 = await db
    .from('profiles')
    .update({ attribution_channel: 'not-a-real-channel' })
    .eq('id', realId);
  check(
    'CHECK constraint rejects bad channel',
    Boolean(r4.error) && /constraint|check/i.test(r4.error.message),
    r4.error ? r4.error.message.slice(0, 80) : 'constraint did NOT fire — is the DO block applied?'
  );

  // Confirm the rejected statement really left the row untouched.
  const { data: after } = await db
    .from('profiles')
    .select('attribution_channel')
    .eq('id', realId)
    .maybeSingle();
  check('rejected write left row unchanged', after?.attribution_channel == null,
    `attribution_channel is now ${after?.attribution_channel}`);
}

console.log(`\n${failed ? `${failed} check(s) FAILED` : 'all checks passed'}`);
process.exit(failed ? 1 : 0);
