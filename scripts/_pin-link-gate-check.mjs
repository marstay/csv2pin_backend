/**
 * Read-only regression check: run the new coercePinDestinationUrl gate against every
 * distinct link value that already exists in scheduled_pins, and report which live
 * links would now be rejected. Any rejection of a link on a POSTED pin is a regression.
 *
 * Usage: cd backend && node scripts/_pin-link-gate-check.mjs
 */
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

// Imported, not copied — this must test the rule the server actually runs.
import { coercePinDestinationUrl } from '../src/pinPosting.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

let all = [];
for (let page = 0; page < 40; page++) {
  const { data, error } = await supabase
    .from('scheduled_pins')
    .select('id,status,link')
    .order('created_at', { ascending: false })
    .range(page * 1000, page * 1000 + 999);
  if (error) { console.error(error.message); process.exit(1); }
  if (!data.length) break;
  all = all.concat(data);
  if (data.length < 1000) break;
}

const withLink = all.filter((r) => String(r.link || '').trim());
console.log(`rows: ${all.length}  (with a non-empty link: ${withLink.length})`);

const rejected = withLink.filter((r) => !coercePinDestinationUrl(r.link));
const rewritten = withLink.filter((r) => {
  const c = coercePinDestinationUrl(r.link);
  return c && c !== String(r.link).trim();
});

console.log(`\nwould be REJECTED: ${rejected.length}`);
const byStatus = {};
for (const r of rejected) byStatus[r.status] = (byStatus[r.status] || 0) + 1;
for (const [k, v] of Object.entries(byStatus)) console.log(`  ${String(v).padStart(4)}  ${k}`);
for (const r of rejected) console.log(`  ${r.status.padEnd(10)} ${JSON.stringify(String(r.link).slice(0, 70))}`);

const posted = rejected.filter((r) => r.status === 'posted');
console.log(`\nREGRESSION CHECK — rejected links on already-posted pins: ${posted.length}`);
if (posted.length) for (const r of posted) console.log(`  !! ${r.id} ${JSON.stringify(r.link)}`);

console.log(`\nwould be NORMALIZED (scheme added / href-canonicalized): ${rewritten.length}`);
const samples = [...new Set(rewritten.map((r) => String(r.link).trim()))].slice(0, 12);
for (const s of samples) console.log(`  ${JSON.stringify(s.slice(0, 60))} -> ${coercePinDestinationUrl(s).slice(0, 70)}`);
