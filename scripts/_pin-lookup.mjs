/**
 * Read-only: inspect one scheduled_pins row and probe its image_url / link
 * the way Pinterest would when creating a pin.
 *
 * Usage:
 *   cd backend && node scripts/_pin-lookup.mjs 64d152ee-ab76-4859-ac54-dd6eed6868f4
 */
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const PIN_ID = (process.argv[2] || '').trim();
if (!PIN_ID) {
  console.error('Usage: node scripts/_pin-lookup.mjs <scheduled_pin_id>');
  process.exit(1);
}

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env');
  process.exit(1);
}

const supabase = createClient(url, key, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const { data: pin, error } = await supabase
  .from('scheduled_pins')
  .select('*')
  .eq('id', PIN_ID)
  .single();

if (error || !pin) {
  console.error('Pin not found:', error?.message || 'no row');
  process.exit(1);
}

console.log('=== scheduled_pins row ===');
for (const k of [
  'id', 'user_id', 'status', 'scheduled_for', 'posted_at', 'retry_count',
  'next_retry_at', 'error_message', 'board_id', 'pinterest_account_id',
  'pinterest_pin_id', 'credits_deducted', 'deleted_at', 'created_at', 'updated_at',
]) {
  if (k in pin) console.log(`${k.padEnd(22)} ${pin[k] ?? 'null'}`);
}
console.log(`title                  ${pin.title}`);
console.log(`description len        ${(pin.description || '').length}`);
console.log(`link                   ${pin.link || 'null'}`);
console.log(`image_url              ${pin.image_url || 'null'}`);
console.log(`image_url length       ${(pin.image_url || '').length}`);

async function probe(label, target) {
  if (!target) return;
  console.log(`\n=== probing ${label} ===`);
  try {
    const res = await fetch(target, { method: 'GET', redirect: 'follow' });
    console.log(`status         ${res.status} ${res.statusText}`);
    console.log(`final url      ${res.url}`);
    console.log(`content-type   ${res.headers.get('content-type')}`);
    console.log(`content-length ${res.headers.get('content-length')}`);
    if (label === 'image_url' && res.ok) {
      const buf = Buffer.from(await res.arrayBuffer());
      console.log(`bytes received ${buf.length}`);
      const sig = buf.subarray(0, 4).toString('hex');
      const kind = sig.startsWith('ffd8ff') ? 'JPEG'
        : sig === '89504e47' ? 'PNG'
        : buf.subarray(0, 4).toString('ascii') === 'RIFF' ? 'WEBP?'
        : `unknown (${sig})`;
      console.log(`magic bytes    ${kind}`);
    } else if (!res.ok) {
      const body = (await res.text()).slice(0, 300);
      console.log(`body           ${body}`);
    }
  } catch (e) {
    console.log(`fetch failed   ${e?.message || e}`);
  }
}

await probe('image_url', pin.image_url);
await probe('link', pin.link);
