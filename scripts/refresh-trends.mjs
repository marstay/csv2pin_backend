/**
 * Force-refresh the Pinterest trends catalog (same as POST /api/trends/refresh).
 *
 * Prerequisites (backend/.env or environment):
 *   TRENDS_ADMIN_KEY          — must match the server; server returns 404 if unset.
 *   TRENDS_API_BASE_URL       — optional; e.g. https://api.yourdomain.com (no trailing slash)
 *                               Falls back to API_URL, then http://localhost:4000
 *
 * Usage (from backend directory):
 *   cd backend && node scripts/refresh-trends.mjs
 *   node scripts/refresh-trends.mjs https://api.example.com
 *
 * Cron (daily 06:00 UTC example):
 *   0 6 * * * cd /path/to/pinFactory/backend && /usr/bin/node scripts/refresh-trends.mjs >> /var/log/url2pin-trends.log 2>&1
 *
 * curl equivalent:
 *   curl -sS -X POST "$TRENDS_API_BASE_URL/api/trends/refresh" -H "x-trends-admin-key: $TRENDS_ADMIN_KEY" -H "Accept: application/json"
 *
 * If you only see "fetch failed", the TCP/TLS connection did not complete (wrong URL, server down, or blocked).
 * Run again after this update — the script prints the URL and a clearer cause when possible.
 */

import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const adminKey = String(process.env.TRENDS_ADMIN_KEY || '').trim();
const baseArg = process.argv[2] ? String(process.argv[2]).trim().replace(/\/$/, '') : '';
const base =
  baseArg ||
  String(process.env.TRENDS_API_BASE_URL || process.env.API_URL || '').trim().replace(/\/$/, '') ||
  'http://localhost:4000';

if (!adminKey) {
  console.error('Missing TRENDS_ADMIN_KEY (set in backend/.env or export before running).');
  process.exit(1);
}

const url = `${base}/api/trends/refresh`;

function explainFetchError(err) {
  const lines = [`Request URL: ${url}`, `Message: ${err?.message || err}`];
  const c = err?.cause;
  if (c) {
    lines.push(`Cause: ${c.message || c}`);
    if (c.code) lines.push(`Code: ${c.code}`);
    if (c.errors && Array.isArray(c.errors)) {
      for (const sub of c.errors) {
        lines.push(`  — ${sub?.message || sub}`);
      }
    }
  }
  if (err?.code) lines.push(`Errno: ${err.code}`);
  lines.push('');
  lines.push('Typical fixes:');
  lines.push('  • Backend not running? Start it (e.g. npm start in backend) or use your real TRENDS_API_BASE_URL / API_URL.');
  lines.push('  • Wrong host/port? Default is http://localhost:4000 — pass the base URL: node scripts/refresh-trends.mjs https://your-api-host');
  lines.push('  • HTTPS / DNS / firewall blocking outbound POST from this machine.');
  return lines.join('\n');
}

async function main() {
  let res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'x-trends-admin-key': adminKey,
      },
    });
  } catch (err) {
    console.error(explainFetchError(err));
    process.exit(1);
  }
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  if (!res.ok) {
    console.error(`HTTP ${res.status}`, body);
    process.exit(1);
  }
  console.log(JSON.stringify(body, null, 2));

  const isLocal =
    /localhost|127\.0\.0\.1|^http:\/\/0\.0\.0\.0/i.test(base) ||
    /^http:\/\/(192\.168\.|10\.|172\.(1[6-9]|2\d|3[01])\.)/i.test(base);
  if (isLocal && !baseArg && !String(process.env.TRENDS_API_BASE_URL || process.env.API_URL || '').trim()) {
    console.log('');
    console.log(
      'Note: You used the default http://localhost:4000. The live site uses whatever NEXT_PUBLIC_API_URL points to (see frontend/.env). If the trends page still looks old after a browser refresh, run the script with your production API base URL, for example:'
    );
    console.log('  node scripts/refresh-trends.mjs https://your-backend-host');
    console.log('Or set TRENDS_API_BASE_URL in backend/.env.');
  }

  let verifyRes;
  try {
    verifyRes = await fetch(`${base}/api/trends`, {
      headers: { Accept: 'application/json' },
      cache: 'no-store',
    });
  } catch (e) {
    console.log('');
    console.log('Could not verify with GET /api/trends:', e?.message || e);
    return;
  }
  const verifyText = await verifyRes.text();
  let verifyBody;
  try {
    verifyBody = JSON.parse(verifyText);
  } catch {
    verifyBody = verifyText;
  }
  if (!verifyRes.ok) {
    console.log('');
    console.log(`GET /api/trends returned HTTP ${verifyRes.status}`, verifyBody);
    return;
  }
  const postAt = body?.generatedAt;
  const getAt = verifyBody?.generatedAt;
  console.log('');
  console.log('GET /api/trends check:', { generatedAt: getAt, trendCount: Array.isArray(verifyBody?.trends) ? verifyBody.trends.length : 0 });
  if (postAt && getAt && postAt !== getAt) {
    console.log('');
    console.warn(
      'WARNING: POST refresh reported a different generatedAt than GET /api/trends. Common causes: multiple backend instances (each has its own in-memory/disk cache), or a proxy still serving an old GET response. Try again or consolidate to one instance / shared storage.'
    );
  }
}

main().catch((e) => {
  console.error(e?.message || e);
  process.exit(1);
});
