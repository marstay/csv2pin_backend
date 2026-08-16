/**
 * Read-only SEO snapshot: current 28 days vs the prior 28, plus pages, queries and
 * a branded/non-branded split. GSC data lags ~2 days, so windows end there.
 */
import dotenv from 'dotenv';
import { google } from 'googleapis';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const BACKEND = join(dirname(fileURLToPath(import.meta.url)), '..');
dotenv.config({ path: join(BACKEND, '.env') });

const auth = new google.auth.GoogleAuth({
  keyFile: process.env.GSC_SERVICE_ACCOUNT_KEY,
  scopes: ['https://www.googleapis.com/auth/webmasters.readonly'],
});
const api = google.searchconsole({ version: 'v1', auth: await auth.getClient() });
const siteUrl = 'https://url2pin.com/';

const q = async (body) => {
  const r = await api.searchanalytics.query({ siteUrl, requestBody: { rowLimit: 25000, ...body } });
  return r.data.rows || [];
};
const iso = (d) => d.toISOString().slice(0, 10);
const end = new Date(Date.now() - 2 * 86400000);
const cur = { startDate: iso(new Date(end.getTime() - 27 * 86400000)), endDate: iso(end) };
const prevEnd = new Date(end.getTime() - 28 * 86400000);
const prev = { startDate: iso(new Date(prevEnd.getTime() - 27 * 86400000)), endDate: iso(prevEnd) };

const tot = (rows) => rows.reduce(
  (a, r) => ({ c: a.c + r.clicks, i: a.i + r.impressions, p: a.p + r.position * r.impressions }),
  { c: 0, i: 0, p: 0 }
);

const curRows = await q({ ...cur, dimensions: ['date'] });
const prevRows = await q({ ...prev, dimensions: ['date'] });
const C = tot(curRows), P = tot(prevRows);
const pct = (a, b) => (b === 0 ? '—' : `${a >= b ? '+' : ''}${(((a - b) / b) * 100).toFixed(0)}%`);

console.log(`GOOGLE SEARCH CONSOLE — ${siteUrl}\n`);
console.log(`current : ${cur.startDate} -> ${cur.endDate}`);
console.log(`previous: ${prev.startDate} -> ${prev.endDate}\n`);
console.log('metric          current   previous   change');
console.log(`clicks       ${String(C.c).padStart(9)} ${String(P.c).padStart(10)}   ${pct(C.c, P.c)}`);
console.log(`impressions  ${String(C.i).padStart(9)} ${String(P.i).padStart(10)}   ${pct(C.i, P.i)}`);
console.log(`CTR          ${(100 * C.c / (C.i || 1)).toFixed(2).padStart(8)}% ${(100 * P.c / (P.i || 1)).toFixed(2).padStart(9)}%`);
console.log(`avg position ${(C.p / (C.i || 1)).toFixed(1).padStart(9)} ${(P.p / (P.i || 1)).toFixed(1).padStart(10)}`);

const pages = await q({ ...cur, dimensions: ['page'] });
console.log(`\n=== TOP PAGES (${pages.length} with impressions) ===`);
console.log('clicks   impr    CTR     pos   page');
for (const r of pages.sort((a, b) => b.clicks - a.clicks || b.impressions - a.impressions).slice(0, 14)) {
  console.log(
    `${String(r.clicks).padStart(6)} ${String(r.impressions).padStart(6)} ${(100 * r.ctr).toFixed(1).padStart(6)}% ${r.position.toFixed(1).padStart(6)}   ${r.keys[0].replace('https://url2pin.com', '') || '/'}`
  );
}

const queries = await q({ ...cur, dimensions: ['query'] });
const BRAND = /url2pin|url 2 pin|url to pin/i;
const b = queries.filter((r) => BRAND.test(r.keys[0]));
const nb = queries.filter((r) => !BRAND.test(r.keys[0]));
const B = tot(b), N = tot(nb);
console.log(`\n=== BRANDED vs DISCOVERY (${queries.length} queries) ===`);
console.log(`branded      clicks=${String(B.c).padStart(4)}  impr=${String(B.i).padStart(6)}  CTR=${(100 * B.c / (B.i || 1)).toFixed(1)}%`);
console.log(`non-branded  clicks=${String(N.c).padStart(4)}  impr=${String(N.i).padStart(6)}  CTR=${(100 * N.c / (N.i || 1)).toFixed(1)}%`);

console.log('\n=== TOP NON-BRANDED QUERIES BY CLICKS ===');
for (const r of nb.sort((a, b2) => b2.clicks - a.clicks).slice(0, 12)) {
  console.log(`${String(r.clicks).padStart(4)} clicks ${String(r.impressions).padStart(6)} impr  pos ${r.position.toFixed(1).padStart(5)}  ${r.keys[0].slice(0, 60)}`);
}

console.log('\n=== BIGGEST IMPRESSION SINKS (impressions, near-zero clicks) ===');
for (const r of nb.filter((x) => x.impressions >= 100 && x.clicks <= 2).sort((a, b2) => b2.impressions - a.impressions).slice(0, 10)) {
  console.log(`${String(r.impressions).padStart(6)} impr ${String(r.clicks).padStart(3)} clicks  pos ${r.position.toFixed(1).padStart(5)}  ${r.keys[0].slice(0, 60)}`);
}

const posBands = { '1-3': 0, '4-10': 0, '11-20': 0, '21+': 0 };
for (const r of queries) {
  const p = r.position;
  if (p <= 3) posBands['1-3'] += r.impressions;
  else if (p <= 10) posBands['4-10'] += r.impressions;
  else if (p <= 20) posBands['11-20'] += r.impressions;
  else posBands['21+'] += r.impressions;
}
console.log('\n=== IMPRESSIONS BY POSITION BAND ===');
for (const [k, v] of Object.entries(posBands)) {
  console.log(`  pos ${k.padEnd(6)} ${String(v).padStart(6)}  ${((100 * v) / (C.i || 1)).toFixed(0)}%`);
}
