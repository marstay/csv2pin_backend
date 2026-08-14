/**
 * Read-only: is Google sending traffic that fails to convert, or not sending it at all?
 *
 * Every attributed paying customer since attribution went live (2026-08-07) arrived from
 * Bing / DuckDuckGo / Brave / ChatGPT and landed on "/". This asks what Google did over the
 * same window: clicks overall, clicks to "/" specifically, and the daily trend.
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
  const res = await api.searchanalytics.query({ siteUrl, requestBody: { rowLimit: 25000, ...body } });
  return res.data.rows || [];
};

// GSC lags ~2 days.
const end = new Date(Date.now() - 2 * 86400000);
const start = new Date(end.getTime() - 13 * 86400000);
const iso = (d) => d.toISOString().slice(0, 10);
const range = { startDate: iso(start), endDate: iso(end) };
console.log(`property: ${siteUrl}`);
console.log(`range   : ${range.startDate} -> ${range.endDate} (Google organic only)\n`);

const byDate = await q({ ...range, dimensions: ['date'] });
console.log('=== GOOGLE CLICKS BY DAY ===');
console.log('date         clicks  impressions   ctr     avg pos');
let tc = 0, ti = 0;
for (const r of byDate.sort((a, b) => a.keys[0].localeCompare(b.keys[0]))) {
  tc += r.clicks; ti += r.impressions;
  console.log(
    `${r.keys[0]}  ${String(r.clicks).padStart(6)}  ${String(r.impressions).padStart(11)}  ${(r.ctr * 100).toFixed(2).padStart(6)}%  ${r.position.toFixed(1).padStart(7)}`
  );
}
console.log(`TOTAL        ${String(tc).padStart(6)}  ${String(ti).padStart(11)}`);

const byPage = await q({ ...range, dimensions: ['page'] });
console.log(`\n=== TOP GOOGLE LANDING PAGES (${byPage.length} pages with impressions) ===`);
console.log('clicks  impr   page');
for (const r of byPage.sort((a, b) => b.clicks - a.clicks || b.impressions - a.impressions).slice(0, 15)) {
  console.log(`${String(r.clicks).padStart(6)}  ${String(r.impressions).padStart(5)}  ${r.keys[0].replace('https://url2pin.com', '') || '/'}`);
}

// The homepage specifically — that is where all four attributed payers landed.
const home = byPage.filter((r) => /^https:\/\/url2pin\.com\/?$/.test(r.keys[0]));
const homeClicks = home.reduce((s, r) => s + r.clicks, 0);
const homeImpr = home.reduce((s, r) => s + r.impressions, 0);
console.log(`\n=== GOOGLE -> HOMEPAGE "/" (the page all 4 payers converted on) ===`);
console.log(`  clicks: ${homeClicks}   impressions: ${homeImpr}`);
console.log(`  share of all Google clicks: ${tc ? ((100 * homeClicks) / tc).toFixed(1) : 0}%`);

const byCountry = await q({ ...range, dimensions: ['country'] });
const tier1 = new Set(['usa', 'gbr', 'can', 'aus']);
const t1 = byCountry.filter((r) => tier1.has(r.keys[0]));
console.log(`\n=== GOOGLE CLICKS: TIER-1 (US/UK/CA/AU) vs REST ===`);
console.log(`  tier-1: ${t1.reduce((s, r) => s + r.clicks, 0)} clicks / ${t1.reduce((s, r) => s + r.impressions, 0)} impr`);
console.log(`  rest  : ${tc - t1.reduce((s, r) => s + r.clicks, 0)} clicks`);
for (const r of t1.sort((a, b) => b.clicks - a.clicks)) {
  console.log(`     ${r.keys[0].toUpperCase()}  ${String(r.clicks).padStart(4)} clicks  ${String(r.impressions).padStart(5)} impr  pos ${r.position.toFixed(1)}`);
}
