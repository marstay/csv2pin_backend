/**
 * Clean a Google Search Console performance export.
 *
 * Raw GSC totals for url2pin.com are dominated by traffic that is not real demand:
 *   - branded queries ("url2pin") — measures memory, not reach
 *   - synthetic bot queries ("keyword7954", "keyword-pmripymy", "keyword_test_045")
 *   - a phantom "pinterest affiliate links policy" permutation cluster whose keywords
 *     return ZERO search volume in DataForSEO (verified 2026-07-30)
 *   - LLM chat fragments logged as queries ("so give me the link", "batao")
 *
 * This script strips those and reports what is left, plus a tier-1 geography split.
 *
 * Usage:
 *   node backend/scripts/gsc-clean-report.mjs "<path to GSC export folder>"
 *   node backend/scripts/gsc-clean-report.mjs "C:/Users/me/Downloads/...-Performance-on-Search-2026-07-30"
 */
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2];
if (!dir || !existsSync(dir)) {
  console.error('Pass the path to a GSC export folder (the one containing Queries.csv).');
  process.exit(1);
}

/** Minimal CSV parser: handles quoted fields and embedded newlines/commas. */
function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  const s = text.replace(/^\uFEFF/, '');
  for (let i = 0; i < s.length; i += 1) {
    const c = s[i];
    if (inQuotes) {
      if (c === '"') {
        if (s[i + 1] === '"') { field += '"'; i += 1; } else { inQuotes = false; }
      } else field += c;
    } else if (c === '"') inQuotes = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows.filter((r) => r.length > 1 || (r[0] || '').trim());
}

function loadCsv(name) {
  const p = join(dir, name);
  if (!existsSync(p)) return [];
  const rows = parseCsv(readFileSync(p, 'utf8'));
  const header = rows.shift() || [];
  return rows.map((r) => Object.fromEntries(header.map((h, i) => [h.trim(), (r[i] ?? '').trim()])));
}

const num = (v) => Number(String(v).replace(/[%,]/g, '')) || 0;

// --- classification -------------------------------------------------------

const BRAND = /url2pin|url\s?2\s?pin|urltopin/i;

/** Synthetic bot queries: keyword + digits, keyword-<random letters>, keyword_test_NN. */
const SYNTHETIC = /^keyword[\s_-]?(new\s?)?\d+$|^keyword[-_][a-z]{6,}$|^keyword[\s_]?test[\s_-]?\d+$|^p\d+\s+keyword$/i;

/**
 * Phantom policy cluster: long permutations of "pinterest/amazon associates affiliate links policy".
 * Verified zero search volume in DataForSEO. Requires 4+ words so genuine short queries
 * (e.g. "pinterest affiliate links policy") are still counted as real.
 */
function isPhantomPolicy(q) {
  const words = q.split(/\s+/).length;
  return words >= 5 && /affiliate links?/i.test(q) && /(policy|policies|allowed|disclosure|operating agreement|community guidelines)/i.test(q);
}

/** Search-operator scrapes: queries containing site:/inurl:/quoted operators. */
const OPERATOR = /site:|inurl:|intitle:|filetype:/i;

function classify(q) {
  if (!q) return 'other';
  if (BRAND.test(q)) return 'brand';
  if (SYNTHETIC.test(q)) return 'synthetic';
  if (OPERATOR.test(q)) return 'operator';
  if (isPhantomPolicy(q)) return 'phantom_policy';
  return 'real';
}

// --- report ---------------------------------------------------------------

const queries = loadCsv('Queries.csv');
const qKey = Object.keys(queries[0] || {})[0] || 'Top queries';

const buckets = {};
for (const r of queries) {
  const q = r[qKey] || '';
  const k = classify(q);
  if (!buckets[k]) buckets[k] = { queries: 0, clicks: 0, impressions: 0, rows: [] };
  const b = buckets[k];
  b.queries += 1;
  b.clicks += num(r.Clicks);
  b.impressions += num(r.Impressions);
  b.rows.push({ q, clicks: num(r.Clicks), impressions: num(r.Impressions), position: num(r.Position) });
}

const totalClicks = Object.values(buckets).reduce((s, b) => s + b.clicks, 0);
const totalImpr = Object.values(buckets).reduce((s, b) => s + b.impressions, 0);

const pct = (n, d) => (d ? ((n / d) * 100).toFixed(1) + '%' : '—');

console.log(`GSC export: ${dir}\n`);
console.log('bucket           | queries | clicks | impressions | % of impr');
const order = ['real', 'brand', 'synthetic', 'phantom_policy', 'operator', 'other'];
for (const k of order) {
  const b = buckets[k];
  if (!b) continue;
  console.log(
    `${k.padEnd(16)} | ${String(b.queries).padEnd(7)} | ${String(b.clicks).padEnd(6)} | ${String(b.impressions).padEnd(11)} | ${pct(b.impressions, totalImpr)}`
  );
}
console.log(`${'TOTAL'.padEnd(16)} | ${String(queries.length).padEnd(7)} | ${String(totalClicks).padEnd(6)} | ${String(totalImpr).padEnd(11)} | 100%`);

const real = buckets.real || { clicks: 0, impressions: 0, rows: [] };
const junkClicks =
  (buckets.synthetic?.clicks || 0) + (buckets.phantom_policy?.clicks || 0) + (buckets.operator?.clicks || 0);
const junkImpr =
  (buckets.synthetic?.impressions || 0) +
  (buckets.phantom_policy?.impressions || 0) +
  (buckets.operator?.impressions || 0);

// GSC only names its top ~1000 queries and anonymises rare ones, so Queries.csv never
// reconciles to the site total. Devices.csv IS complete — use it to size the gap.
const devices = loadCsv('Devices.csv');
const siteClicks = devices.reduce((s, r) => s + num(r.Clicks), 0);
const siteImpr = devices.reduce((s, r) => s + num(r.Impressions), 0);
const hiddenClicks = siteClicks - totalClicks;
const hiddenImpr = siteImpr - totalImpr;

console.log('\n--- COVERAGE (Queries.csv vs whole site) ---');
console.log(`site total (Devices.csv) : ${siteClicks} clicks / ${siteImpr} impressions`);
console.log(`named in Queries.csv     : ${totalClicks} clicks / ${totalImpr} impressions`);
console.log(`anonymised by Google     : ${hiddenClicks} clicks / ${hiddenImpr} impressions (${pct(hiddenClicks, siteClicks)} of clicks)`);

// --- tier-1 gate ------------------------------------------------------------
// Decision 2026-08-06: tier-3 traffic (Pakistan, India, Morocco, Turkey, Algeria,
// Nigeria, Kenya, Bangladesh) is explicitly NOT counted as a win — those visitors
// will not buy a USD SaaS subscription. Tier-1 is the headline number now.
const TIER1 = new Set(['United States', 'United Kingdom', 'Canada', 'Australia', 'Germany', 'Netherlands', 'France', 'Sweden', 'Norway', 'Denmark', 'Switzerland', 'Ireland', 'New Zealand', 'Austria', 'Belgium', 'Finland', 'Japan', 'Singapore']);

const countries = loadCsv('Countries.csv');
const cKey = countries.length ? Object.keys(countries[0])[0] : null;
let tier1 = { clicks: 0, impressions: 0 };
let tier3 = { clicks: 0, impressions: 0 };
const tier3Top = [];
for (const r of countries) {
  const name = r[cKey];
  const t = TIER1.has(name) ? tier1 : tier3;
  t.clicks += num(r.Clicks);
  t.impressions += num(r.Impressions);
  if (!TIER1.has(name) && num(r.Clicks) > 0) tier3Top.push({ name, clicks: num(r.Clicks) });
}

console.log('\n--- HEADLINE (tier-1 only) ---');
console.log(`tier-1 clicks       : ${tier1.clicks} / ${tier1.impressions} impr (CTR ${pct(tier1.clicks, tier1.impressions)})`);
console.log(`tier-1 share        : ${pct(tier1.clicks, tier1.clicks + tier3.clicks)} of all clicks`);
console.log(`excluded (non-tier-1): ${tier3.clicks} clicks — not counted as wins by decision`);
// Countries.csv cannot be split by query, so brand cannot be netted out per country.
// Report brand separately rather than inventing a precise "tier-1 non-brand" figure.
console.log(`for reference, brand clicks across ALL geos: ${buckets.brand?.clicks || 0}`);

console.log('\n--- CLEAN SCOREBOARD ---');
console.log(`named real non-brand clicks : ${real.clicks}  (CTR ${pct(real.clicks, real.impressions)} over ${real.impressions} impr)`);
console.log(`junk clicks                 : ${junkClicks}  <-- if 0, the junk is provably non-human`);
console.log(`junk impressions            : ${junkImpr} (${pct(junkImpr, totalImpr)} of named impressions)`);
console.log(`TRUE non-brand clicks       : ${siteClicks - (buckets.brand?.clicks || 0)}  (site total minus brand)`);
console.log(`  of which traceable        : ${real.clicks}`);
console.log(`  of which anonymised long-tail: ${siteClicks - (buckets.brand?.clicks || 0) - real.clicks}`);

console.log('\n--- TOP 25 REAL QUERIES BY IMPRESSIONS ---');
real.rows
  .slice()
  .sort((a, b) => b.impressions - a.impressions)
  .slice(0, 25)
  .forEach((r) => {
    console.log(`${String(r.impressions).padStart(6)} impr | ${String(r.clicks).padStart(3)} clk | pos ${String(r.position).padStart(5)} | ${r.q}`);
  });

console.log('\n--- REAL QUERIES WITH CLICKS ---');
real.rows
  .filter((r) => r.clicks > 0)
  .sort((a, b) => b.clicks - a.clicks)
  .forEach((r) => console.log(`${String(r.clicks).padStart(3)} clk | ${String(r.impressions).padStart(6)} impr | pos ${String(r.position).padStart(5)} | ${r.q}`));

// --- geography ------------------------------------------------------------

if (countries.length) {
  console.log('\n--- GEOGRAPHY (all queries incl. brand) ---');
  console.log(`tier-1 : ${tier1.clicks} clicks / ${tier1.impressions} impr (${pct(tier1.clicks, tier1.clicks + tier3.clicks)} of clicks)`);
  console.log(`rest   : ${tier3.clicks} clicks / ${tier3.impressions} impr (${pct(tier3.clicks, tier1.clicks + tier3.clicks)} of clicks)`);
  console.log('top non-tier-1 by clicks:', tier3Top.sort((a, b) => b.clicks - a.clicks).slice(0, 8).map((c) => `${c.name} ${c.clicks}`).join(', '));
}
