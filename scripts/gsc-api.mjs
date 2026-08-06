/**
 * Google Search Console API client.
 *
 * Replaces the manual "export CSVs from the GSC UI" step. Fetches the same
 * dimensions and writes them in the exact export format, so gsc-clean-report.mjs
 * consumes the output unchanged.
 *
 * Auth: a service account whose client_email has been added as a user on the
 * property in Search Console (Settings -> Users and permissions).
 * Key path comes from GSC_SERVICE_ACCOUNT_KEY in backend/.env.
 *
 * Usage:
 *   node backend/scripts/gsc-api.mjs sites
 *   node backend/scripts/gsc-api.mjs fetch [--days=90] [--site=sc-domain:url2pin.com]
 *   node backend/scripts/gsc-api.mjs pages [--days=28]
 *   node backend/scripts/gsc-api.mjs queries [--days=28] [--limit=40]
 *
 * `fetch` writes to backend/data/gsc/<YYYY-MM-DD>/ then print the path to pipe
 * into: node backend/scripts/gsc-clean-report.mjs "<that path>"
 */
import dotenv from 'dotenv';
import { google } from 'googleapis';
import { mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const BACKEND = join(HERE, '..');

// Resolve .env relative to backend/, not cwd — this runs from the repo root.
dotenv.config({ path: join(BACKEND, '.env') });

const args = process.argv.slice(2);
const cmd = args[0] || 'sites';
const flag = (name, fallback) => {
  const hit = args.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

const KEY_FILE = process.env.GSC_SERVICE_ACCOUNT_KEY;
if (!KEY_FILE || !existsSync(KEY_FILE)) {
  console.error(
    `GSC_SERVICE_ACCOUNT_KEY is unset or points at a missing file.\n` +
      `  current value: ${KEY_FILE || '(unset)'}\n` +
      `  set it in backend/.env to the absolute path of the service-account JSON.`
  );
  process.exit(1);
}

async function client() {
  const auth = new google.auth.GoogleAuth({
    keyFile: KEY_FILE,
    scopes: ['https://www.googleapis.com/auth/webmasters.readonly'],
  });
  return google.searchconsole({ version: 'v1', auth: await auth.getClient() });
}

/** GSC caps a single response at 25k rows; page through until exhausted. */
async function queryAll(api, siteUrl, body) {
  const rows = [];
  const pageSize = 25000;
  for (let startRow = 0; ; startRow += pageSize) {
    const res = await api.searchanalytics.query({
      siteUrl,
      requestBody: { ...body, rowLimit: pageSize, startRow },
    });
    const batch = res.data.rows || [];
    rows.push(...batch);
    if (batch.length < pageSize) break;
  }
  return rows;
}

function dateRange(days) {
  // GSC data lags ~2 days; end there so the final buckets are not half-empty.
  const end = new Date(Date.now() - 2 * 86400000);
  const start = new Date(end.getTime() - (days - 1) * 86400000);
  const iso = (d) => d.toISOString().slice(0, 10);
  return { startDate: iso(start), endDate: iso(end) };
}

async function resolveSite(api) {
  const explicit = flag('site', process.env.GSC_SITE_URL);
  if (explicit) return explicit;
  const { data } = await api.sites.list();
  const entries = data.siteEntry || [];
  const owned = entries.filter((e) => e.permissionLevel !== 'siteUnverifiedUser');
  const match =
    owned.find((e) => /url2pin/i.test(e.siteUrl) && e.siteUrl.startsWith('sc-domain:')) ||
    owned.find((e) => /url2pin/i.test(e.siteUrl)) ||
    owned[0];
  if (!match) {
    throw new Error(
      'No accessible properties. Add the service-account email as a user in Search Console.'
    );
  }
  return match.siteUrl;
}

// --- CSV emission ---------------------------------------------------------

const csvCell = (v) => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

/** Match the GSC UI export: CTR as a percentage string, position to 2dp. */
function toCsv(headerLabel, rows, mapKey) {
  const lines = [`${headerLabel},Clicks,Impressions,CTR,Position`];
  for (const r of rows) {
    lines.push(
      [
        csvCell(mapKey(r.keys[0])),
        r.clicks,
        r.impressions,
        `${(r.ctr * 100).toFixed(2)}%`,
        r.position.toFixed(2),
      ].join(',')
    );
  }
  return lines.join('\n') + '\n';
}

/**
 * The API returns ISO-3166 alpha-3 codes; the UI export uses display names, and
 * gsc-clean-report.mjs matches its tier-1 set on those names. Cover tier-1 plus
 * the non-tier-1 countries that actually show up in this property's traffic.
 */
const COUNTRY = {
  usa: 'United States', gbr: 'United Kingdom', can: 'Canada', aus: 'Australia',
  deu: 'Germany', nld: 'Netherlands', fra: 'France', swe: 'Sweden',
  nor: 'Norway', dnk: 'Denmark', che: 'Switzerland', irl: 'Ireland',
  nzl: 'New Zealand', aut: 'Austria', bel: 'Belgium', fin: 'Finland',
  jpn: 'Japan', sgp: 'Singapore',
  pak: 'Pakistan', ind: 'India', mar: 'Morocco', tur: 'Turkey',
  dza: 'Algeria', nga: 'Nigeria', ken: 'Kenya', bgd: 'Bangladesh',
  idn: 'Indonesia', phl: 'Philippines', bra: 'Brazil', mex: 'Mexico',
  esp: 'Spain', ita: 'Italy', pol: 'Poland', zaf: 'South Africa',
  egy: 'Egypt', vnm: 'Vietnam', tha: 'Thailand', rus: 'Russia',
  grc: 'Greece', prt: 'Portugal', rou: 'Romania', ukr: 'Ukraine',
};
const countryName = (code) => COUNTRY[code] || code.toUpperCase();
const deviceName = (d) => d.charAt(0) + d.slice(1).toLowerCase();

// --- commands -------------------------------------------------------------

async function cmdSites() {
  const api = await client();
  const { data } = await api.sites.list();
  const entries = data.siteEntry || [];
  if (!entries.length) {
    console.log(
      'No properties visible to this service account.\n' +
        'In Search Console: Settings -> Users and permissions -> Add user\n' +
        'Email: (the client_email from the JSON key), permission: Full'
    );
    return;
  }
  console.log('Accessible properties:');
  for (const e of entries) console.log(`  ${e.permissionLevel.padEnd(16)} ${e.siteUrl}`);
}

async function cmdFetch() {
  const api = await client();
  const siteUrl = await resolveSite(api);
  const days = Number(flag('days', 90));
  const range = dateRange(days);
  console.log(`property : ${siteUrl}`);
  console.log(`range    : ${range.startDate} -> ${range.endDate} (${days}d)\n`);

  const specs = [
    ['Queries.csv', 'query', 'Top queries', (k) => k],
    ['Pages.csv', 'page', 'Top pages', (k) => k],
    ['Countries.csv', 'country', 'Country', countryName],
    ['Devices.csv', 'device', 'Device', deviceName],
    ['Dates.csv', 'date', 'Date', (k) => k],
  ];

  const outDir = join(BACKEND, 'data', 'gsc', new Date().toISOString().slice(0, 10));
  mkdirSync(outDir, { recursive: true });

  for (const [file, dimension, label, mapKey] of specs) {
    const rows = await queryAll(api, siteUrl, { ...range, dimensions: [dimension] });
    writeFileSync(join(outDir, file), toCsv(label, rows, mapKey), 'utf8');
    const clicks = rows.reduce((s, r) => s + r.clicks, 0);
    const impr = rows.reduce((s, r) => s + r.impressions, 0);
    console.log(`${file.padEnd(15)} ${String(rows.length).padStart(6)} rows | ${clicks} clicks / ${impr} impr`);
  }

  console.log(`\nwrote ${outDir}`);
  console.log(`next : node backend/scripts/gsc-clean-report.mjs "${outDir}"`);
}

async function cmdTable(dimension, label, mapKey) {
  const api = await client();
  const siteUrl = await resolveSite(api);
  const days = Number(flag('days', 28));
  const limit = Number(flag('limit', 30));
  const rows = await queryAll(api, siteUrl, { ...dateRange(days), dimensions: [dimension] });
  rows.sort((a, b) => b.clicks - a.clicks || b.impressions - a.impressions);
  console.log(`${siteUrl} — last ${days}d, top ${limit} by clicks\n`);
  console.log('clicks |   impr |   ctr |   pos | ' + label);
  for (const r of rows.slice(0, limit)) {
    console.log(
      [
        String(r.clicks).padStart(6),
        String(r.impressions).padStart(6),
        `${(r.ctr * 100).toFixed(2)}%`.padStart(6),
        r.position.toFixed(1).padStart(5),
        mapKey(r.keys[0]),
      ].join(' | ')
    );
  }
  const clicks = rows.reduce((s, r) => s + r.clicks, 0);
  const impr = rows.reduce((s, r) => s + r.impressions, 0);
  console.log(`\n${rows.length} rows total | ${clicks} clicks / ${impr} impressions`);
}

const commands = {
  sites: cmdSites,
  fetch: cmdFetch,
  pages: () => cmdTable('page', 'page', (k) => k.replace(/^https?:\/\/[^/]+/, '') || '/'),
  queries: () => cmdTable('query', 'query', (k) => k),
  countries: () => cmdTable('country', 'country', countryName),
};

const run = commands[cmd];
if (!run) {
  console.error(`Unknown command "${cmd}". Try: ${Object.keys(commands).join(', ')}`);
  process.exit(1);
}

run().catch((err) => {
  const msg = err?.response?.data?.error?.message || err.message;
  console.error(`\nFailed: ${msg}`);
  if (/403|permission|insufficient/i.test(msg)) {
    console.error(
      '\nMost likely the service account is not a user on the property.\n' +
        'Search Console -> Settings -> Users and permissions -> Add user\n' +
        'Email: claudetest@csv2pin.iam.gserviceaccount.com  Permission: Full'
    );
  }
  process.exit(1);
});
