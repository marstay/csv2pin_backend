/**
 * Google Search Console URL Inspection.
 *
 * gsc-api.mjs answers "what traffic did this page get". It cannot answer "why did it get
 * none", because a page with zero impressions may be any of:
 *
 *   - not indexed at all               -> fix crawling / quality
 *   - indexed but ranking nowhere      -> authority problem, on-page work will not help
 *   - collapsed into another canonical -> genuine duplicate, consolidation helps
 *
 * Those need completely different responses, and guessing between them wastes weeks. This
 * wraps urlInspection.index.inspect, which states it directly.
 *
 * Two fields carry most of the signal:
 *   coverageState  — "Submitted and indexed" vs "Crawled - currently not indexed"
 *   lastCrawlTime  — Google recrawls pages it values. A page last crawled weeks ago while
 *                    others are hit every few days is being told it is not worth revisiting.
 *
 * Auth: same service account as gsc-api.mjs (GSC_SERVICE_ACCOUNT_KEY in backend/.env),
 * added as a user on the property in Search Console.
 *
 * Usage:
 *   node backend/scripts/gsc-inspect.mjs /blog/some-post /pricing
 *   node backend/scripts/gsc-inspect.mjs --file=urls.txt
 *   node backend/scripts/gsc-inspect.mjs --sitemap --limit=50
 *
 * Quota: 2000 inspections/day, 600/minute. Requests are paced to stay well under.
 */
import dotenv from 'dotenv';
import { google } from 'googleapis';
import { readFileSync } from 'node:fs';
import { dirname, isAbsolute, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const BACKEND = join(HERE, '..');
dotenv.config({ path: join(BACKEND, '.env'), quiet: true });

const SITE = process.env.GSC_SITE_URL;
const ORIGIN = String(SITE || '').replace(/\/$/, '');
// The key path may be absolute (a machine-local secrets dir) or relative to backend/.
const RAW_KEY = String(process.env.GSC_SERVICE_ACCOUNT_KEY || '').trim();
const KEY_FILE = isAbsolute(RAW_KEY) ? RAW_KEY : join(BACKEND, RAW_KEY);

const PACE_MS = 400;

function usage(msg) {
  if (msg) console.error(`\n${msg}`);
  console.error(`
Usage:
  node backend/scripts/gsc-inspect.mjs <url-or-path> [more...]
  node backend/scripts/gsc-inspect.mjs --file=urls.txt
  node backend/scripts/gsc-inspect.mjs --sitemap [--limit=50]
`);
  process.exit(msg ? 1 : 0);
}

/**
 * Git Bash rewrites a bare leading-slash argument into a Windows filesystem path before node
 * ever sees it, turning /blog/x into C:/.../Git/blog/x. Recover the intended path rather than
 * building a nonsense URL and reporting a misleading "you do not own this site" error.
 */
function toUrl(input) {
  let s = String(input || '').trim();
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) return s;
  const mangled = s.match(/(?:Git|MINGW\d*)((?:\/[^/]+)+)$/i);
  if (mangled) s = mangled[1];
  if (!s.startsWith('/')) s = `/${s}`;
  return `${ORIGIN}${s}`;
}

async function readSitemapUrls(limit) {
  const res = await fetch(`${ORIGIN}/sitemap.xml`);
  if (!res.ok) throw new Error(`sitemap.xml returned ${res.status}`);
  const xml = await res.text();
  const urls = [...xml.matchAll(/<loc>\s*([^<\s]+)\s*<\/loc>/g)].map((m) => m[1]);
  return limit ? urls.slice(0, limit) : urls;
}

async function main() {
  const args = process.argv.slice(2);
  if (!args.length || args.includes('--help') || args.includes('-h')) usage();
  if (!SITE) usage('GSC_SITE_URL is not set in backend/.env');
  if (!RAW_KEY) usage('GSC_SERVICE_ACCOUNT_KEY is not set in backend/.env');

  const limitArg = args.find((a) => a.startsWith('--limit='));
  const limit = limitArg ? Number(limitArg.split('=')[1]) || 0 : 0;
  const fileArg = args.find((a) => a.startsWith('--file='));

  let targets;
  if (args.includes('--sitemap')) {
    targets = await readSitemapUrls(limit);
  } else if (fileArg) {
    targets = readFileSync(fileArg.split('=').slice(1).join('='), 'utf8')
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith('#'));
    if (limit) targets = targets.slice(0, limit);
  } else {
    targets = args.filter((a) => !a.startsWith('--'));
  }

  targets = targets.map(toUrl).filter(Boolean);
  if (!targets.length) usage('No URLs to inspect.');

  const auth = new google.auth.GoogleAuth({
    keyFile: KEY_FILE,
    scopes: ['https://www.googleapis.com/auth/webmasters.readonly'],
  });
  const sc = google.searchconsole({ version: 'v1', auth: await auth.getClient() });

  console.log(`site: ${SITE}   inspecting ${targets.length} URL(s)\n`);
  console.log(
    `${'verdict'.padEnd(9)} ${'coverageState'.padEnd(33)} ${'lastCrawl'.padEnd(11)} url`
  );

  const tally = {};
  const stale = [];
  for (const url of targets) {
    try {
      const res = await sc.urlInspection.index.inspect({
        requestBody: { inspectionUrl: url, siteUrl: SITE },
      });
      const r = res.data?.inspectionResult?.indexStatusResult || {};
      const verdict = r.verdict || '?';
      const cov = r.coverageState || '?';
      const crawl = (r.lastCrawlTime || '').slice(0, 10) || '—';
      const gc = (r.googleCanonical || '').replace(ORIGIN, '');
      const uc = (r.userCanonical || '').replace(ORIGIN, '');
      // Google choosing a different canonical is the definitive duplicate signal — it means
      // the page was folded into another and will never rank on its own.
      const note = gc && uc && gc !== uc ? `  CANONICAL -> ${gc}` : '';
      tally[cov] = (tally[cov] || 0) + 1;
      if (r.lastCrawlTime) stale.push({ url, at: r.lastCrawlTime });
      console.log(
        `${verdict.padEnd(9)} ${cov.slice(0, 32).padEnd(33)} ${crawl.padEnd(11)} ${url.replace(ORIGIN, '')}${note}`
      );
    } catch (err) {
      const msg = String(err?.message || err).replace(/\s+/g, ' ').slice(0, 60);
      tally.ERROR = (tally.ERROR || 0) + 1;
      console.log(`${'ERROR'.padEnd(9)} ${msg.padEnd(33)} ${'—'.padEnd(11)} ${url}`);
    }
    await new Promise((r) => setTimeout(r, PACE_MS));
  }

  console.log('\n=== coverage ===');
  for (const [k, v] of Object.entries(tally).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${String(v).padStart(4)}  ${k}`);
  }

  if (stale.length > 1) {
    stale.sort((a, b) => a.at.localeCompare(b.at));
    const days = (t) => Math.round((Date.now() - new Date(t).getTime()) / 86_400_000);
    console.log('\n=== staleness (crawl frequency tracks how much Google values a page) ===');
    console.log(`  most recent : ${days(stale[stale.length - 1].at)}d ago  ${stale[stale.length - 1].url.replace(ORIGIN, '')}`);
    console.log(`  oldest      : ${days(stale[0].at)}d ago  ${stale[0].url.replace(ORIGIN, '')}`);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
