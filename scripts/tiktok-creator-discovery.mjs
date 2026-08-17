/**
 * Find TikTok creators in the Amazon-affiliate / Pinterest niche and extract public contact info.
 *
 * READ-ONLY and unauthenticated: uses the RapidAPI scraper, never signs in as you and never sends
 * anything. Your TikTok account is not touched — which matters, because the previous account was
 * banned and a fresh one has far less tolerance for outreach-shaped behaviour.
 *
 *   node backend/scripts/tiktok-creator-discovery.mjs [--budget 150] [--out creators.csv]
 *
 * Budget: the RapidAPI plan allows 300 requests/month. Costs are:
 *   1 request per keyword search  (returns ~20 videos, ~10 distinct creators)
 *   1 request per creator profile (the only way to get the bio, where emails live)
 * So the strategy is: search broadly and cheaply, rank the pool, then spend the remaining
 * budget on profile lookups for the best candidates only.
 */
import { readFileSync, writeFileSync } from 'node:fs';

const args = process.argv.slice(2);
const flag = (n, d) => {
  const i = args.indexOf(`--${n}`);
  return i === -1 ? d : args[i + 1];
};
const BUDGET = Math.max(10, Number(flag('budget', 150)) || 150);
const OUT = flag('out', 'tiktok-creators.csv');

const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) {
  const t = l.trim();
  if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (m && !env[m[1]]) env[m[1]] = m[2].trim();
}
const KEY = env.RAPIDAPI_KEY;
const HOST = 'tiktok-scraper7.p.rapidapi.com';

let used = 0;
async function api(path) {
  if (used >= BUDGET) return null;
  used += 1;
  try {
    const r = await fetch(`https://${HOST}${path}`, {
      headers: { 'x-rapidapi-key': KEY, 'x-rapidapi-host': HOST },
    });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  }
}

// Keywords chosen to surface people TEACHING this, not people selling unrelated products.
const KEYWORDS = [
  'amazon affiliate pinterest', 'pinterest affiliate marketing', 'amazon associates pinterest',
  'pinterest amazon finds', 'affiliate marketing pinterest 2026', 'pinterest side hustle affiliate',
  'faceless pinterest affiliate', 'amazon influencer pinterest', 'pinterest traffic affiliate',
  'make money on pinterest', 'pinterest marketing tips', 'pinterest pins affiliate links',
  'amazon storefront pinterest', 'pinterest for bloggers', 'pinterest seo affiliate',
  'digital products pinterest', 'pinterest automation tool', 'amazon finds creator',
  'pinterest beginners affiliate', 'blog traffic pinterest',
];

// Do not pitch people selling a competing course/tool.
const COMPETITOR_HINT = /(gumroad|course|ebook|my guide|template).{0,40}(pinterest|affiliate)|pinterest.{0,20}(course|guide|template|blueprint)/i;
const EMAIL_RE = /[a-z0-9._%+-]+\s*(?:@|\(at\)|\[at\]|\s+at\s+)\s*[a-z0-9.-]+\s*\.\s*[a-z]{2,}/i;

console.log(`TikTok creator discovery — budget ${BUDGET} requests\n`);

// --- phase 1: cheap keyword searches to build a ranked pool -------------------
const pool = new Map();
for (const kw of KEYWORDS) {
  if (used >= Math.floor(BUDGET * 0.2)) break; // cap discovery at 20% of budget
  const j = await api(`/feed/search?keywords=${encodeURIComponent(kw)}&region=us&count=20&cursor=0&publish_time=0&sort_type=0`);
  const vids = j?.data?.videos || [];
  for (const v of vids) {
    const h = v?.author?.unique_id;
    if (!h) continue;
    const cur = pool.get(h) || { handle: h, nick: v.author?.nickname || '', hits: 0, plays: 0, kws: new Set() };
    cur.hits += 1;
    cur.plays += Number(v.play_count) || 0;
    cur.kws.add(kw);
    pool.set(h, cur);
  }
  process.stdout.write(`  searched "${kw}" — pool ${pool.size}\r`);
}
console.log(`\nphase 1: ${used} requests, ${pool.size} distinct creators found\n`);

// Rank: appearing across multiple keywords matters more than one viral video.
const ranked = [...pool.values()].sort(
  (a, b) => b.kws.size - a.kws.size || b.plays - a.plays
);

// --- phase 2: spend the rest of the budget on profiles -----------------------
const rows = [];
for (const c of ranked) {
  if (used >= BUDGET) break;
  const j = await api(`/user/info?unique_id=${encodeURIComponent(c.handle)}`);
  const u = j?.data?.user || {};
  const s = j?.data?.stats || {};
  const bio = String(u.signature || '').replace(/\s+/g, ' ').trim();
  const em = EMAIL_RE.exec(bio);
  const email = em ? em[0].replace(/\s*\(at\)\s*|\s*\[at\]\s*|\s+at\s+/i, '@').replace(/\s+/g, '') : '';
  rows.push({
    handle: c.handle,
    nickname: u.nickname || c.nick,
    followers: Number(s.followerCount) || 0,
    videos: Number(s.videoCount) || 0,
    keywordHits: c.kws.size,
    plays: c.plays,
    email,
    instagram: u.ins_id || '',
    youtube: u.youtube_channel_title || '',
    likelyCompetitor: COMPETITOR_HINT.test(bio) ? 'yes' : '',
    bio,
  });
  process.stdout.write(`  profiles ${rows.length}/${ranked.length} — emails ${rows.filter((r) => r.email).length}   (req ${used}/${BUDGET})\r`);
}
console.log('\n');

const withEmail = rows.filter((r) => r.email && r.likelyCompetitor !== 'yes');
withEmail.sort((a, b) => b.followers - a.followers);

const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
const header = ['handle', 'nickname', 'followers', 'videos', 'keywordHits', 'plays', 'email', 'instagram', 'youtube', 'likelyCompetitor', 'bio'];
writeFileSync(OUT, [header.join(','), ...rows.map((r) => header.map((h) => esc(r[h])).join(','))].join('\n'), 'utf8');

console.log(`requests used : ${used} of ${BUDGET}`);
console.log(`creators found: ${pool.size}   profiles fetched: ${rows.length}`);
console.log(`WITH EMAIL    : ${rows.filter((r) => r.email).length}  (${withEmail.length} after excluding likely competitors)`);
console.log(`also reachable: ${rows.filter((r) => !r.email && (r.instagram || r.youtube)).length} via Instagram/YouTube only`);
console.log(`\nwrote ${OUT}\n`);
console.log('TOP CONTACTABLE CREATORS');
console.log('handle                    followers   kw  email');
for (const r of withEmail.slice(0, 40)) {
  console.log(`@${r.handle.padEnd(24)}${String(r.followers).padStart(9)}${String(r.keywordHits).padStart(5)}  ${r.email}`);
}
