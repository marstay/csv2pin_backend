import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  fetchPinterestKeywordTrends,
  pinterestSignalsToTrendSeeds,
  resolveSuggestionsForTrend,
} from './trendsSignals.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CACHE_PATH = path.join(__dirname, '..', 'data', 'automated-trends.json');

let openaiClient = null;
let getPinterestAccessToken = null;
let refreshInFlight = null;
let memoryCache = null;

function readIntEnv(name, fallback, min, max) {
  const n = Number(process.env[name] ?? fallback);
  const value = Number.isFinite(n) ? n : fallback;
  return Math.max(min, Math.min(max, value));
}

function getTrendCategoryLimits() {
  return {
    amazon: readIntEnv('TRENDS_AMAZON_COUNT', 8, 2, 16),
    etsy: readIntEnv('TRENDS_ETSY_COUNT', 6, 1, 12),
    blogging: readIntEnv('TRENDS_BLOG_COUNT', 6, 1, 12),
  };
}

function selectTrendSeedsByCategory(seeds, limits, totalMax) {
  const buckets = { amazon: [], etsy: [], blogging: [] };
  for (const seed of seeds || []) {
    if (buckets[seed.category]) buckets[seed.category].push(seed);
  }

  const selected = [];
  const usedSlugs = new Set();
  for (const category of ['amazon', 'etsy', 'blogging']) {
    for (const seed of buckets[category].slice(0, limits[category])) {
      if (usedSlugs.has(seed.slug)) continue;
      usedSlugs.add(seed.slug);
      selected.push(seed);
    }
  }

  for (const seed of seeds || []) {
    if (selected.length >= totalMax) break;
    if (usedSlugs.has(seed.slug)) continue;
    usedSlugs.add(seed.slug);
    selected.push(seed);
  }

  return selected.slice(0, totalMax);
}

function currentSeasonLabel() {
  const m = new Date().getMonth();
  if (m >= 2 && m <= 4) return 'Spring';
  if (m >= 5 && m <= 7) return 'Summer';
  if (m >= 8 && m <= 10) return 'Fall';
  return 'Winter';
}

function parseJsonArrayFromModel(text) {
  const s = String(text || '').trim();
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch {
    const m = s.match(/\[[\s\S]*\]/);
    if (!m) return null;
    try {
      return JSON.parse(m[0]);
    } catch {
      return null;
    }
  }
}

async function generateFallbackTrendIdeasWithAi() {
  if (!openaiClient) return null;
  if (process.env.TRENDS_ALLOW_AI_FALLBACK === '0') return null;
  const season = currentSeasonLabel();
  const limits = getTrendCategoryLimits();
  const amazonCount = limits.amazon;
  const etsyCount = limits.etsy;
  const blogCount = limits.blogging;

  const prompt = `You are a Pinterest affiliate strategist. Today is ${new Date().toISOString().slice(0, 10)} (${season} in the northern hemisphere).

Return ONLY a JSON array of trend objects for URL2Pin users (Amazon affiliates, Etsy sellers, bloggers). No markdown.

Each object fields:
- category: "amazon" | "etsy" | "blogging"
- title: short trend name
- badge: one of trending|rising|high_conversion|seasonal
- season: optional short label
- overview: 2 sentences, practical for Pinterest affiliates
- pinterestAngle: 1 sentence on why it works on Pinterest now
- audience: 1 sentence on who should use it
- productSearchQueries: array of 3-4 short product search phrases (amazon/etsy only; empty array for blogging)
- blogAngles: array of 3-4 blog post angle titles (blogging only; empty for others)

Create exactly ${amazonCount} amazon, ${etsyCount} etsy, ${blogCount} blogging trends. Focus on buyer intent, gifts, seasonal hooks, and clear product categories.`;

  const completion = await openaiClient.chat.completions.create({
    model: process.env.TRENDS_OPENAI_MODEL || 'gpt-4o-mini',
    temperature: 0.7,
    messages: [
      { role: 'system', content: 'Return valid JSON only.' },
      { role: 'user', content: prompt },
    ],
  });
  const raw = completion.choices?.[0]?.message?.content || '';
  const arr = parseJsonArrayFromModel(raw);
  if (!Array.isArray(arr)) return null;
  return arr;
}

function slugify(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 72);
}

async function buildTrendsFromAiIdeas(ideas) {
  const BADGES = new Set(['trending', 'rising', 'high_conversion', 'seasonal']);
  const CATEGORIES = new Set(['amazon', 'etsy', 'blogging']);
  const usedSlugs = new Set();
  const trends = [];

  for (const idea of ideas) {
    const category = String(idea?.category || '').trim();
    if (!CATEGORIES.has(category)) continue;
    const title = String(idea?.title || '').trim();
    if (title.length < 4) continue;
    let slug = slugify(title);
    if (!slug) continue;
    if (usedSlugs.has(slug)) slug = `${slug}-${usedSlugs.size + 1}`;
    usedSlugs.add(slug);

    const badge = BADGES.has(String(idea?.badge || '').trim())
      ? String(idea.badge).trim()
      : 'trending';

    const trend = {
      slug,
      category,
      title,
      badge,
      season: String(idea?.season || currentSeasonLabel()).trim() || undefined,
      overview: String(idea?.overview || '').trim(),
      pinterestAngle: String(idea?.pinterestAngle || '').trim(),
      audience: String(idea?.audience || '').trim(),
      productSearchQueries: idea?.productSearchQueries || [],
      blogAngles: idea?.blogAngles || [],
      suggestions: [],
      source: 'ai_fallback',
      evidence: {
        provider: 'openai',
        note: 'Generated when Pinterest Trends API data was unavailable.',
      },
    };

    trend.suggestions = await resolveSuggestionsForTrend(trend);
    trends.push(trend);
  }

  return trends;
}

async function buildTrendsFromPinterestSignals() {
  const region = String(process.env.TRENDS_PINTEREST_REGION || 'US').trim().toUpperCase() || 'US';
  const limitPerType = readIntEnv('TRENDS_PINTEREST_LIMIT', 20, 4, 50);
  const { trends: signals, error } = await fetchPinterestKeywordTrends({
    getAccessToken: getPinterestAccessToken,
    region,
    limitPerType,
  });
  if (!signals?.length) {
    const err = new Error(error || 'pinterest_trends_empty');
    err.code = error || 'pinterest_trends_empty';
    throw err;
  }

  const seeds = pinterestSignalsToTrendSeeds(signals);
  const limits = getTrendCategoryLimits();
  const maxTrends = readIntEnv('TRENDS_MAX_ITEMS', 36, 6, 60);
  const trends = [];
  for (const seed of selectTrendSeedsByCategory(seeds, limits, maxTrends)) {
    const trend = { ...seed, suggestions: [], source: 'pinterest_trends' };
    trend.suggestions = await resolveSuggestionsForTrend(trend);
    trends.push(trend);
  }
  if (!trends.length) throw new Error('trends_build_empty');
  return {
    generatedAt: new Date().toISOString(),
    season: currentSeasonLabel(),
    source: 'pinterest_trends',
    dataProviders: ['pinterest:trends_api', 'rapidapi:amazon_search', 'rapidapi:etsy_search'],
    trends,
  };
}

async function buildAutomatedTrends() {
  try {
    return await buildTrendsFromPinterestSignals();
  } catch (e) {
    console.warn('trendsEngine Pinterest build failed:', e?.code || e?.message || e);
    const ideas = await generateFallbackTrendIdeasWithAi();
    if (!ideas || ideas.length === 0) throw e;
    const trends = await buildTrendsFromAiIdeas(ideas);
    if (!trends.length) throw e;
    return {
      generatedAt: new Date().toISOString(),
      season: currentSeasonLabel(),
      source: 'ai_fallback',
      dataProviders: ['openai', 'rapidapi:amazon_search', 'rapidapi:etsy_search'],
      trends,
    };
  }
}

async function readCacheFromDisk() {
  try {
    const raw = await fs.readFile(CACHE_PATH, 'utf8');
    const json = JSON.parse(raw);
    if (json && Array.isArray(json.trends)) return json;
  } catch {
    /* ignore */
  }
  return null;
}

async function writeCacheToDisk(payload) {
  await fs.mkdir(path.dirname(CACHE_PATH), { recursive: true });
  await fs.writeFile(CACHE_PATH, JSON.stringify(payload, null, 2), 'utf8');
}

function cacheIsFresh(payload) {
  if (!payload?.generatedAt) return false;
  const ttlHours = Math.max(1, Number(process.env.TRENDS_CACHE_TTL_HOURS || 24) || 24);
  const ageMs = Date.now() - new Date(payload.generatedAt).getTime();
  return Number.isFinite(ageMs) && ageMs < ttlHours * 60 * 60 * 1000;
}

export function initTrendsEngine(openai, deps = {}) {
  openaiClient = openai;
  getPinterestAccessToken = deps.getPinterestAccessToken || null;
}

export async function getTrendsCatalog({ force = false } = {}) {
  if (!force && memoryCache && cacheIsFresh(memoryCache)) {
    return memoryCache;
  }
  if (!force) {
    const disk = await readCacheFromDisk();
    if (disk && cacheIsFresh(disk)) {
      memoryCache = disk;
      return disk;
    }
  }

  if (!refreshInFlight) {
    refreshInFlight = (async () => {
      try {
        const built = await buildAutomatedTrends();
        memoryCache = built;
        await writeCacheToDisk(built);
        return built;
      } catch (e) {
        const disk = await readCacheFromDisk();
        if (disk?.trends?.length) {
          memoryCache = { ...disk, stale: true };
          return memoryCache;
        }
        throw e;
      } finally {
        refreshInFlight = null;
      }
    })();
  }
  try {
    return await refreshInFlight;
  } catch (e) {
    const disk = await readCacheFromDisk();
    if (disk?.trends?.length) return { ...disk, stale: true };
    throw e;
  }
}

export async function getTrendBySlug(slug, opts = {}) {
  const s = String(slug || '').trim();
  if (!s) return null;

  if (!opts.force) {
    if (memoryCache?.trends?.length) {
      const cached = memoryCache.trends.find((t) => t.slug === s);
      if (cached) return cached;
    }
    const disk = await readCacheFromDisk();
    if (disk?.trends?.length) {
      if (!memoryCache) memoryCache = disk;
      const fromDisk = disk.trends.find((t) => t.slug === s);
      if (fromDisk) return fromDisk;
    }
  }

  const catalog = await getTrendsCatalog(opts);
  return (catalog?.trends || []).find((t) => t.slug === s) || null;
}

export function startTrendsScheduler() {
  if (process.env.TRENDS_AUTOMATION_ENABLED === '0') return;
  const refreshMs = Math.max(
    60 * 60 * 1000,
    (Number(process.env.TRENDS_REFRESH_HOURS || 24) || 24) * 60 * 60 * 1000
  );
  setTimeout(() => {
    getTrendsCatalog({ force: true }).catch((e) =>
      console.warn('trendsEngine initial refresh failed:', e?.message || e)
    );
  }, 90_000);
  setInterval(() => {
    getTrendsCatalog({ force: true }).catch((e) =>
      console.warn('trendsEngine scheduled refresh failed:', e?.message || e)
    );
  }, refreshMs);
}
