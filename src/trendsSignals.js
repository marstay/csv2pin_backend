const PINTEREST_TREND_TYPES = ['growing', 'seasonal', 'monthly'];

let warnedRapidApiKeyMissing = false;

function readIntEnv(name, fallback, min, max) {
  const n = Number(process.env[name] ?? fallback);
  const value = Number.isFinite(n) ? n : fallback;
  return Math.max(min, Math.min(max, value));
}

function slugify(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 72);
}

function extractProductImageUrl(row) {
  if (!row || typeof row !== 'object') return '';
  const direct = [
    row.product_photo,
    row.product_image,
    row.image,
    row.image_url,
    row.thumbnail,
    row.thumbnail_url,
    row.main_image,
    row.listing_image,
    row.img,
  ];
  for (const value of direct) {
    const url = String(value || '').trim();
    if (url.startsWith('http')) return url;
  }
  const nested = row.images || row.image_urls || row.product_images;
  if (Array.isArray(nested)) {
    for (const value of nested) {
      if (typeof value === 'string' && value.trim().startsWith('http')) return value.trim();
      if (value && typeof value === 'object') {
        const url = String(value.url || value.link || value.src || '').trim();
        if (url.startsWith('http')) return url;
      }
    }
  }
  return '';
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 20000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

function titleCaseKeyword(keyword) {
  return String(keyword || '')
    .trim()
    .split(/\s+/)
    .map((w) => (w ? w.charAt(0).toUpperCase() + w.slice(1) : ''))
    .join(' ');
}

export function classifyPinterestKeywordCategory(keyword) {
  const k = String(keyword || '').toLowerCase();
  if (/\b(etsy|handmade|printable|svg|crochet|knit|macrame|embroidery|sticker|wedding invite|digital download)\b/.test(k)) {
    return 'etsy';
  }
  if (/\b(recipe|workout|blog|checklist|guide|tips|ideas|meal prep|cleaning hack|outfit ideas|itinerary)\b/.test(k)) {
    return 'blogging';
  }
  if (/\b(gift|gadget|amazon|deals|under \$|prime|buy|product|organizer|tool|kit|set)\b/.test(k)) {
    return 'amazon';
  }
  if (/\b(nails|decor|furniture|kitchen|garden|beauty|skincare|shoes|dress|jewelry)\b/.test(k)) {
    return 'amazon';
  }
  return 'blogging';
}

function pickBadgeFromMetrics({ trendType, pctGrowthWow, pctGrowthMom }) {
  if (trendType === 'seasonal') return 'seasonal';
  const wow = Number(pctGrowthWow);
  const mom = Number(pctGrowthMom);
  if (Number.isFinite(wow) && wow >= 40) return 'rising';
  if (Number.isFinite(mom) && mom >= 80) return 'trending';
  if (Number.isFinite(mom) && mom >= 35) return 'high_conversion';
  return 'trending';
}

function formatGrowthLine({ pctGrowthWow, pctGrowthMom, pctGrowthYoy }) {
  const parts = [];
  const wow = Number(pctGrowthWow);
  const mom = Number(pctGrowthMom);
  const yoy = Number(pctGrowthYoy);
  if (Number.isFinite(wow)) parts.push(`${wow}% week-over-week`);
  if (Number.isFinite(mom)) parts.push(`${mom}% month-over-month`);
  if (Number.isFinite(yoy)) parts.push(`${yoy}% year-over-year`);
  return parts.join(', ');
}

export function buildTrendCopyFromSignal(signal) {
  const keyword = String(signal.keyword || '').trim();
  const growth = formatGrowthLine(signal);
  const trendType = String(signal.trendType || 'growing');
  const region = String(signal.region || 'US');
  const overview = growth
    ? `Pinterest ${region} searches for “${keyword}” are up (${growth}) in Pinterest’s ${trendType} trends feed.`
    : `“${keyword}” appears in Pinterest’s ${trendType} trends feed for ${region}.`;
  const pinterestAngle = growth
    ? `Search demand is rising on Pinterest now — pins that match “${keyword}” can ride current discovery.`
    : `Pinterest is surfacing “${keyword}” as a current search theme in ${region}.`;
  const audience =
    signal.category === 'amazon'
      ? 'Amazon affiliates who can match pins to buyer-intent products for this search theme.'
      : signal.category === 'etsy'
        ? 'Etsy sellers and handmade creators targeting this Pinterest search theme.'
        : 'Bloggers and publishers who can publish or refresh content around this Pinterest search theme.';
  return { overview, pinterestAngle, audience };
}

function buildProductSearchQueries(keyword) {
  const k = String(keyword || '').trim();
  if (!k) return [];
  const maxQueries = readIntEnv('TRENDS_SEARCH_QUERIES_PER_TREND', 4, 1, 6);
  const variants = [k, `${k} gifts`, `best ${k}`, `${k} ideas`];
  return [...new Set(variants.map((q) => q.trim()).filter(Boolean))].slice(0, maxQueries);
}

function buildBlogAngles(keyword) {
  const k = String(keyword || '').trim();
  if (!k) return [];
  const maxAngles = readIntEnv('TRENDS_BLOG_ANGLES_MAX', 6, 1, 10);
  const title = titleCaseKeyword(k);
  const variants = [
    title,
    `${title}: ideas and inspiration`,
    `Best ${title} tips`,
    `${title} for beginners`,
    `${title} checklist`,
    `${title} mistakes to avoid`,
  ];
  return [...new Set(variants.map((q) => q.trim()).filter(Boolean))].slice(0, maxAngles);
}

export async function fetchPinterestKeywordTrends({
  getAccessToken,
  region = 'US',
  trendTypes = PINTEREST_TREND_TYPES,
  limitPerType = 12,
} = {}) {
  const accessToken = typeof getAccessToken === 'function' ? await getAccessToken() : null;
  if (!accessToken) return { trends: [], error: 'pinterest_token_missing' };

  const reg = String(region || 'US').trim().toUpperCase();
  const limit = Math.max(1, Math.min(50, Number(limitPerType) || 12));
  const out = [];
  const seen = new Set();

  for (const trendType of trendTypes) {
    const url =
      `https://api.pinterest.com/v5/trends/keywords/${encodeURIComponent(reg)}/top/` +
      `${encodeURIComponent(trendType)}?limit=${limit}`;
    try {
      const resp = await fetchWithTimeout(
        url,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            Accept: 'application/json',
          },
        },
        25000
      );
      if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        console.warn('Pinterest trends API:', trendType, resp.status, body.slice(0, 240));
        continue;
      }
      const json = await resp.json().catch(() => null);
      const rows = Array.isArray(json?.trends) ? json.trends : [];
      for (const row of rows) {
        const keyword = String(row?.keyword || '').trim();
        if (!keyword) continue;
        const key = keyword.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        const category = classifyPinterestKeywordCategory(keyword);
        out.push({
          keyword,
          category,
          trendType,
          region: reg,
          pctGrowthWow: row?.pct_growth_wow ?? null,
          pctGrowthMom: row?.pct_growth_mom ?? null,
          pctGrowthYoy: row?.pct_growth_yoy ?? null,
          timeSeries: row?.time_series && typeof row.time_series === 'object' ? row.time_series : null,
          badge: pickBadgeFromMetrics({
            trendType,
            pctGrowthWow: row?.pct_growth_wow,
            pctGrowthMom: row?.pct_growth_mom,
          }),
        });
      }
    } catch (e) {
      console.warn('fetchPinterestKeywordTrends:', trendType, e?.message || e);
    }
  }

  return { trends: out, error: out.length ? null : 'pinterest_trends_empty' };
}

export function pinterestSignalsToTrendSeeds(signals) {
  const usedSlugs = new Set();
  return (signals || []).map((signal) => {
    const title = titleCaseKeyword(signal.keyword);
    let slug = slugify(title);
    if (!slug) slug = `trend-${usedSlugs.size + 1}`;
    if (usedSlugs.has(slug)) slug = `${slug}-${usedSlugs.size + 1}`;
    usedSlugs.add(slug);
    const copy = buildTrendCopyFromSignal(signal);
    return {
      slug,
      category: signal.category,
      title,
      badge: signal.badge || 'trending',
      season: signal.trendType === 'seasonal' ? 'Seasonal' : undefined,
      overview: copy.overview,
      pinterestAngle: copy.pinterestAngle,
      audience: copy.audience,
      productSearchQueries:
        signal.category === 'blogging' ? [] : buildProductSearchQueries(signal.keyword),
      blogAngles: signal.category === 'blogging' ? buildBlogAngles(signal.keyword) : [],
      evidence: {
        provider: 'pinterest',
        api: 'trends/keywords/top',
        region: signal.region,
        trendType: signal.trendType,
        keyword: signal.keyword,
        pctGrowthWow: signal.pctGrowthWow,
        pctGrowthMom: signal.pctGrowthMom,
        pctGrowthYoy: signal.pctGrowthYoy,
        timeSeries: signal.timeSeries || null,
      },
    };
  });
}

export async function fetchAmazonSearchProducts(query, limit = 3) {
  const key = String(process.env.RAPIDAPI_KEY || '').trim();
  const host =
    String(process.env.RAPIDAPI_AMAZON_HOST || '').trim() ||
    'real-time-amazon-data-the-most-complete.p.rapidapi.com';
  if (!key || !query) {
    if (!key && !warnedRapidApiKeyMissing) {
      warnedRapidApiKeyMissing = true;
      console.warn(
        'trendsSignals: RAPIDAPI_KEY is unset — product images and rich listings need RapidAPI on the server; rebuild trends after setting it.'
      );
    }
    return [];
  }

  const paths = [
    `/search?query=${encodeURIComponent(query)}&page=1&country=US`,
    `/product-search?query=${encodeURIComponent(query)}&page=1&country=US`,
  ];

  for (const p of paths) {
    try {
      const resp = await fetchWithTimeout(
        `https://${host}${p}`,
        {
          headers: {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host,
            Accept: 'application/json',
          },
        },
        20000
      );
      if (!resp.ok) continue;
      const json = await resp.json().catch(() => null);
      const bucket =
        json?.data?.products ||
        json?.data?.results ||
        json?.data?.items ||
        json?.products ||
        json?.results ||
        [];
      if (!Array.isArray(bucket) || bucket.length === 0) continue;
      const out = [];
      for (const row of bucket) {
        if (!row || typeof row !== 'object') continue;
        const asin = String(row.asin || row.ASIN || '').trim();
        const title = String(row.title || row.product_title || row.name || '').trim();
        const url = String(row.product_url || row.url || row.link || '').trim();
        const built = asin ? `https://www.amazon.com/dp/${asin}` : url;
        if (!title || !built) continue;
        const imageUrl = extractProductImageUrl(row);
        out.push({
          id: asin || slugify(title) || `p-${out.length}`,
          title: title.slice(0, 120),
          url: built,
          ...(imageUrl ? { imageUrl } : {}),
        });
        if (out.length >= limit) break;
      }
      if (out.length) return out;
    } catch (e) {
      console.warn('trendsSignals Amazon search:', e?.message || e);
    }
  }
  return [];
}

export async function fetchEtsySearchProducts(query, limit = 3) {
  const key = String(process.env.RAPIDAPI_KEY || '').trim();
  const host = String(process.env.RAPIDAPI_ETSY_HOST || '').trim() || 'etsy-api2.p.rapidapi.com';
  if (!key || !query) {
    if (!key && !warnedRapidApiKeyMissing) {
      warnedRapidApiKeyMissing = true;
      console.warn(
        'trendsSignals: RAPIDAPI_KEY is unset — product images and rich listings need RapidAPI on the server; rebuild trends after setting it.'
      );
    }
    return [];
  }

  const paths = [
    `/search?query=${encodeURIComponent(query)}`,
    `/search?search_term=${encodeURIComponent(query)}`,
  ];

  for (const p of paths) {
    try {
      const resp = await fetchWithTimeout(
        `https://${host}${p}`,
        {
          headers: {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host,
            Accept: 'application/json',
          },
        },
        20000
      );
      if (!resp.ok) continue;
      const json = await resp.json().catch(() => null);
      const bucket = json?.data?.results || json?.data?.items || json?.results || json?.items || [];
      if (!Array.isArray(bucket) || bucket.length === 0) continue;
      const out = [];
      for (const row of bucket) {
        if (!row || typeof row !== 'object') continue;
        const listingId = String(row.listing_id || row.listingId || row.id || '').trim();
        const title = String(row.title || row.name || '').trim();
        const url =
          String(row.url || row.listing_url || '').trim() ||
          (listingId ? `https://www.etsy.com/listing/${listingId}` : '');
        if (!title || !url) continue;
        const imageUrl = extractProductImageUrl(row);
        out.push({
          id: listingId || slugify(title) || `e-${out.length}`,
          title: title.slice(0, 120),
          url,
          ...(imageUrl ? { imageUrl } : {}),
        });
        if (out.length >= limit) break;
      }
      if (out.length) return out;
    } catch (e) {
      console.warn('trendsSignals Etsy search:', e?.message || e);
    }
  }
  return [];
}

export async function resolveSuggestionsForTrend(trend) {
  const cat = trend.category;
  const maxQueries = readIntEnv('TRENDS_SEARCH_QUERIES_PER_TREND', 4, 1, 6);
  const queries = Array.isArray(trend.productSearchQueries)
    ? trend.productSearchQueries.map((q) => String(q || '').trim()).filter(Boolean).slice(0, maxQueries)
    : [];

  if (cat === 'amazon') {
    const maxProducts = readIntEnv('TRENDS_AMAZON_PRODUCTS_MAX', 10, 3, 16);
    const perQuery = readIntEnv('TRENDS_AMAZON_PRODUCTS_PER_QUERY', 5, 2, 8);
    const suggestions = [];
    for (const q of queries) {
      const rows = await fetchAmazonSearchProducts(q, perQuery);
      suggestions.push(...rows);
      if (suggestions.length >= maxProducts) break;
    }
    if (suggestions.length) return suggestions.slice(0, maxProducts);
    if (queries.length) {
      return queries.map((q, i) => ({
        id: `amazon-search-${i}`,
        title: q,
        url: `https://www.amazon.com/s?k=${encodeURIComponent(q)}`,
        note:
          'No product thumbnails returned (check RAPIDAPI_KEY and your Amazon RapidAPI subscription on the server). Open Amazon search and pick a product URL for URL2Pin.',
      }));
    }
    return [];
  }

  if (cat === 'etsy') {
    const maxProducts = readIntEnv('TRENDS_ETSY_PRODUCTS_MAX', 8, 3, 14);
    const perQuery = readIntEnv('TRENDS_ETSY_PRODUCTS_PER_QUERY', 4, 2, 8);
    const suggestions = [];
    for (const q of queries) {
      const rows = await fetchEtsySearchProducts(q, perQuery);
      suggestions.push(...rows);
      if (suggestions.length >= maxProducts) break;
    }
    if (suggestions.length) return suggestions.slice(0, maxProducts);
    return queries.map((q, i) => ({
      id: `etsy-search-${i}`,
      title: q,
      url: `https://www.etsy.com/search?q=${encodeURIComponent(q)}`,
      note: 'Etsy search results for this Pinterest trend keyword.',
    }));
  }

  const maxAngles = readIntEnv('TRENDS_BLOG_ANGLES_MAX', 6, 2, 10);
  const angles = Array.isArray(trend.blogAngles)
    ? trend.blogAngles.map((a) => String(a || '').trim()).filter(Boolean).slice(0, maxAngles)
    : [];
  return angles.map((title, i) => ({
    id: `blog-angle-${i}`,
    title,
    url: '',
    note: 'Publish or update a post targeting this Pinterest search theme, then paste the URL into URL2Pin.',
  }));
}
