/**
 * Winning Product Finder — Pinterest opportunity scoring for Amazon affiliate products.
 *
 * Combines Amazon product signals (RapidAPI), Pinterest Trends API matches,
 * keyword expansion heuristics, and Amazon search saturation proxies.
 */
import fetch from 'node-fetch';
import { fetchPinterestKeywordTrends, fetchPinterestTrendsForProductSeeds, fetchAmazonSearchProducts } from './trendsSignals.js';
import { fetchPinterestSearchSnapshot } from './pinterestSearchSnapshot.js';
import { fetchRapidApiSuggestions } from './pinterestRapidApi.js';

const PINTEREST_TYPEAHEAD_URL =
  'https://www.pinterest.com/resource/AdvancedTypeaheadResource/get/';

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

// --- Product normalization ---

function parsePriceUsd(raw) {
  const s = String(raw || '').replace(/,/g, '');
  const m = s.match(/[\d.]+/);
  const n = m ? Number(m[0]) : NaN;
  return Number.isFinite(n) ? n : null;
}

function pickMainImage(data) {
  const images = Array.isArray(data?.images) ? data.images : [];
  for (const im of images) {
    if (!im) continue;
    if (typeof im === 'string' && im.startsWith('http')) return im;
    if (typeof im === 'object') {
      const url = im.hi_res || im.large || im.image || '';
      if (url) return String(url);
    }
  }
  return '';
}

const CATEGORY_RULES = [
  {
    id: 'bathroom',
    label: 'Bathroom',
    patterns:
      /\b(bathroom|bath mat|shower curtain|shower head|shower caddy|bathtub|toilet|vanity|faucet|towel rack|towel bar|washroom|restroom|soap dispenser|bath accessories)\b/i,
    visualBase: 16,
  },
  {
    id: 'home_decor',
    label: 'Home Decor',
    patterns:
      /\b(decor|blanket|throw|pillow|cushion|vase|lamp|rug|curtain|wall art|shelf|organizer basket|cozy home|living room|bedroom decor)\b/i,
    visualBase: 17,
  },
  {
    id: 'kitchen',
    label: 'Kitchen',
    patterns:
      /\b(kitchen(?!ware department)|cookware|air fryer|blender|utensil|cutting board|coffee maker|toaster|knife set|meal prep)\b/i,
    visualBase: 16,
  },
  { id: 'beauty', label: 'Beauty', patterns: /\b(beauty|skincare|makeup|serum|moisturizer|shampoo|hair dryer|curling|nail|lipstick|foundation|self care)\b/i, visualBase: 18 },
  { id: 'fashion', label: 'Fashion', patterns: /\b(dress|shoes|sneaker|jewelry|necklace|bracelet|earring|handbag|purse|outfit|wardrobe|sunglasses)\b/i, visualBase: 17 },
  { id: 'office', label: 'Office Setup', patterns: /\b(office|desk|monitor|keyboard|mouse pad|ergonomic|workspace|standing desk|planner|notebook)\b/i, visualBase: 14 },
  { id: 'gifts', label: 'Gifts', patterns: /\b(gift|birthday present|wedding|stocking stuffer|gift guide|for her|for him|for mom|for dad)\b/i, visualBase: 16 },
  { id: 'fitness', label: 'Fitness', patterns: /\b(workout|yoga|dumbbell|resistance band|fitness|gym|activewear|leggings)\b/i, visualBase: 15 },
  { id: 'garden', label: 'Garden & Outdoor', patterns: /\b(garden|planter|outdoor|patio|grill|lawn|backyard)\b/i, visualBase: 15 },
];

/** Amazon top-level departments that should not alone assign a niche category. */
const AMAZON_DEPARTMENT_NOISE = /\b(home\s*&\s*kitchen|tools\s*&\s*home improvement|patio,\s*lawn\s*&\s*garden)\b/i;

export function inferPinterestCategory(title, amazonCategories = []) {
  const titleHay = String(title || '').toLowerCase();
  const cats = (amazonCategories || []).map((c) => String(c || '').trim()).filter(Boolean);
  const leaf = cats.length ? cats[cats.length - 1].toLowerCase() : '';
  const parent = cats.length > 1 ? cats[cats.length - 2].toLowerCase() : '';

  let best = null;
  let bestScore = 0;
  let matchedFrom = 'no strong keyword match';

  for (const rule of CATEGORY_RULES) {
    let score = 0;
    let from = '';

    if (rule.patterns.test(titleHay)) {
      score += 10;
      from = 'product title';
    }
    if (leaf && rule.patterns.test(leaf)) {
      score += 8;
      from = from || `Amazon category “${cats[cats.length - 1]}”`;
    }
    if (parent && rule.patterns.test(parent)) {
      score += 4;
      from = from || `Amazon subcategory “${cats[cats.length - 2]}”`;
    }

    for (let i = 0; i < Math.max(0, cats.length - 2); i++) {
      const node = cats[i].toLowerCase();
      if (
        rule.id === 'kitchen' &&
        AMAZON_DEPARTMENT_NOISE.test(node) &&
        !rule.patterns.test(titleHay) &&
        !rule.patterns.test(leaf) &&
        !rule.patterns.test(parent)
      ) {
        continue;
      }
      if (rule.patterns.test(node)) score += 1;
    }

    if (score > bestScore) {
      bestScore = score;
      best = rule;
      matchedFrom = from || 'Amazon browse path';
    }
  }

  if (best && bestScore > 0) {
    return {
      id: best.id,
      label: best.label,
      visualBase: best.visualBase,
      matchedFrom,
      confidence: bestScore >= 8 ? 'high' : 'medium',
    };
  }

  return {
    id: 'general',
    label: 'General Product',
    visualBase: 10,
    matchedFrom: 'no strong keyword match',
    confidence: 'low',
  };
}

const LOW_VISUAL_PATTERNS =
  /\b(cable|adapter|charger|replacement part|filter cartridge|ink cartridge|refill|usb hub|extension cord|battery pack|screw|bracket|mount only)\b/i;

const AESTHETIC_PATTERNS = /\b(aesthetic|cute|cozy|modern|minimalist|luxury|boho|farmhouse|scandinavian|elegant|stylish)\b/i;

export function normalizeAmazonProduct(data, url = '') {
  if (!data || typeof data !== 'object') return null;
  const title = String(data.title || '').trim();
  if (!title) return null;

  const amazonCategories = Array.isArray(data.category_path)
    ? data.category_path.map((c) => (c && c.name ? String(c.name).trim() : '')).filter(Boolean)
    : [];

  const category = inferPinterestCategory(title, amazonCategories);
  const imageUrl = pickMainImage(data);
  const imageCount = Array.isArray(data.images) ? data.images.length : imageUrl ? 1 : 0;
  const priceRaw = String(data.price || data.product_price || '').trim();
  const rating = Number(data.rating ?? data.product_star_rating);
  const reviewCount = Number(data.reviews_count ?? data.product_num_ratings ?? 0);

  return {
    title,
    imageUrl,
    category: category.label,
    categoryId: category.id,
    categoryMatchedFrom: category.matchedFrom,
    categoryConfidence: category.confidence,
    amazonCategories,
    priceUsd: parsePriceUsd(priceRaw),
    rating: Number.isFinite(rating) && rating > 0 ? rating : null,
    reviewCount: Number.isFinite(reviewCount) && reviewCount > 0 ? reviewCount : 0,
    asin: String(data.asin || '').trim() || null,
    url: String(url || '').trim(),
    imageCount,
    price: priceRaw || null,
    isBestSeller: Boolean(data.is_best_seller),
  };
}

// --- Keyword extraction ---

const STOP_WORDS = new Set([
  'the', 'a', 'an', 'and', 'or', 'for', 'with', 'without', 'of', 'in', 'on', 'to', 'from', 'by',
  'new', 'pack', 'set', 'kit', 'piece', 'count', 'size', 'color', 'black', 'white', 'blue', 'red',
  'inch', 'inches', 'oz', 'lb', 'lbs', 'mm', 'cm', 'xl', 'large', 'small', 'medium',
  'amazon', 'brand', 'premium', 'professional', 'portable', 'wireless', 'bluetooth', 'usb', 'hd',
]);

const COLOR_WORDS = new Set([
  'sage', 'green', 'blue', 'red', 'white', 'black', 'gray', 'grey', 'beige', 'pink', 'navy', 'brown',
  'cream', 'ivory', 'charcoal', 'teal', 'coral', 'gold', 'silver', 'multicolor', 'tan', 'burgundy',
  'purple', 'orange', 'yellow', 'lavender', 'mint', 'blush', 'nude', 'bronze',
]);

const PRODUCT_TYPE_WORDS = new Set([
  'bathroom', 'bath', 'rug', 'rugs', 'mat', 'mats', 'shower', 'curtain', 'towel', 'vanity', 'toilet',
  'kitchen', 'air', 'fryer', 'blender', 'cookware', 'utensil', 'organizer', 'decor', 'blanket',
  'pillow', 'cushion', 'lamp', 'furniture', 'beauty', 'skincare', 'makeup', 'shampoo', 'dress',
  'shoes', 'sneaker', 'jewelry', 'necklace', 'handbag', 'office', 'desk', 'monitor', 'gift', 'gifts',
  'workout', 'yoga', 'dumbbell', 'garden', 'planter', 'outdoor', 'patio', 'grill', 'gadget', 'storage',
  'shelf', 'rack', 'holder', 'basket', 'bin', 'hanger', 'hook', 'brush', 'soap', 'dispenser',
]);

function singularizePhrase(phrase) {
  return String(phrase || '')
    .split(/\s+/)
    .map((w) => {
      if (w.endsWith('ies') && w.length > 4) return `${w.slice(0, -3)}y`;
      if (w.endsWith('s') && !w.endsWith('ss') && w.length > 3) return w.slice(0, -1);
      return w;
    })
    .join(' ');
}

function isProductTypeWord(word) {
  const w = String(word || '').toLowerCase();
  if (PRODUCT_TYPE_WORDS.has(w)) return true;
  return CATEGORY_RULES.some((rule) => rule.patterns.test(w));
}

function tokenizeTitle(title) {
  const raw = String(title || '')
    .replace(/[®™©]/g, '')
    .replace(/\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/[^\w\s'-]/g, ' ')
    .toLowerCase();
  return raw
    .split(/\s+/)
    .filter((w) => w.length > 2 && !STOP_WORDS.has(w) && !COLOR_WORDS.has(w) && !/^\d+$/.test(w));
}

/**
 * Derive generic Pinterest/Amazon search terms — strips brand names and colors.
 * e.g. "Versailtex Sage Green Bathroom Rug" → primary "bath rug" (from Amazon category)
 */
export function derivePinterestSearchTerms(title, categoryId = 'general', amazonCategories = []) {
  const leafRaw = amazonCategories?.length ? String(amazonCategories[amazonCategories.length - 1]).trim() : '';
  const leafPhrase = singularizePhrase(
    leafRaw.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim()
  );

  let words = tokenizeTitle(title);
  while (words.length > 1 && !isProductTypeWord(words[0])) {
    words = words.slice(1);
  }

  const candidates = [];
  if (leafPhrase.length >= 4) candidates.push({ q: leafPhrase, score: 12 });

  for (let i = 0; i < words.length - 1; i++) {
    const bg = `${words[i]} ${words[i + 1]}`;
    if (isProductTypeWord(words[i]) || isProductTypeWord(words[i + 1])) {
      candidates.push({ q: bg, score: 9 });
    }
  }
  if (words.length >= 2) candidates.push({ q: words.slice(0, 2).join(' '), score: 7 });
  if (words.length >= 3) candidates.push({ q: words.slice(0, 3).join(' '), score: 6 });

  const catDefaults = {
    bathroom: 'bathroom rug',
    home_decor: 'home decor',
    kitchen: 'kitchen gadget',
    beauty: 'beauty product',
    fashion: 'outfit ideas',
    gifts: 'gift ideas',
    office: 'desk setup',
    fitness: 'workout gear',
    garden: 'garden decor',
  };
  if (catDefaults[categoryId]) candidates.push({ q: catDefaults[categoryId], score: 5 });

  candidates.sort((a, b) => b.score - a.score);
  const seen = new Set();
  const ranked = [];
  for (const c of candidates) {
    const k = c.q.toLowerCase();
    if (seen.has(k) || k.length < 3) continue;
    seen.add(k);
    ranked.push(c.q);
  }

  const primary = ranked[0] || leafPhrase || words.slice(0, 2).join(' ') || 'product';
  return { primary, seeds: ranked.slice(0, 8) };
}

export function extractProductSeeds(title) {
  const { seeds } = derivePinterestSearchTerms(title);
  return seeds.length ? seeds : tokenizeTitle(title).slice(0, 8);
}

function filterRelevantSuggestions(suggestions, searchQuery, categoryId) {
  const queryWords = new Set(
    `${searchQuery} ${categoryId}`.toLowerCase().split(/\s+/).filter((w) => w.length > 2)
  );
  const relevant = (suggestions || []).filter((s) => {
    const words = String(s).toLowerCase().split(/\s+/);
    return words.some((w) => queryWords.has(w) || [...queryWords].some((q) => w.includes(q) || q.includes(w)));
  });
  return relevant.length >= 2 ? relevant : (suggestions || []).slice(0, 8);
}

export function expandPinterestKeywords(seeds, categoryId = 'general') {
  const out = new Set();
  const year = new Date().getFullYear();
  const modifiers = [
    'ideas',
    'aesthetic',
    'gift idea',
    'amazon find',
    'must have',
    'home upgrade',
    'budget friendly',
    'small space',
    'before and after',
    'best',
    `best ${year}`,
    'for beginners',
    'under $50',
    'cozy',
    'modern',
  ];

  const categoryMods = {
    home_decor: ['living room decor', 'cozy home ideas', 'room makeover', 'home inspo'],
    kitchen: ['kitchen gadgets', 'kitchen organization', 'meal prep ideas'],
    beauty: ['beauty finds', 'skincare routine', 'hair care tips'],
    fashion: ['outfit ideas', 'style inspo', 'wardrobe essentials'],
    gifts: ['gift guide', 'gift ideas', 'presents under $50'],
    office: ['desk setup', 'workspace inspo', 'office organization'],
    fitness: ['workout essentials', 'home gym'],
    garden: ['garden ideas', 'outdoor living'],
    bathroom: ['bathroom organization', 'bathroom ideas', 'small bathroom'],
  };

  for (const seed of seeds) {
    out.add(seed);
    for (const m of modifiers) out.add(`${seed} ${m}`);
    for (const m of categoryMods[categoryId] || []) out.add(`${m} ${seed}`.trim());
  }

  return [...out].map((k) => k.trim()).filter(Boolean).slice(0, 40);
}

// --- Pinterest search suggestions (RapidAPI; official typeahead is blocked server-side) ---

async function fetchPinterestSearchSuggestions(query) {
  const term = String(query || '').trim();
  if (term.length < 2) return [];

  const rapid = await fetchRapidApiSuggestions(term);
  if (rapid.length > 0) return rapid;

  // Legacy typeahead — usually blocked (403) but kept as last resort
  return fetchPinterestTypeaheadSuggestions(term);
}

async function fetchPinterestTypeaheadSuggestions(query) {
  const term = String(query || '').trim();
  if (term.length < 2) return [];

  const dataPayload = JSON.stringify({
    options: {
      term,
      term_meta: [],
      pin_scope: 'pins',
      auto_correction_disabled: '',
      search_type: 'pins',
    },
  });

  const url = `${PINTEREST_TYPEAHEAD_URL}?source_url=${encodeURIComponent('/search/pins/')}&data=${encodeURIComponent(dataPayload)}`;

  try {
    const resp = await fetchWithTimeout(url, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        Accept: 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Pinterest-AppState': 'active',
      },
    }, 8000);
    if (!resp.ok) return [];
    const json = await resp.json().catch(() => null);
    const items = json?.resource_response?.data?.items;
    if (!Array.isArray(items)) return [];
    return items
      .map((item) => {
        if (typeof item === 'string') return item.trim();
        return String(item?.label || item?.query || item?.display || '').trim();
      })
      .filter(Boolean)
      .slice(0, 12);
  } catch {
    return [];
  }
}

function keywordOverlap(a, b) {
  const aw = new Set(String(a).toLowerCase().split(/\s+/).filter((w) => w.length > 2));
  const bw = String(b).toLowerCase().split(/\s+/).filter((w) => w.length > 2);
  if (!aw.size || !bw.length) return 0;
  let hits = 0;
  for (const w of bw) if (aw.has(w)) hits++;
  return hits / Math.max(aw.size, bw.length);
}

function findTrendMatches(seeds, trends) {
  const matches = [];
  for (const trend of trends || []) {
    const kw = String(trend.keyword || '').toLowerCase();
    if (!kw) continue;
    for (const seed of seeds) {
      const seedL = seed.toLowerCase();
      if (kw.includes(seedL) || seedL.includes(kw) || keywordOverlap(seed, kw) >= 0.4) {
        matches.push({ seed, trend });
        break;
      }
    }
  }
  return matches;
}

// --- Scoring ---

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function scoreToLevel(score, max, labels = ['Low', 'Medium', 'High', 'Excellent']) {
  const pct = max > 0 ? score / max : 0;
  if (pct >= 0.8) return labels[3] || 'Excellent';
  if (pct >= 0.55) return labels[2] || 'High';
  if (pct >= 0.35) return labels[1] || 'Medium';
  return labels[0] || 'Low';
}


function scoreDemand({ trendMatches, typeaheadSuggestions, seasonalHits, pinterestSnapshot }) {
  let score = 0;

  if (trendMatches.length > 0) {
    const bestGrowth = trendMatches.reduce((best, m) => {
      const mom = Number(m.trend?.pctGrowthMom);
      const wow = Number(m.trend?.pctGrowthWow);
      const g = Number.isFinite(mom) ? mom : Number.isFinite(wow) ? wow : 0;
      return Math.max(best, g);
    }, 0);
    score += clamp(6 + Math.log10(Math.max(bestGrowth, 1)) * 3, 6, 18);
    score += clamp(trendMatches.length * 1.5, 0, 6);
  }

  const suggestionCount = typeaheadSuggestions.length;
  if (suggestionCount >= 6) score += 6;
  else if (suggestionCount >= 3) score += 4;
  else if (suggestionCount >= 1) score += 2;

  const pinCount = pinterestSnapshot?.stats?.pinsInSample || 0;
  if (pinCount >= 25) score += 5;
  else if (pinCount >= 12) score += 3;
  else if (pinCount >= 5) score += 1;

  if (seasonalHits > 0) score += clamp(seasonalHits * 2, 2, 4);

  return clamp(Math.round(score), 0, 30);
}

function scoreCompetition({ amazonSearchCount, trendMatches, typeaheadSuggestions, pinterestSnapshot }) {
  let saturation = 0;

  if (pinterestSnapshot?.available && pinterestSnapshot.stats?.competitionLevel) {
    const level = pinterestSnapshot.stats.competitionLevel;
    if (level === 'High') saturation += 14;
    else if (level === 'Medium') saturation += 8;
    else saturation += 3;
  } else {
    saturation += clamp(Math.round(amazonSearchCount * 0.7), 0, 12);
  }

  saturation += clamp(trendMatches.length * 1.0, 0, 4);
  saturation += clamp(typeaheadSuggestions.length * 0.35, 0, 3);

  const favorability = 20 - clamp(Math.round(saturation), 0, 18);
  return clamp(favorability, 2, 20);
}

function scoreVisualAppeal({ title, categoryId, imageCount }) {
  const cat = CATEGORY_RULES.find((c) => c.id === categoryId) || { visualBase: 10 };
  let score = cat.visualBase;

  if (imageCount >= 5) score += 2;
  else if (imageCount >= 3) score += 1;

  if (AESTHETIC_PATTERNS.test(title)) score += 2;
  if (LOW_VISUAL_PATTERNS.test(title)) score -= 6;

  return clamp(Math.round(score), 0, 20);
}

function scoreSocialProof({ rating, reviewCount, isBestSeller }) {
  let score = 0;
  if (rating != null) {
    if (rating >= 4.6) score += 6;
    else if (rating >= 4.4) score += 5;
    else if (rating >= 4.0) score += 3;
    else if (rating >= 3.5) score += 1;
  }
  if (reviewCount >= 5000) score += 6;
  else if (reviewCount >= 1000) score += 5;
  else if (reviewCount >= 200) score += 3;
  else if (reviewCount >= 50) score += 2;
  else if (reviewCount > 0) score += 1;

  if (isBestSeller) score += 3;
  return clamp(score, 0, 15);
}

function scoreCommercial({ priceUsd, categoryId }) {
  let score = 0;
  if (priceUsd != null) {
    if (priceUsd >= 12 && priceUsd <= 45) score += 8;
    else if (priceUsd >= 8 && priceUsd <= 70) score += 6;
    else if (priceUsd >= 5 && priceUsd <= 120) score += 4;
    else score += 2;
  } else {
    score += 4;
  }

  const impulseCategories = new Set(['gifts', 'kitchen', 'beauty', 'home_decor']);
  if (impulseCategories.has(categoryId)) score += 4;
  else score += 2;

  // Affiliate-friendly price band bonus
  if (priceUsd != null && priceUsd >= 20 && priceUsd <= 80) score += 3;

  return clamp(score, 0, 15);
}

function overallBand(score) {
  if (score >= 81) return 'Strong fit';
  if (score >= 61) return 'Good fit';
  if (score >= 41) return 'Mixed';
  return 'Weak fit';
}

function recommendVerdict(score) {
  if (score >= 81) {
    return {
      verdict: 'Worth pinning',
      note: 'Strong search interest, good visuals, and solid buyer reviews — this product is a good candidate for Pinterest pins.',
    };
  }
  if (score >= 61) {
    return {
      verdict: 'Good candidate',
      note: 'Overall signals look solid. Create a few pins and post them to 2+ relevant boards.',
    };
  }
  if (score >= 41) {
    return {
      verdict: 'Proceed with caution',
      note: 'Some signals are weak. Consider pinning this inside a roundup or review post rather than as a standalone product pin.',
    };
  }
  return {
    verdict: 'Probably skip',
    note: 'Low search interest or weak visual appeal. You may get better results linking to a blog review or gift guide instead.',
  };
}

function buildScoreEvidence({
  product,
  searchQuery,
  trendMatches,
  amazonSearchResults,
  typeaheadSuggestions,
  competitionLevel,
  pinterestSnapshot,
  userPinHistory,
}) {
  const items = [];

  const productParts = [];
  if (product.rating != null && product.reviewCount > 0) {
    productParts.push(`${product.rating}★ (${product.reviewCount.toLocaleString()} reviews)`);
  } else if (product.reviewCount > 0) {
    productParts.push(`${product.reviewCount.toLocaleString()} reviews`);
  }
  if (product.price) productParts.push(product.price);
  if (product.isBestSeller) productParts.push('Amazon Best Seller');
  items.push({
    text: `${productParts.join(' · ')} — ${product.category} product`,
  });

  if (trendMatches.length > 0) {
    const top = trendMatches[0].trend;
    const mom = Number(top?.pctGrowthMom);
    const wow = Number(top?.pctGrowthWow);
    let growth = '';
    if (Number.isFinite(mom) && mom > 0) growth = `, up ${mom}% this month`;
    else if (Number.isFinite(wow) && wow > 0) growth = `, up ${wow}% this week`;
    items.push({ text: `Rising on Pinterest: “${top.keyword}”${growth}` });
  } else if (typeaheadSuggestions.length >= 2) {
    items.push({
      text: `People search Pinterest for: ${typeaheadSuggestions.slice(0, 3).join(', ')}`,
    });
  } else if (pinterestSnapshot?.available && (pinterestSnapshot.stats?.pinsInSample || 0) >= 8) {
    items.push({
      text: `“${searchQuery}” is an active Pinterest search (${pinterestSnapshot.stats.pinsInSample} top pins found)`,
    });
  } else {
    items.push({
      text: `Not trending on Pinterest right now for “${searchQuery}” — can still work with strong pin visuals`,
    });
  }

  const compParts = [];
  if (pinterestSnapshot?.available && pinterestSnapshot.stats) {
    compParts.push(
      `${pinterestSnapshot.stats.pinsInSample} pins rank for “${searchQuery}” (${competitionLevel.toLowerCase()} on Pinterest)`
    );
  }
  if (amazonSearchResults.length >= 15) {
    compParts.push('many similar listings on Amazon');
  } else if (amazonSearchResults.length >= 6) {
    compParts.push('some competition on Amazon');
  } else if (amazonSearchResults.length > 0) {
    compParts.push('few direct competitors on Amazon');
  }
  if (compParts.length) {
    const line = compParts.join(' · ');
    items.push({ text: line.charAt(0).toUpperCase() + line.slice(1) });
  }

  if (userPinHistory?.pinsPosted > 0) {
    const stats = [];
    if (userPinHistory.impressions > 0) stats.push(`${userPinHistory.impressions.toLocaleString()} impressions`);
    if (userPinHistory.outboundClicks > 0) stats.push(`${userPinHistory.outboundClicks.toLocaleString()} clicks`);
    if (userPinHistory.saves > 0) stats.push(`${userPinHistory.saves.toLocaleString()} saves`);
    items.push({
      text:
        stats.length > 0
          ? `Your ${userPinHistory.pinsPosted} posted pin(s) for this product: ${stats.join(', ')}`
          : `You've posted ${userPinHistory.pinsPosted} pin(s) for this product already`,
    });
  }

  return items;
}

const BOARD_TEMPLATES = {
  bathroom: ['Bathroom Organization', 'Bathroom Ideas', 'Small Bathroom Ideas', 'Bathroom Decor'],
  home_decor: ['Living Room Decor', 'Cozy Home Ideas', 'Modern Home Decor', 'Home Organization'],
  kitchen: ['Kitchen Gadgets', 'Kitchen Organization', 'Meal Prep Ideas', 'Amazon Kitchen Finds'],
  beauty: ['Beauty Finds', 'Skincare Routine', 'Hair Care Tips', 'Self Care Ideas'],
  fashion: ['Outfit Ideas', 'Style Inspiration', 'Fashion Finds', 'Wardrobe Essentials'],
  office: ['Desk Setup Ideas', 'Home Office Inspo', 'Workspace Organization', 'Productivity Setup'],
  gifts: ['Gift Ideas', 'Gifts Under $50', 'Birthday Gift Ideas', 'Amazon Gift Guide'],
  fitness: ['Workout Essentials', 'Home Gym Ideas', 'Fitness Motivation', 'Active Lifestyle'],
  garden: ['Garden Ideas', 'Outdoor Living', 'Patio Decor', 'Backyard Inspiration'],
  general: ['Amazon Finds', 'Must Have Products', 'Shopping Inspo', 'Best Products'],
};

// --- Main analyzer ---

export async function analyzeWinningProduct(product, { getPinterestAccessToken, userPinHistory } = {}) {
  if (!product || !product.title) {
    throw new Error('Invalid product data');
  }
  const { primary: searchQuery, seeds } = derivePinterestSearchTerms(
    product.title,
    product.categoryId,
    product.amazonCategories
  );
  const expandedKeywords = expandPinterestKeywords(seeds, product.categoryId);
  const suggestionSeed = searchQuery.split(/\s+/)[0] || searchQuery;

  // Pinterest trends — targeted query for product keywords, broad scan as fallback
  let trends = [];
  let trendsError = null;
  let trendsSource = 'none';
  try {
    const token = typeof getPinterestAccessToken === 'function' ? await getPinterestAccessToken() : null;
    const region = String(process.env.TRENDS_PINTEREST_REGION || 'US').trim();
    const getToken = async () => token;

    const targeted = await fetchPinterestTrendsForProductSeeds(seeds, {
      getAccessToken: getToken,
      region,
      limitPerType: 15,
    });
    trends = targeted.trends || [];
    trendsError = targeted.error;
    if (trends.length > 0) {
      trendsSource = 'targeted';
    } else if (token) {
      const broad = await fetchPinterestKeywordTrends({
        getAccessToken: getToken,
        region,
        limitPerType: 20,
      });
      const broadMatches = findTrendMatches(seeds, broad.trends);
      trends = broadMatches.map((m) => m.trend);
      trendsError = trends.length ? null : broad.error || targeted.error;
      if (trends.length > 0) trendsSource = 'broad_match';
    }
  } catch (e) {
    trendsError = e?.message || 'trends_fetch_failed';
  }

  const trendMatches = trends.map((trend) => {
    const seed = seeds.find((s) => {
      const kw = String(trend.keyword || '').toLowerCase();
      const seedL = s.toLowerCase();
      return kw.includes(seedL) || seedL.includes(kw) || keywordOverlap(s, kw) >= 0.4;
    });
    return { seed: seed || searchQuery, trend };
  });
  const seasonalHits = trendMatches.filter((m) => m.trend?.trendType === 'seasonal').length;

  const [rawSuggestions, amazonSearchResults, pinterestSnapshot] = await Promise.all([
    fetchPinterestSearchSuggestions(suggestionSeed),
    fetchAmazonSearchProducts(searchQuery, 30),
    fetchPinterestSearchSnapshot(searchQuery, { getPinterestAccessToken }),
  ]);
  const typeaheadSuggestions = filterRelevantSuggestions(rawSuggestions, searchQuery, product.categoryId);

  const demandScore = scoreDemand({
    trendMatches,
    typeaheadSuggestions,
    seasonalHits,
    pinterestSnapshot,
  });
  const competitionScore = scoreCompetition({
    amazonSearchCount: amazonSearchResults.length,
    trendMatches,
    typeaheadSuggestions,
    pinterestSnapshot,
  });
  const visualScore = scoreVisualAppeal({
    title: product.title,
    categoryId: product.categoryId,
    imageCount: product.imageCount,
  });
  const socialScore = scoreSocialProof({
    rating: product.rating,
    reviewCount: product.reviewCount,
    isBestSeller: product.isBestSeller,
  });
  const commercialScore = scoreCommercial({
    priceUsd: product.priceUsd,
    categoryId: product.categoryId,
  });

  const totalScore = clamp(
    demandScore + competitionScore + visualScore + socialScore + commercialScore,
    0,
    100
  );

  const band = overallBand(totalScore);
  const verdict = recommendVerdict(totalScore);
  const boards = (BOARD_TEMPLATES[product.categoryId] || BOARD_TEMPLATES.general).slice(0, 4);

  const saturation =
    amazonSearchResults.length +
    trendMatches.length * 2 +
    (pinterestSnapshot?.available ? (pinterestSnapshot.sampleSize || 0) * 0.5 : 0);
  let competitionLevel = pinterestSnapshot?.stats?.competitionLevel || 'Medium';
  if (!pinterestSnapshot?.available) {
    if (saturation >= 14) competitionLevel = 'High';
    else if (saturation <= 5) competitionLevel = 'Low';
  }

  const breakdown = {
    demand: { score: demandScore, max: 30, level: scoreToLevel(demandScore, 30) },
    competition: {
      score: competitionScore,
      max: 20,
      level: scoreToLevel(competitionScore, 20, ['Crowded', 'Moderate', 'Good', 'Wide open']),
      saturation: amazonSearchResults.length,
      similarProductsOnAmazon: amazonSearchResults.length,
      marketLevel: competitionLevel,
    },
    visualAppeal: { score: visualScore, max: 20, level: scoreToLevel(visualScore, 20) },
    socialProof: { score: socialScore, max: 15, level: scoreToLevel(socialScore, 15) },
    commercial: {
      score: commercialScore,
      max: 15,
      level: scoreToLevel(commercialScore, 15, ['Low', 'Fair', 'Good', 'Strong']),
    },
  };

  const evidence = buildScoreEvidence({
    product,
    searchQuery,
    trendMatches,
    amazonSearchResults,
    typeaheadSuggestions,
    competitionLevel,
    pinterestSnapshot,
    userPinHistory,
  });

  const relatedTrendKeywords = trendMatches.slice(0, 6).map((m) => ({
    keyword: m.trend.keyword,
    trendType: m.trend.trendType,
    pctGrowthMom: m.trend.pctGrowthMom,
    pctGrowthWow: m.trend.pctGrowthWow,
  }));

  return {
    product,
    score: {
      total: totalScore,
      band,
      breakdown,
    },
    analysis: {
      searchQuery,
      seeds,
      pinterestSuggestions: typeaheadSuggestions.slice(0, 10),
      relatedKeywords: expandedKeywords.slice(0, 12),
      matchingTrends: relatedTrendKeywords,
      seasonalInterest: seasonalHits > 0,
      evidence,
      pinterestSnapshot,
    },
    userHistory: userPinHistory || { pinsCreated: 0 },
    recommendations: {
      verdict: verdict.verdict,
      note: verdict.note,
      boards,
    },
  };
}
