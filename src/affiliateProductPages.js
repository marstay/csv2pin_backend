/**
 * AI affiliate product pages — JSON file store (Phase 1).
 * Public at /page/[slug], noindex; generator at /ai-product-page-generator.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STORE_PATH = path.join(__dirname, '..', 'data', 'affiliate-product-pages.json');

const DEFAULT_DISCLOSURE =
  'Disclosure: As an Amazon Associate and affiliate partner, I earn from qualifying purchases. ' +
  'If you buy through links on this page, I may receive a commission at no extra cost to you.';

function slugifyTitle(title) {
  const base = String(title || 'product')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
  const suffix = crypto.randomBytes(2).toString('hex');
  return `${base || 'product'}-${suffix}`;
}

function detectMerchant(hostname, flags = {}) {
  const h = String(hostname || '').toLowerCase();
  if (flags.amazonLanding || /amazon\.|amzn\.|a\.co/.test(h)) return 'amazon';
  if (flags.etsyLanding || /etsy\.com/.test(h)) return 'etsy';
  if (flags.walmartLanding || /walmart\.|walmrt\./.test(h)) return 'walmart';
  if (/shopify\.com|myshopify\.com/.test(h)) return 'shopify';
  return 'other';
}

function merchantLabel(merchant) {
  const map = {
    amazon: 'Amazon',
    etsy: 'Etsy',
    shopify: 'Shopify',
    walmart: 'Walmart',
    other: 'Store',
  };
  return map[merchant] || 'Store';
}

async function readStore() {
  try {
    const raw = await fs.readFile(STORE_PATH, 'utf8');
    const json = JSON.parse(raw);
    if (json && typeof json.pages === 'object') return json;
  } catch {
    /* fresh store */
  }
  return { pages: {} };
}

async function writeStore(store) {
  await fs.mkdir(path.dirname(STORE_PATH), { recursive: true });
  await fs.writeFile(STORE_PATH, JSON.stringify(store, null, 2), 'utf8');
}

export async function getAffiliateProductPageBySlug(slug) {
  const key = String(slug || '').trim().toLowerCase();
  if (!key || key.length > 80) return null;
  const store = await readStore();
  return store.pages[key] || null;
}

export async function createAffiliateProductPage({
  productUrl,
  affiliateUrl,
  scrapeMeta,
  aiContent,
  userId = null,
}) {
  const resolvedProductUrl = String(productUrl || '').trim();
  const buyUrl = String(affiliateUrl || '').trim() || resolvedProductUrl;
  const title = String(aiContent?.title || scrapeMeta?.title || 'Product overview').trim().slice(0, 160);
  let slug = slugifyTitle(title);
  const store = await readStore();
  while (store.pages[slug]) {
    slug = slugifyTitle(`${title}-${crypto.randomBytes(1).toString('hex')}`);
  }

  const hostname = scrapeMeta?.domain || '';
  const merchant = detectMerchant(hostname, scrapeMeta);

  const rawImageUrls = Array.isArray(aiContent?.imageUrls)
    ? aiContent.imageUrls
    : Array.isArray(scrapeMeta?.imageUrls)
      ? scrapeMeta.imageUrls
      : [];
  const imageUrls = rawImageUrls
    .map((u) => String(u || '').trim())
    .filter(Boolean)
    .slice(0, 6);
  const imageUrl =
    String(aiContent?.imageUrl || scrapeMeta?.imageUrl || imageUrls[0] || '').trim();

  const page = {
    slug,
    productUrl: resolvedProductUrl,
    affiliateUrl: affiliateUrl ? buyUrl : null,
    buyUrl,
    merchant,
    merchantLabel: merchantLabel(merchant),
    title,
    imageUrl,
    imageUrls: imageUrls.length ? imageUrls : imageUrl ? [imageUrl] : [],
    summary: String(aiContent?.summary || scrapeMeta?.description || '').trim(),
    pros: Array.isArray(aiContent?.pros) ? aiContent.pros.slice(0, 6) : [],
    cons: Array.isArray(aiContent?.cons) ? aiContent.cons.slice(0, 5) : [],
    bestFor: Array.isArray(aiContent?.bestFor) ? aiContent.bestFor.slice(0, 5) : [],
    specifications: Array.isArray(aiContent?.specifications) ? aiContent.specifications.slice(0, 10) : [],
    disclosure: String(aiContent?.disclosure || DEFAULT_DISCLOSURE).trim(),
    userId: userId ? String(userId).trim() : null,
    manageToken: crypto.randomBytes(16).toString('hex'),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    views: 0,
    outboundClicks: 0,
  };

  store.pages[slug] = page;
  await writeStore(store);
  return page;
}

export async function incrementAffiliateProductPageViews(slug) {
  const key = String(slug || '').trim().toLowerCase();
  const store = await readStore();
  const page = store.pages[key];
  if (!page) return null;
  page.views = (Number(page.views) || 0) + 1;
  page.updatedAt = new Date().toISOString();
  store.pages[key] = page;
  await writeStore(store);
  return page;
}

export async function incrementAffiliateProductPageOutboundClicks(slug) {
  const key = String(slug || '').trim().toLowerCase();
  const store = await readStore();
  const page = store.pages[key];
  if (!page) return null;
  page.outboundClicks = (Number(page.outboundClicks) || 0) + 1;
  page.updatedAt = new Date().toISOString();
  store.pages[key] = page;
  await writeStore(store);
  return page;
}

/** Strip secrets before public API responses. */
export function sanitizeAffiliateProductPageForPublic(page) {
  if (!page || typeof page !== 'object') return page;
  const { manageToken, ...rest } = page;
  return rest;
}

export async function getAffiliateProductPageStats(slug, manageToken) {
  const key = String(slug || '').trim().toLowerCase();
  const token = String(manageToken || '').trim();
  if (!key || !token) return null;
  const page = await getAffiliateProductPageBySlug(key);
  if (!page || String(page.manageToken || '') !== token) return null;
  return {
    slug: page.slug,
    title: page.title,
    views: Number(page.views) || 0,
    outboundClicks: Number(page.outboundClicks) || 0,
    createdAt: page.createdAt,
    updatedAt: page.updatedAt,
  };
}

export async function deleteAffiliateProductPage(slug, manageTokenOrOpts) {
  const key = String(slug || '').trim().toLowerCase();
  if (!key) return false;
  let manageToken = '';
  let userId = '';
  if (typeof manageTokenOrOpts === 'object' && manageTokenOrOpts !== null) {
    manageToken = String(manageTokenOrOpts.manageToken || '').trim();
    userId = String(manageTokenOrOpts.userId || '').trim();
  } else {
    manageToken = String(manageTokenOrOpts || '').trim();
  }
  const store = await readStore();
  const page = store.pages[key];
  if (!page) return false;
  if (userId && page.userId === userId) {
    delete store.pages[key];
    await writeStore(store);
    return true;
  }
  if (manageToken && String(page.manageToken || '') === manageToken) {
    delete store.pages[key];
    await writeStore(store);
    return true;
  }
  return false;
}

export async function listAffiliateProductPagesByUserId(userId) {
  const uid = String(userId || '').trim();
  if (!uid) return [];
  const store = await readStore();
  return Object.values(store.pages)
    .filter((p) => p.userId === uid)
    .sort(
      (a, b) =>
        new Date(b.updatedAt || b.createdAt || 0).getTime() -
        new Date(a.updatedAt || a.createdAt || 0).getTime()
    )
    .map((p) => sanitizeAffiliateProductPageForPublic(p));
}

export async function updateAffiliateProductPageByUser(slug, userId, patches) {
  const key = String(slug || '').trim().toLowerCase();
  const uid = String(userId || '').trim();
  if (!key || !uid || !patches || typeof patches !== 'object') return null;
  const store = await readStore();
  const page = store.pages[key];
  if (!page || page.userId !== uid) return null;

  if (patches.title !== undefined) {
    page.title = String(patches.title || '').trim().slice(0, 160);
  }
  if (patches.summary !== undefined) {
    page.summary = String(patches.summary || '').trim().slice(0, 2000);
  }
  if (patches.disclosure !== undefined) {
    page.disclosure = String(patches.disclosure || '').trim().slice(0, 500);
  }
  for (const listKey of ['pros', 'cons', 'bestFor']) {
    if (patches[listKey] !== undefined && Array.isArray(patches[listKey])) {
      const max = listKey === 'specifications' ? 10 : listKey === 'cons' ? 5 : 6;
      page[listKey] = patches[listKey]
        .map((s) => String(s || '').trim())
        .filter(Boolean)
        .slice(0, max);
    }
  }
  if (patches.specifications !== undefined && Array.isArray(patches.specifications)) {
    page.specifications = patches.specifications
      .map((s) => String(s || '').trim())
      .filter(Boolean)
      .slice(0, 10);
  }

  page.updatedAt = new Date().toISOString();
  store.pages[key] = page;
  await writeStore(store);
  return page;
}

export async function claimAffiliateProductPage(slug, manageToken, userId) {
  const key = String(slug || '').trim().toLowerCase();
  const token = String(manageToken || '').trim();
  const uid = String(userId || '').trim();
  if (!key || !token || !uid) return null;
  const store = await readStore();
  const page = store.pages[key];
  if (!page || String(page.manageToken || '') !== token) return null;
  if (page.userId && page.userId !== uid) return null;
  page.userId = uid;
  page.updatedAt = new Date().toISOString();
  store.pages[key] = page;
  await writeStore(store);
  return page;
}

const HOSTED_PAGE_SLUG_RE = /\/page\/([^/?#]+)/i;

function isUrl2PinAppHost(hostname) {
  const h = String(hostname || '').toLowerCase();
  return (
    h === 'url2pin.com' ||
    h === 'www.url2pin.com' ||
    h === 'localhost' ||
    h === '127.0.0.1' ||
    h.endsWith('.netlify.app')
  );
}

/** @returns {string|null} slug from url2pin.com/page/{slug} or localhost/page/{slug} */
export function parseAffiliateProductPageSlugFromUrl(urlString) {
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return null;
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (!isUrl2PinAppHost(u.hostname)) return null;
    const m = u.pathname.match(HOSTED_PAGE_SLUG_RE);
    if (!m) return null;
    const slug = decodeURIComponent(m[1]).trim().toLowerCase();
    if (!slug || slug === '_') return null;
    return slug;
  } catch {
    return null;
  }
}

/**
 * Build URL→Pin scrape metadata from a hosted affiliate product page (no HTML fetch).
 * @returns {{ base: object, articleSummary: string }}
 */
export function buildUrlToPinArticleFromHostedProductPage(page, hostedPageUrl) {
  const title = String(page?.title || '').trim();
  const description = String(page?.summary || '').trim();
  const imageUrls = (Array.isArray(page?.imageUrls) ? page.imageUrls : [])
    .map((u) => String(u || '').trim())
    .filter(Boolean)
    .slice(0, 6);
  const imageUrl = String(page?.imageUrl || imageUrls[0] || '').trim();

  let domain = 'url2pin.com';
  let canonicalUrl = String(hostedPageUrl || '').trim();
  try {
    const u = new URL(canonicalUrl.startsWith('http') ? canonicalUrl : `https://${canonicalUrl}`);
    domain = u.hostname;
    canonicalUrl = u.href;
  } catch {
    /* keep defaults */
  }

  const summaryParts = [
    title,
    description,
    ...(Array.isArray(page?.pros) ? page.pros.slice(0, 4) : []),
    ...(Array.isArray(page?.bestFor) ? page.bestFor.slice(0, 3) : []),
  ].filter(Boolean);
  const articleSummary = summaryParts.join('. ').slice(0, 1200);

  const underlyingProductUrl = String(page?.productUrl || '').trim();
  const merchant = String(page?.merchant || 'other');

  const base = {
    title,
    description,
    canonicalUrl,
    domain,
    keyword: title
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter(Boolean)
      .slice(0, 6)
      .join(' '),
    linkDisplay: domain,
    underlyingProductUrl,
    affiliateHostedProductPage: true,
    affiliateProductPageSlug: page?.slug,
    affiliateHostedProductPageImageUrls: imageUrls.length ? imageUrls : imageUrl ? [imageUrl] : [],
    imageUrl,
    amazonLanding: merchant === 'amazon',
    walmartLanding: merchant === 'walmart',
    etsyLanding: merchant === 'etsy',
    creatorAffiliateLanding: false,
  };

  return { base, articleSummary };
}

/** Resolve URL2Pin-hosted /page/{slug} to article metadata for pin generation. */
export async function tryResolveUrlToPinHostedProductPage(urlString) {
  const slug = parseAffiliateProductPageSlugFromUrl(urlString);
  if (!slug) return null;
  const page = await getAffiliateProductPageBySlug(slug);
  if (!page) return null;
  return buildUrlToPinArticleFromHostedProductPage(page, urlString);
}

/**
 * @param {import('openai').OpenAI} openai
 * @param {{ title: string, description: string, domain: string, articleSummary?: string, merchant: string }} ctx
 */
export async function generateAffiliateProductPageContent(openai, ctx) {
  const title = String(ctx?.title || '').trim() || 'Product';
  const description = String(ctx?.description || '').trim();
  const summary = String(ctx?.articleSummary || '').trim();
  const merchant = String(ctx?.merchant || 'store');

  const prompt =
    `You write honest, useful affiliate bridge pages for Pinterest traffic (not fake reviews).\n` +
    `Product: ${title}\n` +
    `Merchant: ${merchant}\n` +
    (description ? `Listing description: ${description.slice(0, 1200)}\n` : '') +
    (summary ? `Extra context: ${summary.slice(0, 800)}\n` : '') +
    `\nReturn JSON only with keys:\n` +
    `{"title":"short product headline","summary":"2-3 sentence overview","pros":["..."],"cons":["..."],"bestFor":["..."],"specifications":[{"label":"...","value":"..."}]}\n` +
    `Rules: 3-5 pros, 2-4 cons, 2-4 bestFor bullets, 3-6 specifications. Be plausible from listing data; hedge when unsure ("based on listing details"). No fake star ratings or prices. No markdown.`;

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 900,
      temperature: 0.65,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      return {
        title: String(parsed.title || title).slice(0, 160),
        summary: String(parsed.summary || description).slice(0, 600),
        pros: (parsed.pros || []).map((p) => String(p).trim()).filter(Boolean),
        cons: (parsed.cons || []).map((p) => String(p).trim()).filter(Boolean),
        bestFor: (parsed.bestFor || []).map((p) => String(p).trim()).filter(Boolean),
        specifications: (parsed.specifications || [])
          .map((row) => ({
            label: String(row?.label || '').trim(),
            value: String(row?.value || '').trim(),
          }))
          .filter((row) => row.label && row.value),
        disclosure: DEFAULT_DISCLOSURE,
      };
    }
  } catch (e) {
    console.warn('generateAffiliateProductPageContent error:', e.message || e);
  }

  return {
    title,
    summary: description || `Overview of ${title} — see the listing for current price and availability.`,
    pros: ['Useful starting point before you buy', 'Summarizes key product details from the listing'],
    cons: ['Verify current price and availability on the merchant site'],
    bestFor: ['Shoppers comparing options on Pinterest', 'Readers who want a quick overview before clicking through'],
    specifications: [],
    disclosure: DEFAULT_DISCLOSURE,
  };
}
