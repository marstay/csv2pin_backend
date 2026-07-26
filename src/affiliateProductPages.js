/**
 * AI affiliate product pages — Supabase-backed store (durable across redeploys).
 * Public at /page/[slug], noindex; generator at /ai-product-page-generator.
 * Table: public.affiliate_product_pages (see supabase/affiliate_product_pages.sql).
 */
import crypto from 'node:crypto';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
);
const TABLE = 'affiliate_product_pages';

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

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

/** Map a snake_case DB row to the camelCase page object callers expect. */
function rowToPage(row) {
  if (!row || typeof row !== 'object') return null;
  return {
    slug: row.slug,
    productUrl: row.product_url || '',
    affiliateUrl: row.affiliate_url || null,
    buyUrl: row.buy_url || '',
    merchant: row.merchant || 'other',
    merchantLabel: row.merchant_label || merchantLabel(row.merchant || 'other'),
    title: row.title || 'Product overview',
    imageUrl: row.image_url || '',
    imageUrls: asArray(row.image_urls),
    summary: row.summary || '',
    pros: asArray(row.pros),
    cons: asArray(row.cons),
    bestFor: asArray(row.best_for),
    specifications: asArray(row.specifications),
    disclosure: row.disclosure || DEFAULT_DISCLOSURE,
    price: row.price || '',
    priceUpdatedAt: row.price_updated_at || null,
    userId: row.user_id || null,
    manageToken: row.manage_token || '',
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    views: Number(row.views) || 0,
    outboundClicks: Number(row.outbound_clicks) || 0,
  };
}

export async function getAffiliateProductPageBySlug(slug) {
  const key = String(slug || '').trim().toLowerCase();
  if (!key || key.length > 80) return null;
  const { data, error } = await supabase.from(TABLE).select('*').eq('slug', key).maybeSingle();
  if (error) {
    console.error('getAffiliateProductPageBySlug:', error.message || error);
    return null;
  }
  return rowToPage(data);
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

  const row = {
    product_url: resolvedProductUrl,
    affiliate_url: affiliateUrl ? buyUrl : null,
    buy_url: buyUrl,
    merchant,
    merchant_label: merchantLabel(merchant),
    title,
    image_url: imageUrl,
    image_urls: imageUrls.length ? imageUrls : imageUrl ? [imageUrl] : [],
    summary: String(aiContent?.summary || scrapeMeta?.description || '').trim(),
    pros: Array.isArray(aiContent?.pros) ? aiContent.pros.slice(0, 6) : [],
    cons: Array.isArray(aiContent?.cons) ? aiContent.cons.slice(0, 5) : [],
    best_for: Array.isArray(aiContent?.bestFor) ? aiContent.bestFor.slice(0, 5) : [],
    specifications: Array.isArray(aiContent?.specifications) ? aiContent.specifications.slice(0, 10) : [],
    disclosure: String(aiContent?.disclosure || DEFAULT_DISCLOSURE).trim(),
    user_id: userId ? String(userId).trim() : null,
    manage_token: crypto.randomBytes(16).toString('hex'),
  };

  // slugifyTitle already appends a random suffix; retry on the rare unique collision.
  for (let attempt = 0; attempt < 5; attempt++) {
    const slug = slugifyTitle(title);
    const { data, error } = await supabase
      .from(TABLE)
      .insert({ slug, ...row })
      .select('*')
      .single();
    if (!error) return rowToPage(data);
    if (error.code === '23505') continue; // unique_violation on slug — retry with a new suffix
    console.error('createAffiliateProductPage:', error.message || error);
    throw new Error('Could not save product page.');
  }
  throw new Error('Could not generate a unique page slug. Please try again.');
}

export async function incrementAffiliateProductPageViews(slug) {
  const key = String(slug || '').trim().toLowerCase();
  const current = await getAffiliateProductPageBySlug(key);
  if (!current) return null;
  const { data, error } = await supabase
    .from(TABLE)
    .update({ views: (Number(current.views) || 0) + 1, updated_at: new Date().toISOString() })
    .eq('slug', key)
    .select('*')
    .single();
  if (error) {
    console.error('incrementAffiliateProductPageViews:', error.message || error);
    return current; // still serve the page even if the counter write fails
  }
  return rowToPage(data);
}

export async function incrementAffiliateProductPageOutboundClicks(slug) {
  const key = String(slug || '').trim().toLowerCase();
  const current = await getAffiliateProductPageBySlug(key);
  if (!current) return null;
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      outbound_clicks: (Number(current.outboundClicks) || 0) + 1,
      updated_at: new Date().toISOString(),
    })
    .eq('slug', key)
    .select('*')
    .single();
  if (error) {
    console.error('incrementAffiliateProductPageOutboundClicks:', error.message || error);
    return current;
  }
  return rowToPage(data);
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
  const page = await getAffiliateProductPageBySlug(key);
  if (!page) return false;
  const authorized =
    (userId && page.userId === userId) ||
    (manageToken && String(page.manageToken || '') === manageToken);
  if (!authorized) return false;
  const { error } = await supabase.from(TABLE).delete().eq('slug', key);
  if (error) {
    console.error('deleteAffiliateProductPage:', error.message || error);
    return false;
  }
  return true;
}

export async function listAffiliateProductPagesByUserId(userId) {
  const uid = String(userId || '').trim();
  if (!uid) return [];
  const { data, error } = await supabase
    .from(TABLE)
    .select('*')
    .eq('user_id', uid)
    .order('updated_at', { ascending: false });
  if (error) {
    console.error('listAffiliateProductPagesByUserId:', error.message || error);
    return [];
  }
  return (data || []).map((row) => sanitizeAffiliateProductPageForPublic(rowToPage(row)));
}

export async function updateAffiliateProductPageByUser(slug, userId, patches) {
  const key = String(slug || '').trim().toLowerCase();
  const uid = String(userId || '').trim();
  if (!key || !uid || !patches || typeof patches !== 'object') return null;
  const page = await getAffiliateProductPageBySlug(key);
  if (!page || page.userId !== uid) return null;

  const update = { updated_at: new Date().toISOString() };
  if (patches.title !== undefined) {
    update.title = String(patches.title || '').trim().slice(0, 160);
  }
  if (patches.summary !== undefined) {
    update.summary = String(patches.summary || '').trim().slice(0, 2000);
  }
  if (patches.disclosure !== undefined) {
    update.disclosure = String(patches.disclosure || '').trim().slice(0, 500);
  }
  // Destination the buy button points to. Never allow it to be cleared to empty
  // (a live pin must still land somewhere); reject clearly if it is not a URL.
  if (patches.buyUrl !== undefined) {
    const raw = String(patches.buyUrl || '').trim();
    if (raw) {
      let normalized;
      try {
        normalized = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`).toString();
      } catch {
        throw new Error('That destination URL does not look valid.');
      }
      update.buy_url = normalized;
    }
  }
  // Optional, owner-entered price string (e.g. "$49.99"); stamp when it was set.
  if (patches.price !== undefined) {
    const priceStr = String(patches.price || '').trim().slice(0, 40);
    update.price = priceStr;
    update.price_updated_at = priceStr ? new Date().toISOString() : null;
  }
  const listColumn = { pros: 'pros', cons: 'cons', bestFor: 'best_for' };
  for (const listKey of ['pros', 'cons', 'bestFor']) {
    if (patches[listKey] !== undefined && Array.isArray(patches[listKey])) {
      const max = listKey === 'cons' ? 5 : 6;
      update[listColumn[listKey]] = patches[listKey]
        .map((s) => String(s || '').trim())
        .filter(Boolean)
        .slice(0, max);
    }
  }
  if (patches.specifications !== undefined && Array.isArray(patches.specifications)) {
    update.specifications = patches.specifications
      .map((s) => String(s || '').trim())
      .filter(Boolean)
      .slice(0, 10);
  }

  const { data, error } = await supabase
    .from(TABLE)
    .update(update)
    .eq('slug', key)
    .eq('user_id', uid)
    .select('*')
    .single();
  if (error) {
    console.error('updateAffiliateProductPageByUser:', error.message || error);
    return null;
  }
  return rowToPage(data);
}

export async function claimAffiliateProductPage(slug, manageToken, userId) {
  const key = String(slug || '').trim().toLowerCase();
  const token = String(manageToken || '').trim();
  const uid = String(userId || '').trim();
  if (!key || !token || !uid) return null;
  const page = await getAffiliateProductPageBySlug(key);
  if (!page || String(page.manageToken || '') !== token) return null;
  if (page.userId && page.userId !== uid) return null;
  const { data, error } = await supabase
    .from(TABLE)
    .update({ user_id: uid, updated_at: new Date().toISOString() })
    .eq('slug', key)
    .select('*')
    .single();
  if (error) {
    console.error('claimAffiliateProductPage:', error.message || error);
    return null;
  }
  return rowToPage(data);
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
