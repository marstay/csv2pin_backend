// End-to-end diagnostic for the Walmart pipeline:
//   short link → resolve to walmart.com/ip/<id> → walmart-data RapidAPI → full-res images.
// Usage: node scripts/_walmart-test.mjs "https://walmrt.us/4xpSVfY"
import 'dotenv/config';

const inputUrl = process.argv[2] || 'https://walmrt.us/4xpSVfY';
const key = String(process.env.RAPIDAPI_KEY || '').trim();
const host = String(process.env.RAPIDAPI_WALMART_HOST || '').trim() || 'walmart-data.p.rapidapi.com';
const template =
  String(process.env.RAPIDAPI_WALMART_URL_TEMPLATE || '').trim() ||
  'https://{host}/product-details.php?url={url}';

function isWalmartShortLinkHost(h) {
  h = String(h || '').replace(/^www\./, '').toLowerCase();
  return /(^|\.)walmrt\.us$/.test(h) || /(^|\.)goto\.walmart\.com$/.test(h) || /(^|\.)linkst\.walmart\.com$/.test(h);
}

async function resolveShortLink(shortUrl) {
  let current = String(shortUrl || '').trim();
  for (let hop = 0; hop < 4; hop++) {
    let u;
    try { u = new URL(current); } catch { return ''; }
    const h = u.hostname.replace(/^www\./, '').toLowerCase();
    if ((h === 'walmart.com' || h.endsWith('.walmart.com')) && /\/ip\//i.test(u.pathname)) return current;
    if (h === 'goto.walmart.com' || h.endsWith('.goto.walmart.com')) {
      const uParam = u.searchParams.get('u');
      if (uParam) {
        let dest = uParam;
        try { dest = decodeURIComponent(uParam); } catch {}
        const id = dest.match(/\/ip\/(\d{5,15})/i)?.[1];
        if (id) return `https://www.walmart.com/ip/${id}`;
        if (/\/ip\//i.test(dest)) return dest;
      }
      const sku = u.searchParams.get('prodsku');
      if (sku && /^\d{5,15}$/.test(sku)) return `https://www.walmart.com/ip/${sku}`;
    }
    const resp = await fetch(current, { redirect: 'manual' }).catch(() => null);
    const loc = resp && resp.headers ? resp.headers.get('location') : null;
    if (!loc) return '';
    try { current = new URL(loc, current).href; } catch { return ''; }
  }
  return '';
}

function normalizeWalmartImg(raw) {
  if (!raw) return null;
  try {
    const u = new URL(raw);
    if (!u.hostname.toLowerCase().endsWith('walmartimages.com')) return null;
    if (/\.svg(\?|$)/i.test(u.pathname)) return null;
    return `${u.origin}${u.pathname}`;
  } catch { return null; }
}

let productUrl = inputUrl;
if (isWalmartShortLinkHost(new URL(inputUrl).hostname)) {
  console.log('Short link detected — resolving…');
  productUrl = (await resolveShortLink(inputUrl)) || inputUrl;
}
console.log('Resolved product URL:', productUrl);

const endpoint = template.replace(/\{host\}/g, host).replace(/\{itemId\}/g, '').replace(/\{url\}/g, encodeURIComponent(productUrl));
console.log('Endpoint:', endpoint);
console.log('---');

try {
  const resp = await fetch(endpoint, {
    headers: { 'x-rapidapi-key': key, 'x-rapidapi-host': host, Accept: 'application/json' },
  });
  console.log('HTTP status:', resp.status, resp.statusText);
  const json = await resp.json().catch(() => null);
  const body = json && json.body && typeof json.body === 'object' ? json.body : json || {};
  const seen = new Set();
  const images = [];
  for (const im of Array.isArray(body.images) ? body.images : []) {
    const n = normalizeWalmartImg(typeof im === 'string' ? im : im?.url);
    if (n && !seen.has(n)) { seen.add(n); images.push(n); }
  }
  console.log('title:', body.title || '(none)');
  console.log('description:', (body.description || '(none)').slice(0, 120));
  console.log('full-res images (deduped, svg dropped):', images.length);
  images.slice(0, 5).forEach((u, i) => console.log(`  [${i}] ${u}`));
} catch (e) {
  console.log('FETCH ERROR:', e.message || e);
}
