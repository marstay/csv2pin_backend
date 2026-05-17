/**
 * Amazon product tags — unit + optional live Pinterest test.
 *
 * Unit only (default):
 *   node scripts/test-amazon-product-tags.mjs
 *
 * Live Pinterest (needs token + existing hero pin on your account):
 *   set PINTEREST_ACCESS_TOKEN=...
 *   set TEST_HERO_PIN_ID=1234567890
 *   set TEST_AMAZON_URL=https://www.amazon.com/dp/B0XXXX?tag=yoursite-20
 *   set TEST_BOARD_ID=...
 *   node scripts/test-amazon-product-tags.mjs --live
 */
import {
  applyAmazonAssociateTag,
  isAmazonRelatedHost,
  parseAmazonProductTagsConfig,
  resolveAmazonProductTagUrl,
} from '../src/pinterestProductTags.js';

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

console.log('--- Unit: URL helpers ---');
assert(isAmazonRelatedHost('www.amazon.com'), 'amazon.com host');
assert(isAmazonRelatedHost('amzn.to'), 'amzn.to host');
assert(!isAmazonRelatedHost('example.com'), 'not amazon');

const tagged = applyAmazonAssociateTag('https://www.amazon.com/dp/B0TEST', 'mysite-20');
assert(tagged.includes('tag=mysite-20'), 'appends tag');
assert(
  applyAmazonAssociateTag('https://www.amazon.com/dp/B0TEST?tag=existing-20', 'mysite-20').includes('tag=mysite-20'),
  'overwrites tag param'
);

const cfg = parseAmazonProductTagsConfig({
  amazon_product_tags: {
    enabled: true,
    source: 'custom',
    custom_url: 'https://www.amazon.com/dp/B0CUSTOM',
    associate_tag: 'mysite-20',
  },
});
assert(cfg?.source === 'custom', 'parse config source');
const resolved = resolveAmazonProductTagUrl(
  { link: 'https://blog.example.com/post' },
  cfg
);
assert(resolved.includes('amazon.com') && resolved.includes('tag=mysite-20'), 'custom url resolved');

const pinLinkCfg = parseAmazonProductTagsConfig({
  amazon_product_tags: { enabled: true, source: 'pin_link', associate_tag: 'x-20' },
});
const fromPin = resolveAmazonProductTagUrl(
  { link: 'https://www.amazon.com/dp/B0PIN' },
  pinLinkCfg
);
assert(fromPin.includes('tag=x-20'), 'pin_link uses scheduled pin link');

console.log('✅ Unit tests passed\n');

const live = process.argv.includes('--live');
if (!live) {
  console.log('Skip live test (pass --live to run against Pinterest API).');
  process.exit(0);
}

const token = String(process.env.PINTEREST_ACCESS_TOKEN || '').trim();
const heroPinId = String(process.env.TEST_HERO_PIN_ID || '').trim();
const amazonUrl = String(
  process.env.TEST_AMAZON_URL || 'https://www.amazon.com/dp/B0D1XD1ZV3'
).trim();
const boardId = String(process.env.TEST_BOARD_ID || '').trim();

if (!token) {
  console.error('Set PINTEREST_ACCESS_TOKEN for --live');
  process.exit(1);
}
if (!heroPinId) {
  console.error('Set TEST_HERO_PIN_ID (an existing pin on your account) for --live');
  process.exit(1);
}
if (!boardId) {
  console.error('Set TEST_BOARD_ID for --live (board to create helper product pin)');
  process.exit(1);
}

console.log('--- Live: create product pin + bulk_add ---');
console.log('Hero pin:', heroPinId);
console.log('Amazon URL:', amazonUrl);

async function pinterestJson(url, body) {
  const res = await fetch(url, {
    method: body ? 'POST' : 'GET',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, data };
}

// Step 1: product pin (pin_url beta, then image fallback)
let product = await pinterestJson('https://api.pinterest.com/v5/pins', {
  board_id: boardId,
  title: 'URL2Pin product tag test',
  description: 'Automated test — safe to delete',
  link: amazonUrl,
  media_source: { source_type: 'pin_url', is_affiliate_link: true },
});
if (!product.ok) {
  console.warn('pin_url create failed:', product.status, product.data?.message || product.data);
  product = await pinterestJson('https://api.pinterest.com/v5/pins', {
    board_id: boardId,
    title: 'URL2Pin product tag test',
    description: 'Automated test — safe to delete',
    link: amazonUrl,
    media_source: {
      source_type: 'image_url',
      url: 'https://i.pinimg.com/originals/1a/2b/3c/4d/5e/6f/7a/8b/9c/0d/1e/2f/test.jpg',
    },
  });
}
if (!product.ok) {
  console.error('❌ Could not create product pin:', product.data);
  process.exit(1);
}
const productPinId = product.data.id;
console.log('✅ Product pin created:', productPinId);

// Step 2: bulk_add
const bulk = await pinterestJson(
  `https://api.pinterest.com/v5/pins/${encodeURIComponent(heroPinId)}/product_tags`,
  { product_tags: [{ pin_id: String(productPinId) }] }
);
if (!bulk.ok) {
  console.error('❌ bulk_add failed:', bulk.status, bulk.data);
  process.exit(1);
}
console.log('✅ bulk_add OK:', JSON.stringify(bulk.data, null, 2));

// Step 3: list tags
const list = await pinterestJson(
  `https://api.pinterest.com/v5/pins/${encodeURIComponent(heroPinId)}/product_tags`
);
console.log('List product_tags:', list.ok ? JSON.stringify(list.data, null, 2) : list.data);
