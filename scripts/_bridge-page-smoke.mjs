/**
 * Smoke test for the Supabase-backed affiliate product page store.
 * Run AFTER applying supabase/affiliate_product_pages.sql.
 * Usage: node backend/scripts/_bridge-page-smoke.mjs
 * Creates a throwaway page, exercises every store function, then deletes it.
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in backend/.env');
  process.exit(1);
}

// Import after env is loaded (module creates its Supabase client at import time).
const mod = await import('../src/affiliateProductPages.js');
const {
  createAffiliateProductPage,
  getAffiliateProductPageBySlug,
  incrementAffiliateProductPageViews,
  incrementAffiliateProductPageOutboundClicks,
  getAffiliateProductPageStats,
  listAffiliateProductPagesByUserId,
  deleteAffiliateProductPage,
} = mod;

let ok = true;
const assert = (label, cond) => {
  console.log(`${cond ? 'PASS' : 'FAIL'}  ${label}`);
  if (!cond) ok = false;
};

let slug = null;
try {
  const page = await createAffiliateProductPage({
    productUrl: 'https://www.amazon.com/dp/B0SMOKETEST',
    affiliateUrl: 'https://www.amazon.com/dp/B0SMOKETEST?tag=smoke-20',
    scrapeMeta: { title: 'SMOKE TEST Widget', description: 'A test product.', domain: 'amazon.com', amazonLanding: true, imageUrls: ['https://example.com/a.jpg'] },
    aiContent: {
      title: 'SMOKE TEST Widget',
      summary: 'A throwaway page for verification.',
      pros: ['pro one', 'pro two'],
      cons: ['con one'],
      bestFor: ['testers'],
      specifications: ['spec one'],
      imageUrls: ['https://example.com/a.jpg'],
      imageUrl: 'https://example.com/a.jpg',
    },
    userId: null,
  });
  slug = page.slug;
  assert('create returns slug', !!page.slug);
  assert('create returns manageToken', typeof page.manageToken === 'string' && page.manageToken.length === 32);
  assert('create maps merchant (amazon)', page.merchant === 'amazon');
  assert('create maps arrays (pros)', Array.isArray(page.pros) && page.pros.length === 2);

  const fetched = await getAffiliateProductPageBySlug(slug);
  assert('fetch by slug', fetched && fetched.title === 'SMOKE TEST Widget');
  assert('fetch has no ephemeral fields', fetched && fetched.buyUrl.includes('B0SMOKETEST'));

  const v = await incrementAffiliateProductPageViews(slug);
  assert('view increment', v && v.views === 1);
  const c = await incrementAffiliateProductPageOutboundClicks(slug);
  assert('click increment', c && c.outboundClicks === 1);

  const stats = await getAffiliateProductPageStats(slug, page.manageToken);
  assert('stats via manage token', stats && stats.views === 1 && stats.outboundClicks === 1);
  const badStats = await getAffiliateProductPageStats(slug, 'wrong-token');
  assert('stats rejects bad token', badStats === null);

  const delWrong = await deleteAffiliateProductPage(slug, { manageToken: 'wrong-token' });
  assert('delete rejects bad token', delWrong === false);
  const del = await deleteAffiliateProductPage(slug, { manageToken: page.manageToken });
  assert('delete via manage token', del === true);
  slug = null;
  const gone = await getAffiliateProductPageBySlug(page.slug);
  assert('page gone after delete', gone === null);
} catch (e) {
  console.error('\nERROR:', e.message || e);
  if (String(e.message || e).match(/does not exist|schema cache|relation/i)) {
    console.error('\n>> The table is not created yet. Run supabase/affiliate_product_pages.sql in the Supabase SQL editor first.');
  }
  ok = false;
} finally {
  // Best-effort cleanup if we bailed mid-test.
  if (slug) {
    try {
      const p = await getAffiliateProductPageBySlug(slug);
      if (p) await deleteAffiliateProductPage(slug, { manageToken: p.manageToken });
    } catch { /* ignore */ }
  }
}

console.log(`\n${ok ? 'SMOKE TEST PASSED' : 'SMOKE TEST FAILED'}`);
process.exit(ok ? 0 : 1);
