const query = process.argv[2] || 'air fryer';
const url = `https://www.pinterest.com/search/pins/?q=${encodeURIComponent(query)}`;
const r = await fetch(url, {
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    Accept: 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
  },
});
const html = await r.text();
const m = html.match(/<script id="__PWS_DATA__"[^>]*>([\s\S]*?)<\/script>/);
if (!m) {
  console.log('no PWS_DATA');
  process.exit(1);
}
const j = JSON.parse(m[1]);

function walk(obj, hits, depth = 0) {
  if (!obj || depth > 14 || hits.length > 40) return;
  if (typeof obj !== 'object') return;

  const pin = obj;
  const id = pin.pin_id || pin.id;
  const hasPinShape =
    id &&
    (pin.images || pin.image_medium_url || pin.grid_title || pin.title || pin.description);
  if (hasPinShape && String(id).match(/^\d+$/)) {
    const saves =
      pin.aggregated_pin_data?.aggregated_stats?.saves ??
      pin.repin_count ??
      pin.save_count ??
      null;
    const title = String(pin.title || pin.grid_title || pin.description || '').trim();
    const imageUrl =
      pin.images?.['236x']?.url ||
      pin.images?.['474x']?.url ||
      pin.image_medium_url ||
      pin.image_large_url ||
      null;
    if (title || imageUrl || saves != null) {
      hits.push({ id, title: title.slice(0, 80), saves, imageUrl: imageUrl?.slice(0, 60) });
    }
  }

  if (Array.isArray(obj)) {
    for (const v of obj) walk(v, hits, depth + 1);
    return;
  }
  for (const v of Object.values(obj)) walk(v, hits, depth + 1);
}

const hits = [];
walk(j, hits);
const uniq = [];
const seen = new Set();
for (const h of hits) {
  if (seen.has(h.id)) continue;
  seen.add(h.id);
  uniq.push(h);
}
console.log('found', uniq.length);
console.log(JSON.stringify(uniq.slice(0, 8), null, 2));
