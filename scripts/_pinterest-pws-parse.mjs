import fetch from 'node-fetch';
import fs from 'fs';

const query = process.argv[2] || 'bathroom organizer';
const url = `https://www.pinterest.com/search/pins/?q=${encodeURIComponent(query)}`;
const r = await fetch(url, {
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  },
});
const html = await r.text();

function walk(obj, hits, depth = 0) {
  if (!obj || depth > 12 || hits.length > 30) return;
  if (typeof obj !== 'object') return;
  if (obj.pin_id || obj.id) {
    const saves =
      obj.aggregated_pin_data?.aggregated_stats?.saves ??
      obj.repin_count ??
      obj.save_count ??
      null;
    const title = obj.title || obj.grid_title || obj.description || '';
    if (title || saves != null) {
      hits.push({ title: String(title).slice(0, 80), saves, id: obj.pin_id || obj.id });
    }
  }
  if (Array.isArray(obj)) {
    for (const v of obj) walk(v, hits, depth + 1);
    return;
  }
  for (const v of Object.values(obj)) walk(v, hits, depth + 1);
}

const m = html.match(/<script id="__PWS_INITIAL_PROPS__" type="application\/json">([\s\S]*?)<\/script>/);
if (!m) {
  console.log('no PWS_INITIAL_PROPS');
  process.exit(1);
}
const j = JSON.parse(m[1]);
const hits = [];
walk(j, hits);
const uniq = [];
const seen = new Set();
for (const h of hits) {
  const k = String(h.id || h.title);
  if (seen.has(k)) continue;
  seen.add(k);
  uniq.push(h);
}
console.log('pins found', uniq.length);
console.log(JSON.stringify(uniq.slice(0, 8), null, 2));
