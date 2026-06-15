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
console.log('status', r.status, 'len', html.length);

for (const pat of ['grid_title', 'repin_count', 'aggregated_stats', 'pin_id', 'closeup_unified_title']) {
  console.log(pat, (html.match(new RegExp(pat, 'g')) || []).length);
}

const m = html.match(/<script id="__PWS_INITIAL_PROPS__"[^>]*>([\s\S]*?)<\/script>/);
if (m) {
  const j = JSON.parse(m[1]);
  const s = JSON.stringify(j);
  console.log('props json len', s.length);
  console.log('grid_title in props', (s.match(/grid_title/g) || []).length);
  console.log('top keys', Object.keys(j).slice(0, 20));
}
