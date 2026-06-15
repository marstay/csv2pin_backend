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

// Find pin_id occurrences with surrounding context
const re = /"pin_id":"(\d+)"/g;
let m;
const ids = [];
while ((m = re.exec(html)) && ids.length < 5) ids.push(m[1]);
console.log('pin_ids sample', ids);

// Try alternate embedded JSON blobs
for (const id of ['__PWS_DATA__', '__PWS_INITIAL_PROPS__', 'initial-state']) {
  const rx = new RegExp(`<script id="${id}"[^>]*>([\\s\\S]*?)<\\/script>`);
  const match = html.match(rx);
  if (!match) {
    console.log(id, 'missing');
    continue;
  }
  try {
    const j = JSON.parse(match[1]);
    const flat = JSON.stringify(j);
    console.log(id, 'size', flat.length, 'pin_id count', (flat.match(/pin_id/g) || []).length);
  } catch (e) {
    console.log(id, 'parse error', e.message);
  }
}

// Look for image URLs that look like pin images
const imgs = [...html.matchAll(/https:\/\/i\.pinimg\.com\/[^"\\]+/g)].slice(0, 5).map((x) => x[0]);
console.log('pinimg urls', imgs.length, imgs[0]?.slice(0, 80));
