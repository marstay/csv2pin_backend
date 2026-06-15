const query = process.argv[2] || 'air fryer';
const url = `https://www.pinterest.com/search/pins/?q=${encodeURIComponent(query)}`;
const r = await fetch(url, {
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  },
});
const html = await r.text();
const m = html.match(/<script id="__PWS_DATA__"[^>]*>([\s\S]*?)<\/script>/);
const j = JSON.parse(m[1]);

function findPinIdPaths(obj, path = '', out = []) {
  if (!obj || out.length > 10) return;
  if (typeof obj !== 'object') return;
  if (obj.pin_id) out.push({ path, pin_id: obj.pin_id, keys: Object.keys(obj).slice(0, 15) });
  if (Array.isArray(obj)) {
    obj.slice(0, 3).forEach((v, i) => findPinIdPaths(v, `${path}[${i}]`, out));
    return;
  }
  for (const [k, v] of Object.entries(obj).slice(0, 30)) {
    findPinIdPaths(v, path ? `${path}.${k}` : k, out);
  }
}

const paths = [];
findPinIdPaths(j, '', paths);
console.log(JSON.stringify(paths, null, 2));
