const q = process.argv[2] || 'air fryer';
const data = JSON.stringify({
  options: { term: q, term_meta: [], pin_scope: 'pins', auto_correction_disabled: '', search_type: 'pins' },
});
const u =
  'https://www.pinterest.com/resource/AdvancedTypeaheadResource/get/?source_url=%2Fsearch%2Fpins%2F&data=' +
  encodeURIComponent(data);
const r = await fetch(u, {
  headers: {
    'User-Agent': 'Mozilla/5.0',
    'X-Requested-With': 'XMLHttpRequest',
    Accept: 'application/json',
  },
});
const t = await r.text();
console.log('status', r.status);
try {
  const j = JSON.parse(t);
  const items = j?.resource_response?.data?.items || [];
  console.log('suggestions:', items.map((i) => i.label || i.query || i).slice(0, 15));
} catch {
  console.log(t.slice(0, 600));
}
