const query = process.argv[2] || 'bathroom organizer';
const data = JSON.stringify({
  options: {
    query,
    scope: 'pins',
    filters: '',
    applied_unified_filters: null,
    appliedProductFilters: '---',
    auto_correction_disabled: '',
    domain: null,
  },
  context: {},
});
const url =
  'https://www.pinterest.com/resource/BaseSearchResource/get/?source_url=%2Fsearch%2Fpins%2F&data=' +
  encodeURIComponent(data);

const r = await fetch(url, {
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    Accept: 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'X-Requested-With': 'XMLHttpRequest',
    'X-Pinterest-AppState': 'active',
  },
});
const t = await r.text();
console.log('status', r.status, 'len', t.length);
try {
  const j = JSON.parse(t);
  const results = j?.resource_response?.data?.results || [];
  console.log('results', results.length);
  const first = results[0];
  if (first) {
    console.log('keys', Object.keys(first).join(', '));
    const pin = first.pin || first;
    console.log('pin keys', Object.keys(pin).slice(0, 30).join(', '));
    console.log('title', (pin.title || pin.grid_title || '').slice(0, 80));
    console.log('saves', pin.aggregated_pin_data?.aggregated_stats?.saves, pin.repin_count, pin.save_count);
    console.log('link', pin.link || pin.rich_summary?.display_name);
  }
} catch {
  console.log(t.slice(0, 500));
}
