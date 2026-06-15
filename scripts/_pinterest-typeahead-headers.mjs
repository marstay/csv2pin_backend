const query = process.argv[2] || 'air fryer';
const dataPayload = JSON.stringify({
  options: {
    term: query,
    term_meta: [],
    pin_scope: 'pins',
    auto_correction_disabled: '',
    search_type: 'pins',
  },
});
const url =
  'https://www.pinterest.com/resource/AdvancedTypeaheadResource/get/?source_url=' +
  encodeURIComponent('/search/pins/') +
  '&data=' +
  encodeURIComponent(dataPayload);

const headerSets = [
  {
    label: 'minimal',
    headers: {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    },
  },
  {
    label: 'xhr',
    headers: {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      Accept: 'application/json',
      'Accept-Language': 'en-US,en;q=0.9',
      'X-Requested-With': 'XMLHttpRequest',
      'X-Pinterest-AppState': 'active',
      Referer: 'https://www.pinterest.com/search/pins/',
      Origin: 'https://www.pinterest.com',
    },
  },
];

for (const { label, headers } of headerSets) {
  const r = await fetch(url, { headers });
  const t = await r.text();
  console.log(label, 'status', r.status, 'len', t.length, 'preview', t.slice(0, 120));
  try {
    const j = JSON.parse(t);
    const items = j?.resource_response?.data?.items || [];
    console.log(' items', items.length, items.slice(0, 4));
  } catch {}
}
