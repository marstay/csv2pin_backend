import fetch from 'node-fetch';

const query = process.argv[2] || 'bathroom organizer';
const url = `https://www.pinterest.com/search/pins/?q=${encodeURIComponent(query)}`;

const r = await fetch(url, {
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    Accept: 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
  },
  redirect: 'follow',
});
const html = await r.text();
console.log('status', r.status, 'len', html.length);

const pwsMatch = html.match(/<script id="__PWS_INITIAL_PROPS__"[^>]*>([\s\S]*?)<\/script>/);
const pwsDataMatch = html.match(/<script id="__PWS_DATA__"[^>]*>([\s\S]*?)<\/script>/);
console.log('PWS_INITIAL_PROPS', !!pwsMatch, 'PWS_DATA', !!pwsDataMatch);

if (pwsDataMatch) {
  try {
    const j = JSON.parse(pwsDataMatch[1]);
    const keys = Object.keys(j?.props?.initialReduxState || j || {}).slice(0, 15);
    console.log('redux keys', keys);
  } catch (e) {
    console.log('parse err', e.message);
  }
}

const saveMatches = [...html.matchAll(/"repin_count":(\d+)/g)].slice(0, 10);
const titleMatches = [...html.matchAll(/"grid_title":"([^"\\]+)"/g)].slice(0, 5);
console.log('repin samples', saveMatches.map((m) => m[1]));
console.log('titles', titleMatches.map((m) => m[1].slice(0, 60)));
