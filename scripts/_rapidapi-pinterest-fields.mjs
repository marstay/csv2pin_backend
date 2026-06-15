import dotenv from 'dotenv';
import fetch from 'node-fetch';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const key = String(process.env.RAPIDAPI_KEY || '').trim();
const host = 'unofficial-pinterest-api.p.rapidapi.com';
const keyword = process.argv[2] || 'air fryer';

const r = await fetch(
  `https://${host}/pinterest/pins/relevance?keyword=${encodeURIComponent(keyword)}&num=20`,
  { headers: { 'x-rapidapi-key': key, 'x-rapidapi-host': host } }
);
const j = await r.json();
const pins = j?.data || [];

console.log('pins', pins.length);
for (const pin of pins.slice(0, 5)) {
  const title = String(pin.grid_title || pin.title || pin.description || '').slice(0, 60);
  const saves =
    pin.aggregated_pin_data?.aggregated_stats?.saves ??
    pin.repin_count ??
    pin.save_count ??
    null;
  const imageUrl =
    pin.images?.['236x']?.url ||
    pin.images?.['474x']?.url ||
    pin.images?.orig?.url ||
    pin.image_medium_url ||
    null;
  console.log({
    title,
    saves,
    link: String(pin.link || '').slice(0, 60),
    imageUrl: imageUrl?.slice(0, 50),
    keys: Object.keys(pin).filter((k) => /save|repin|aggregat|count|stat/i.test(k)),
  });
}

// Deep search for save-like fields in first pin
if (pins[0]) {
  const flat = JSON.stringify(pins[0]);
  const saveMatches = flat.match(/"(repin_count|save_count|saves)":\s*\d+/g);
  console.log('\nsave fields in pin[0]:', saveMatches || 'none found');
}

// suggestions structure
const sr = await fetch(
  `https://${host}/pinterest/helper/suggestions?keyword=${encodeURIComponent(keyword.split(' ')[0])}`,
  { headers: { 'x-rapidapi-key': key, 'x-rapidapi-host': host } }
);
const sj = await sr.json();
console.log('\nsuggestions raw data type:', typeof sj?.data, Array.isArray(sj?.data));
console.log('suggestions sample:', JSON.stringify(sj?.data).slice(0, 400));
