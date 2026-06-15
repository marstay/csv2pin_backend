import dotenv from 'dotenv';
import fetch from 'node-fetch';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const key = String(process.env.RAPIDAPI_KEY || '').trim();
const host = 'unofficial-pinterest-api.p.rapidapi.com';

const r = await fetch(
  `https://${host}/pinterest/pins/relevance?keyword=bathroom%20organizer&num=10`,
  { headers: { 'x-rapidapi-key': key, 'x-rapidapi-host': host } }
);
const j = await r.json();
const pins = (j?.data || []).filter((p) => p && typeof p === 'object' && p.images);

for (const pin of pins.slice(0, 3)) {
  console.log('---');
  console.log('title', pin.grid_title || pin.title);
  console.log('aggregated_pin_data', JSON.stringify(pin.aggregated_pin_data, null, 2));
  console.log('reaction_counts', JSON.stringify(pin.reaction_counts, null, 2));
}
