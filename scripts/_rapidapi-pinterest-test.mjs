import dotenv from 'dotenv';
import fetch from 'node-fetch';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const key = String(process.env.RAPIDAPI_KEY || '').trim();
const host = 'unofficial-pinterest-api.p.rapidapi.com';
const keyword = process.argv[2] || 'air fryer';

if (!key) {
  console.log('RAPIDAPI_KEY not set in backend/.env');
  process.exit(1);
}

const paths = [
  `/pinterest/pins/relevance?keyword=${encodeURIComponent(keyword)}&num=20`,
  `/pins/relevance?keyword=${encodeURIComponent(keyword)}&num=20`,
  `/pinterest/helper/suggestions?keyword=${encodeURIComponent(keyword.split(' ')[0])}`,
  `/helper/suggestions?keyword=${encodeURIComponent(keyword.split(' ')[0])}`,
];

for (const p of paths) {
  const t0 = Date.now();
  try {
    const r = await fetch(`https://${host}${p}`, {
      headers: {
        'x-rapidapi-key': key,
        'x-rapidapi-host': host,
        Accept: 'application/json',
      },
    });
    const text = await r.text();
    const ms = Date.now() - t0;
    console.log('\n---', p.split('?')[0], '---');
    console.log('status', r.status, 'ms', ms, 'len', text.length);
    try {
      const j = JSON.parse(text);
      const topKeys = j && typeof j === 'object' ? Object.keys(j).slice(0, 10) : [];
      console.log('top keys', topKeys);
      const bucket =
        j?.data ||
        j?.pins ||
        j?.results ||
        j?.items ||
        (Array.isArray(j) ? j : null);
      if (Array.isArray(bucket)) {
        console.log('items', bucket.length);
        const first = bucket[0];
        if (first) {
          console.log('first keys', Object.keys(first).slice(0, 20));
          console.log('sample', JSON.stringify(first, null, 2).slice(0, 800));
        }
      } else if (j?.suggestions || j?.data?.suggestions) {
        const sug = j.suggestions || j.data?.suggestions || j.data;
        console.log('suggestions', JSON.stringify(sug).slice(0, 400));
      } else {
        console.log('preview', text.slice(0, 500));
      }
    } catch {
      console.log('preview', text.slice(0, 300));
    }
  } catch (e) {
    console.log('error', e.message);
  }
}
