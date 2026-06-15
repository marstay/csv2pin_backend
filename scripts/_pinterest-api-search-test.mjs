import dotenv from 'dotenv';
import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const query = process.argv[2] || 'air fryer';
const q = encodeURIComponent(query);

async function getToken() {
  const envTok = String(process.env.PINTEREST_TRENDS_ACCESS_TOKEN || '').trim();
  if (envTok) return { token: envTok, source: 'env' };
  const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
  const { data: accounts } = await supabase
    .from('pinterest_accounts')
    .select('access_token, account_name')
    .not('access_token', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(1);
  if (accounts?.[0]?.access_token) {
    return { token: accounts[0].access_token, source: accounts[0].account_name || 'db' };
  }
  return { token: null, source: 'none' };
}

const { token, source } = await getToken();
console.log('token source:', source, 'has token:', !!token);
if (!token) process.exit(1);

const endpoints = [
  `https://api.pinterest.com/v5/search/pins?query=${q}&country_code=US&limit=10`,
  `https://api.pinterest.com/v5/search/partner/pins?term=${q}&country_code=US&limit=10`,
  `https://api.pinterest.com/v5/trends/keywords/US/top/growing?limit=3&include_keywords=${q}`,
];

for (const url of endpoints) {
  const r = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
  });
  const body = await r.text();
  let count = '?';
  try {
    const j = JSON.parse(body);
    const bucket = j?.items || j?.data || j?.pins || j?.results || j?.trends || [];
    count = Array.isArray(bucket) ? bucket.length : 'n/a';
  } catch {
    count = 'parse fail';
  }
  console.log('\n', url.split('?')[0].split('/v5/')[1]);
  console.log(' status', r.status, 'items', count, 'body', body.slice(0, 200));
}
