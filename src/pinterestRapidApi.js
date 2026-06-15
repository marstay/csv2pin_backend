/**
 * Unofficial Pinterest API via RapidAPI — pin search + keyword suggestions.
 * https://rapidapi.com/asyncsolutions-asyncsolutions-default/api/unofficial-pinterest-api
 *
 * Used when Pinterest's official partner search is unavailable (pin_search restriction).
 * Requires RAPIDAPI_KEY and a subscription to the API on RapidAPI.
 */
import fetch from 'node-fetch';

const DEFAULT_HOST = 'unofficial-pinterest-api.p.rapidapi.com';

function readIntEnv(name, fallback, min, max) {
  const n = Number(process.env[name] ?? fallback);
  const value = Number.isFinite(n) ? n : fallback;
  return Math.max(min, Math.min(max, value));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 20000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

function rapidConfig() {
  const key = String(process.env.RAPIDAPI_KEY || '').trim();
  const host = String(process.env.RAPIDAPI_PINTEREST_HOST || DEFAULT_HOST).trim();
  return {
    key,
    host,
    headers: {
      'x-rapidapi-key': key,
      'x-rapidapi-host': host,
      Accept: 'application/json',
    },
  };
}

export function isRapidApiPinterestConfigured() {
  return !!String(process.env.RAPIDAPI_KEY || '').trim();
}

export function normalizeRapidApiPinRow(row) {
  if (!row || typeof row !== 'object') return null;

  let title = row.grid_title || row.title || row.description || '';
  if (title && typeof title === 'object') title = '';
  title = String(title).trim();
  if (title === '[object Object]') title = '';

  const rawSaves =
    row.aggregated_pin_data?.aggregated_stats?.saves ??
    row.repin_count ??
    row.save_count ??
    null;
  const savesNum = Number(rawSaves);
  const saves = Number.isFinite(savesNum) && savesNum > 0 ? savesNum : null;

  const imageUrl =
    row.images?.['236x']?.url ||
    row.images?.['474x']?.url ||
    row.images?.orig?.url ||
    row.image_medium_url ||
    row.image_large_url ||
    '';

  const link = String(row.link || row.rich_summary?.site_url || '').trim();
  if (!title && !imageUrl) return null;

  return {
    title: title.slice(0, 120) || 'Pin',
    saves,
    imageUrl: imageUrl || null,
    link: link || null,
  };
}

/**
 * Search top relevant Pinterest pins for a keyword.
 */
export async function fetchRapidApiPinSearch(query, { num = 40 } = {}) {
  const q = String(query || '').trim();
  const { key, host, headers } = rapidConfig();
  if (!key) return { pins: [], error: 'rapidapi_key_missing' };
  if (!q) return { pins: [], error: 'no_query' };

  const limit = readIntEnv('RAPIDAPI_PINTEREST_PINS_NUM', num, 5, 50);
  const url =
    `https://${host}/pinterest/pins/relevance?keyword=${encodeURIComponent(q)}&num=${limit}`;

  try {
    const resp = await fetchWithTimeout(url, { headers }, 25000);
    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      console.warn('RapidAPI Pinterest pin search:', resp.status, body.slice(0, 200));
      return { pins: [], error: `rapidapi_http_${resp.status}` };
    }
    const json = await resp.json().catch(() => null);
    const bucket = Array.isArray(json?.data) ? json.data : [];
    const pins = bucket.map(normalizeRapidApiPinRow).filter(Boolean);
    return { pins, error: pins.length ? null : 'rapidapi_empty' };
  } catch (e) {
    console.warn('RapidAPI Pinterest pin search:', e?.message || e);
    return { pins: [], error: 'rapidapi_fetch_failed' };
  }
}

/**
 * Pinterest keyword suggestions for a seed term.
 */
export async function fetchRapidApiSuggestions(keyword) {
  const term = String(keyword || '').trim();
  const { key, host, headers } = rapidConfig();
  if (!key || term.length < 2) return [];

  const url =
    `https://${host}/pinterest/helper/suggestions?keyword=${encodeURIComponent(term)}`;

  try {
    const resp = await fetchWithTimeout(url, { headers }, 12000);
    if (!resp.ok) return [];
    const json = await resp.json().catch(() => null);
    const data = json?.data;
    if (!Array.isArray(data)) return [];
    return data
      .map((item) => (typeof item === 'string' ? item.trim() : String(item || '').trim()))
      .filter((s) => s.length >= 2)
      .slice(0, 12);
  } catch {
    return [];
  }
}
