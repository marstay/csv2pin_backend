/**
 * Pinterest search snapshot — sample of pins ranking for a keyword.
 *
 * 1. Official Pinterest partner search (when app has pin_search access)
 * 2. RapidAPI unofficial Pinterest search (fallback — works for most apps)
 */
import fetch from 'node-fetch';
import { fetchRapidApiPinSearch, isRapidApiPinterestConfigured } from './pinterestRapidApi.js';

const SNAPSHOT_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const snapshotCache = new Map();

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

function normalizeOfficialPinRow(row) {
  const pin = row?.pin || row || {};
  const saves =
    pin.aggregated_pin_data?.aggregated_stats?.saves ??
    pin.repin_count ??
    pin.save_count ??
    null;
  const title = String(pin.title || pin.grid_title || pin.description || '').trim();
  const imageUrl =
    pin.images?.['236x']?.url ||
    pin.images?.['474x']?.url ||
    pin.image_medium_url ||
    pin.image_large_url ||
    '';
  const link = String(pin.link || pin.rich_summary?.site_url || '').trim();
  if (!title && saves == null && !imageUrl) return null;
  return {
    title: title.slice(0, 120),
    saves: Number.isFinite(Number(saves)) ? Number(saves) : null,
    imageUrl: imageUrl || null,
    link: link || null,
  };
}

function competitionFromPinCount(count, avgSaves) {
  if (count >= 30 || (avgSaves != null && avgSaves >= 500)) return 'High';
  if (count >= 10) return 'Medium';
  if (count <= 4 && (avgSaves == null || avgSaves < 100)) return 'Low';
  return 'Medium';
}

function summarizePins(query, pins, source) {
  const withSaves = pins.filter((p) => p.saves != null && p.saves > 0);
  const avgSaves =
    withSaves.length > 0
      ? Math.round(withSaves.reduce((s, p) => s + p.saves, 0) / withSaves.length)
      : null;
  const maxSaves = withSaves.length > 0 ? Math.max(...withSaves.map((p) => p.saves)) : null;

  const competitionLevel = competitionFromPinCount(pins.length, avgSaves);
  const hasSaves = withSaves.length > 0;

  return {
    available: pins.length > 0,
    query,
    source,
    sampleSize: pins.length,
    isSample: true,
    disclaimer: hasSaves
      ? 'A sample of top pins Pinterest shows for this search — not a full count, and not a guarantee of how your pins will perform.'
      : 'Top pins Pinterest shows for this search (save counts not included). Sample only — not a guarantee of performance.',
    pins: pins.slice(0, 10),
    stats: {
      pinsInSample: pins.length,
      pinsWithVisibleSaves: withSaves.length,
      avgSaves,
      maxSaves,
      competitionLevel,
    },
  };
}

function classifyPinterestSearchError(status, bodyText) {
  const body = String(bodyText || '').toLowerCase();
  if (status === 401 && body.includes('pin_search')) return 'pinterest_pin_search_restricted';
  if (status === 401 && body.includes('scope')) return 'pinterest_scope_missing';
  if (status === 403) return 'pinterest_forbidden';
  if (status === 429) return 'pinterest_rate_limited';
  return 'pinterest_search_api_unavailable';
}

async function tryOfficialPinterestSearch(query, token) {
  const q = encodeURIComponent(query);
  const url = `https://api.pinterest.com/v5/search/partner/pins?term=${q}&country_code=US&limit=25`;

  try {
    const resp = await fetchWithTimeout(url, {
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
    });
    const bodyText = await resp.text().catch(() => '');
    if (!resp.ok) {
      return {
        ok: false,
        reason: classifyPinterestSearchError(resp.status, bodyText),
        status: resp.status,
      };
    }
    const json = JSON.parse(bodyText || '{}');
    const bucket = json?.items || json?.data || json?.pins || json?.results || [];
    if (!Array.isArray(bucket) || bucket.length === 0) {
      return { ok: false, reason: 'pinterest_search_empty', status: resp.status };
    }
    const pins = bucket.map(normalizeOfficialPinRow).filter(Boolean);
    if (pins.length > 0) {
      return { ok: true, data: summarizePins(query, pins, 'pinterest_partner_search') };
    }
    return { ok: false, reason: 'pinterest_search_empty', status: resp.status };
  } catch {
    return { ok: false, reason: 'pinterest_search_api_unavailable', status: 0 };
  }
}

async function tryRapidApiPinterestSearch(query) {
  if (!isRapidApiPinterestConfigured()) {
    return { ok: false, reason: 'rapidapi_key_missing' };
  }
  const { pins, error } = await fetchRapidApiPinSearch(query, { num: 40 });
  if (pins.length > 0) {
    return { ok: true, data: summarizePins(query, pins, 'rapidapi_pinterest_search') };
  }
  return { ok: false, reason: error || 'rapidapi_empty' };
}

function unavailableSnapshot(query, reason) {
  const notes = {
    pinterest_token_missing:
      'Connect your Pinterest account for trend data. Pin previews need RAPIDAPI_KEY on the server.',
    pinterest_pin_search_restricted:
      'Could not load a pin preview for this search. Trend data and Amazon competition still feed your score.',
    pinterest_scope_missing:
      'Your Pinterest connection needs to be refreshed. Disconnect and reconnect in My Account.',
    pinterest_search_empty: 'Pinterest returned no pins for this exact search term.',
    pinterest_rate_limited: 'Pinterest is rate-limiting requests — try again in a few minutes.',
    rapidapi_key_missing:
      'Pin search preview unavailable — add RAPIDAPI_KEY and subscribe to the Unofficial Pinterest API on RapidAPI.',
    rapidapi_empty: 'No Pinterest pins found for this search term.',
    rapidapi_fetch_failed: 'Pinterest pin search temporarily unavailable — try again shortly.',
  };

  return {
    available: false,
    query,
    source: null,
    sampleSize: 0,
    isSample: true,
    reason,
    disclaimer:
      'We sample top Pinterest pins for this keyword when available. Trend data and Amazon competition still feed your score.',
    pins: [],
    stats: null,
    note: notes[reason] || 'Pin search preview unavailable — your score uses Pinterest Trends and Amazon data instead.',
  };
}

/**
 * Fetch a sample of Pinterest pins for the primary product keyword.
 */
export async function fetchPinterestSearchSnapshot(query, { getAccessToken } = {}) {
  const q = String(query || '').trim();
  if (!q) return unavailableSnapshot('', 'no_query');

  const cacheKey = q.toLowerCase();
  const cached = snapshotCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < SNAPSHOT_CACHE_TTL_MS) {
    return cached.data;
  }

  let result = null;
  const token = typeof getAccessToken === 'function' ? await getAccessToken() : null;

  if (token) {
    const official = await tryOfficialPinterestSearch(q, token);
    if (official.ok) {
      result = official.data;
    }
  }

  if (!result?.available) {
    const rapid = await tryRapidApiPinterestSearch(q);
    if (rapid.ok) {
      result = rapid.data;
    } else if (!result) {
      result = unavailableSnapshot(q, rapid.reason || 'rapidapi_fetch_failed');
    } else {
      // Official failed but we had a reason — keep unavailable with combined context
      result = unavailableSnapshot(q, rapid.reason || 'pinterest_pin_search_restricted');
    }
  }

  snapshotCache.set(cacheKey, { ts: Date.now(), data: result });
  return result;
}
