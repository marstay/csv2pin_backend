/**
 * READ-ONLY feasibility check for a "we analyzed N Pinterest pins" data report.
 * Aggregates only — writes nothing, exposes no individual user/pin.
 *
 * Usage: node backend/scripts/_analytics-feasibility.mjs
 */
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in backend/.env');
  process.exit(1);
}
const sb = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

async function count(table, apply = (q) => q) {
  try {
    const { count, error } = await apply(sb.from(table).select('*', { count: 'exact', head: true }));
    if (error) return `err(${error.message.slice(0, 60)})`;
    return count;
  } catch (e) {
    return `err(${String(e.message || e).slice(0, 60)})`;
  }
}

async function fetchAll(table, columns, apply = (q) => q) {
  const out = [];
  const size = 1000;
  for (let from = 0; from < 300000; from += size) {
    const { data, error } = await apply(sb.from(table).select(columns).range(from, from + size - 1));
    if (error) {
      console.error(`  fetch ${table} error: ${error.message}`);
      break;
    }
    out.push(...(data || []));
    if (!data || data.length < size) break;
  }
  return out;
}

const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

(async () => {
  console.log('READ-ONLY analytics feasibility check\n');

  for (const table of ['scheduled_pins', 'user_images']) {
    console.log(`=== ${table} ===`);
    console.log('  total rows:            ', await count(table));
    console.log('  impressions > 0:       ', await count(table, (q) => q.gt('impressions', 0)));
    console.log('  saves > 0:             ', await count(table, (q) => q.gt('saves', 0)));
    console.log('  outbound_clicks > 0:   ', await count(table, (q) => q.gt('outbound_clicks', 0)));
    console.log('  pin_clicks > 0:        ', await count(table, (q) => q.gt('pin_clicks', 0)));
    console.log('');
  }

  // Deeper look at the primary source: pins with real Pinterest data.
  console.log('=== scheduled_pins with impressions>0 — shape ===');
  const rows = await fetchAll(
    'scheduled_pins',
    'user_id, posted_at, updated_at, impressions, saves, pin_clicks, outbound_clicks, engagement_rate',
    (q) => q.gt('impressions', 0)
  );
  console.log('  rows pulled:           ', rows.length);
  if (rows.length) {
    const users = new Set(rows.map((r) => r.user_id).filter(Boolean));
    const withOutbound = rows.filter((r) => (r.outbound_clicks || 0) > 0).length;
    const withTime = rows.filter((r) => r.posted_at || r.updated_at);
    const dates = withTime
      .map((r) => new Date(r.posted_at || r.updated_at))
      .filter((d) => !Number.isNaN(d.getTime()));
    const min = dates.length ? new Date(Math.min(...dates)) : null;
    const max = dates.length ? new Date(Math.max(...dates)) : null;

    const sum = (k) => rows.reduce((a, r) => a + (Number(r[k]) || 0), 0);

    const byHour = new Array(24).fill(0);
    const byDow = new Array(7).fill(0);
    for (const d of dates) {
      byHour[d.getUTCHours()] += 1;
      byDow[d.getUTCDay()] += 1;
    }

    console.log('  distinct users:        ', users.size);
    console.log('  with outbound_clicks>0:', withOutbound);
    console.log('  with a timestamp:      ', withTime.length);
    console.log('  date span:             ', min ? min.toISOString().slice(0, 10) : '—', '→', max ? max.toISOString().slice(0, 10) : '—');
    console.log('  TOTAL impressions:     ', sum('impressions').toLocaleString());
    console.log('  TOTAL saves:           ', sum('saves').toLocaleString());
    console.log('  TOTAL pin_clicks:      ', sum('pin_clicks').toLocaleString());
    console.log('  TOTAL outbound_clicks: ', sum('outbound_clicks').toLocaleString());
    console.log('  posts by day-of-week:  ', DOW.map((d, i) => `${d}:${byDow[i]}`).join(' '));
    console.log('  posts by hour (UTC):   ', byHour.map((n, h) => (n ? `${h}h:${n}` : '')).filter(Boolean).join(' '));

    // ---- Phase 2: the actual findings ----
    const eng = (r) => (Number(r.saves) || 0) + (Number(r.pin_clicks) || 0) + (Number(r.outbound_clicks) || 0);
    const timed = rows
      .map((r) => ({ ...r, d: new Date(r.posted_at || r.updated_at) }))
      .filter((r) => !Number.isNaN(r.d.getTime()));

    // Average impressions + engagement by day-of-week
    const dayAgg = DOW.map(() => ({ n: 0, impr: 0, eng: 0 }));
    const hourAgg = Array.from({ length: 24 }, () => ({ n: 0, impr: 0, eng: 0 }));
    for (const r of timed) {
      const dw = r.d.getUTCDay();
      const h = r.d.getUTCHours();
      dayAgg[dw].n += 1; dayAgg[dw].impr += Number(r.impressions) || 0; dayAgg[dw].eng += eng(r);
      hourAgg[h].n += 1; hourAgg[h].impr += Number(r.impressions) || 0; hourAgg[h].eng += eng(r);
    }
    const avg = (a) => (a.n ? a.impr / a.n : 0);
    console.log('\n  --- AVG IMPRESSIONS per pin, by day (UTC) ---');
    DOW.forEach((d, i) => console.log(`    ${d}: ${avg(dayAgg[i]).toFixed(0)} avg impr  (n=${dayAgg[i].n})`));
    const bestDay = DOW.map((d, i) => [d, avg(dayAgg[i])]).sort((a, b) => b[1] - a[1]);
    console.log('    → best days:', bestDay.slice(0, 3).map(([d, v]) => `${d}(${v.toFixed(0)})`).join(', '));

    console.log('\n  --- AVG IMPRESSIONS per pin, by hour (UTC), where n>=20 ---');
    const hourRanked = hourAgg
      .map((a, h) => ({ h, n: a.n, avgImpr: avg(a) }))
      .filter((x) => x.n >= 20)
      .sort((a, b) => b.avgImpr - a.avgImpr);
    hourRanked.slice(0, 6).forEach((x) => console.log(`    ${String(x.h).padStart(2, '0')}:00 UTC — ${x.avgImpr.toFixed(0)} avg impr (n=${x.n})`));

    // Distribution / rates
    const imprArr = timed.map((r) => Number(r.impressions) || 0).sort((a, b) => a - b);
    const median = imprArr[Math.floor(imprArr.length / 2)];
    const totalImpr = sum('impressions'), totalPinClicks = sum('pin_clicks'), totalOut = sum('outbound_clicks');
    console.log('\n  --- distribution / rates ---');
    console.log('    median impressions/pin:', median);
    console.log('    mean impressions/pin:  ', (totalImpr / timed.length).toFixed(1));
    console.log('    overall pin-click rate:', ((totalPinClicks / totalImpr) * 100).toFixed(2) + '%');
    console.log('    overall outbound rate: ', ((totalOut / totalImpr) * 100).toFixed(2) + '%');

    // Concentration: is one account dominating?
    const byUser = new Map();
    for (const r of rows) {
      const u = r.user_id || 'null';
      const cur = byUser.get(u) || { n: 0, impr: 0 };
      cur.n += 1; cur.impr += Number(r.impressions) || 0;
      byUser.set(u, cur);
    }
    const userRanked = [...byUser.values()].sort((a, b) => b.impr - a.impr);
    const top1 = userRanked[0], top3 = userRanked.slice(0, 3).reduce((a, b) => a + b.impr, 0);
    console.log('\n  --- concentration (skew check) ---');
    console.log(`    top account: ${top1.n} pins, ${((top1.impr / totalImpr) * 100).toFixed(0)}% of impressions`);
    console.log(`    top 3 accounts: ${((top3 / totalImpr) * 100).toFixed(0)}% of impressions`);
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
