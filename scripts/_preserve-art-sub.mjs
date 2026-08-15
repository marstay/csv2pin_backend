/**
 * Does the copy step still hold when the SUBHEADLINE changes too, not just the headline?
 * Read-only: direct provider call, no quota, no DB, no endpoint.
 */
import { readFileSync, writeFileSync } from 'node:fs';

const envRaw = readFileSync(new URL('../.env', import.meta.url), 'utf8');
const env = {};
for (const l of envRaw.split(/\r?\n/)) {
  const t = l.trim();
  if (!t || t.startsWith('#')) continue;
  const m = t.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (m && !env[m[1]]) env[m[1]] = m[2].trim();
}
const BASE = (env.NANO_BANANA_API_URL || '').replace(/\/$/, '');
const KEY = env.NANO_BANANA_API_KEY;
const MODEL = env.NANO_BANANA_MODEL || 'nano-banana-2';
const OUTDIR = process.argv[2];

const SOURCE_PIN =
  'https://kowuqfmmrtwbkgntfxdi.supabase.co/storage/v1/object/public/ai-images/' +
  'urltopin-3b0b151e-015d-4462-aac7-e8bd9179322d-1786683616193-9787-minimal_elegant.png';

const PRESERVE =
  'The attached image is an EXISTING pin that must be reproduced, not reinterpreted. Recreate it ' +
  'as faithfully as you can: identical composition, layout, framing, background, product placement ' +
  'and scale, colour palette, lighting, and typographic style. Change exactly ONE thing — replace ' +
  'the written words with the specified headline, subheadline and footer line, keeping them in the ' +
  'same position, size, weight and colour as the text already present in the attached image. Do not ' +
  'redesign, restyle, re-crop, re-light or re-imagine any other element, and do not add or remove ' +
  'objects. Output a vertical 2:3 Pinterest pin.';

const FOOTER = 'As an Amazon Associate I earn from qualifying purchases';

const TESTS = [
  {
    id: 'D-subheadline-changed',
    // Original sub: "Hydrating toner pads for youthful, radiant skin."
    head: 'Say Goodbye to Dark Spots & Dull Skin!',
    sub: 'Snail mucin and niacinamide, 100 pads per jar.',
  },
  {
    id: 'E-sub-longer-rewrap',
    // Forces the subheadline from two lines to three.
    head: 'Achieve Your Best Glow with JIYU',
    sub: 'Korean toner pads with snail mucin and niacinamide that visibly soften dark spots in about four weeks.',
  },
];

async function run(t) {
  const prompt = `${PRESERVE}\n\nHEADLINE: "${t.head}"\nSUBHEADLINE: "${t.sub}"\nFOOTER: "${FOOTER}"`;
  const started = Date.now();
  const cj = await (
    await fetch(`${BASE}/createTask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` },
      body: JSON.stringify({
        model: MODEL,
        input: { prompt, aspect_ratio: '2:3', google_search: false, output_format: 'png', image_input: [SOURCE_PIN] },
      }),
    })
  ).json().catch(() => ({}));
  const taskId = cj?.data?.taskId;
  if (!taskId) return `createTask failed: ${JSON.stringify(cj).slice(0, 180)}`;

  for (let i = 0; i < 140; i += 1) {
    await new Promise((r) => setTimeout(r, 2500));
    const info = await (
      await fetch(`${BASE}/recordInfo?taskId=${encodeURIComponent(taskId)}`, { headers: { Authorization: `Bearer ${KEY}` } })
    ).json().catch(() => ({}));
    const d = info?.data;
    const state = String(d?.state || '').toLowerCase();
    if (state === 'success') {
      let url = null;
      try { url = JSON.parse(d?.resultJson || '{}')?.resultUrls?.[0] || null; } catch { /* ignore */ }
      if (!url) return 'success but no url';
      const buf = Buffer.from(await (await fetch(url)).arrayBuffer());
      writeFileSync(`${OUTDIR}/${t.id}.png`, buf);
      return `ok ${Math.round((Date.now() - started) / 1000)}s -> ${t.id}.png`;
    }
    if (state === 'fail' || state === 'failed') return `FAILED ${d?.failCode ?? ''} ${d?.failMsg ?? ''}`;
  }
  return 'timed out';
}

for (const t of TESTS) {
  process.stdout.write(`${t.id} ... `);
  console.log(await run(t));
}
