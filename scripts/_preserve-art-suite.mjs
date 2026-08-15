/**
 * Read-only experiment suite for "keep the art, change the words".
 * No DB writes, no quota, no endpoint — calls the image provider directly.
 *
 * A  production-shaped STYLE prompt + preserve suffix   -> does the style text fight the copy?
 * B  preserve-only, LONG headline                       -> does a rewrap wreck the layout?
 * C  preserve-only, repeat of the first run             -> how stable is it run to run?
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

const SAME_SUB = 'Hydrating toner pads for youthful, radiant skin.';
const SAME_FOOTER = 'As an Amazon Associate I earn from qualifying purchases';

// Verbatim from appendNanoBananaReferencePromptSuffix(..., 'previous_pin')
const PRESERVE =
  'The attached image is an EXISTING pin that must be reproduced, not reinterpreted. Recreate it ' +
  'as faithfully as you can: identical composition, layout, framing, background, product placement ' +
  'and scale, colour palette, lighting, and typographic style. Change exactly ONE thing — replace ' +
  'the written words with the specified headline, subheadline and footer line, keeping them in the ' +
  'same position, size, weight and colour as the text already present in the attached image. Do not ' +
  'redesign, restyle, re-crop, re-light or re-imagine any other element, and do not add or remove ' +
  'objects. Output a vertical 2:3 Pinterest pin.';

// Production-shaped style prompt: real constants from index.js (REALISTIC_PREFIX_LONG,
// SCROLL_STOPPING_RULE_LONG) plus a minimal_elegant subject/overlay clause of the usual shape.
const STYLE_PROMPT =
  'Vertical Pinterest pin 1000x1500 px. The image must immediately stand out in a Pinterest feed ' +
  'and create a strong visual contrast compared to typical pins in this niche. Photorealistic, ' +
  'high-quality photograph. Natural lighting, lifelike imagery, professional photography style. ' +
  'Minimal elegant editorial layout with generous negative space, a single hero product photographed ' +
  'from above on a soft seamless background, refined serif-free typography, and a calm premium mood. ' +
  'Place the headline across the upper third in large bold type, the subheadline directly beneath it ' +
  'in a lighter weight, and a small footer line centred at the bottom. Keep all text crisp, legible ' +
  'on mobile, and free of spelling errors. Use a cohesive brand palette of soft greens and warm neutrals.';

const textBlock = (h) => `\n\nHEADLINE: "${h}"\nSUBHEADLINE: "${SAME_SUB}"\nFOOTER: "${SAME_FOOTER}"`;

const TESTS = [
  {
    id: 'A-style-prompt-interference',
    headline: 'Say Goodbye to Dark Spots & Dull Skin!',
    prompt: `${STYLE_PROMPT} ${PRESERVE}${textBlock('Say Goodbye to Dark Spots & Dull Skin!')}`,
  },
  {
    id: 'B-long-headline',
    headline: 'The Korean Toner Pads That Finally Cleared My Dull, Uneven Skin After Years of Trying',
    prompt: `${PRESERVE}${textBlock('The Korean Toner Pads That Finally Cleared My Dull, Uneven Skin After Years of Trying')}`,
  },
  {
    id: 'C-consistency-repeat',
    headline: 'Say Goodbye to Dark Spots & Dull Skin!',
    prompt: `${PRESERVE}${textBlock('Say Goodbye to Dark Spots & Dull Skin!')}`,
  },
];

async function generate(t) {
  const started = Date.now();
  const createRes = await fetch(`${BASE}/createTask`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` },
    body: JSON.stringify({
      model: MODEL,
      input: {
        prompt: t.prompt,
        aspect_ratio: '2:3',
        google_search: false,
        output_format: 'png',
        image_input: [SOURCE_PIN],
      },
    }),
  });
  const cj = await createRes.json().catch(() => ({}));
  const taskId = cj?.data?.taskId;
  if (!taskId) return { ...t, error: `createTask: ${JSON.stringify(cj).slice(0, 200)}` };

  for (let i = 0; i < 140; i += 1) {
    await new Promise((r) => setTimeout(r, 2500));
    const info = await (
      await fetch(`${BASE}/recordInfo?taskId=${encodeURIComponent(taskId)}`, {
        headers: { Authorization: `Bearer ${KEY}` },
      })
    ).json().catch(() => ({}));
    const d = info?.data;
    const state = String(d?.state || '').toLowerCase();
    if (state === 'success') {
      let url = null;
      try {
        const urls = JSON.parse(d?.resultJson || '{}')?.resultUrls;
        url = Array.isArray(urls) ? urls[0] : null;
      } catch { /* ignore */ }
      if (!url) return { ...t, error: 'success but no url' };
      const buf = Buffer.from(await (await fetch(url)).arrayBuffer());
      const file = `${OUTDIR}/${t.id}.png`;
      writeFileSync(file, buf);
      const dim =
        buf.slice(0, 8).toString('hex') === '89504e470d0a1a0a'
          ? `${buf.readUInt32BE(16)}x${buf.readUInt32BE(20)}`
          : 'n/a';
      return { ...t, file, secs: Math.round((Date.now() - started) / 1000), mb: (buf.length / 1048576).toFixed(2), dim };
    }
    if (state === 'fail' || state === 'failed') {
      return { ...t, error: `${d?.failCode ?? ''} ${d?.failMsg ?? ''}`.trim() };
    }
  }
  return { ...t, error: 'timed out' };
}

console.log(`model=${MODEL}  tests=${TESTS.length}\n`);
for (const t of TESTS) {
  process.stdout.write(`running ${t.id} ... `);
  const r = await generate(t);
  console.log(r.error ? `FAILED: ${r.error}` : `ok  ${r.secs}s  ${r.mb}MB  ${r.dim}  -> ${r.id}.png`);
}
console.log('\ndone');
