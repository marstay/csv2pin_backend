/**
 * Generate a real pin image through a provider and report quality/latency/cost signals.
 *
 * Run this BEFORE switching production. It uses the same request shape the app uses (2:3, 1K,
 * png) so the result is representative, and it downloads the image so you can look at it.
 *
 *   node backend/scripts/test-image-provider.mjs kie
 *   node backend/scripts/test-image-provider.mjs crun   CRUN_API_KEY=xxx
 *   node backend/scripts/test-image-provider.mjs both   CRUN_API_KEY=xxx    # A/B the same prompt
 *
 * Reads NANO_BANANA_API_KEY from backend/.env for kie, and CRUN_API_KEY from the environment
 * (or --key) for crun. Nothing is written to the database and production is untouched.
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { writeFileSync, mkdirSync } from 'node:fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const WHICH = (process.argv[2] || 'both').toLowerCase();
const keyArgIdx = process.argv.indexOf('--key');
const CRUN_KEY = (keyArgIdx > -1 ? process.argv[keyArgIdx + 1] : process.env.CRUN_API_KEY) || '';

// Mirrors IMAGE_PROVIDERS in src/index.js — keep in sync if that changes.
const PROVIDERS = {
  kie: {
    label: 'Kie.ai',
    base: 'https://api.kie.ai/api/v1/jobs',
    createPath: 'createTask',
    pollPath: 'recordInfo',
    pollParam: 'taskId',
    model: 'nano-banana-2',
    imagesField: 'image_input',
    key: process.env.NANO_BANANA_API_KEY,
    authHeaders: (k) => ({ Authorization: `Bearer ${k}` }),
    extractTaskId: (j) => j?.data?.taskId || null,
    extractState: (d) => d?.state,
    extractUrl: (d) => {
      try {
        const u = JSON.parse(d?.resultJson || '{}')?.resultUrls;
        return Array.isArray(u) ? u[0] : null;
      } catch { return null; }
    },
    credits: () => null,
  },
  crun: {
    label: 'crun.ai',
    base: 'https://api.crun.ai/api/v1/client/job',
    createPath: 'CreateTask',
    pollPath: 'TaskInfo',
    pollParam: 'task_id',
    model: process.env.CRUN_MODEL || 'google/nano-banana-2',
    imagesField: 'img_urls',
    key: CRUN_KEY,
    authHeaders: (k) => ({ 'X-API-KEY': k }),
    extractTaskId: (j) => j?.data?.task_id || null,
    extractState: (d) => d?.status,
    extractUrl: (d) => (Array.isArray(d?.result?.media_urls) ? d.result.media_urls[0] : null),
    credits: (d) => d?.credits ?? null,
  },
};

// A realistic pin prompt — not a trivial one, so quality differences actually show.
const PROMPT =
  'A bright, airy flat-lay Pinterest pin for a stainless steel insulated water bottle. ' +
  'Soft natural window light, warm neutral background with subtle linen texture, a sprig of ' +
  'eucalyptus and a folded towel beside the bottle. Clean lifestyle product photography, ' +
  'shallow depth of field, generous empty space in the upper third for a text overlay.';

const IN_PROGRESS = new Set(['pending', 'running', 'waiting', 'queued', 'queuing', 'generating', 'processing', 'in_progress', 'submitted', 'created']);

async function run(p) {
  if (!p.key) return console.log(`\n${p.label}: SKIPPED — no API key provided`);
  console.log(`\n=== ${p.label} (${p.model}) ===`);
  const t0 = Date.now();

  const createRes = await fetch(`${p.base}/${p.createPath}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...p.authHeaders(p.key) },
    body: JSON.stringify({
      model: p.model,
      input: { prompt: PROMPT, aspect_ratio: '2:3', google_search: false, resolution: '1K', output_format: 'png' },
    }),
  });
  const createJson = await createRes.json().catch(() => ({}));
  const taskId = p.extractTaskId(createJson);
  if (!createRes.ok || !taskId) {
    console.log(`  CREATE FAILED  HTTP ${createRes.status}  ${JSON.stringify(createJson).slice(0, 300)}`);
    return;
  }
  console.log(`  task created in ${Date.now() - t0}ms  id=${taskId}`);

  let url = null, state = '', credits = null;
  for (let i = 0; i < 150; i++) {
    await new Promise((r) => setTimeout(r, 2000));
    const r = await fetch(`${p.base}/${p.pollPath}?${p.pollParam}=${encodeURIComponent(taskId)}`, {
      headers: p.authHeaders(p.key),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || j.code !== 200 || !j.data) { console.log(`  poll error HTTP ${r.status} ${JSON.stringify(j).slice(0, 160)}`); continue; }
    state = String(p.extractState(j.data) || '').toLowerCase();
    credits = p.credits(j.data);
    if (IN_PROGRESS.has(state)) { if (i % 10 === 9) console.log(`  ...${state} (${Math.round((Date.now() - t0) / 1000)}s)`); continue; }
    url = p.extractUrl(j.data);
    if (!url) console.log(`  finished with state="${state}" and no image: ${JSON.stringify(j.data).slice(0, 300)}`);
    break;
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  if (!url) return console.log(`  RESULT: no image after ${elapsed}s (last state "${state}")`);

  const img = await fetch(url);
  const buf = Buffer.from(await img.arrayBuffer());
  const isPng = buf[0] === 0x89 && buf[1] === 0x50;
  const isJpg = buf[0] === 0xff && buf[1] === 0xd8;
  const dir = resolve(__dirname, '../../provider-test');
  mkdirSync(dir, { recursive: true });
  const out = resolve(dir, `${p.label.replace(/\W/g, '')}.${isPng ? 'png' : isJpg ? 'jpg' : 'bin'}`);
  writeFileSync(out, buf);

  console.log(`  RESULT: success in ${elapsed}s`);
  console.log(`    format      ${isPng ? 'PNG' : isJpg ? 'JPG (NOT png — output_format ignored?)' : 'unknown'}`);
  console.log(`    size        ${(buf.length / 1024).toFixed(0)} KB`);
  if (credits != null) console.log(`    credits     ${credits}`);
  console.log(`    saved to    ${out}`);
}

console.log('Generating the SAME prompt on each provider — open the saved files side by side.');
if (WHICH === 'kie' || WHICH === 'both') await run(PROVIDERS.kie);
if (WHICH === 'crun' || WHICH === 'both') await run(PROVIDERS.crun);
console.log('\nCompare: visual quality, latency, PNG vs JPG, and whether 2:3 was respected.');
