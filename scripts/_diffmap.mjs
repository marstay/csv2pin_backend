/**
 * Where do two generations actually differ? Row-band difference profile.
 * If the copy step works, differences concentrate in the text bands and the
 * product region stays near-identical.
 */
import sharp from 'sharp';

const D = 'C:/Users/ARIS~1.GEO/AppData/Local/Temp/claude/c--Users-aris-georgiopoulos-backup-28-7-25-pinFactory/e4b39c16-c9a6-47c2-a4cc-a2d7ae10835e/scratchpad';
const [fa, fb] = process.argv.slice(2);

// removeAlpha() is essential: the source pin is 3-channel RGB while provider output is 4-channel
// RGBA. Indexing both with one stride reads misaligned bytes and reports ~80/255 difference
// between images that are in fact nearly identical.
const load = async (f) => {
  const { data, info } = await sharp(`${D}/${f}`)
    .resize(424, 632, { fit: 'fill' })
    .removeAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });
  if (info.channels !== 3) throw new Error(`${f}: expected 3 channels, got ${info.channels}`);
  return { data, W: info.width, H: info.height, C: 3 };
};

const A = await load(fa);
const B = await load(fb);
const { W, H, C } = A;

const BANDS = 20;
const bandH = Math.floor(H / BANDS);
console.log(`${fa}\n  vs\n${fb}\n`);
console.log('band   y-range        mean abs diff (0-255)   changed >12');
let totalDiff = 0;
for (let b = 0; b < BANDS; b += 1) {
  let sum = 0, n = 0, changed = 0;
  for (let y = b * bandH; y < (b + 1) * bandH && y < H; y += 1) {
    for (let x = 0; x < W; x += 1) {
      const i = (y * W + x) * C;
      const d = (Math.abs(A.data[i] - B.data[i]) + Math.abs(A.data[i + 1] - B.data[i + 1]) + Math.abs(A.data[i + 2] - B.data[i + 2])) / 3;
      sum += d; n += 1;
      if (d > 12) changed += 1;
    }
  }
  const mean = sum / n;
  totalDiff += sum;
  const pct = (100 * changed) / n;
  const bar = '#'.repeat(Math.min(40, Math.round(pct / 2)));
  console.log(
    `${String(b).padStart(3)}   ${String(b * bandH).padStart(4)}-${String((b + 1) * bandH).padStart(4)}   ${mean.toFixed(2).padStart(8)}            ${pct.toFixed(1).padStart(5)}%  ${bar}`
  );
}
console.log(`\noverall mean abs diff: ${(totalDiff / (W * H)).toFixed(2)} / 255`);
