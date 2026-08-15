/**
 * Objective drift measurement.
 *
 * The "Viral on TikTok" badge is a saturated pink disc unique in the frame, so its bounding box
 * is a clean proxy for the jar's scale and vertical position. Eyeballing "about 10-15% larger"
 * is not a measurement; this is.
 */
import sharp from 'sharp';

const D = 'C:/Users/ARIS~1.GEO/AppData/Local/Temp/claude/c--Users-aris-georgiopoulos-backup-28-7-25-pinFactory/e4b39c16-c9a6-47c2-a4cc-a2d7ae10835e/scratchpad';

const isPink = (r, g, b) => r > 170 && g < 120 && r - g > 85 && b > 60 && b < 190;

async function badge(file) {
  const { data, info } = await sharp(`${D}/${file}`)
    .raw()
    .toBuffer({ resolveWithObject: true });
  const { width: W, height: H, channels: C } = info;
  let minX = 1e9, minY = 1e9, maxX = -1, maxY = -1, n = 0;
  for (let y = 0; y < H; y += 1) {
    for (let x = 0; x < W; x += 1) {
      const i = (y * W + x) * C;
      if (isPink(data[i], data[i + 1], data[i + 2])) {
        n += 1;
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (n === 0) return { file, found: false, W, H };
  return {
    file,
    found: true,
    W,
    H,
    px: n,
    w: maxX - minX + 1,
    h: maxY - minY + 1,
    cx: Math.round((minX + maxX) / 2),
    cy: Math.round((minY + maxY) / 2),
    // normalised so a resize can't confound the comparison
    wPct: ((maxX - minX + 1) / W) * 100,
    cyPct: (((minY + maxY) / 2) / H) * 100,
  };
}

const files = process.argv.slice(2);
const rows = [];
for (const f of files) rows.push(await badge(f));

const ref = rows[0];
console.log('badge = the pink "Viral on TikTok" disc\n');
console.log('file                              px    diam   diam%W   centreY%H   vs REF diam   vs REF centreY');
for (const r of rows) {
  if (!r.found) { console.log(`${r.file.padEnd(34)} NOT FOUND`); continue; }
  const dDiam = ref.found ? ((r.wPct / ref.wPct - 1) * 100) : 0;
  const dCy = ref.found ? (r.cyPct - ref.cyPct) : 0;
  console.log(
    r.file.padEnd(34) +
      String(r.px).padStart(6) +
      String(r.w).padStart(7) +
      r.wPct.toFixed(2).padStart(9) +
      r.cyPct.toFixed(2).padStart(12) +
      (r === ref ? '            —' : `${dDiam >= 0 ? '+' : ''}${dDiam.toFixed(1)}%`.padStart(13)) +
      (r === ref ? '            —' : `${dCy >= 0 ? '+' : ''}${dCy.toFixed(2)}pp`.padStart(16))
  );
}
