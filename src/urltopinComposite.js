import sharp from 'sharp';
import fetch from 'node-fetch';

const W = 1000;
const H = 1500;
const MAX_DOWNLOAD_BYTES = 18 * 1024 * 1024;

/**
 * Only https URLs on the project's Supabase storage host (SSRF-safe).
 */
export function isAllowedUserImageUrl(urlString, supabaseUrlEnv) {
  if (!urlString || typeof urlString !== 'string') return false;
  try {
    const u = new URL(urlString.trim());
    if (u.protocol !== 'https:') return false;
    const host = u.hostname.toLowerCase();
    if (host.endsWith('.supabase.co')) return true;
    if (supabaseUrlEnv) {
      try {
        const base = new URL(supabaseUrlEnv);
        if (host === base.hostname.toLowerCase()) return true;
      } catch {
        /* invalid SUPABASE_URL env, still allowed .supabase.co above */
      }
    }
    return false;
  } catch {
    return false;
  }
}

function escapeXml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function wrapLines(text, maxLen, maxLines) {
  const words = String(text || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  const lines = [];
  let cur = '';
  for (const w of words) {
    const next = cur ? `${cur} ${w}` : w;
    if (next.length <= maxLen) {
      cur = next;
    } else {
      if (cur) lines.push(cur);
      if (w.length > maxLen) {
        for (let i = 0; i < w.length && lines.length < maxLines; i += maxLen) {
          lines.push(w.slice(i, i + maxLen));
        }
        cur = '';
      } else {
        cur = w;
      }
    }
    if (lines.length >= maxLines) break;
  }
  if (lines.length < maxLines && cur) lines.push(cur);
  return lines.slice(0, maxLines);
}

function normalizeHex(c) {
  if (!c || typeof c !== 'string') return null;
  const s = c.trim();
  if (/^#[0-9A-Fa-f]{6}$/.test(s)) return s;
  if (/^#[0-9A-Fa-f]{3}$/.test(s)) {
    const r = s[1];
    const g = s[2];
    const b = s[3];
    return `#${r}${r}${g}${g}${b}${b}`.toUpperCase();
  }
  return null;
}

function hexToRgb(hex) {
  const h = normalizeHex(hex);
  if (!h) return null;
  const r = parseInt(h.slice(1, 3), 16);
  const g = parseInt(h.slice(3, 5), 16);
  const b = parseInt(h.slice(5, 7), 16);
  return { r, g, b };
}

function relativeLuminance({ r, g, b }) {
  const srgb = [r, g, b].map((v) => v / 255).map((c) => (c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4)));
  return 0.2126 * srgb[0] + 0.7152 * srgb[1] + 0.0722 * srgb[2];
}

function clamp01(n, fallback = 0) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  if (x < 0) return 0;
  if (x > 1) return 1;
  return x;
}

function clamp(n, min, max, fallback) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.min(max, Math.max(min, x));
}

function getTextTheme(renderOptions) {
  const preferred = normalizeHex(renderOptions?.textColor) || '#FFFFFF';
  const rgb = hexToRgb(preferred) || { r: 255, g: 255, b: 255 };
  const lum = relativeLuminance(rgb);
  // "Dark text" means we should use light scrims and contrasting strokes (when outline is on).
  const isDarkText = lum < 0.35;
  return {
    textFill: preferred,
    subFill: preferred,
    stroke: isDarkText ? '#FFFFFF' : '#000000',
    isDarkText,
  };
}

/** SVG stroke around text: 'standard' (legibility), 'thin', or 'none' / 'clean' (no halo — works best with a solid text panel). */
function textStrokeSvgAttrs(theme, renderOptions, role) {
  const mode = String(renderOptions?.textOutline ?? 'none').toLowerCase();
  if (mode === 'none' || mode === 'off' || mode === 'clean' || mode === 'no') {
    return '';
  }
  const thin = mode === 'thin' || mode === 'subtle';
  const w =
    role === 'head'
      ? thin
        ? 1.5
        : 4
      : role === 'sub'
        ? thin
          ? 1
          : 2.5
        : thin
          ? 0.85
          : 1.75;
  return ` stroke="${escapeXml(theme.stroke)}" stroke-width="${w}" paint-order="stroke fill"`;
}

function getFontFamily(renderOptions) {
  const v = String(renderOptions?.fontFamily || 'sans').toLowerCase();
  if (v === 'serif') return 'DejaVu Serif, Georgia, Times New Roman, serif';
  if (v === 'mono') return 'DejaVu Sans Mono, Menlo, Consolas, monospace';
  if (v === 'condensed')
    return 'Bahnschrift Condensed, Arial Narrow, DejaVu Sans Condensed, Liberation Sans Narrow, Arial, Helvetica, sans-serif';
  if (v === 'display') return 'Impact, Haettenschweiler, Arial Black, DejaVu Sans, Arial, Helvetica, sans-serif';
  if (v === 'rounded') return 'Segoe UI Rounded, Trebuchet MS, DejaVu Sans, Arial, Helvetica, sans-serif';
  return 'DejaVu Sans, Liberation Sans, Arial, Helvetica, sans-serif';
}

/** Approximate max line width in SVG units (for sizing the text scrim with fontScale). */
function charWidthRatios(renderOptions) {
  const v = String(renderOptions?.fontFamily || 'sans').toLowerCase();
  if (v === 'mono') return { head: 0.62, sub: 0.58 };
  if (v === 'condensed') return { head: 0.48, sub: 0.44 };
  if (v === 'display') return { head: 0.52, sub: 0.48 };
  if (v === 'rounded') return { head: 0.56, sub: 0.52 };
  if (v === 'serif') return { head: 0.54, sub: 0.5 };
  return { head: 0.58, sub: 0.52 };
}

function estimateLinesWidth(lines, fontSize, charRatio) {
  let m = fontSize * 2;
  for (const line of lines) {
    m = Math.max(m, line.length * fontSize * charRatio);
  }
  return m;
}

/**
 * Pinterest 2:3 pin: user photo (cover) + bottom gradient + crisp SVG typography.
 * @param {object} opts
 * @param {string} opts.sourceImageUrl
 * @param {{ headline?: string, subheadline?: string, source?: string }} opts.overlayText
 * @param {{ primaryColor?: string|null, secondaryColor?: string|null, accentColor?: string|null, brandName?: string|null } | null} opts.brand
 * @param {{ overlayStrength?: number, fontFamily?: 'sans'|'serif'|'mono', fontScale?: number, textColor?: string, position?: 'upper'|'middle'|'lower', overlayScrimColor?: string } | null} [opts.renderOptions]
 * @returns {Promise<Buffer>} PNG buffer
 */
export async function compositeUserPhotoPin({ sourceImageUrl, overlayText, brand, renderOptions }) {
  const ac = new AbortController();
  const fetchTimeout = setTimeout(() => ac.abort(), 45000);
  let res;
  try {
    res = await fetch(sourceImageUrl, {
      headers: { Accept: 'image/*' },
      signal: ac.signal,
    });
  } finally {
    clearTimeout(fetchTimeout);
  }
  if (!res.ok) {
    throw new Error(`Failed to download source image: ${res.status}`);
  }
  const len = res.headers.get('content-length');
  if (len && Number(len) > MAX_DOWNLOAD_BYTES) {
    throw new Error('Source image too large');
  }
  const arrayBuf = await res.arrayBuffer();
  if (arrayBuf.byteLength > MAX_DOWNLOAD_BYTES) {
    throw new Error('Source image too large');
  }

  const base = sharp(Buffer.from(arrayBuf))
    .rotate()
    .resize(W, H, { fit: 'cover', position: 'centre' });

  let headlineLines = wrapLines(overlayText?.headline || '', 22, 4);
  if (headlineLines.length === 0) headlineLines = ['Pin'];
  const subLines = wrapLines(overlayText?.subheadline || '', 28, 2);
  const sourceLine = String(overlayText?.source || '').trim().slice(0, 80);
  const brandNameLine = String(brand?.brandName || '').trim().slice(0, 80);
  const footerLine =
    sourceLine && brandNameLine && sourceLine.toLowerCase() !== brandNameLine.toLowerCase()
      ? `${brandNameLine} · ${sourceLine}`.slice(0, 90)
      : sourceLine || brandNameLine;

  const primary = normalizeHex(brand?.primaryColor);
  const secondary = normalizeHex(brand?.secondaryColor);
  const accent = normalizeHex(brand?.accentColor);
  // Thin stripes: use kit colors without hurting text contrast (subhead stays light on dark gradient).
  const accentBar = accent || primary || secondary || '#ffffff';

  const overlayStrength = clamp01(renderOptions?.overlayStrength, 1); // 0..1; default full scrims
  const fontScale = clamp(renderOptions?.fontScale, 0.75, 1.6, 1.0);
  const fontFamily = getFontFamily(renderOptions);
  const theme = getTextTheme(renderOptions);

  // Larger, Pinterest-readable type by default (mobile-friendly).
  const headlineSize = Math.round((headlineLines.length >= 4 ? 56 : headlineLines.length >= 3 ? 62 : 70) * fontScale);
  const subSize = Math.round(42 * fontScale);
  const footerSize = Math.max(12, Math.min(subSize - 2, Math.round(subSize * 0.62))); // always smaller than subheadline

  const headlineLineStep = Math.round(headlineSize * 1.18);
  const subLineStep = Math.round(subSize * 1.2);

  // Vertical placement: % of canvas height for first-line baseline (old fixed offsets sat ~35% for "upper").
  const pos = String(renderOptions?.position || 'middle').toLowerCase();
  const basePct = pos === 'upper' ? 0.175 : pos === 'lower' ? 0.69 : 0.485;
  const nHead = headlineLines.length;
  const lineLift = (nHead - 1) * Math.round(headlineSize * 0.38);
  let dyHeadline = Math.round(H * basePct) - lineLift;

  const computeTextBlock = (dy) => {
    const subStartY =
      subLines.length > 0 ? dy + headlineLines.length * headlineSize * 1.15 + 8 : 0;
    const hFirst = dy;
    const hLast = dy + (headlineLines.length - 1) * headlineLineStep;
    let top = hFirst - Math.round(headlineSize * 0.92);
    let bottom = hLast + Math.round(headlineSize * 0.22);
    if (subLines.length) {
      const sLast = subStartY + (subLines.length - 1) * subLineStep;
      top = Math.min(top, subStartY - Math.round(subSize * 0.88));
      bottom = Math.max(bottom, sLast + Math.round(subSize * 0.28));
    }
    return { subStartY, blockTop: top, blockBottom: bottom };
  };

  let { subStartY, blockTop, blockBottom } = computeTextBlock(dyHeadline);
  const bottomReserve = footerLine ? (pos === 'lower' ? 145 : 210) : pos === 'lower' ? 95 : 150;
  const bottomGuard = H - bottomReserve;
  if (blockBottom > bottomGuard) {
    dyHeadline = Math.round(dyHeadline - (blockBottom - bottomGuard));
    dyHeadline = Math.max(Math.round(H * 0.1), dyHeadline);
    ({ subStartY, blockTop, blockBottom } = computeTextBlock(dyHeadline));
  }

  const tspansHead = headlineLines
    .map((line, i) => {
      const dy = i === 0 ? 0 : headlineLineStep;
      return `<tspan x="500" dy="${i === 0 ? 0 : dy}" font-size="${headlineSize}">${escapeXml(line)}</tspan>`;
    })
    .join('');

  const tspansSub = subLines
    .map((line, i) => {
      const dy = i === 0 ? 0 : subLineStep;
      return `<tspan x="500" dy="${i === 0 ? 0 : dy}" font-size="${subSize}">${escapeXml(line)}</tspan>`;
    })
    .join('');

  // Overlay modes:
  // - none: no scrim/gradient
  // - text: scrim only behind headline/sub
  // - footer: scrim only behind footer
  // - text_footer: scrim behind both blocks
  // - full: full bottom gradient (no extra mid-rect to avoid seam)
  const scrimMode = String(renderOptions?.scrimMode || 'text_footer').toLowerCase();
  const wantTextScrim = scrimMode === 'text' || scrimMode === 'text_footer';
  const wantFooterScrim = scrimMode === 'footer' || scrimMode === 'text_footer';
  const wantFull = scrimMode === 'full';

  const autoScrim = theme.isDarkText ? '#FFFFFF' : '#000000';
  const customScrim = normalizeHex(renderOptions?.overlayScrimColor);
  const scrimColor = customScrim || autoScrim;
  // At overlayStrength === 0: no scrims / no gradient tint (all opacities scale to 0).
  const grad0 = wantFull ? (overlayStrength * (0.70 + 0.22 * overlayStrength)).toFixed(2) : '0.00';
  const grad1 = wantFull ? (overlayStrength * (0.34 + 0.22 * overlayStrength)).toFixed(2) : '0.00';
  const grad2 = wantFull ? (overlayStrength * (0.10 + 0.18 * overlayStrength)).toFixed(2) : '0.00';
  // Text panel: 0 at slider min, fully opaque at max (quadratic keeps f'(0)>0 so low end isn’t stuck).
  const textScrimOpacity = Math.min(1, overlayStrength * (0.24 + 0.76 * overlayStrength)).toFixed(2);
  const footerScrimOpacity =
    renderOptions?.footerBackgroundOpacity != null && Number.isFinite(Number(renderOptions.footerBackgroundOpacity))
      ? Math.min(1, clamp01(renderOptions.footerBackgroundOpacity)).toFixed(2)
      : Math.min(1, overlayStrength * (0.16 + 0.84 * overlayStrength)).toFixed(2);
  // Scrim: vertical from glyph metrics; horizontal from estimated line width × fontSize (reacts to text size slider).
  const textPad = clamp(renderOptions?.textPaddingPx, 0, 90, 28);
  const strokePad = Math.round(6 + (theme.isDarkText ? 2 : 0));
  const vertPadMul = 0.82 + 0.18 * fontScale;
  const vPad = Math.round(textPad * 0.55 * vertPadMul);
  const paddedTop = Math.max(0, Math.round(blockTop - vPad - strokePad));
  const paddedBottom = Math.min(H, Math.round(blockBottom + vPad + strokePad));
  const ratios = charWidthRatios(renderOptions);
  const headW = estimateLinesWidth(headlineLines, headlineSize, ratios.head);
  const subW = subLines.length ? estimateLinesWidth(subLines, subSize, ratios.sub) : 0;
  const contentW = Math.max(headW, subW);
  const sidePad = Math.round(textPad * 0.85 + strokePad + 16 + 14 * fontScale);
  let brickW = Math.ceil(contentW + sidePad * 2);
  const marginX = Math.max(28, 56 - Math.round(textPad * 0.45));
  const maxBrick = W - 2 * marginX;
  const minBrick = Math.min(maxBrick, Math.max(160, Math.round(headlineSize * 3.2 + (subLines.length ? subSize * 2.2 : 0))));
  brickW = Math.min(maxBrick, Math.max(minBrick, brickW));
  const textScrimX = Math.round(W / 2 - brickW / 2);
  const textScrimW = brickW;
  const textScrimTop = paddedTop;
  const textScrimH = Math.max(100, paddedBottom - paddedTop);
  const textScrimRx = Math.round(28 + textPad * 0.35 + 6 * (fontScale - 1));
  // Shorter footer band (tighter, Pinterest-like).
  const footerBandTop = Math.max(0, H - 74);
  const footerBandMidY = Math.round(footerBandTop + (H - footerBandTop) / 2);

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="ug" x1="0" y1="1" x2="0" y2="0">
      <stop offset="0" stop-color="${scrimColor}" stop-opacity="${grad0}"/>
      <stop offset="0.38" stop-color="${scrimColor}" stop-opacity="${grad1}"/>
      <stop offset="0.68" stop-color="${scrimColor}" stop-opacity="${grad2}"/>
      <stop offset="1" stop-color="${scrimColor}" stop-opacity="0"/>
    </linearGradient>
  </defs>
  <rect width="${W}" height="${H}" fill="url(#ug)"/>
  ${wantTextScrim ? `<rect x="${textScrimX}" y="${textScrimTop}" width="${textScrimW}" height="${Math.min(H - textScrimTop, textScrimH)}" rx="${textScrimRx}" fill="${scrimColor}" opacity="${textScrimOpacity}"/>` : ''}
  ${wantFooterScrim ? `<rect x="0" y="${footerBandTop}" width="${W}" height="${H - footerBandTop}" fill="${scrimColor}" opacity="${footerScrimOpacity}"/>` : ''}
  <rect x="0" y="${H - 5}" width="${W}" height="5" fill="${escapeXml(accentBar)}"/>
  <text
    x="500"
    y="${dyHeadline}"
    text-anchor="middle"
    fill="${escapeXml(theme.textFill)}"
    font-family="${escapeXml(fontFamily)}"
    font-weight="700"
    ${textStrokeSvgAttrs(theme, renderOptions, 'head')}
  >${tspansHead}</text>
  ${
    subLines.length
      ? `<text
    x="500"
    y="${subStartY}"
    text-anchor="middle"
    fill="${escapeXml(theme.subFill)}"
    font-family="${escapeXml(fontFamily)}"
    font-weight="600"
    ${textStrokeSvgAttrs(theme, renderOptions, 'sub')}
  >${tspansSub}</text>`
      : ''
  }
  ${
    footerLine
      ? `<text
    x="500"
    y="${footerBandMidY}"
    text-anchor="middle"
    dominant-baseline="middle"
    fill="${escapeXml(theme.textFill)}"
    font-family="${escapeXml(fontFamily)}"
    font-size="${footerSize}"
    font-weight="500"
    ${textStrokeSvgAttrs(theme, renderOptions, 'foot')}
  >${escapeXml(footerLine)}</text>`
      : ''
  }
</svg>`;

  const overlay = Buffer.from(svg);

  const out = await base
    .composite([{ input: overlay, top: 0, left: 0 }])
    .png({ compressionLevel: 7 })
    .toBuffer();

  return out;
}
