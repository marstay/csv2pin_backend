/**
 * Pure SVG builder for text-based pins (gradient + blobs + typography + scrims).
 * Keep in sync with frontend/src/urltopinTextBasedSvg.js
 */

const W = 1000;
const H = 1500;

const PRESET_KEYS = new Set([
  'bold_loud',
  'minimal_clean',
  'soft_aesthetic',
  'high_contrast',
  'editorial',
]);

const PRESET_DEFAULT_ANCHOR = {
  bold_loud: '#E60023',
  minimal_clean: '#94A3B8',
  soft_aesthetic: '#F472B6',
  high_contrast: '#0F172A',
  editorial: '#A8907C',
};

function escapeXml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function normalizeHex(c) {
  if (!c || typeof c !== 'string') return null;
  const s = c.trim();
  if (/^#[0-9A-Fa-f]{6}$/.test(s)) return s;
  if (/^#[0-9A-Fa-f]{3}$/.test(s)) {
    const r = s[1];
    const g = s[2];
    const b = s[3];
    return `#${r}${r}${g}${g}${b}${b}`;
  }
  return null;
}

function hexToRgb(hex) {
  const h = normalizeHex(hex);
  if (!h) return null;
  return {
    r: parseInt(h.slice(1, 3), 16),
    g: parseInt(h.slice(3, 5), 16),
    b: parseInt(h.slice(5, 7), 16),
  };
}

function rgbToHex({ r, g, b }) {
  const t = (n) => n.toString(16).padStart(2, '0');
  return `#${t(Math.round(r))}${t(Math.round(g))}${t(Math.round(b))}`.toUpperCase();
}

function mixHex(a, b, t) {
  const A = hexToRgb(a);
  const B = hexToRgb(b);
  if (!A || !B) return normalizeHex(a) || '#888888';
  const u = Math.max(0, Math.min(1, t));
  return rgbToHex({
    r: A.r + (B.r - A.r) * u,
    g: A.g + (B.g - A.g) * u,
    b: A.b + (B.b - A.b) * u,
  });
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

function getTextTheme(renderOptions, fallbackFill) {
  const preferred = normalizeHex(renderOptions?.textColor) || normalizeHex(fallbackFill) || '#FFFFFF';
  const rgb = hexToRgb(preferred) || { r: 255, g: 255, b: 255 };
  const lum = relativeLuminance(rgb);
  const isDarkText = lum < 0.35;
  return {
    textFill: preferred,
    subFill: preferred,
    stroke: isDarkText ? '#FFFFFF' : '#000000',
    isDarkText,
  };
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

function presetTypographyWeights(preset) {
  switch (preset) {
    case 'bold_loud':
      return { headlineWeight: '800', subWeight: '600', headlineStroke: 5, subStroke: 2.5 };
    case 'minimal_clean':
      return { headlineWeight: '700', subWeight: '500', headlineStroke: 2, subStroke: 1.5 };
    case 'soft_aesthetic':
      return { headlineWeight: '700', subWeight: '600', headlineStroke: 3, subStroke: 2 };
    case 'high_contrast':
      return { headlineWeight: '800', subWeight: '700', headlineStroke: 4, subStroke: 3 };
    case 'editorial':
      return { headlineWeight: '700', subWeight: '600', headlineStroke: 2.5, subStroke: 2 };
    default:
      return { headlineWeight: '800', subWeight: '600', headlineStroke: 5, subStroke: 2.5 };
  }
}

export function presetDefaultTextColor(preset) {
  switch (preset) {
    case 'minimal_clean':
    case 'soft_aesthetic':
    case 'editorial':
      return '#0F172A';
    default:
      return '#FFFFFF';
  }
}

function gradientStops(preset, anchor, secondary) {
  const a = anchor || PRESET_DEFAULT_ANCHOR.bold_loud;
  const s = secondary || null;
  switch (preset) {
    case 'minimal_clean': {
      // Very light, neutral gradient for clean blog-style look
      const base = '#FFFFFF';
      const edge = '#E5E7EB';
      return { c0: mixHex(base, edge, 0.2), c1: edge };
    }
    case 'soft_aesthetic': {
      // Pastel, soft aesthetic using pinks/peaches
      const base = mixHex(a, '#FDF2F8', 0.6);
      const edge = mixHex(a, '#F9A8D4', 0.5);
      return { c0: base, c1: edge };
    }
    case 'high_contrast': {
      // Dark, high-contrast background with bright accent
      const dark = '#020617';
      const edge = s || mixHex(a, '#F97316', 0.6);
      return { c0: dark, c1: edge };
    }
    case 'editorial': {
      // Warm, editorial beige gradient
      const base = mixHex(a, '#F5F5F4', 0.7);
      const edge = mixHex(a, '#D4BBA0', 0.6);
      return { c0: base, c1: edge };
    }
    case 'bold_loud':
    default: {
      // Saturated, bold gradient using brand anchor
      const c0 = s ? mixHex(a, s, 0.25) : mixHex(a, '#FFFFFF', 0.18);
      const c1 = mixHex(a, '#000000', 0.35);
      return { c0, c1 };
    }
  }
}

function gradientVector(seed) {
  const rad = ((seed * 47) % 360) * (Math.PI / 180);
  const x2 = Math.round(500 + Math.cos(rad) * 500);
  const y2 = Math.round(750 + Math.sin(rad) * 750);
  return { x1: 0, y1: 0, x2, y2 };
}

function blobShapes(seed, color1, color2, preset) {
  const n = 1 + (Math.abs(seed) % 3);
  const baseOp = preset === 'minimal_clean' ? 0.06 : 0.1;
  const parts = [];
  for (let i = 0; i < n; i++) {
    const c = i % 2 === 0 ? color1 : color2;
    const cx = 120 + ((seed * (i + 3) * 73) % 760);
    const cy = 150 + ((seed * (i + 7) * 61) % 1200);
    const rx = 160 + ((seed * (i + 1) * 37) % 200);
    const ry = 120 + ((seed * (i + 2) * 29) % 180);
    const rot = ((seed * (i + 5)) % 80) - 40;
    const op = baseOp + ((seed + i * 3) % 6) / 100;
    parts.push(
      `<ellipse cx="${cx}" cy="${cy}" rx="${rx}" ry="${ry}" fill="${escapeXml(c)}" opacity="${op.toFixed(
        2
      )}" transform="rotate(${rot} ${cx} ${cy})"/>`
    );
  }
  return parts.join('\n');
}

export function normalizeTextBasedInput(raw) {
  const o = raw && typeof raw === 'object' ? raw : {};
  const p = String(o.preset || 'bold_loud').toLowerCase().replace(/\s+/g, '_');
  const preset = PRESET_KEYS.has(p) ? p : 'bold_loud';
  return {
    preset,
    primaryColor: normalizeHex(o.primaryColor) || null,
    secondaryColor: normalizeHex(o.secondaryColor) || null,
  };
}

/**
 * @param {{ headline?: string, subheadline?: string, source?: string }} overlayText
 * @param {{ primaryColor?: string|null, secondaryColor?: string|null, accentColor?: string|null, brandName?: string|null } | null} brand
 * @param {{ preset?: string, primaryColor?: string|null, secondaryColor?: string|null }} textBased
 * @param {{ overlayStrength?: number, fontFamily?: string, fontScale?: number, textColor?: string, position?: string, scrimMode?: string, textPaddingPx?: number } | null} renderOptions
 */
export function buildTextBasedPinSvgString({
  overlayText,
  brand,
  textBased,
  variationSeed = 0,
  renderOptions = null,
}) {
  const ro = renderOptions && typeof renderOptions === 'object' ? renderOptions : {};
  const rawPreset = String(textBased?.preset || 'bold_loud').toLowerCase().replace(/\s+/g, '_');
  const preset = PRESET_KEYS.has(rawPreset) ? rawPreset : 'bold_loud';

  const pickPrimary =
    normalizeHex(textBased?.primaryColor) ||
    normalizeHex(brand?.primaryColor) ||
    PRESET_DEFAULT_ANCHOR[preset];
  const pickSecondary =
    normalizeHex(textBased?.secondaryColor) || normalizeHex(brand?.secondaryColor) || null;

  const primary = normalizeHex(brand?.primaryColor);
  const secondary = normalizeHex(brand?.secondaryColor);
  const accent = normalizeHex(brand?.accentColor);
  const accentBar = accent || primary || secondary || pickPrimary;

  const { c0, c1 } = gradientStops(preset, pickPrimary, pickSecondary);
  const gv = gradientVector(Number(variationSeed) || 0);

  let headlineLines = wrapLines(overlayText?.headline || '', 22, 4);
  if (headlineLines.length === 0) headlineLines = ['Pin'];
  const subLines = wrapLines(overlayText?.subheadline || '', 28, 2);
  const sourceLine = String(overlayText?.source || '').trim().slice(0, 80);
  const brandNameLine = String(brand?.brandName || '').trim().slice(0, 80);
  const footerLine =
    sourceLine && brandNameLine && sourceLine.toLowerCase() !== brandNameLine.toLowerCase()
      ? `${brandNameLine} · ${sourceLine}`.slice(0, 90)
      : sourceLine || brandNameLine;

  const typoW = presetTypographyWeights(preset);
  const defaultColorHint = presetDefaultTextColor(preset);
  const overlayStrength = clamp01(ro.overlayStrength, 0.45);
  const fontScale = clamp(ro.fontScale, 0.75, 1.6, 1.0);
  const fontFamily = getFontFamily(ro);
  const theme = getTextTheme(ro, defaultColorHint);

  const headlineSize = Math.round((headlineLines.length >= 4 ? 56 : headlineLines.length >= 3 ? 62 : 70) * fontScale);
  const subSize = Math.round(42 * fontScale);
  const footerSize = Math.max(12, Math.min(subSize - 2, Math.round(subSize * 0.62)));

  const headlineLineStep = Math.round(headlineSize * 1.18);
  const subLineStep = Math.round(subSize * 1.2);

  const pos = String(ro.position || 'middle').toLowerCase();
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

  const headlineStroke = Math.round(clamp(typoW.headlineStroke * (0.85 + 0.15 * fontScale), 2, 8, typoW.headlineStroke));
  const subStroke = Math.round(clamp(typoW.subStroke * (0.85 + 0.15 * fontScale), 1, 5, typoW.subStroke));

  const scrimMode = String(ro.scrimMode || 'text_footer').toLowerCase();
  const wantTextScrim = scrimMode === 'text' || scrimMode === 'text_footer';
  const wantFooterScrim = scrimMode === 'footer' || scrimMode === 'text_footer';
  const wantFull = scrimMode === 'full';

  const scrimColor = theme.isDarkText ? '#FFFFFF' : '#000000';
  const grad0 = wantFull ? (0.7 + 0.22 * overlayStrength).toFixed(2) : '0.00';
  const grad1 = wantFull ? (0.34 + 0.22 * overlayStrength).toFixed(2) : '0.00';
  const grad2 = wantFull ? (0.1 + 0.18 * overlayStrength).toFixed(2) : '0.00';
  const textScrimOpacity = (0.24 + 0.4 * overlayStrength).toFixed(2);
  const footerScrimOpacity = (0.16 + 0.3 * overlayStrength).toFixed(2);

  const textPad = clamp(ro.textPaddingPx, 0, 90, 28);
  const strokePad = Math.round(6 + (theme.isDarkText ? 2 : 0));
  const vertPadMul = 0.82 + 0.18 * fontScale;
  const vPad = Math.round(textPad * 0.55 * vertPadMul);
  const paddedTop = Math.max(0, Math.round(blockTop - vPad - strokePad));
  const paddedBottom = Math.min(H, Math.round(blockBottom + vPad + strokePad));
  const ratios = charWidthRatios(ro);
  const headW = estimateLinesWidth(headlineLines, headlineSize, ratios.head);
  const subW = subLines.length ? estimateLinesWidth(subLines, subSize, ratios.sub) : 0;
  const contentW = Math.max(headW, subW);
  const sidePad = Math.round(textPad * 0.85 + strokePad + 16 + 14 * fontScale);
  let brickW = Math.ceil(contentW + sidePad * 2);
  const marginX = Math.max(28, 56 - Math.round(textPad * 0.45));
  const maxBrick = W - 2 * marginX;
  const minBrick = Math.min(
    maxBrick,
    Math.max(160, Math.round(headlineSize * 3.2 + (subLines.length ? subSize * 2.2 : 0)))
  );
  brickW = Math.min(maxBrick, Math.max(minBrick, brickW));
  const textScrimX = Math.round(W / 2 - brickW / 2);
  const textScrimW = brickW;
  const textScrimTop = paddedTop;
  const textScrimH = Math.max(100, paddedBottom - paddedTop);
  const textScrimRx = Math.round(28 + textPad * 0.35 + 6 * (fontScale - 1));
  const footerBandTop = Math.max(0, H - 74);
  const footerBandMidY = Math.round(footerBandTop + (H - footerBandTop) / 2);

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

  const midMix = hexToRgb(mixHex(c0, c1, 0.5));
  const barHex = midMix ? rgbToHex(midMix) : pickPrimary;
  const blobs = blobShapes(Number(variationSeed) || 0, mixHex(c0, '#FFFFFF', 0.2), mixHex(c1, pickPrimary, 0.15), preset);

  const fullScrimDef = wantFull
    ? `<linearGradient id="tbfullscrim" x1="0" y1="1" x2="0" y2="0">
      <stop offset="0" stop-color="${scrimColor}" stop-opacity="${grad0}"/>
      <stop offset="0.38" stop-color="${scrimColor}" stop-opacity="${grad1}"/>
      <stop offset="0.68" stop-color="${scrimColor}" stop-opacity="${grad2}"/>
      <stop offset="1" stop-color="${scrimColor}" stop-opacity="0"/>
    </linearGradient>`
    : '';

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="tbbg" gradientUnits="userSpaceOnUse" x1="${gv.x1}" y1="${gv.y1}" x2="${gv.x2}" y2="${gv.y2}">
      <stop offset="0%" stop-color="${escapeXml(c0)}"/>
      <stop offset="100%" stop-color="${escapeXml(c1)}"/>
    </linearGradient>
    ${fullScrimDef}
  </defs>
  <rect width="${W}" height="${H}" fill="url(#tbbg)"/>
  <g>${blobs}</g>
  ${wantFull ? `<rect width="${W}" height="${H}" fill="url(#tbfullscrim)"/>` : ''}
  ${wantTextScrim ? `<rect x="${textScrimX}" y="${textScrimTop}" width="${textScrimW}" height="${Math.min(H - textScrimTop, textScrimH)}" rx="${textScrimRx}" fill="${scrimColor}" opacity="${textScrimOpacity}"/>` : ''}
  ${wantFooterScrim ? `<rect x="0" y="${footerBandTop}" width="${W}" height="${H - footerBandTop}" fill="${scrimColor}" opacity="${footerScrimOpacity}"/>` : ''}
  <rect x="0" y="${H - 5}" width="${W}" height="5" fill="${escapeXml(accentBar)}"/>
  <text
    x="500"
    y="${dyHeadline}"
    text-anchor="middle"
    fill="${escapeXml(theme.textFill)}"
    font-family="${escapeXml(fontFamily)}"
    font-weight="${typoW.headlineWeight}"
    stroke="${escapeXml(theme.stroke)}"
    stroke-width="${headlineStroke}"
    paint-order="stroke fill"
  >${tspansHead}</text>
  ${
    subLines.length
      ? `<text
    x="500"
    y="${subStartY}"
    text-anchor="middle"
    fill="${escapeXml(theme.subFill)}"
    font-family="${escapeXml(fontFamily)}"
    font-weight="${typoW.subWeight}"
    stroke="${escapeXml(theme.stroke)}"
    stroke-width="${subStroke}"
    paint-order="stroke fill"
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
    stroke="${escapeXml(theme.stroke)}"
    stroke-width="1.75"
    paint-order="stroke fill"
  >${escapeXml(footerLine)}</text>`
      : ''
  }
</svg>`;
}
