import sharp from 'sharp';
import { buildTextBasedPinSvgString, normalizeTextBasedInput } from './urltopinTextBasedSvg.js';

export { normalizeTextBasedInput };

export async function renderTextBasedPin({ overlayText, brand, textBased, variationSeed = 0, renderOptions = null }) {
  const svg = buildTextBasedPinSvgString({
    overlayText,
    brand,
    textBased,
    variationSeed,
    renderOptions,
  });
  return sharp(Buffer.from(svg)).png({ compressionLevel: 7 }).toBuffer();
}
