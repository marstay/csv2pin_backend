/**
 * Pinterest organic product tags (hero pin + product pin_id via bulk_add).
 * @see https://developers.pinterest.com/docs/api/v5/product_tags-bulk_add
 */

function normalizeUrlHostname(host) {
  return String(host || '')
    .trim()
    .replace(/^www\./i, '')
    .toLowerCase();
}

export function isAmazonRelatedHost(host) {
  const h = normalizeUrlHostname(host);
  if (h === 'a.co' || h.startsWith('amzn.')) return true;
  if (h === 'amzn.to' || h.endsWith('.amzn.to')) return true;
  if (h === 'amzlink.to' || h.endsWith('.amzlink.to')) return true;
  if (h === 'amznlink.to' || h.endsWith('.amznlink.to')) return true;
  if (h.startsWith('amazon.')) return true;
  if (h.endsWith('.amazon.com')) return true;
  return false;
}

function normalizeAmazonAssociateTag(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  return s.replace(/^tag=/i, '').slice(0, 64);
}

/** Append Associates tag to amazon.* URLs; leave other URLs unchanged. */
export function applyAmazonAssociateTag(urlString, tagRaw) {
  const tag = normalizeAmazonAssociateTag(tagRaw);
  const base = String(urlString || '').trim();
  if (!base) return '';
  if (!tag) return base;
  try {
    const u = new URL(/^https?:\/\//i.test(base) ? base : `https://${base}`);
    if (!isAmazonRelatedHost(u.hostname)) return u.toString();
    u.searchParams.set('tag', tag);
    return u.toString();
  } catch {
    return base;
  }
}

export function parseAmazonProductTagsConfig(originalPinData) {
  try {
    const raw = originalPinData?.amazon_product_tags;
    if (!raw || typeof raw !== 'object' || !raw.enabled) return null;
    const source = raw.source === 'custom' ? 'custom' : 'pin_link';
    return {
      enabled: true,
      source,
      custom_url: String(raw.custom_url || '').trim(),
      associate_tag: normalizeAmazonAssociateTag(raw.associate_tag),
    };
  } catch {
    return null;
  }
}

export function resolveAmazonProductTagUrl(scheduledPin, cfg) {
  const raw =
    cfg.source === 'custom'
      ? cfg.custom_url
      : String(scheduledPin?.link || '').trim();
  return applyAmazonAssociateTag(raw, cfg.associate_tag);
}

async function pinterestJsonFetch(url, accessToken, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });
  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }
  return { ok: res.ok, status: res.status, data };
}

async function pinterestCreatePin(accessToken, body) {
  return pinterestJsonFetch('https://api.pinterest.com/v5/pins', accessToken, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

async function pinterestBulkAddProductTags(accessToken, heroPinId, productPinIds) {
  const tags = (productPinIds || [])
    .map((id) => String(id || '').trim())
    .filter((id) => /^\d+$/.test(id))
    .map((pin_id) => ({ pin_id }));
  if (!tags.length) {
    return { ok: false, status: 400, data: { message: 'No valid product pin IDs' } };
  }
  return pinterestJsonFetch(
    `https://api.pinterest.com/v5/pins/${encodeURIComponent(heroPinId)}/product_tags`,
    accessToken,
    {
      method: 'POST',
      body: JSON.stringify({ product_tags: tags }),
    }
  );
}

async function createProductPinForTagging({
  accessToken,
  boardId,
  amazonUrl,
  title,
  description,
  imageUrl,
}) {
  const safeTitle = String(title || 'Product').slice(0, 100);
  const safeDesc = String(description || '').slice(0, 800);

  const pinUrlAttempt = await pinterestCreatePin(accessToken, {
    board_id: boardId,
    title: safeTitle,
    description: safeDesc,
    link: amazonUrl,
    media_source: { source_type: 'pin_url', is_affiliate_link: true },
  });
  if (pinUrlAttempt.ok && pinUrlAttempt.data?.id) {
    return { ok: true, pinId: pinUrlAttempt.data.id, method: 'pin_url' };
  }

  if (!imageUrl) {
    return {
      ok: false,
      error:
        pinUrlAttempt.data?.message ||
        pinUrlAttempt.data?.error ||
        'Could not create product pin (pin_url beta may be required)',
      details: pinUrlAttempt.data,
    };
  }

  const imageAttempt = await pinterestCreatePin(accessToken, {
    board_id: boardId,
    title: safeTitle,
    description: safeDesc,
    link: amazonUrl,
    media_source: { source_type: 'image_url', url: imageUrl },
  });
  if (imageAttempt.ok && imageAttempt.data?.id) {
    return { ok: true, pinId: imageAttempt.data.id, method: 'image_url' };
  }

  return {
    ok: false,
    error:
      imageAttempt.data?.message ||
      imageAttempt.data?.error ||
      pinUrlAttempt.data?.message ||
      'Failed to create product pin for tagging',
    details: imageAttempt.data || pinUrlAttempt.data,
  };
}

/**
 * After hero pin is posted, create a product pin (Amazon link) and tag it on the hero pin.
 * Failures are non-fatal — the hero pin remains published.
 */
export async function applyAmazonProductTagsToHeroPin({ accessToken, scheduledPin }) {
  const cfg = parseAmazonProductTagsConfig(scheduledPin?.original_pin_data);
  if (!cfg) return { skipped: true, reason: 'disabled' };

  const heroPinId = String(scheduledPin?.pinterest_pin_id || '').trim();
  if (!heroPinId) return { skipped: true, reason: 'no_hero_pin_id' };

  const amazonUrl = resolveAmazonProductTagUrl(scheduledPin, cfg);
  if (!amazonUrl) {
    return { ok: false, error: 'No Amazon product URL configured for tags' };
  }

  try {
    const u = new URL(amazonUrl);
    if (!isAmazonRelatedHost(u.hostname)) {
      return { ok: false, error: 'Product tag URL must be an Amazon link' };
    }
  } catch {
    return { ok: false, error: 'Invalid Amazon product URL for tags' };
  }

  const product = await createProductPinForTagging({
    accessToken,
    boardId: scheduledPin.board_id,
    amazonUrl,
    title: scheduledPin.title,
    description: scheduledPin.description,
    imageUrl: scheduledPin.image_url,
  });

  if (!product.ok) {
    return { ok: false, error: product.error, details: product.details };
  }

  const bulk = await pinterestBulkAddProductTags(accessToken, heroPinId, [product.pinId]);
  if (!bulk.ok) {
    return {
      ok: false,
      error: bulk.data?.message || bulk.data?.error || 'product_tags bulk_add failed',
      status: bulk.status,
      product_pin_id: product.pinId,
      details: bulk.data,
    };
  }

  return {
    ok: true,
    hero_pin_id: heroPinId,
    product_pin_id: product.pinId,
    product_pin_method: product.method,
    amazon_url: amazonUrl,
    bulk: bulk.data,
  };
}
