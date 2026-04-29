import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import JSZip from 'jszip';
import {
  enrichContentProfile,
  planStrategies,
  generateStrategicPinMetadata,
  extractArticleKeyIdeas,
  pickAngle,
  checkDiversity,
  rankPins,
  getStrategyReason,
} from './strategicPin.js';
import { compositeUserPhotoPin, isAllowedUserImageUrl } from './urltopinComposite.js';
import { renderTextBasedPin, normalizeTextBasedInput } from './urltopinTextBased.js';
dotenv.config();

console.log('SUPABASE_URL:', process.env.SUPABASE_URL);
console.log('SUPABASE_SERVICE_ROLE_KEY:', process.env.SUPABASE_SERVICE_ROLE_KEY);

const supabaseAdmin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Dodo Payments config
const DODO_BASE_URL = (process.env.DODO_BASE_URL || 'https://test.dodopayments.com').replace(/\/$/, '');
const DODO_API_KEY = process.env.DODO_API_KEY || process.env.DODO_PAYMENTS_API_KEY || '';

/**
 * Partner / influencer links: ?ref=slug on /pricing (stored in sessionStorage, sent as referralKey).
 * Map slugs to Dodo discount *codes* (customer-facing strings), not dsc_ ids.
 * Env example: DODO_PARTNER_DISCOUNT_MAP={"jane":"JANE20","firstusers":"WELCOME50"}
 */
function resolvePartnerDiscountCode(referralKey) {
  const rawKey = String(referralKey || '').trim();
  if (!rawKey) return null;
  const raw = process.env.DODO_PARTNER_DISCOUNT_MAP || '';
  if (!raw.trim()) return null;
  try {
    const map = JSON.parse(raw);
    if (!map || typeof map !== 'object') return null;
    const exact = map[rawKey];
    if (typeof exact === 'string' && exact.trim()) return exact.trim();
    const lower = rawKey.toLowerCase();
    for (const [k, v] of Object.entries(map)) {
      if (String(k).toLowerCase() === lower && typeof v === 'string' && v.trim()) return v.trim();
    }
    return null;
  } catch {
    return null;
  }
}

// --- Plan & usage helpers (pin_usage / metadata_usage) ---

const PLAN_PIN_LIMITS = {
  free: 10,
  starter: 60,
  creator: 150,
  pro: 450,
  agency: 1200,
};

/** Monthly caps for “your photo + text overlay” pins (no image model). Separate from AI pin quota. */
const PLAN_USER_PHOTO_PIN_LIMITS = {
  free: 40,
  starter: 240,
  creator: 600,
  pro: 1800,
  agency: 4800,
};

const PLAN_METADATA_LIMITS = {
  free: 500,
  starter: 2000,
  creator: 5000,
  pro: 20000,
  agency: 100000,
};

const pendingDodoActivations = new Map();

function currentYearMonthDate() {
  const now = new Date();
  // Use UTC month start to avoid timezone edge cases with Postgres date
  const monthStartUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  return monthStartUtc.toISOString().slice(0, 10); // 'YYYY-MM-DD'
}

async function getActiveSubscriptionForUser(userId) {
  try {
    const { data, error } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('plan_type, pins_limit_per_month')
      .eq('user_id', userId)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1);

    if (error) {
      console.warn('billing_subscriptions fetch error:', error.message || error);
      return null;
    }

    if (!data || data.length === 0) return null;
    return data[0];
  } catch (err) {
    console.warn('getActiveSubscriptionForUser error:', err.message || err);
    return null;
  }
}

function markPendingDodoActivation(userId, planType, sessionId = null) {
  if (!userId || !planType) return;
  pendingDodoActivations.set(String(userId), {
    planType: String(planType),
    sessionId: sessionId ? String(sessionId) : null,
    createdAt: Date.now(),
  });
}

function consumePendingDodoActivation(userId, requestedPlanType) {
  const key = String(userId || '');
  const row = pendingDodoActivations.get(key);
  if (!row) return { ok: false, reason: 'missing_pending_checkout' };
  const maxAgeMs = 2 * 60 * 60 * 1000;
  if (Date.now() - row.createdAt > maxAgeMs) {
    pendingDodoActivations.delete(key);
    return { ok: false, reason: 'pending_checkout_expired' };
  }
  if (String(row.planType) !== String(requestedPlanType)) {
    return { ok: false, reason: 'plan_mismatch_with_pending_checkout', pendingPlanType: row.planType };
  }
  pendingDodoActivations.delete(key);
  return { ok: true, pending: row };
}

async function applyPlanActivationForUser(userId, planType, source = 'unknown') {
  if (!planType || !PLAN_PIN_LIMITS[planType]) {
    return { ok: false, error: 'Invalid planType' };
  }

  const now = new Date();
  const periodStart = now.toISOString();
  const nextMonth = new Date(now);
  nextMonth.setMonth(nextMonth.getMonth() + 1);
  const periodEnd = nextMonth.toISOString();
  const pinsLimit = PLAN_PIN_LIMITS[planType];

  await supabaseAdmin
    .from('billing_subscriptions')
    .update({ status: 'cancelled', updated_at: now.toISOString() })
    .eq('user_id', userId)
    .eq('status', 'active');

  const { error: insertError } = await supabaseAdmin
    .from('billing_subscriptions')
    .insert({
      user_id: userId,
      plan_type: planType,
      pins_limit_per_month: pinsLimit,
      status: 'active',
      current_period_start: periodStart,
      current_period_end: periodEnd,
    });

  if (insertError) {
    return { ok: false, error: insertError.message || String(insertError) };
  }

  await supabaseAdmin
    .from('profiles')
    .update({
      plan_type: planType,
      is_pro: planType !== 'free',
      updated_at: now.toISOString(),
    })
    .eq('id', userId);

  console.log('✅ plan activated', { userId, planType, source });
  return { ok: true, planType, pinsLimit };
}

async function resolvePlanTypeForUser(userId) {
  const sub = await getActiveSubscriptionForUser(userId);
  if (sub?.plan_type) return sub.plan_type;
  try {
    const { data: profile, error } = await supabaseAdmin
      .from('profiles')
      .select('plan_type')
      .eq('id', userId)
      .maybeSingle();
    if (error) return 'free';
    return profile?.plan_type || 'free';
  } catch {
    return 'free';
  }
}

async function enforcePaidSchedulingOrThrow(res, userId) {
  const planType = await resolvePlanTypeForUser(userId);
  if (planType === 'free') {
    res.status(402).json({
      error: 'Upgrade required to schedule pins. You can still export/download pins on the Free plan.',
      code: 'upgrade_required',
      feature: 'scheduling',
      planType,
    });
    return false;
  }
  return true;
}

// Serialize pin consumption per user so concurrent requests (e.g. 2+ styles in URL→Pin)
// don't all read the same counters and only write one increment.
const pinUsageLocks = new Map();

function planAiPinsLimit(sub) {
  const planType = sub?.plan_type || 'free';
  // Always use canonical limits for standard tiers so plan changes apply without DB backfills.
  if (Object.prototype.hasOwnProperty.call(PLAN_PIN_LIMITS, planType)) {
    return PLAN_PIN_LIMITS[planType];
  }
  if (typeof sub?.pins_limit_per_month === 'number' && sub.pins_limit_per_month > 0) {
    return sub.pins_limit_per_month;
  }
  return PLAN_PIN_LIMITS.free;
}

function resolveUserPhotoPinLimitForPlan(sub) {
  const planType = sub?.plan_type || 'free';
  return PLAN_USER_PHOTO_PIN_LIMITS[planType] || PLAN_USER_PHOTO_PIN_LIMITS.free;
}

function pickAmazonContextUrl(inputUrl, canonicalUrl) {
  const rawInput = String(inputUrl || '').trim();
  const rawCanon = String(canonicalUrl || '').trim();
  try {
    if (rawCanon) {
      const cu = new URL(rawCanon);
      if (isAmazonRelatedHost(cu.hostname)) return rawCanon;
    }
  } catch {
    // ignore
  }
  return rawInput;
}

/**
 * @param {string} userId
 * @param {{ aiDelta?: number, userPhotoDelta?: number }} deltas, positive consume, negative refund
 */
async function applyPinQuotaDelta(userId, { aiDelta = 0, userPhotoDelta = 0 }) {
  const key = String(userId);
  let promise = pinUsageLocks.get(key);
  const run = async () => {
    const yearMonth = currentYearMonthDate();
    const sub = await getActiveSubscriptionForUser(userId);
    const planType = sub?.plan_type || 'free';
    const planPinsLimit = planAiPinsLimit(sub);
    const planUserPhotoPinsLimit = resolveUserPhotoPinLimitForPlan(sub);

    try {
      const { data: usageRows, error: usageError } = await supabaseAdmin
        .from('pin_usage')
        .select('pins_used, user_photo_pins_used')
        .eq('user_id', userId)
        .eq('year_month', yearMonth)
        .limit(1);

      if (usageError) {
        console.warn('pin_usage fetch error:', usageError.message || usageError);
      }

      const row = usageRows && usageRows.length ? usageRows[0] : null;
      const currentAi = row?.pins_used ?? 0;
      const currentUserPhoto = row?.user_photo_pins_used ?? 0;

      const tentativeAi = currentAi + aiDelta;
      const tentativeUserPhoto = currentUserPhoto + userPhotoDelta;

      if (tentativeAi < 0 || tentativeUserPhoto < 0) {
        console.warn('applyPinQuotaDelta: negative usage prevented', {
          userId,
          currentAi,
          currentUserPhoto,
          aiDelta,
          userPhotoDelta,
        });
        return {
          allowed: false,
          planType,
          planPinsLimit,
          planUserPhotoPinsLimit,
          limitKind: 'invalid_delta',
          currentUsed: currentAi,
          currentUserPhotoPinsUsed: currentUserPhoto,
        };
      }

      if (aiDelta > 0 && tentativeAi > planPinsLimit) {
        return {
          allowed: false,
          limitKind: 'ai',
          planType,
          planPinsLimit,
          planUserPhotoPinsLimit,
          currentUsed: currentAi,
          wouldUseAi: aiDelta,
          currentUserPhotoPinsUsed: currentUserPhoto,
        };
      }

      if (userPhotoDelta > 0 && tentativeUserPhoto > planUserPhotoPinsLimit) {
        return {
          allowed: false,
          limitKind: 'user_photo',
          planType,
          planPinsLimit,
          planUserPhotoPinsLimit,
          currentUsed: currentAi,
          currentUserPhotoPinsUsed: currentUserPhoto,
          wouldUseUserPhoto: userPhotoDelta,
        };
      }

      const newAi = tentativeAi;
      const newUserPhoto = tentativeUserPhoto;

      const { error: upsertError } = await supabaseAdmin.from('pin_usage').upsert(
        {
          user_id: userId,
          year_month: yearMonth,
          pins_used: newAi,
          user_photo_pins_used: newUserPhoto,
        },
        { onConflict: 'user_id,year_month' }
      );

      if (upsertError) {
        console.warn('pin_usage upsert error:', upsertError.message || upsertError);
      }

      return {
        allowed: true,
        planType,
        planPinsLimit,
        planUserPhotoPinsLimit,
        previousUsed: currentAi,
        newUsed: newAi,
        previousUserPhotoPinsUsed: currentUserPhoto,
        newUserPhotoPinsUsed: newUserPhoto,
      };
    } catch (err) {
      console.warn('applyPinQuotaDelta error (falling back to allow):', err.message || err);
      return {
        allowed: true,
        planType,
        planPinsLimit,
        planUserPhotoPinsLimit,
        previousUsed: 0,
        newUsed: Math.max(0, aiDelta),
        previousUserPhotoPinsUsed: 0,
        newUserPhotoPinsUsed: Math.max(0, userPhotoDelta),
      };
    }
  };

  promise = promise ? promise.then(() => run()) : run();
  pinUsageLocks.set(key, promise);
  const result = await promise;
  if (pinUsageLocks.get(key) === promise) {
    pinUsageLocks.delete(key);
  }
  return result;
}

async function recordMetadataUsage(userId, calls = 1) {
  const yearMonth = currentYearMonthDate();

  const sub = await getActiveSubscriptionForUser(userId);
  const planType = sub?.plan_type || 'free';
  const planMetaLimit = PLAN_METADATA_LIMITS[planType] || PLAN_METADATA_LIMITS.free;

  try {
    const { data: usageRows, error: usageError } = await supabaseAdmin
      .from('metadata_usage')
      .select('metadata_calls')
      .eq('user_id', userId)
      .eq('year_month', yearMonth)
      .limit(1);

    if (usageError) {
      console.warn('metadata_usage fetch error:', usageError.message || usageError);
    }

    const currentCalls = usageRows && usageRows.length ? usageRows[0].metadata_calls || 0 : 0;
    const newCalls = currentCalls + calls;

    const { error: upsertError } = await supabaseAdmin
      .from('metadata_usage')
      .upsert(
        {
          user_id: userId,
          year_month: yearMonth,
          metadata_calls: newCalls,
        },
        { onConflict: 'user_id,year_month' }
      );

    if (upsertError) {
      console.warn('metadata_usage upsert error:', upsertError.message || upsertError);
    }

    if (newCalls > planMetaLimit) {
      console.warn(
        `User ${userId} exceeded soft metadata limit: ${newCalls}/${planMetaLimit} calls for plan ${planType}`
      );
    }

    return {
      planType,
      planMetaLimit,
      previousCalls: currentCalls,
      newCalls,
    };
  } catch (err) {
    console.warn('recordMetadataUsage error:', err.message || err);
    return {
      planType,
      planMetaLimit,
      previousCalls: 0,
      newCalls: calls,
    };
  }
}

async function getCurrentUsageSnapshot(userId) {
  const yearMonth = currentYearMonthDate();

  // Active subscription row (if any)
  const subscription = await getActiveSubscriptionForUser(userId);
  const planType = subscription?.plan_type || 'free';
  const planPinsLimit = planAiPinsLimit(subscription);
  const planUserPhotoPinsLimit = resolveUserPhotoPinLimitForPlan(subscription);

  // Profile info (for email, created_at, etc.)
  const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('id, email, plan_type')
    .eq('id', userId)
    .single();

  // Pin usage for current month
  let pinsUsed = 0;
  let userPhotoPinsUsed = 0;
  try {
    const { data: pinUsageRow, error: pinError } = await supabaseAdmin
      .from('pin_usage')
      .select('pins_used, user_photo_pins_used')
      .eq('user_id', userId)
      .eq('year_month', yearMonth)
      .single();
    if (pinError) {
      // Ignore "no rows" error, log others
      if (pinError.code !== 'PGRST116') {
        console.warn('pin_usage single error:', pinError.message || pinError);
      }
    } else if (pinUsageRow) {
      pinsUsed = pinUsageRow.pins_used || 0;
      userPhotoPinsUsed = pinUsageRow.user_photo_pins_used ?? 0;
    }
  } catch (e) {
    console.warn('pin_usage fetch unexpected error:', e.message || e);
  }

  // Metadata usage for current month
  let metadataCalls = 0;
  try {
    const { data: metaUsageRow, error: metaError } = await supabaseAdmin
      .from('metadata_usage')
      .select('metadata_calls')
      .eq('user_id', userId)
      .eq('year_month', yearMonth)
      .single();
    if (metaError) {
      if (metaError.code !== 'PGRST116') {
        console.warn('metadata_usage single error:', metaError.message || metaError);
      }
    } else if (metaUsageRow) {
      metadataCalls = metaUsageRow.metadata_calls || 0;
    }
  } catch (e) {
    console.warn('metadata_usage fetch unexpected error:', e.message || e);
  }
  const planMetaLimit = PLAN_METADATA_LIMITS[planType] || PLAN_METADATA_LIMITS.free;

  return {
    user: {
      id: profile?.id || userId,
      email: profile?.email || null,
    },
    plan: {
      type: planType,
      pins_limit_per_month: planPinsLimit,
      user_photo_pins_limit_per_month: planUserPhotoPinsLimit,
      metadata_limit_per_month: planMetaLimit,
    },
    usage: {
      year_month: yearMonth,
      pins_used: pinsUsed,
      pins_remaining: Math.max(0, planPinsLimit - pinsUsed),
      user_photo_pins_used: userPhotoPinsUsed,
      user_photo_pins_remaining: Math.max(0, planUserPhotoPinsLimit - userPhotoPinsUsed),
      metadata_calls: metadataCalls,
      metadata_soft_limit: planMetaLimit,
    },
  };
}

async function requirePro(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !user) return res.status(401).json({ error: 'Unauthorized' });
  const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('is_pro')
    .eq('id', user.id)
    .single();
  if (!profile?.is_pro) {
    return res.status(403).json({ error: 'Pro membership required.' });
  }
  req.user = user;
  next();
}

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

/** Hostnames commonly used for URL shortening / tracking — path is required for identity, not just hostname. */
const URL_SHORTENER_HOSTNAMES = new Set([
  'a.co',
  'amzn.to',
  'amzn.eu',
  'netlify.app',
  'netlify.com',
  'etsy.me',
  'bit.ly',
  'bitly.com',
  'bitly.ws',
  't.co',
  'goo.gl',
  'ow.ly',
  'tinyurl.com',
  'buff.ly',
  'rebrand.ly',
  'rb.gy',
  'is.gd',
  'cutt.ly',
  'tiny.cc',
  'lnkd.in',
  'youtu.be',
  'fb.me',
  'l.facebook.com',
  'spr.ly',
  'smarturl.it',
  'surl.li',
  'short.link',
  'shorturl.at',
  'pst.cr',
  'vm.tiktok.com',
  'tr.ee',
  'linktr.ee',
  'j.mp',
  't.ly',
  'dub.sh',
  'dub.link',
  'geni.us',
  'ift.tt',
  'hubs.ly',
  'hubs.la',
  'qr.ae',
  'trib.al',
  's.id',
  'v.gd',
  'da.gd',
  'u.to',
  'chilp.it',
  'wp.me',
  'eepurl.com',
  'fb.watch',
  'l.instagram.com',
  'pin.it',
]);

function normalizeUrlHostname(host) {
  return String(host || '')
    .trim()
    .replace(/^www\./i, '')
    .toLowerCase();
}

function isYouTubeHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (h === 'youtu.be') return true;
  if (h === 'youtube.com') return true;
  if (h.endsWith('.youtube.com')) return true; // m.youtube.com, music.youtube.com, etc.
  return false;
}

function isEtsyHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  // etsy.me short links (user-pasted URL before/without redirect metadata)
  if (h === 'etsy.me') return true;
  return h === 'etsy.com' || h.endsWith('.etsy.com');
}

function isPrintifyHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  return h === 'printify.com' || h.endsWith('.printify.com') || h === 'printify.me' || h.endsWith('.printify.me');
}

/**
 * Affiliate / network tracking hosts — not a merchant brand; pin footer must be user’s CTA.
 * Keep in sync with frontend/src/utils/urlBrandingGate.js
 */
const AFFILIATE_TRACKING_HOST_EXACT = new Set([
  // ClickBank
  'hop.clickbank.net',
  // CJ / Conversant classic tracking domains
  'anrdoezrs.net',
  'tkqlhce.com',
  'jdoqocy.com',
  'dpbolvw.net',
  'syocnh.net',
  'emjcd.com',
  // Rakuten Advertising / LinkShare
  'click.linksynergy.com',
  'linksynergy.com',
  // Awin
  'awin1.com',
  'aw.click',
  // ShareASale
  'shareasale.com',
  // JVZoo / WarriorPlus style marketplaces (affiliate checkout URLs)
  'jvzoo.com',
  'warriorplus.com',
  // Impact
  'sjv.io',
  'ojrq.net',
  // PartnerStack
  'prf.hn',
  // Pepperjam / partner tracking
  'pjtra.com',
  // Skimlinks
  'go.skimresources.com',
  'redirect.skimresources.com',
  // FlexOffers
  'track.flexlinks.com',
  'track.flexlinkspro.com',
  // Refersion
  'rfer.us',
]);

function isAffiliateTrackingRedirectHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (AFFILIATE_TRACKING_HOST_EXACT.has(h)) return true;
  if (h.endsWith('.hop.clickbank.net')) return true;
  if (h.endsWith('.sjv.io')) return true;
  if (h.endsWith('.ojrq.net')) return true;
  if (h.endsWith('.linksynergy.com')) return true;
  if (h.endsWith('.awin1.com')) return true;
  if (h.endsWith('.aw.click')) return true;
  if (h.endsWith('.pxf.io')) return true; // e.g. ct.pxf.io, go.pxf.io
  return false;
}

function isLikelyUrlShortenerHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (URL_SHORTENER_HOSTNAMES.has(h)) return true;
  // Netlify deploy previews & custom subdomains behave like shorteners for identity/keywords.
  if (h.endsWith('.netlify.app') || h.endsWith('.netlify.com')) return true;
  // Very short branded hosts on TLDs often used for redirects (e.g. a.co, x.co)
  if (/^[a-z0-9]{1,4}\.co$/i.test(h)) return true;
  if (/^[a-z0-9]{1,4}\.(me|io|ly)$/i.test(h)) return true;
  return false;
}

/** Drop path segments that are Amazon/search tracking (e.g. ref=sr_1_2_sspa) so they never become keywords or prompts. */
function pathSegmentsStripTracking(parts) {
  if (!Array.isArray(parts)) return [];
  return parts.filter((p) => {
    if (!p) return false;
    if (/^ref=/i.test(p)) return false;
    if (/^ref[_-]/i.test(p)) return false;
    return true;
  });
}

/** Footer / “source” line used in prompts/overlays. */
function buildLinkDisplayLabelFromUrl(urlString, maxLen = 80) {
  try {
    const u = new URL(String(urlString || '').trim());
    const host = normalizeUrlHostname(u.hostname);
    let path = u.pathname || '';
    if (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
    let parts = pathSegmentsStripTracking(path.split('/').filter(Boolean));
    if (isYouTubeHost(host)) {
      let id = '';
      if (host === 'youtu.be') {
        id = parts[0] || '';
      } else if (parts[0] === 'shorts' && parts[1]) {
        id = parts[1];
      } else if (parts[0] === 'embed' && parts[1]) {
        id = parts[1];
      } else {
        id = (u.searchParams.get('v') || '').trim();
      }
      id = id.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32);
      if (id) return `youtu.be/${id}`.slice(0, maxLen);
      // Fallback: show host + first path segment only (no query)
      const fb = parts.length ? `youtube.com/${parts[0]}` : 'youtube.com';
      return fb.slice(0, maxLen);
    }
    // Short / tracking links: keep host + path so it's not just "bit.ly" or a bare hop subdomain.
    if (isLikelyUrlShortenerHost(host) || isAffiliateTrackingRedirectHost(host)) {
      path = parts.length ? `/${parts.join('/')}` : '';
      if (!path || path === '/') return host.slice(0, maxLen);
      return `${host}${path}`.slice(0, maxLen);
    }

    // Amazon: keep host only (footer is expected to be user's brand/CTA anyway; avoid long paths).
    if (isAmazonRelatedHost(host)) {
      return host.slice(0, maxLen);
    }

    // Normal sites: host only (prevents long article slugs becoming the footer line).
    return host.slice(0, maxLen);
  } catch {
    return '';
  }
}

/**
 * Keyword from URL path for prompts — skip shorteners and opaque slug segments (e.g. asdf123).
 * Amazon: prefer product slug before /dp/ASIN; never use trailing ref=… segments.
 */
function deriveKeywordFromArticleUrl(urlString) {
  try {
    const u = new URL(String(urlString || '').trim());
    const host = normalizeUrlHostname(u.hostname);
    if (isLikelyUrlShortenerHost(host) || isAffiliateTrackingRedirectHost(host)) return '';
    if (isEtsyHost(host) || isPrintifyHost(host)) return '';

    let parts = pathSegmentsStripTracking((u.pathname || '').split('/').filter(Boolean));
    if (parts.length === 0) return '';

    if (isAmazonRelatedHost(host)) {
      const dpIdx = parts.findIndex((p) => p === 'dp');
      if (dpIdx > 0) {
        const slug = parts[dpIdx - 1];
        if (slug && !/^dp$/i.test(slug)) {
          let keyword = slug
            .replace(/[-_]/g, ' ')
            .replace(/\.[a-zA-Z0-9]+$/, '')
            .trim();
          if (keyword.length >= 3) return keyword;
        }
      }
      const last = parts[parts.length - 1] || '';
      if (/^B[0-9A-Z]{9}$/i.test(last) || /^[0-9A-Z]{10}$/i.test(last)) return '';
    }

    const last = parts[parts.length - 1] || '';
    if (!last) return '';

    if (
      parts.length >= 1 &&
      /^[a-z0-9]{5,14}$/i.test(last) &&
      !/[-_]/.test(last)
    ) {
      return '';
    }

    let keyword = last
      .replace(/[-_]/g, ' ')
      .replace(/\.[a-zA-Z0-9]+$/, '')
      .trim();
    if (keyword.length < 3) return '';
    if (/^ref\s*=/i.test(keyword) || /\bsspa\b/i.test(keyword)) return '';
    return keyword;
  } catch {
    return '';
  }
}

function isAmazonRelatedHost(host) {
  const h = normalizeUrlHostname(host);
  // Amazon short domains + regional amzn TLDs (amzn.asia, amzn.in, etc.)
  if (h === 'a.co' || h.startsWith('amzn.')) return true;
  if (h === 'amzn.to' || h.endsWith('.amzn.to')) return true;
  if (h.startsWith('amazon.')) return true;
  if (h.endsWith('.amazon.com')) return true;
  return false;
}

/** Stops models from painting Amazon tracking path/query tokens (e.g. ref=sr_1_2_sspa) into the bitmap. */
function appendNanoBananaAmazonUrlGarbageGuard(imagePrompt, urlString) {
  try {
    if (!imagePrompt || !urlString) return imagePrompt;
    const u = new URL(String(urlString).trim());
    if (!isAmazonRelatedHost(u.hostname)) return imagePrompt;
    return (
      `${imagePrompt} ` +
      promptTier(
        'Do not render Amazon URL tracking or path fragments as text anywhere on the pin: no "ref=", "sr_", "sspa", query-style codes, or small boxes mimicking URL parameters. Only the specified headline, subheadline, and footer line may appear as readable text.',
        'Never paint ref=/sspa/URL tracking text—only headline, sub, footer.',
      )
    );
  } catch {
    return imagePrompt;
  }
}

/**
 * Pinterest-friendly max width for scraped listing titles (topic + fallbacks).
 * @param {string} t
 * @param {number} [softMax]
 */
function truncateListingTitleForPins(t, softMax = 58) {
  const s = String(t || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!s) return s;
  const firstComma = s.indexOf(',');
  if (firstComma !== -1 && firstComma >= 18 && firstComma <= softMax + 18) {
    const head = s.slice(0, firstComma).trim();
    if (head.length >= 18) return head;
  }
  if (s.length <= softMax) return s;
  const slice = s.slice(0, softMax + 1);
  const lastSpace = slice.lastIndexOf(' ');
  const cut = lastSpace > 32 ? slice.slice(0, lastSpace).trim() : slice.slice(0, softMax).trim();
  return `${cut}…`;
}

function isShopifyMyshopifyHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  return h.endsWith('.myshopify.com') || h === 'myshopify.com';
}

/**
 * Etsy, Shopify-style URLs, or obvious stacked SEO product titles — shorten so topic / image fallbacks stay readable.
 */
function shouldApplyGenericShopTitleSafety(pageHostname, pagePath, canonicalHostname, title) {
  const h = normalizeUrlHostname(pageHostname);
  const ch = normalizeUrlHostname(canonicalHostname || '');
  const path = String(pagePath || '');
  const t = String(title || '');
  if (isEtsyHost(h) || isEtsyHost(ch)) return true;
  if (isShopifyMyshopifyHost(h) || isShopifyMyshopifyHost(ch)) return true;
  if (/\/products\//.test(path)) return true;
  const pipes = (t.match(/\|/g) || []).length;
  if (t.length >= 92 && pipes >= 2) return true;
  if (t.length >= 118) return true;
  return false;
}

/**
 * Non-Amazon ecommerce / long SEO listing titles: strip marketplace suffixes, prefer first pipe segment, then truncate.
 */
function shortenGenericShopListingTitleForPins(rawTitle) {
  let t = String(rawTitle || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!t) return t;
  t = t.replace(/\s*[\|–—]\s*Etsy[^|]*$/i, '').trim();
  t = t.replace(/\s+on\s+Etsy\s*$/i, '').trim();
  const pipeParts = t
    .split('|')
    .map((p) => p.trim())
    .filter(Boolean);
  if (pipeParts.length >= 3 && t.length >= 70) {
    const first = pipeParts[0];
    if (first.length >= 12) t = first;
  } else if (pipeParts.length === 2 && t.length >= 85) {
    const first = pipeParts[0];
    if (first.length >= 18) t = first;
  }
  return truncateListingTitleForPins(t, 58);
}

/**
 * Amazon listing titles are long SEO strings; shorten for pin topic / UI (still descriptive).
 */
function shortenAmazonListingTitleForPins(rawTitle) {
  let t = String(rawTitle || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!t) return t;
  // Common Amazon patterns:
  // - "Amazon.co.uk: Foo Bar"
  // - "Foo Bar : Amazon.co.uk: Home & Kitchen"
  // - "Foo Bar | Amazon.co.uk"
  t = t.replace(/^Amazon(\.[a-z.]+)?\s*:\s*/i, '').trim();
  t = t.replace(/\s*(?:\||:|–|-)\s*Amazon\.[a-z.]+(?:\s*:\s*[^|]{0,120})?\s*$/i, '').trim();
  // Remove trailing category breadcrumbs like ": Home & Kitchen" (often chained).
  for (let i = 0; i < 3; i++) {
    const next = t.replace(/\s*:\s*[A-Za-z0-9,&'’\- ]{3,100}\s*$/i, '').trim();
    if (next === t) break;
    t = next;
  }
  return truncateListingTitleForPins(t, 58);
}

/**
 * Pinterest-friendly line via OpenAI; falls back to {@link shortenAmazonListingTitleForPins} on error or if disabled.
 */
async function shortenAmazonListingTitleWithAi(rawTitle, openaiClient) {
  const fallback = shortenAmazonListingTitleForPins(rawTitle);
  if (
    process.env.URLTOPIN_AMAZON_TITLE_AI === '0' ||
    !process.env.OPENAI_API_KEY ||
    !openaiClient
  ) {
    return fallback;
  }
  const cleaned = String(rawTitle || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return fallback;
  if (cleaned.length <= 52) {
    return shortenAmazonListingTitleForPins(rawTitle);
  }
  try {
    const model = process.env.URLTOPIN_AMAZON_TITLE_MODEL || 'gpt-4o-mini';
    const completion = await openaiClient.chat.completions.create({
      model,
      messages: [
        {
          role: 'user',
          content:
            'Rewrite this Amazon-style product listing title as ONE short line for a Pinterest pin.\n' +
            'Rules: max 58 characters; name the product clearly; keep one concrete detail (size, piece count, color, or key feature); no ALL CAPS; no quotation marks; no "Amazon", "Buy now", or price; no ellipsis; output only the line, nothing else.\n\n' +
            `Title:\n${cleaned.slice(0, 480)}`,
        },
      ],
      max_tokens: 100,
      temperature: 0.35,
    });
    let out = (completion.choices[0]?.message?.content || '')
      .trim()
      .replace(/^["'«»]+|["'«»]+$/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (!out) return fallback;
    if (out.length > 65) {
      out = out.slice(0, 65);
      const sp = out.lastIndexOf(' ');
      if (sp > 35) out = out.slice(0, sp);
      out = out.trim();
    }
    if (out.length < 10) return fallback;
    return out;
  } catch (e) {
    console.warn('shortenAmazonListingTitleWithAi:', e.message || e);
    return fallback;
  }
}

/**
 * Etsy / Shopify / stacked SEO titles — optional AI rewrite (see URLTOPIN_SHOP_TITLE_AI).
 */
async function shortenGenericShopListingTitleWithAi(rawTitle, openaiClient) {
  const fallback = shortenGenericShopListingTitleForPins(rawTitle);
  if (
    process.env.URLTOPIN_SHOP_TITLE_AI === '0' ||
    !process.env.OPENAI_API_KEY ||
    !openaiClient
  ) {
    return fallback;
  }
  const cleaned = String(rawTitle || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return fallback;
  if (cleaned.length <= 52) {
    return shortenGenericShopListingTitleForPins(rawTitle);
  }
  try {
    const model = process.env.URLTOPIN_SHOP_TITLE_MODEL || process.env.URLTOPIN_AMAZON_TITLE_MODEL || 'gpt-4o-mini';
    const completion = await openaiClient.chat.completions.create({
      model,
      messages: [
        {
          role: 'user',
          content:
            'Rewrite this ecommerce product or shop listing title as ONE short line for a Pinterest pin.\n' +
            'Rules: max 58 characters; name the product clearly; keep one concrete detail (size, material, color, or key use); no ALL CAPS; no quotation marks; no marketplace name (Etsy, Shopify, Amazon); no "Buy" or price; no ellipsis; output only the line, nothing else.\n\n' +
            `Title:\n${cleaned.slice(0, 480)}`,
        },
      ],
      max_tokens: 100,
      temperature: 0.35,
    });
    let out = (completion.choices[0]?.message?.content || '')
      .trim()
      .replace(/^["'«»]+|["'«»]+$/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (!out) return fallback;
    if (out.length > 65) {
      out = out.slice(0, 65);
      const sp = out.lastIndexOf(' ');
      if (sp > 35) out = out.slice(0, sp);
      out = out.trim();
    }
    if (out.length < 10) return fallback;
    return out;
  } catch (e) {
    console.warn('shortenGenericShopListingTitleWithAi:', e.message || e);
    return fallback;
  }
}

/**
 * Shorten long marketplace / product listing titles for `topic` (Amazon, Etsy, Shopify, /products/ URLs, stacked SEO).
 */
async function maybeShortenPageTitleForUrlToPin(urlString, title, openaiClient, canonicalUrl = '') {
  try {
    if (!title || !urlString) return title;
    const looksLikeAmazonTitle = /(?:^|\s)(?:Amazon)\.[a-z.]+/i.test(String(title));
    const u = new URL(String(urlString).trim());
    let canonicalHostIsAmazon = false;
    let canonicalHostname = '';
    try {
      if (canonicalUrl) {
        const cu = new URL(String(canonicalUrl).trim());
        canonicalHostname = cu.hostname;
        canonicalHostIsAmazon = isAmazonRelatedHost(cu.hostname);
      }
    } catch {
      canonicalHostIsAmazon = false;
    }

    const isAmazon =
      looksLikeAmazonTitle || canonicalHostIsAmazon || isAmazonRelatedHost(u.hostname);
    if (isAmazon) {
      return await shortenAmazonListingTitleWithAi(title, openaiClient);
    }

    if (shouldApplyGenericShopTitleSafety(u.hostname, u.pathname || '', canonicalHostname, title)) {
      return await shortenGenericShopListingTitleWithAi(title, openaiClient);
    }
    return title;
  } catch {
    return shortenGenericShopListingTitleForPins(title);
  }
}

/**
 * Short links, Amazon store/product/affiliate URLs: pin footer should be the user's brand/CTA, not the raw URL host.
 * @returns {{ requiresManualBrandOrCta: boolean, brandingGateReason: string|null, brandingGateMessage: string|null }}
 */
function assessUrlBrandingGate(urlString) {
  const none = {
    requiresManualBrandOrCta: false,
    brandingGateReason: null,
    brandingGateMessage: null,
  };
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return none;
    const u = new URL(raw);
    const host = normalizeUrlHostname(u.hostname);
    const path = u.pathname || '';
    const search = u.search || '';

    if (isYouTubeHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'youtube',
        brandingGateMessage:
          'This looks like a YouTube link. Pins should show your brand or CTA in the footer, not YouTube. Before generating, open Pin look & brand and add your brand name or CTA (e.g. your site name).',
      };
    }

    if (isAffiliateTrackingRedirectHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'affiliate_tracking',
        brandingGateMessage:
          'This looks like an affiliate or network tracking link (not your own site). Pins should show your brand or CTA in the footer, not the tracking URL. Before generating, open Pin look & brand and add your brand name or CTA.',
      };
    }

    if (isEtsyHost(host)) {
      // Etsy shop/product pages should not show Etsy as the footer; require creator brand/CTA.
      const isShop = /^\/shop\/[^/]+/i.test(path);
      const isListing = /^\/listing\/\d+/i.test(path);
      const reason = isShop || isListing ? 'marketplace' : 'marketplace';
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: reason,
        brandingGateMessage:
          'This looks like an Etsy shop/product link. Pins should show your brand or CTA in the footer, not Etsy. Before generating, open Pin look & brand and add your brand name or CTA.',
      };
    }

    if (isPrintifyHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'print_on_demand',
        brandingGateMessage:
          'This looks like a Printify link (print-on-demand storefront). Pins should show your brand or CTA in the footer, not Printify. Before generating, open Pin look & brand and add your brand name or CTA.',
      };
    }

    if (isLikelyUrlShortenerHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'short_link',
        brandingGateMessage:
          'This URL looks like a short or redirect link. Before generating pins, open Pin look & brand and enter your brand name or CTA (that text is what we use in the pin footer).',
      };
    }

    if (isAmazonRelatedHost(host)) {
      const hasAffiliate =
        /[?&]tag=[^&]+/i.test(search) ||
        /[?&]linkCode=/i.test(search) ||
        /[?&](?:creative|creativeASIN)=/i.test(search) ||
        /[?&]ref_=?(?:a|gp|as_li|cm_cr|pd|d)/i.test(search);
      const productish =
        /\/dp\//i.test(path) ||
        /\/gp\/product/i.test(path) ||
        /\/gp\/aw\/d\//i.test(path) ||
        /\/d\/[a-z0-9]/i.test(path) ||
        /\/stores\//i.test(path) ||
        /\/shop\//i.test(path);

      const reason = hasAffiliate || productish ? 'amazon_product_affiliate' : 'amazon_store';
      const msg =
        reason === 'amazon_product_affiliate'
          ? 'This looks like an Amazon product or affiliate link. Pins should show your brand in the footer, not Amazon. Before generating, open Pin look & brand and add your brand name or CTA (e.g. your site name).'
          : 'This looks like an Amazon page. Before generating pins, open Pin look & brand and add your brand name or CTA so the footer represents you, not the store.';

      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: reason,
        brandingGateMessage: msg,
      };
    }

    return none;
  } catch {
    return none;
  }
}

/** Product / affiliate pages where we try to attach Amazon CDN images to Nano Banana (not generic store hubs). */
function isAmazonProductPageForNanoReference(urlString) {
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return false;
    const u = new URL(raw);
    if (!isAmazonRelatedHost(u.hostname)) return false;
    const path = u.pathname || '';
    const search = u.search || '';
    const hasAffiliate =
      /[?&]tag=[^&]+/i.test(search) ||
      /[?&]linkCode=/i.test(search) ||
      /[?&](?:creative|creativeASIN)=/i.test(search) ||
      /[?&]ref_=?(?:a|gp|as_li|cm_cr|pd|d)/i.test(search);
    const productish =
      /\/dp\//i.test(path) ||
      /\/gp\/product/i.test(path) ||
      /\/gp\/aw\/d\//i.test(path) ||
      /\/d\/[A-Za-z0-9]/i.test(path);
    return productish || hasAffiliate;
  } catch {
    return false;
  }
}

/** Nano Banana + Amazon reference images often 422 or look wrong on these infographic layouts. */
const AMAZON_REF_EXCLUDED_INFOGRAPHIC_STYLES = new Set(['timeline_infographic', 'step_cards_3']);

const AMAZON_REF_NON_INFOGRAPHIC_FALLBACKS = [
  'minimal_elegant',
  'before_after',
  'curiosity_shock',
  'offset_collage_3',
  'clean_appetizing',
];

function remapStylesAvoidingInfographicsForAmazonRefs(effectiveStyles, strategicPlan) {
  if (!Array.isArray(effectiveStyles) || effectiveStyles.length === 0) {
    return { styles: effectiveStyles, plan: strategicPlan };
  }
  const styles = [...effectiveStyles];
  const plan = strategicPlan ? strategicPlan.map((p) => ({ ...p })) : null;
  let fb = 0;
  for (let i = 0; i < styles.length; i++) {
    if (!AMAZON_REF_EXCLUDED_INFOGRAPHIC_STYLES.has(styles[i])) continue;
    const replacement =
      AMAZON_REF_NON_INFOGRAPHIC_FALLBACKS[fb % AMAZON_REF_NON_INFOGRAPHIC_FALLBACKS.length];
    fb++;
    styles[i] = replacement;
    if (plan && plan[i]) plan[i].layoutId = replacement;
  }
  return { styles, plan };
}

function replaceInfographicStyleIdForAmazonNanoRefs(styleId, hasReferenceImages) {
  if (!hasReferenceImages || !styleId) return styleId;
  if (!AMAZON_REF_EXCLUDED_INFOGRAPHIC_STYLES.has(styleId)) return styleId;
  return AMAZON_REF_NON_INFOGRAPHIC_FALLBACKS[0];
}

function isAllowedAmazonCdnImageUrl(urlString) {
  try {
    const u = new URL(urlString);
    if (!/^https:/i.test(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (h.includes('media-amazon.com')) return true;
    if (h.includes('ssl-images-amazon.com')) return true;
    if (h.endsWith('images-amazon.com')) return true;
    return false;
  } catch {
    return false;
  }
}

function normalizeAmazonImageUrlString(raw) {
  if (!raw || typeof raw !== 'string') return null;
  let s = raw.trim();
  if (s.startsWith('//')) s = `https:${s}`;
  try {
    const p = new URL(s);
    if (!isAllowedAmazonCdnImageUrl(p.href)) return null;
    return `${p.origin}${p.pathname}`;
  } catch {
    return null;
  }
}

function collectLdJsonProductImages(node, set) {
  if (node == null) return;
  if (Array.isArray(node)) {
    node.forEach((x) => collectLdJsonProductImages(x, set));
    return;
  }
  if (typeof node !== 'object') return;
  if (node['@graph']) {
    collectLdJsonProductImages(node['@graph'], set);
  }
  const typesRaw = node['@type'];
  const types = Array.isArray(typesRaw)
    ? typesRaw.map((t) => String(t).toLowerCase())
    : typesRaw
      ? [String(typesRaw).toLowerCase()]
      : [];
  const isProduct = types.some((t) => t.includes('product'));
  if (isProduct && node.image != null) {
    const imgs = Array.isArray(node.image) ? node.image : [node.image];
    for (const im of imgs) {
      if (typeof im === 'string') {
        const n = normalizeAmazonImageUrlString(im);
        if (n) set.add(n);
      } else if (im && typeof im === 'object' && im.url) {
        const n = normalizeAmazonImageUrlString(im.url);
        if (n) set.add(n);
      }
    }
  }
  for (const key of Object.keys(node)) {
    if (key === '@context' || key === '@type' || key === '@id' || key === '@graph') continue;
    collectLdJsonProductImages(node[key], set);
  }
}

/** Ordered Product images from JSON-LD (gallery order preserved per block). */
function collectLdJsonProductImagesOrdered(node, out, seen) {
  if (node == null) return;
  if (Array.isArray(node)) {
    node.forEach((x) => collectLdJsonProductImagesOrdered(x, out, seen));
    return;
  }
  if (typeof node !== 'object') return;
  if (node['@graph']) {
    collectLdJsonProductImagesOrdered(node['@graph'], out, seen);
  }
  const typesRaw = node['@type'];
  const types = Array.isArray(typesRaw)
    ? typesRaw.map((t) => String(t).toLowerCase())
    : typesRaw
      ? [String(typesRaw).toLowerCase()]
      : [];
  const isProduct = types.some((t) => t.includes('product'));
  if (isProduct && node.image != null) {
    const imgs = Array.isArray(node.image) ? node.image : [node.image];
    for (const im of imgs) {
      let raw = null;
      if (typeof im === 'string') raw = im;
      else if (im && typeof im === 'object' && im.url) raw = im.url;
      const n = raw ? normalizeAmazonImageUrlString(raw) : null;
      if (n && !seen.has(n)) {
        seen.add(n);
        out.push(n);
      }
    }
  }
  for (const key of Object.keys(node)) {
    if (key === '@context' || key === '@type' || key === '@id' || key === '@graph') continue;
    collectLdJsonProductImagesOrdered(node[key], out, seen);
  }
}

function extractAmazonAsinFromUrl(urlString) {
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return null;
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    const path = u.pathname || '';
    let m = path.match(/\/(?:dp|gp\/product|gp\/aw\/d|d)\/([A-Z0-9]{10})/i);
    if (m) return m[1].toUpperCase();
    m = String(u.search || '').match(/[?&]asin=([A-Z0-9]{10})/i);
    if (m) return m[1].toUpperCase();
    return null;
  } catch {
    return null;
  }
}

function pushUniqueAmazonImageUrl(ordered, seen, raw) {
  const n = normalizeAmazonImageUrlString(raw);
  if (!n || seen.has(n)) return;
  seen.add(n);
  ordered.push(n);
}

/**
 * Primary PDP gallery: Amazon embeds hi-res URLs inside a colorImages / ImageBlock JSON blob.
 * Restrict extraction to a window after "colorImages" so we don't harvest unrelated media-amazon
 * thumbnails from the rest of the page.
 */
function extractAmazonColorImagesGalleryUrls(html) {
  const ordered = [];
  const seen = new Set();
  if (!html || typeof html !== 'string') return ordered;
  const needle = 'colorImages';
  let start = 0;
  const hiResRe = /"hiRes"\s*:\s*"([^"]+)"/gi;
  const largeRe = /"large"\s*:\s*"([^"]+)"/gi;
  const maxRegion = 140000;
  const maxTotal = 24;
  while (ordered.length < maxTotal) {
    const idx = html.indexOf(needle, start);
    if (idx < 0) break;
    const region = html.slice(idx, idx + maxRegion);
    let m;
    hiResRe.lastIndex = 0;
    while ((m = hiResRe.exec(region)) !== null && ordered.length < maxTotal) {
      pushUniqueAmazonImageUrl(ordered, seen, m[1]);
    }
    largeRe.lastIndex = 0;
    while ((m = largeRe.exec(region)) !== null && ordered.length < maxTotal) {
      pushUniqueAmazonImageUrl(ordered, seen, m[1]);
    }
    start = idx + needle.length;
  }
  return ordered;
}

function rankAmazonImageUrlCandidates(urls) {
  const score = (u) => {
    let s = 0;
    if (/SL1[5-9]\d{2}_|SL2\d{3}_/i.test(u)) s += 60;
    else if (/SL1[0-4]\d{2}_/i.test(u)) s += 35;
    if (/_AC_UL\d{2,3}_/i.test(u)) s -= 45;
    return s;
  };
  return [...urls].sort((a, b) => score(b) - score(a)).slice(0, 12);
}

function extractAmazonProductImageUrlsFromHtml(html, pageUrl = '') {
  if (!html || typeof html !== 'string') return [];

  const ordered = [];
  const seen = new Set();
  let m;

  const ogRe = /<meta[^>]+property=["']og:image["'][^>]*content=["']([^"']+)["'][^>]*>/gi;
  while ((m = ogRe.exec(html)) !== null) {
    pushUniqueAmazonImageUrl(ordered, seen, m[1]);
  }
  const ogReAlt = /<meta[^>]+content=["']([^"']+)["'][^>]*property=["']og:image["'][^>]*>/gi;
  while ((m = ogReAlt.exec(html)) !== null) {
    pushUniqueAmazonImageUrl(ordered, seen, m[1]);
  }
  const twRe = /<meta[^>]+name=["']twitter:image["'][^>]*content=["']([^"']+)["'][^>]*>/gi;
  while ((m = twRe.exec(html)) !== null) {
    pushUniqueAmazonImageUrl(ordered, seen, m[1]);
  }

  const scriptRe = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  while ((m = scriptRe.exec(html)) !== null) {
    try {
      const jsonText = m[1].trim();
      if (!jsonText) continue;
      collectLdJsonProductImagesOrdered(JSON.parse(jsonText), ordered, seen);
    } catch {
      /* ignore */
    }
  }

  for (const u of extractAmazonColorImagesGalleryUrls(html)) {
    pushUniqueAmazonImageUrl(ordered, seen, u);
  }

  const asin = extractAmazonAsinFromUrl(pageUrl);
  if (asin && ordered.length > 0) {
    const tagged = ordered.filter((u) => u.includes(`_${asin}_`) || u.includes(`.${asin}.`));
    if (tagged.length > 0) {
      return rankAmazonImageUrlCandidates(tagged);
    }
  }

  return rankAmazonImageUrlCandidates(ordered);
}

const MAX_AMAZON_REF_IMAGE_BYTES = 4 * 1024 * 1024;
const AMAZON_NANO_MAX_REFERENCE_IMAGES = 3;

async function mirrorAmazonImageUrlsForNanoBanana(sourceUrls, userId) {
  if (!supabaseAdmin || !userId || !Array.isArray(sourceUrls) || sourceUrls.length === 0) {
    return [];
  }
  const out = [];
  for (let i = 0; i < sourceUrls.length && out.length < AMAZON_NANO_MAX_REFERENCE_IMAGES; i++) {
    const src = sourceUrls[i];
    if (!src || !isAllowedAmazonCdnImageUrl(src)) continue;
    try {
      const res = await fetchWithTimeout(
        src,
        {
          headers: {
            ...URL_SCRAPE_HEADERS,
            Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
          },
        },
        20000
      );
      if (!res.ok) continue;
      const ct = (res.headers.get('content-type') || '').split(';')[0].trim();
      if (!ct.startsWith('image/')) continue;
      const ab = await res.arrayBuffer();
      if (ab.byteLength > MAX_AMAZON_REF_IMAGE_BYTES || ab.byteLength < 1500) continue;
      const buf = Buffer.from(ab);
      const ext = ct.includes('png') ? 'png' : ct.includes('webp') ? 'webp' : 'jpg';
      const fileName = `amazon-ref-${userId}-${Date.now()}-${i}.${ext}`;
      const { error: uploadError } = await supabaseAdmin.storage.from('ai-images').upload(fileName, buf, {
        contentType: ct || 'image/jpeg',
        upsert: true,
      });
      if (uploadError) continue;
      const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
      if (publicUrlData?.publicUrl) out.push(publicUrlData.publicUrl);
    } catch (e) {
      console.warn('mirrorAmazonImageUrlsForNanoBanana:', e.message || e);
    }
  }
  return out;
}

const PAGE_REF_MIN_SCORE = 30;
const PAGE_REF_MAX_CANDIDATES = 14;

function resolveUrlAgainstPage(pageUrlString, href) {
  if (!href || typeof href !== 'string') return null;
  let s = href.trim();
  if (!s || s.startsWith('data:') || s.startsWith('blob:')) return null;
  if (s.startsWith('//')) s = `https:${s}`;
  try {
    if (/^https?:\/\//i.test(s)) return new URL(s).href;
    const base = String(pageUrlString || '').trim();
    if (!base) return null;
    const b = /^https?:\/\//i.test(base) ? base : `https://${base}`;
    return new URL(s, b).href;
  } catch {
    return null;
  }
}

function isAllowedGenericReferenceImageUrl(href) {
  try {
    const u = new URL(String(href || '').trim());
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return false;
    const path = (u.pathname || '').toLowerCase();
    if (/\.(svg)(\?|$)/i.test(path)) return false;
    const bad =
      /(icon|logo|favicon|sprite|pixel|tracking|spacer|badge|button|emoji|avatar|gravatar)/i.test(path);
    if (bad) return false;
    return true;
  } catch {
    return false;
  }
}

function scoreGenericImageCandidate(urlStr, meta) {
  let s = meta.baseScore || 0;
  try {
    const u = new URL(urlStr);
    const path = (u.pathname || '').toLowerCase();
    if (/(thumb|thumbnail|small|mini|tiny|\b50x50\b|\b100x100\b)/i.test(path)) s -= 35;
    const w = meta.width || 0;
    const h = meta.height || 0;
    if (w > 0 && h > 0) {
      const px = w * h;
      if (px >= 800 * 800) s += 55;
      else if (px >= 500 * 500) s += 40;
      else if (px >= 320 * 320) s += 22;
      else if (px < 120 * 120) s -= 40;
    }
  } catch {
    /* ignore */
  }
  return s;
}

function pushGenericCandidate(bucket, href, pageUrl, meta) {
  const abs = resolveUrlAgainstPage(pageUrl, href);
  if (!abs || !isAllowedGenericReferenceImageUrl(abs)) return;
  const score = scoreGenericImageCandidate(abs, meta);
  bucket.push({ url: abs, score });
}

function parseSrcsetLargestUrl(srcsetRaw, pageUrl) {
  if (!srcsetRaw || typeof srcsetRaw !== 'string') return null;
  const parts = srcsetRaw.split(',').map((p) => p.trim()).filter(Boolean);
  let bestUrl = null;
  let bestW = -1;
  for (const p of parts) {
    const segs = p.split(/\s+/);
    const rawU = segs[0];
    let w = 0;
    for (let i = 1; i < segs.length; i++) {
      const m = segs[i].match(/^(\d+)w$/i);
      if (m) w = Math.max(w, parseInt(m[1], 10));
    }
    const abs = resolveUrlAgainstPage(pageUrl, rawU);
    if (!abs) continue;
    if (w > bestW) {
      bestW = w;
      bestUrl = abs;
    } else if (bestW <= 0 && !bestUrl) {
      bestUrl = abs;
    }
  }
  return bestUrl;
}

function collectLdJsonContentImages(node, pageUrl, bucket) {
  if (node == null) return;
  if (Array.isArray(node)) {
    node.forEach((x) => collectLdJsonContentImages(x, pageUrl, bucket));
    return;
  }
  if (typeof node !== 'object') return;
  if (node['@graph']) {
    collectLdJsonContentImages(node['@graph'], pageUrl, bucket);
  }
  const typesRaw = node['@type'];
  const types = Array.isArray(typesRaw)
    ? typesRaw.map((t) => String(t).toLowerCase())
    : typesRaw
      ? [String(typesRaw).toLowerCase()]
      : [];
  const wantsImage = types.some((t) =>
    /(article|blogposting|newsarticle|recipe|product|webpage)/i.test(t)
  );
  if (wantsImage && node.image != null) {
    const imgs = Array.isArray(node.image) ? node.image : [node.image];
    for (const im of imgs) {
      if (typeof im === 'string') {
        pushGenericCandidate(bucket, im, pageUrl, { baseScore: 38 });
      } else if (im && typeof im === 'object') {
        if (im.url) pushGenericCandidate(bucket, im.url, pageUrl, { baseScore: 38 });
        if (Array.isArray(im['@list'])) collectLdJsonContentImages(im['@list'], pageUrl, bucket);
      }
    }
  }
  for (const key of Object.keys(node)) {
    if (key === '@context' || key === '@type' || key === '@id' || key === '@graph') continue;
    collectLdJsonContentImages(node[key], pageUrl, bucket);
  }
}

/**
 * Best-effort harvest of hero/content images from arbitrary pages (blogs, shops, etc.).
 * Returns absolute URLs sorted by score (higher first).
 */
function extractGenericPageImageUrlsFromHtml(html, pageUrl = '') {
  if (!html || typeof html !== 'string' || !pageUrl) return [];
  const bucket = [];
  let m;

  const ogRe = /<meta[^>]+property=["']og:image["'][^>]*content=["']([^"']+)["'][^>]*>/gi;
  while ((m = ogRe.exec(html)) !== null) {
    pushGenericCandidate(bucket, m[1], pageUrl, { baseScore: 42 });
  }
  const ogReAlt = /<meta[^>]+content=["']([^"']+)["'][^>]*property=["']og:image["'][^>]*>/gi;
  while ((m = ogReAlt.exec(html)) !== null) {
    pushGenericCandidate(bucket, m[1], pageUrl, { baseScore: 42 });
  }
  const twRe = /<meta[^>]+name=["']twitter:image["'][^>]*content=["']([^"']+)["'][^>]*>/gi;
  while ((m = twRe.exec(html)) !== null) {
    pushGenericCandidate(bucket, m[1], pageUrl, { baseScore: 40 });
  }

  const scriptRe = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  while ((m = scriptRe.exec(html)) !== null) {
    try {
      const jsonText = m[1].trim();
      if (!jsonText) continue;
      collectLdJsonContentImages(JSON.parse(jsonText), pageUrl, bucket);
    } catch {
      /* ignore */
    }
  }

  const imgRe = /<img\b[^>]*>/gi;
  while ((m = imgRe.exec(html)) !== null) {
    const tag = m[0];
    const srcM = /\bsrc\s*=\s*["']([^"']+)["']/i.exec(tag);
    const srcsetM = /\bsrcset\s*=\s*["']([^"']+)["']/i.exec(tag);
    const wM = /\bwidth\s*=\s*["']?(\d+)/i.exec(tag);
    const hM = /\bheight\s*=\s*["']?(\d+)/i.exec(tag);
    const width = wM ? parseInt(wM[1], 10) : 0;
    const height = hM ? parseInt(hM[1], 10) : 0;
    const fromSet = srcsetM ? parseSrcsetLargestUrl(srcsetM[1], pageUrl) : null;
    if (fromSet) {
      pushGenericCandidate(bucket, fromSet, pageUrl, { baseScore: 12, width, height });
    } else if (srcM) {
      pushGenericCandidate(bucket, srcM[1], pageUrl, { baseScore: 8, width, height });
    }
  }

  const byUrl = new Map();
  for (const c of bucket) {
    const prev = byUrl.get(c.url);
    if (!prev || c.score > prev) byUrl.set(c.url, c.score);
  }
  return [...byUrl.entries()]
    .map(([url, score]) => ({ url, score }))
    .filter((x) => x.score >= PAGE_REF_MIN_SCORE)
    .sort((a, b) => b.score - a.score)
    .slice(0, PAGE_REF_MAX_CANDIDATES)
    .map((x) => x.url);
}

async function mirrorGenericPageImageUrlsForNanoBanana(sourceUrls, userId) {
  if (!supabaseAdmin || !userId || !Array.isArray(sourceUrls) || sourceUrls.length === 0) {
    return [];
  }
  const out = [];
  for (let i = 0; i < sourceUrls.length && out.length < AMAZON_NANO_MAX_REFERENCE_IMAGES; i++) {
    const src = sourceUrls[i];
    if (!src || !isAllowedGenericReferenceImageUrl(src)) continue;
    try {
      let referer = '';
      try {
        referer = `${new URL(src).origin}/`;
      } catch {
        referer = '';
      }
      const res = await fetchWithTimeout(
        src,
        {
          headers: {
            ...URL_SCRAPE_HEADERS,
            Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
            ...(referer ? { Referer: referer } : {}),
          },
        },
        20000
      );
      if (!res.ok) continue;
      const ct = (res.headers.get('content-type') || '').split(';')[0].trim();
      if (!ct.startsWith('image/')) continue;
      const ab = await res.arrayBuffer();
      if (ab.byteLength > MAX_AMAZON_REF_IMAGE_BYTES || ab.byteLength < 1500) continue;
      const buf = Buffer.from(ab);
      const ext = ct.includes('png') ? 'png' : ct.includes('webp') ? 'webp' : 'jpg';
      const fileName = `page-ref-${userId}-${Date.now()}-${i}.${ext}`;
      const { error: uploadError } = await supabaseAdmin.storage.from('ai-images').upload(fileName, buf, {
        contentType: ct || 'image/jpeg',
        upsert: true,
      });
      if (uploadError) continue;
      const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
      if (publicUrlData?.publicUrl) out.push(publicUrlData.publicUrl);
    } catch (e) {
      console.warn('mirrorGenericPageImageUrlsForNanoBanana:', e.message || e);
    }
  }
  return out;
}

function extractMetaFromHtml(html, url) {
  let title = '';
  let description = '';
  let canonicalUrl = '';
  try {
    const titleMatch = html.match(/<title[^>]*>([^<]*)<\/title>/i);
    if (titleMatch && titleMatch[1]) {
      title = titleMatch[1].trim();
    }
    const ogTitleMatch = html.match(/<meta[^>]+property=["']og:title["'][^>]*content=["']([^"']*)["'][^>]*>/i);
    if (ogTitleMatch && ogTitleMatch[1]) {
      title = ogTitleMatch[1].trim();
    }
    const canonicalMatch = html.match(/<link[^>]+rel=["']canonical["'][^>]*href=["']([^"']+)["'][^>]*>/i);
    if (canonicalMatch && canonicalMatch[1]) {
      canonicalUrl = canonicalMatch[1].trim();
    }
    const descMatch = html.match(/<meta[^>]+name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i);
    if (descMatch && descMatch[1]) {
      description = descMatch[1].trim();
    }
    const ogDescMatch = html.match(/<meta[^>]+property=["']og:description["'][^>]*content=["']([^"']*)["'][^>]*>/i);
    if (ogDescMatch && ogDescMatch[1]) {
      description = ogDescMatch[1].trim();
    }
  } catch (_) {
    // best-effort only
  }

  let domain = '';
  let keyword = '';
  let linkDisplay = '';
  try {
    const u = new URL(url);
    domain = u.hostname.replace(/^www\./i, '');
    linkDisplay = buildLinkDisplayLabelFromUrl(url, 80);
    keyword = deriveKeywordFromArticleUrl(url);
  } catch (_) {
    domain = '';
    keyword = '';
    linkDisplay = '';
  }

  return { title, description, canonicalUrl, domain, keyword, linkDisplay };
}

/** Browser-like headers — bare Node fetch gets 403 from Medium and similar CDNs */
const URL_SCRAPE_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9',
};

/**
 * Load public article HTML: fetch with browser headers first; on 403/401/503/429 or network error,
 * retry with Puppeteer (Medium often blocks datacenter fetch).
 */
async function fetchArticleHtml(url) {
  try {
    const resp = await fetch(url, { redirect: 'follow', headers: URL_SCRAPE_HEADERS });
    if (resp.ok) {
      return await resp.text();
    }
    const status = resp.status;
    console.warn('fetchArticleHtml non-OK:', String(url).slice(0, 96), status);
    if (status === 403 || status === 401 || status === 503 || status === 429) {
      const puppetHtml = await fetchArticleHtmlViaPuppeteer(url);
      if (puppetHtml) return puppetHtml;
    }
    return '';
  } catch (e) {
    console.warn('fetchArticleHtml error:', e.message || e);
    const puppetHtml = await fetchArticleHtmlViaPuppeteer(url);
    return puppetHtml || '';
  }
}

async function fetchArticleHtmlViaPuppeteer(pageUrl) {
  if (process.env.DISABLE_URLTOPIN_PUPPETEER === '1') {
    return '';
  }
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
      timeout: 60000,
    });
    const page = await browser.newPage();
    await page.setUserAgent(URL_SCRAPE_HEADERS['User-Agent']);
    await page.setExtraHTTPHeaders({
      'Accept-Language': URL_SCRAPE_HEADERS['Accept-Language'],
    });
    await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 45000 });
    await new Promise((r) => setTimeout(r, 2000));
    return await page.content();
  } catch (e) {
    console.warn('fetchArticleHtmlViaPuppeteer:', e.message || e);
    return '';
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (_) {
        /* ignore */
      }
    }
  }
}

/**
 * Fetch full article HTML and build richer base metadata + summary.
 * Falls back gracefully to meta tags only if fetch or parsing fails.
 */
async function fetchArticleBaseAndSummary(url, clientArticleData, opts = null) {
  const fast = !!opts?.fast;
  const hasClientMeta =
    clientArticleData &&
    typeof clientArticleData === 'object' &&
    (clientArticleData.title || clientArticleData.description || clientArticleData.domain);

  // In "fast" mode (used for strategic_single fan-out requests), avoid refetching full HTML.
  // We already scraped client-side and pass basic metadata; the slight summary quality drop is
  // worth the latency win and reduces user abandonment.
  let html = '';
  if (!fast || !hasClientMeta) {
    try {
      html = await fetchArticleHtml(url);
    } catch (e) {
      console.warn('fetchArticleBaseAndSummary fetch error:', e.message || e);
    }
  }

  const metaFromHtml = extractMetaFromHtml(html || '', url);
  const base = {
    ...metaFromHtml,
    ...(clientArticleData || {}),
  };
  // Canonical article URL always wins for display + keyword (fixes short links & opaque path slugs).
  const derivedDisplay = buildLinkDisplayLabelFromUrl(url, 80);
  const derivedKw = deriveKeywordFromArticleUrl(url);
  base.linkDisplay = derivedDisplay || base.linkDisplay || '';
  base.keyword = derivedKw;
  Object.assign(base, assessUrlBrandingGate(url));
  if (base.title) {
    base.title = await maybeShortenPageTitleForUrlToPin(url, base.title, openai, base.canonicalUrl);
  }

  // Very lightweight body text extraction from HTML
  let bodyText = '';
  if (html) {
    try {
      bodyText = html
        .replace(/<script[\s\S]*?<\/script>/gi, ' ')
        .replace(/<style[\s\S]*?<\/style>/gi, ' ')
        .replace(/<!--[\s\S]*?-->/g, ' ')
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    } catch (e) {
      console.warn('fetchArticleBaseAndSummary body parse error:', e.message || e);
    }
  }

  const summaryParts = [
    base.title || '',
    base.description || '',
    bodyText ? bodyText.slice(0, 1000) : '',
  ].filter(Boolean);

  const articleSummary = summaryParts.join('. ').slice(0, 1200);

  return { base, articleSummary };
}

// Background job processor for scheduled pins
async function processScheduledPins() {
  console.log('🔄 Processing scheduled pins...');
  
  try {
    const nowIso = new Date().toISOString();
    const fetchLimit = 30; // fetch enough from each bucket to merge; final batch is capped below

    // Due "scheduled" pins (normal queue).
    const scheduledQuery = supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('status', 'scheduled')
      .lte('scheduled_for', nowIso)
      .is('deleted_at', null)
      .order('scheduled_for', { ascending: true })
      .limit(fetchLimit);

    // Due "failed" pins that still have a retry time — NOT permanent failures
    // (those have next_retry_at = null and must not match, or they clog every run).
    const failedRetryQuery = supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('status', 'failed')
      .lte('scheduled_for', nowIso)
      .not('next_retry_at', 'is', null)
      .lte('next_retry_at', nowIso)
      .is('deleted_at', null)
      .order('scheduled_for', { ascending: true })
      .limit(fetchLimit);

    const [{ data: scheduledDue, error: errScheduled }, { data: failedRetryDue, error: errFailed }] =
      await Promise.all([scheduledQuery, failedRetryQuery]);

    if (errScheduled) {
      console.error('❌ Error fetching due scheduled pins:', errScheduled);
      return;
    }
    if (errFailed) {
      console.error('❌ Error fetching failed pins due for retry:', errFailed);
      return;
    }

    const merged = [...(scheduledDue || []), ...(failedRetryDue || [])].sort(
      (a, b) => new Date(a.scheduled_for) - new Date(b.scheduled_for)
    );
    const pinsToPost = merged.slice(0, 10); // Process 10 pins at a time

    if (!pinsToPost || pinsToPost.length === 0) {
      console.log('✅ No scheduled pins to process');
      return;
    }

    console.log(`📌 Found ${pinsToPost.length} pins to process`);

    // Group pins by link to detect potential spam patterns
    const linkGroups = {};
    pinsToPost.forEach(pin => {
      const link = pin.link || 'no-link';
      if (!linkGroups[link]) linkGroups[link] = [];
      linkGroups[link].push(pin);
    });

    // Process pins with anti-spam delays
    for (const pin of pinsToPost) {
      await processScheduledPin(pin);
      
      // Add delay between pins with the same link to avoid spam detection
      const link = pin.link || 'no-link';
      const pinsWithSameLink = linkGroups[link];
      
      if (pinsWithSameLink && pinsWithSameLink.length > 1) {
        // If multiple pins have the same link, add extra delay
        const delayMinutes = Math.min(5, pinsWithSameLink.length); // 1-5 minutes based on count
        console.log(`⏱️ Adding ${delayMinutes} minute delay to avoid spam detection for link: ${link}`);
        await new Promise(resolve => setTimeout(resolve, delayMinutes * 60 * 1000));
      } else {
        // Standard delay between different pins
        console.log('⏱️ Adding 30 second delay between pins');
        await new Promise(resolve => setTimeout(resolve, 30 * 1000));
      }
    }

  } catch (error) {
    console.error('❌ Error in processScheduledPins:', error);
  }
}

async function processScheduledPin(pin) {
  console.log(`🚀 Processing pin: ${pin.title.substring(0, 50)}...`);
  
  try {
    // Mark as posting to prevent duplicate processing
    const { error: updateError } = await supabaseAdmin
      .from('scheduled_pins')
      .update({ 
        status: 'posting',
        updated_at: new Date().toISOString()
      })
      .eq('id', pin.id);

    if (updateError) {
      console.error('❌ Error updating pin status to posting:', updateError);
      return;
    }

    const accountId = pin.pinterest_account_id;
    if (!accountId) {
      await handlePinError(pin.id, 'No Pinterest account linked to this scheduled pin');
      return;
    }

    const { data: accRow, error: accFetchErr } = await supabaseAdmin
      .from('pinterest_accounts')
      .select('id, access_token, refresh_token, token_expires_at')
      .eq('id', accountId)
      .single();

    if (accFetchErr || !accRow) {
      await handlePinError(pin.id, 'Pinterest account not found for scheduled pin');
      return;
    }

    let { accessToken, error: tokErr } = await ensureValidPinterestAccessToken(accRow);
    if (!accessToken) {
      await handlePinError(pin.id, tokErr || 'No Pinterest access token');
      return;
    }

    if (!pin.board_id) {
      await handlePinError(
        pin.id,
        'Permanent failure: No board_id set for this scheduled pin. User must select a Pinterest board before posting.',
        pin.retry_count || 0,
        { retryable: false }
      );
      return;
    }

    // Pre-flight validation: confirm this token can access the target board.
    // This prevents wasting retries on 403 "not permitted" errors caused by mismatched board/account.
    if ((pin.retry_count || 0) === 0) {
      const boardAccess = await pinterestValidateBoardAccess(accessToken, pin.board_id);
      if (!boardAccess.ok) {
        const reason = boardAccess.status
          ? `status ${boardAccess.status}`
          : 'unknown status';
        await handlePinError(
          pin.id,
          `Pinterest board access check failed (${reason}) for board_id=${pin.board_id}. ${boardAccess.message} Likely causes: board belongs to a different connected account, board was deleted, or the user must reconnect Pinterest and reselect a board.`,
          pin.retry_count || 0,
          { retryable: false, status: boardAccess.status }
        );
        return;
      }
    }

    // Create Pinterest pin using existing logic
    const requestBody = {
      board_id: pin.board_id,
      title: pin.title,
      description: pin.description,
      media_source: {
        source_type: 'image_url',
        url: pin.image_url,
      },
      link: pin.link || undefined,
    };
    
    console.log(`📤 Posting to Pinterest API for pin: ${pin.id}`);
    
    const postPin = (token) =>
      fetch('https://api.pinterest.com/v5/pins', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });

    let pinterestRes = await postPin(accessToken);
    let pinData = await pinterestRes.json();

    if (!pinterestRes.ok && pinterestResponseIsAuthFailure(pinterestRes.status, pinData)) {
      const { data: accFresh } = await supabaseAdmin
        .from('pinterest_accounts')
        .select('id, access_token, refresh_token, token_expires_at')
        .eq('id', accountId)
        .single();
      const newAccess = await pinterestRefreshAfterAuthFailure(accFresh || accRow);
      if (newAccess) {
        console.log(`Retrying Pinterest post for pin ${pin.id} after token refresh`);
        pinterestRes = await postPin(newAccess);
        pinData = await pinterestRes.json();
      }
    }

    if (pinterestRes.ok) {
      // Success - update pin as posted with analytics data
      const postedAt = new Date();
      const postedHour = postedAt.getHours();
      const postedDayOfWeek = postedAt.getDay();
      
      await supabaseAdmin
        .from('scheduled_pins')
        .update({ 
          status: 'posted',
          posted_at: postedAt.toISOString(),
          pinterest_pin_id: pinData.id,
          error_message: null,
          retry_count: 0,
          next_retry_at: null,
          updated_at: postedAt.toISOString(),
          posted_hour: postedHour,
          posted_day_of_week: postedDayOfWeek,
          posted_timezone: pin.timezone || 'UTC'
        })
        .eq('id', pin.id);

      // Also update the corresponding user_images record
      await supabaseAdmin
        .from('user_images')
        .update({ 
          pinterest_uploaded: true,
          pinterest_pin_id: pinData.id,
          is_scheduled: false,
          scheduled_for: null,
          posted_hour: postedHour,
          posted_day_of_week: postedDayOfWeek,
          posted_timezone: pin.timezone || 'UTC'
        })
        .eq('user_id', pin.user_id)
        .eq('image_url', pin.image_url);

      // Deduct credits if not already done
      if (!pin.credits_deducted) {
        await deductUserCredits(pin.user_id, 1);
        await supabaseAdmin
          .from('scheduled_pins')
          .update({ credits_deducted: true })
          .eq('id', pin.id);
      }

      console.log(`✅ Successfully posted pin: ${pin.id} -> Pinterest ID: ${pinData.id}`);
      
    } else {
      // Handle Pinterest API error
      const status = pinterestRes.status;
      const errorMessage = pinData.message || pinData.error || 'Pinterest API error';
      console.error(`❌ Pinterest API error for pin ${pin.id}:`, errorMessage);
      
      const isPermissionError = pinterestResponseIsPermissionFailure(status, pinData);
      const isValidationError = status === 400 || status === 404;
      const isRateLimit = status === 429;
      const isServerError = status >= 500;

      // Permission and validation errors are not fixed by retries/token refresh.
      const retryable = !isPermissionError && !isValidationError && (isRateLimit || isServerError || true);

      const finalMessage = isPermissionError
        ? `Pinterest permission error (board/account access): ${errorMessage}. Likely causes: board_id not owned by this connected account, board removed, missing app scopes, or the user needs to reconnect Pinterest and reselect a board.`
        : errorMessage;

      await handlePinError(pin.id, finalMessage, pin.retry_count || 0, { retryable, status });
    }

  } catch (error) {
    console.error(`❌ Error processing pin ${pin.id}:`, error);
    await handlePinError(pin.id, error.message, pin.retry_count || 0);
  }
}

async function handlePinError(pinId, errorMessage, currentRetryCount = 0, options = {}) {
  const maxRetries = 3;
  const nextRetryCount = currentRetryCount + 1;
  const retryable = options?.retryable !== false;
  
  // Check if this is a spam-related error
  const isSpamError = errorMessage.toLowerCase().includes('spam') || 
                      errorMessage.toLowerCase().includes('blocked') ||
                      errorMessage.toLowerCase().includes('redirect');
  
  if (!retryable) {
    // Permanent error: don't waste retries (e.g., permission/validation)
    await supabaseAdmin
      .from('scheduled_pins')
      .update({ 
        status: 'failed',
        error_message: `Permanent failure: ${errorMessage}`,
        next_retry_at: null,
        updated_at: new Date().toISOString()
      })
      .eq('id', pinId);
    console.log(`❌ Pin ${pinId} failed permanently (non-retryable error)`);
    return;
  }

  if (nextRetryCount <= maxRetries) {
    // Calculate backoff based on error type
    let backoffMinutes;
    if (isSpamError) {
      // Longer delays for spam errors: 30min, 90min, 270min (4.5 hours)
      backoffMinutes = 30 * Math.pow(3, currentRetryCount);
      console.log(`🚫 Spam-related error detected for pin ${pinId}, using extended retry delay`);
    } else {
      // Standard exponential backoff: 5min, 15min, 45min
      backoffMinutes = 5 * Math.pow(3, currentRetryCount);
    }
    
    const nextRetryAt = new Date();
    nextRetryAt.setMinutes(nextRetryAt.getMinutes() + backoffMinutes);
    
    await supabaseAdmin
      .from('scheduled_pins')
      .update({ 
        status: 'failed',
        error_message: errorMessage,
        retry_count: nextRetryCount,
        next_retry_at: nextRetryAt.toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', pinId);
      
    console.log(`🔄 Pin ${pinId} will retry in ${backoffMinutes} minutes (attempt ${nextRetryCount}/${maxRetries}) - ${isSpamError ? 'SPAM ERROR' : 'STANDARD ERROR'}`);
  } else {
    // Max retries reached
    await supabaseAdmin
      .from('scheduled_pins')
      .update({ 
        status: 'failed',
        error_message: `Max retries reached: ${errorMessage}`,
        next_retry_at: null,
        updated_at: new Date().toISOString()
      })
      .eq('id', pinId);
      
    console.log(`❌ Pin ${pinId} failed permanently after ${maxRetries} retries`);
  }
}

async function deductUserCredits(userId, amount) {
  try {
    const { data: profile, error: fetchError } = await supabaseAdmin
      .from('profiles')
      .select('credits_remaining')
      .eq('id', userId)
      .single();

    if (fetchError || !profile) {
      console.error('Error fetching user profile for credit deduction:', fetchError);
      return;
    }

    const newCredits = Math.max(0, (profile.credits_remaining || 0) - amount);
    
    const { error: updateError } = await supabaseAdmin
      .from('profiles')
      .update({ credits_remaining: newCredits })
      .eq('id', userId);

    if (updateError) {
      console.error('Error updating user credits:', updateError);
    } else {
      console.log(`💳 Deducted ${amount} credits from user ${userId}, remaining: ${newCredits}`);
    }
  } catch (error) {
    console.error('Error in deductUserCredits:', error);
  }
}

// Function to reschedule spam-blocked pins with better spacing
async function rescheduleSpamBlockedPins(userId) {
  try {
    console.log(`🔄 Checking for spam-blocked pins to reschedule for user ${userId}`);
    
    // Get pins that failed due to spam
    const { data: spamBlockedPins, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('user_id', userId)
      .eq('status', 'failed')
      .ilike('error_message', '%spam%')
      .or('error_message.ilike.%blocked%,error_message.ilike.%redirect%')
      .order('scheduled_for', { ascending: true });

    if (fetchError || !spamBlockedPins || spamBlockedPins.length === 0) {
      return;
    }

    console.log(`📌 Found ${spamBlockedPins.length} spam-blocked pins to reschedule`);

    // Group by link to space them out properly
    const linkGroups = {};
    spamBlockedPins.forEach(pin => {
      const link = pin.link || 'no-link';
      if (!linkGroups[link]) linkGroups[link] = [];
      linkGroups[link].push(pin);
    });

    // Reschedule with proper spacing
    for (const [link, pins] of Object.entries(linkGroups)) {
      if (pins.length > 1) {
        console.log(`🔄 Rescheduling ${pins.length} pins with link: ${link}`);
        
        for (let i = 0; i < pins.length; i++) {
          const pin = pins[i];
          const newScheduleTime = new Date();
          // Space pins with same link 2-6 hours apart
          newScheduleTime.setHours(newScheduleTime.getHours() + 2 + (i * 2));
          
          await supabaseAdmin
            .from('scheduled_pins')
            .update({
              status: 'scheduled',
              scheduled_for: newScheduleTime.toISOString(),
              error_message: null,
              retry_count: 0,
              next_retry_at: null,
              updated_at: new Date().toISOString()
            })
            .eq('id', pin.id);
            
          console.log(`⏰ Rescheduled pin ${pin.id} for ${newScheduleTime.toLocaleString()}`);
        }
      }
    }
  } catch (error) {
    console.error('❌ Error rescheduling spam-blocked pins:', error);
  }
}

// Background job processor for Pinterest analytics sync
async function processAnalyticsSync() {
  console.log('📊 Processing automatic Pinterest analytics sync...');
  
  try {
    // Get all unique users who have posted pins with Pinterest pin IDs from scheduled_pins
    const { data: scheduledPostedPins, error: scheduledFetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('user_id')
      .eq('status', 'posted')
      .not('pinterest_pin_id', 'is', null);

    // Also get users who have uploaded pins directly (Images mode) from user_images
    const { data: directUploadPins, error: directFetchError } = await supabaseAdmin
      .from('user_images')
      .select('user_id')
      .eq('pinterest_uploaded', true)
      .not('pinterest_pin_id', 'is', null);

    if (scheduledFetchError) {
      console.error('❌ Error fetching scheduled posted pins for analytics sync:', scheduledFetchError);
    }
    if (directFetchError) {
      console.error('❌ Error fetching direct upload pins for analytics sync:', directFetchError);
    }
    if (scheduledFetchError && directFetchError) {
      console.error('❌ Both queries failed, aborting analytics sync');
      return;
    }

    // Collect unique user_ids across both sources
    const userIdSet = new Set();
    (scheduledPostedPins || []).forEach(pin => {
      if (pin.user_id) userIdSet.add(pin.user_id);
    });
    (directUploadPins || []).forEach(pin => {
      if (pin.user_id) userIdSet.add(pin.user_id);
    });

    const userIds = Array.from(userIdSet);

    if (!userIds || userIds.length === 0) {
      console.log('✅ No users with posted pins found for analytics sync');
      return;
    }

    console.log(`📊 Found ${userIds.length} users for analytics sync`);

    // Process each user's analytics
    for (const userId of userIds) {
      try {
        // Resolve the correct Pinterest access token for this user
        const accessToken = await getPinterestAccessTokenForUser(userId, null);
        if (!accessToken) {
          console.log(`⚠️ No Pinterest access token for user ${userId}, skipping analytics sync`);
          continue;
        }
        await syncUserAnalytics(userId, accessToken);
        // Add delay between users to respect rate limits
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        console.error(`❌ Error syncing analytics for user ${userPin.user_id}:`, error);
      }
    }

    console.log('✅ Automatic analytics sync completed');

  } catch (error) {
    console.error('❌ Error in processAnalyticsSync:', error);
  }
}

async function syncUserAnalytics(userId, accessToken) {
  if (!accessToken) {
    console.log(`⚠️ No access token found for user ${userId}, skipping`);
    return;
  }

  // Get all posted pins for this user that haven't been updated in 12+ hours
  const { data: scheduledPins, error: scheduledError } = await supabaseAdmin
    .from('scheduled_pins')
    .select('id, pinterest_pin_id, metrics_last_updated')
    .eq('user_id', userId)
    .eq('status', 'posted')
    .not('pinterest_pin_id', 'is', null)
    .limit(10); // Limit per user to avoid rate limits

  // Also get direct uploads from user_images
  const { data: userImagePins, error: userImagesError } = await supabaseAdmin
    .from('user_images')
    .select('id, pinterest_pin_id, metrics_last_updated')
    .eq('user_id', userId)
    .eq('pinterest_uploaded', true)
    .not('pinterest_pin_id', 'is', null)
    .limit(10);

  // Combine both sources and deduplicate by pinterest_pin_id
  const allPins = [];
  const pinIdSet = new Set();

  // Add scheduled pins
  if (scheduledPins) {
    scheduledPins.forEach(pin => {
      if (!pinIdSet.has(pin.pinterest_pin_id)) {
        pinIdSet.add(pin.pinterest_pin_id);
        allPins.push({ ...pin, source: 'scheduled_pins' });
      }
    });
  }

  // Add user image pins (if not already added)
  if (userImagePins) {
    userImagePins.forEach(pin => {
      if (!pinIdSet.has(pin.pinterest_pin_id)) {
        pinIdSet.add(pin.pinterest_pin_id);
        allPins.push({ ...pin, source: 'user_images' });
      }
    });
  }

  if (allPins.length === 0) {
    console.log(`📊 No pins found for user ${userId} to sync analytics`);
    return;
  }

  let syncedCount = 0;
  const twelveHoursAgo = new Date();
  twelveHoursAgo.setHours(twelveHoursAgo.getHours() - 12);

  for (const pin of allPins) {
    try {
      // Skip if updated recently (within 12 hours)
      if (pin.metrics_last_updated) {
        const lastUpdate = new Date(pin.metrics_last_updated);
        if (lastUpdate > twelveHoursAgo) {
          continue;
        }
      }

      // Fetch analytics from Pinterest API
      // Pinterest API only allows data from the last 90 days
      const endDate = new Date().toISOString().split('T')[0];
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - 89); // 89 days ago (to be safe)
      const startDateStr = startDate.toISOString().split('T')[0];
      
      const analyticsUrl = `https://api.pinterest.com/v5/pins/${pin.pinterest_pin_id}/analytics?start_date=${startDateStr}&end_date=${endDate}&metric_types=IMPRESSION,OUTBOUND_CLICK,SAVE,PIN_CLICK,CLOSEUP`;
      
      const analyticsResponse = await fetch(analyticsUrl, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        }
      });

      if (!analyticsResponse.ok) {
        console.error(`❌ Pinterest API error for pin ${pin.pinterest_pin_id}`);
        continue;
      }

      const analyticsData = await analyticsResponse.json();
      
      // Handle different Pinterest API response formats
      let metrics = {};
      console.log(`📊 Raw Pinterest API response for pin ${pin.pinterest_pin_id}:`, JSON.stringify(analyticsData, null, 2));
      
      // Try different response structures Pinterest might use
      if (analyticsData.all_time) {
        metrics = analyticsData.all_time;
        console.log(`📊 Using all_time structure for pin ${pin.pinterest_pin_id}:`, metrics);
      } else if (analyticsData.summary) {
        metrics = analyticsData.summary;
        console.log(`📊 Using summary structure for pin ${pin.pinterest_pin_id}:`, metrics);
      } else if (analyticsData.all && analyticsData.all.summary_metrics) {
        metrics = analyticsData.all.summary_metrics;
        console.log(`📊 Using all.summary_metrics structure for pin ${pin.pinterest_pin_id}:`, metrics);
      } else if (analyticsData.all && analyticsData.all.daily_metrics) {
        // Try daily metrics if summary_metrics is empty
        const dailyMetrics = analyticsData.all.daily_metrics;
        if (Array.isArray(dailyMetrics) && dailyMetrics.length > 0) {
          // Sum up daily metrics
          metrics = dailyMetrics.reduce((acc, day) => {
            if (day.data_status === 'READY') {
              acc.IMPRESSION = (acc.IMPRESSION || 0) + (day.metrics?.IMPRESSION || 0);
              acc.SAVE = (acc.SAVE || 0) + (day.metrics?.SAVE || 0);
              acc.PIN_CLICK = (acc.PIN_CLICK || 0) + (day.metrics?.PIN_CLICK || 0);
              acc.OUTBOUND_CLICK = (acc.OUTBOUND_CLICK || 0) + (day.metrics?.OUTBOUND_CLICK || 0);
              acc.CLOSEUP = (acc.CLOSEUP || 0) + (day.metrics?.CLOSEUP || 0);
            }
            return acc;
          }, {});
          console.log(`📊 Using summed daily_metrics for pin ${pin.pinterest_pin_id}:`, metrics);
        }
      } else if (analyticsData.all) {
        // Try the all object directly
        metrics = analyticsData.all;
        console.log(`📊 Using all structure directly for pin ${pin.pinterest_pin_id}:`, metrics);
      } else if (analyticsData.summary_metrics) {
        metrics = analyticsData.summary_metrics;
        console.log(`📊 Using summary_metrics structure for pin ${pin.pinterest_pin_id}:`, metrics);
      } else {
        metrics = analyticsData;
        console.log(`📊 Using root structure for pin ${pin.pinterest_pin_id}:`, metrics);
      }
      
      const impressions = metrics.IMPRESSION || 0;
      const outboundClicks = metrics.OUTBOUND_CLICK || 0;
      const saves = metrics.SAVE || 0;
      const pinClicks = metrics.PIN_CLICK || 0;
      const closeupViews = metrics.CLOSEUP || 0;
      
      console.log(`📊 Extracted metrics for pin ${pin.pinterest_pin_id}:`, {
        impressions, outboundClicks, saves, pinClicks, closeupViews, rawMetrics: metrics
      });
      
      // Calculate engagement metrics
      const engagementRate = impressions > 0 ? ((saves + pinClicks) / impressions) * 100 : 0;
      const clickThroughRate = impressions > 0 ? (outboundClicks / impressions) * 100 : 0;
      const saveRate = impressions > 0 ? (saves / impressions) * 100 : 0;

      // Update database based on source
      const updateData = {
        impressions,
        outbound_clicks: outboundClicks,
        saves,
        pin_clicks: pinClicks,
        closeup_views: closeupViews,
        engagement_rate: Math.round(engagementRate * 100) / 100,
        click_through_rate: Math.round(clickThroughRate * 100) / 100,
        save_rate: Math.round(saveRate * 100) / 100,
        metrics_last_updated: new Date().toISOString()
      };

      let updateError = null;

      // Update scheduled_pins table if pin came from there
      if (pin.source === 'scheduled_pins') {
        const { error: scheduledPinsError } = await supabaseAdmin
          .from('scheduled_pins')
          .update(updateData)
          .eq('id', pin.id);
        updateError = scheduledPinsError;
      }

      // Always try to update user_images table (for both scheduled and direct uploads)
      const { error: userImagesError } = await supabaseAdmin
        .from('user_images')
        .update(updateData)
        .eq('pinterest_pin_id', pin.pinterest_pin_id)
        .eq('user_id', userId);

      // If pin came from user_images but not scheduled_pins, also try to update scheduled_pins by pinterest_pin_id
      if (pin.source === 'user_images') {
        const { error: scheduledPinsError } = await supabaseAdmin
          .from('scheduled_pins')
          .update(updateData)
          .eq('pinterest_pin_id', pin.pinterest_pin_id)
          .eq('user_id', userId);
        updateError = scheduledPinsError;
      }

      if (!updateError && !userImagesError) {
        syncedCount++;
        console.log(`📊 Auto-synced analytics for pin ${pin.pinterest_pin_id} (source: ${pin.source})`);
      } else if (updateError || userImagesError) {
        console.error(`❌ Error updating analytics for pin ${pin.pinterest_pin_id}:`, updateError || userImagesError);
      }

      // Rate limit: 1 second between requests
      await new Promise(resolve => setTimeout(resolve, 1000));

    } catch (error) {
      console.error(`❌ Error syncing pin ${pin.pinterest_pin_id}:`, error);
    }
  }

  if (syncedCount > 0) {
    console.log(`✅ Auto-synced ${syncedCount} pins for user ${userId}`);
  }
}

// Start background job processor
let schedulerInterval;
let analyticsInterval;

function startScheduler() {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
  }
  
  // Process scheduled pins every 1 minute
  schedulerInterval = setInterval(processScheduledPins, 1 * 60 * 1000);
  
  // Process immediately on startup
  setTimeout(processScheduledPins, 5000); // Wait 5 seconds after startup
  
  console.log('📅 Scheduled pin processor started (runs every 1 minute)');
}

function startAnalyticsSync() {
  if (analyticsInterval) {
    clearInterval(analyticsInterval);
  }
  
  // Process analytics sync every 12 hours (12 * 60 * 60 * 1000 ms)
  analyticsInterval = setInterval(processAnalyticsSync, 12 * 60 * 60 * 1000);
  
  // Process immediately on startup (after 30 seconds to let server settle)
  setTimeout(processAnalyticsSync, 30000);
  
  console.log('📊 Analytics sync processor started (runs every 12 hours)');
}

function stopScheduler() {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
    console.log('📅 Scheduled pin processor stopped');
  }
  if (analyticsInterval) {
    clearInterval(analyticsInterval);
    analyticsInterval = null;
    console.log('📊 Analytics sync processor stopped');
  }
}

function sanitizeDescription(input) {
  if (!input) return '';
  let desc = String(input);
  // Remove raw URLs and domain-like tokens
  desc = desc.replace(/https?:\/\/\S+/gi, '');
  desc = desc.replace(/\b([a-z0-9-]+\.)+[a-z]{2,}\b/gi, '');
  // Remove common CTA phrases about visiting/clicking for more info
  const ctaPatterns = [
    /visit\s+[^.\n]+/gi,
    /click( the)? link( below| above| in bio)?/gi,
    /learn more( at| here)?/gi,
    /read more( at| here)?/gi,
    /see more( at| here)?/gi,
    /for more info(rmation)?/gi
  ];
  ctaPatterns.forEach((re) => { desc = desc.replace(re, ''); });
  // Collapse multiple spaces
  desc = desc.replace(/\s{2,}/g, ' ').trim();
  // Ensure hashtags exist (>=3). If not, append some generic but safe hashtags
  const hashtags = (desc.match(/#[A-Za-z0-9_]+/g) || []).length;
  if (hashtags < 3) {
    const toAdd = ['#tips', '#guide', '#inspiration'].slice(0, 3 - hashtags).join(' ');
    desc = (desc + ' ' + toAdd).trim();
  }
  // Enforce 450 char limit
  if (desc.length > 450) desc = desc.slice(0, 450);
  return desc;
}

const REPLICATE_API_TOKEN = process.env.REPLICATE_API_TOKEN;
const NANO_BANANA_API_URL = process.env.NANO_BANANA_API_URL;
const NANO_BANANA_API_KEY = process.env.NANO_BANANA_API_KEY;

/**
 * User photo + on-image text: deterministic Sharp compositor only.
 * We already have the photo, so we avoid calling an image-generation model here.
 */
async function buildUserPhotoPinBuffer(sourceImageUrl, overlayText, brand, renderOptions = null) {
  return compositeUserPhotoPin({ sourceImageUrl, overlayText, brand, renderOptions });
}

// Fetch with timeout to prevent hangs on stuck network requests
async function fetchWithTimeout(url, options = {}, timeoutMs = 25000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (e) {
    clearTimeout(id);
    if (e.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw e;
  }
}

/** Kie.ai / Nano Banana task states vary by model version; normalize and treat unknown in-progress names safely. */
function normalizeNanoBananaState(state) {
  if (state == null) return '';
  return String(state).trim().toLowerCase();
}

const NANO_BANANA_IN_PROGRESS_STATES = new Set([
  'waiting',
  'wait',
  'pending',
  'queuing',
  'queueing',
  'queued',
  'generating',
  'running',
  'processing',
  'in_progress',
  'in-progress',
  'working',
  'started',
  'submitted',
  'created',
]);

function isNanoBananaStateInProgress(stateNorm) {
  if (!stateNorm) return true; // missing state: keep polling
  return NANO_BANANA_IN_PROGRESS_STATES.has(stateNorm);
}

async function generateImageWithNanoBanana(prompt, logLabel = '', options = {}) {
  if (!NANO_BANANA_API_URL || !NANO_BANANA_API_KEY) {
    console.warn('Nano Banana 2 API not configured (NANO_BANANA_API_URL / NANO_BANANA_API_KEY missing)');
    return null;
  }

  const imageInput = Array.isArray(options.imageInput)
    ? options.imageInput.filter((u) => u && String(u).trim()).map((u) => String(u).trim())
    : [];

  const baseUrl = NANO_BANANA_API_URL.replace(/\/$/, ''); // e.g. https://api.kie.ai/api/v1/jobs
  const maxRetries = Math.max(
    1,
    parseInt(process.env.NANO_BANANA_MAX_FLOW_RETRIES || '3', 10) || 3
  );
  const createTaskTimeoutMs = Math.max(
    5000,
    parseInt(process.env.NANO_BANANA_CREATE_TIMEOUT_MS || '25000', 10) || 25000
  );
  const recordInfoTimeoutMs = Math.max(
    5000,
    parseInt(process.env.NANO_BANANA_POLL_REQUEST_TIMEOUT_MS || '20000', 10) || 20000
  );
  const pollDelayMs = Math.max(
    800,
    parseInt(process.env.NANO_BANANA_POLL_INTERVAL_MS || '2000', 10) || 2000
  );
  const maxPollAttempts = Math.max(
    10,
    parseInt(process.env.NANO_BANANA_POLL_MAX_ATTEMPTS || '120', 10) || 120
  );
  /** If > 0, abandon task and start a new createTask when state stays identical this many polls (Kie jobs sometimes hang in `running`). */
  const stuckSameStatePolls = Math.max(
    0,
    parseInt(process.env.NANO_BANANA_STUCK_SAME_STATE_POLLS || '40', 10) || 0
  );

  for (let retry = 0; retry < maxRetries; retry++) {
    try {
      if (retry > 0) {
        const backoffMs = 2000 * retry;
        console.warn(`Nano Banana 2 retry ${retry}/${maxRetries - 1} after ${backoffMs}ms`);
        await new Promise((r) => setTimeout(r, backoffMs));
      }

      // 1) Create async generation task (with timeout to prevent hangs)
      const createRes = await fetchWithTimeout(
        `${baseUrl}/createTask`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${NANO_BANANA_API_KEY}`,
          },
          body: JSON.stringify({
            model: 'nano-banana-2',
            input: {
              prompt,
              aspect_ratio: '2:3',
              google_search: false,
              resolution: '1K',
              output_format: 'png',
              ...(imageInput.length > 0 ? { image_input: imageInput } : {}),
            },
          }),
        },
        createTaskTimeoutMs
      );

      const createJson = await createRes.json().catch(() => ({}));
      if (!createRes.ok || createJson.code !== 200 || !createJson.data?.taskId) {
        console.error('Nano Banana 2 createTask error:', createRes.status, createJson);
        if (retry < maxRetries - 1 && (createRes.status >= 500 || createRes.status === 429)) continue;
        return null;
      }

      const taskId = createJson.data.taskId;

      // 2) Poll recordInfo until success / fail / timeout (defaults ~4 min at 120 * 2s)
      let lastProgressState = null;
      let sameProgressStateCount = 0;

      for (let attempt = 0; attempt < maxPollAttempts; attempt++) {
        await new Promise((r) => setTimeout(r, pollDelayMs));

        let infoRes;
        let infoJson = {};
        const maxPollRetries = 3;
        for (let pollRetry = 0; pollRetry < maxPollRetries; pollRetry++) {
          try {
            infoRes = await fetchWithTimeout(
              `${baseUrl}/recordInfo?taskId=${encodeURIComponent(taskId)}`,
              {
                method: 'GET',
                headers: { 'Authorization': `Bearer ${NANO_BANANA_API_KEY}` },
              },
              recordInfoTimeoutMs
            );
            infoJson = await infoRes.json().catch(() => ({}));
            break;
          } catch (pollErr) {
            if (pollRetry < maxPollRetries - 1) {
              await new Promise((r) => setTimeout(r, 1000 * (pollRetry + 1)));
            } else {
              console.warn('Nano Banana 2 recordInfo fetch failed after retries:', pollErr.message);
              throw pollErr;
            }
          }
        }

        if (!infoRes.ok || infoJson.code !== 200 || !infoJson.data) {
          console.warn('Nano Banana 2 recordInfo error:', infoRes?.status, infoJson);
          continue;
        }

        const stateNorm = normalizeNanoBananaState(infoJson.data.state);

        if (isNanoBananaStateInProgress(stateNorm)) {
          if (stateNorm === lastProgressState) {
            sameProgressStateCount += 1;
          } else {
            lastProgressState = stateNorm;
            sameProgressStateCount = 1;
          }
          if (
            stuckSameStatePolls > 0 &&
            sameProgressStateCount >= stuckSameStatePolls
          ) {
            console.warn(
              `Nano Banana 2 task ${taskId} unchanged in "${stateNorm}" for ${sameProgressStateCount} polls; abandoning and retrying new task` +
                (logLabel ? ` (style: ${logLabel})` : '')
            );
            break;
          }
          if (
            attempt > 0 &&
            attempt % 15 === 0 &&
            stateNorm
          ) {
            console.warn(
              `Nano Banana 2 still in progress: state=${stateNorm} poll ${attempt}/${maxPollAttempts}` +
                (logLabel ? ` (${logLabel})` : '')
            );
          }
          continue;
        }

        lastProgressState = null;
        sameProgressStateCount = 0;

        if (stateNorm === 'fail' || stateNorm === 'failed' || stateNorm === 'error') {
          console.error('Nano Banana 2 generation failed:', infoJson.data.failCode, infoJson.data.failMsg);
          return null; // Don't retry on explicit API failure
        }

        if (stateNorm === 'success' || stateNorm === 'succeeded' || stateNorm === 'completed' || stateNorm === 'done') {
          try {
            const resultJsonStr = infoJson.data.resultJson || '{}';
            const parsed = JSON.parse(resultJsonStr);
            const urls = parsed.resultUrls;
            if (Array.isArray(urls) && urls.length > 0 && typeof urls[0] === 'string') {
              return urls[0];
            }
          } catch (e) {
            console.error('Nano Banana 2 resultJson parse error:', e);
          }
          console.warn('Nano Banana 2 success but no resultUrls found');
          return null;
        }

        console.warn(
          'Nano Banana 2 unexpected state after poll:',
          infoJson.data.state,
          infoJson.data
        );
      }

      console.warn('Nano Banana 2 generation timed out' + (logLabel ? ` (style: ${logLabel})` : ''));
      if (retry < maxRetries - 1) continue;
      return null;
    } catch (err) {
      console.error('Nano Banana 2 API flow failed' + (logLabel ? ` (style: ${logLabel})` : '') + ':', err.message || err);
      if (retry < maxRetries - 1) continue;
      return null;
    }
  }

  return null;
}

const STYLE_ON_IMAGE_TEXT_GUIDANCE = {
  curiosity_shock:
    'Generate a bold curiosity hook that creates shock or surprise. Use patterns like "Most People Get This Wrong About X" or "The Truth About X They Don\'t Tell You". Make people want to click. Max 60 chars for headline. Subheadline: short supporting line like "Check Before You Decide" or "Here\'s What Actually Works".',
  question_style:
    'Generate a viral question that invites clicks. Must be a direct question people want answered. Examples: "Can You Really Trust X?" or "Is X Actually Worth It?". Max 60 chars. Subheadline: promise line like "Get the Real Answer" or "What the Experts Say".',
  viral_curiosity:
    'Generate a story-style headline that creates curiosity. Use patterns like "I Tried X For 30 Days..." or "What Happened When I Stopped X". Max 60 chars. Subheadline: "Here\'s What Actually Happened" or "The Results Surprised Me".',
  money_saving:
    'Generate a value-focused headline about saving time or money. Punchy, practical. Examples: "Stop Wasting Time & Money on X" or "The Smart Way to Do X". Max 60 chars. Subheadline: "Do It The Smart Way Instead" or "Save Hours Every Week".',
  minimal_typography:
    'Generate a short bold statement. Minimal, high-impact. One powerful phrase. Max 50 chars. Subheadline: "Key Things You Should Know" or similar. Keep both very short.',
  cozy_baking:
    'Generate a friendly, practical headline. Warm and inviting. Max 60 chars. Subheadline: "Practical tips you can actually use" or "Simple tips for everyday".',
  clean_appetizing:
    'Generate a clear, appetizing headline. Soft and approachable. Max 60 chars. Subheadline: "Updated guide" or "Everything you need to know".',
  clumpy_fix:
    'Generate a practical how-to headline. Simple fix or method. Examples: "The Simple Fix for X" or "How to Get X Right". Max 60 chars. Subheadline: "Plain-English guide, no fluff".',
  minimal_elegant:
    'Generate an elegant, refined headline. Premium feel, minimal words. Max 50 chars. Subheadline: keep very short or empty.',
  before_after:
    'Generate a before/after contrast headline. Can be the topic or "Before vs After: X". Max 60 chars. Subheadline: "See the difference" or "The transformation".',
  timeline_infographic:
    'Generate a concise step-by-step headline. Examples: "The X Timeline" or "5 Steps to Master X". Max 60 chars. Subheadline: "Step-by-step guide" or "Your roadmap".',
  grid_3_images: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  grid_4_images: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  stacked_strips: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  offset_collage_3: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  circle_cluster_4: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  step_cards_3: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
};

async function generateStyleOnImageText({ styleId, topic, domain, keyword, year, description, avoidText, usedOverlayTexts }) {
  const guidance = STYLE_ON_IMAGE_TEXT_GUIDANCE[styleId] ||
    'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.';
  let avoidNote = '';
  if (avoidText && (avoidText.headline || avoidText.subheadline)) {
    avoidNote = `\n\nIMPORTANT: The current image already has this text - you MUST generate DIFFERENT text. Do NOT use: headline "${avoidText.headline || ''}", subheadline "${avoidText.subheadline || ''}". Create something fresh and varied.`;
  } else if (Array.isArray(usedOverlayTexts) && usedOverlayTexts.length > 0) {
    avoidNote = `\n\nAVOID REPEATING: The following overlay headlines/subheadlines were already used for other pins of this style. Generate DIFFERENT overlay text:\n${usedOverlayTexts
      .map((u) => `- "${u.headline}"${u.subheadline ? ` / "${u.subheadline}"` : ''}`)
      .join('\n')}\n`;
  }
  const content = `Article/topic: ${topic}\n${keyword ? `Keyword: ${keyword}\n` : ''}Domain: ${domain}\nYear: ${year}\n${description ? `Context: ${description.slice(0, 200)}\n` : ''}\nStyle: ${styleId}\n\n${guidance}${avoidNote}\n\nReturn JSON only: {"headline":"...","subheadline":"..."}. No markdown.`;
  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content }],
      max_tokens: 120,
      temperature: 0.8,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      let headline = (parsed.headline || topic || '').slice(0, 120).trim();
      let subheadline = (parsed.subheadline || '').slice(0, 140).trim();
      if (!headline) headline = topic;
      return { headline, subheadline };
    }
  } catch (e) {
    console.warn('generateStyleOnImageText error:', e.message || e);
  }
  return { headline: topic, subheadline: '' };
}

const ILLUSTRATED_STYLES = new Set(['timeline_infographic', 'step_cards_3']);
/** Set to `false` to restore longer, more prescriptive prompts (slower on some image APIs). */
const NANO_BANANA_SIMPLE_PROMPTS = process.env.NANO_BANANA_SIMPLE_PROMPTS !== 'false';

const REALISTIC_PREFIX_LONG =
  'Photorealistic, high-quality photograph. Natural lighting, lifelike imagery, professional photography style. ';
const REALISTIC_PREFIX_SHORT = 'Photorealistic, natural light. ';

const SCROLL_STOPPING_RULE_LONG =
  'The image must immediately stand out in a Pinterest feed and create a strong visual contrast compared to typical pins in this niche. ';
const SCROLL_STOPPING_RULE_SHORT = 'Bold, high-contrast, easy to read on mobile. ';

/** Pick long vs short prompt fragment (Nano Banana / Kie). */
function promptTier(longText, shortText) {
  return NANO_BANANA_SIMPLE_PROMPTS ? shortText : longText;
}

const NICHE_VISUAL_HINTS = {
  recipe: 'Prioritize food, ingredients, kitchen scenes, storage containers, and real cooking or prep visuals rather than abstract icons.',
  finance: 'Prioritize money-related visuals like bills, receipts, budgets, calculators, and simple charts instead of generic office imagery.',
  travel: 'Use locations, landscapes, maps, luggage, and travel scenes that clearly signal destinations or journeys, not generic interiors.',
  self_improvement: 'Show people in everyday life improving habits, routines, or mindset (journals, checklists, calm home scenes) rather than random tech imagery.',
  product_review: 'Show the product or category clearly (packaging, close-ups, comparison layouts) so it is obvious what is being reviewed.',
};

function buildOverlayImagePrompt({ styleId, topic, domain, keyword, year, overlayText, brand, stepCount, niche }) {
  const headline = overlayText?.headline || topic;
  const subheadline = overlayText?.subheadline || '';
  const source = overlayText?.source || domain;
  const brandColorParts = [];
  if (brand?.primaryColor) brandColorParts.push(`primary ${brand.primaryColor}`);
  if (brand?.secondaryColor) brandColorParts.push(`secondary ${brand.secondaryColor}`);
  if (brand?.accentColor) brandColorParts.push(`accent ${brand.accentColor}`);
  const brandColorHint = brandColorParts.length
    ? promptTier(
        ` Use this brand color palette in backgrounds, accents, or typography: ${brandColorParts.join(', ')}.`,
        ` Palette: ${brandColorParts.join(', ')}.`
      )
    : '';
  const footerSourceOnly = overlayText?.footerSourceOnly === true;
  const footerLineTrim = String(source || '').trim();
  const brandTrim = String(brand?.brandName || '').trim();
  // When the footer line already is the brand/CTA, do not also ask for the brand elsewhere (avoids duplicate footer text).
  const brandNameHint =
    footerSourceOnly || !brandTrim || footerLineTrim === brandTrim
      ? ''
      : promptTier(
          ` Use the brand name ${brand.brandName} subtly in the design.`,
          ` Brand: ${brand.brandName}.`
        );
  const brandTail = brandColorHint + brandNameHint;
  const nicheTail =
    niche && NICHE_VISUAL_HINTS[niche]
      ? promptTier(` ${NICHE_VISUAL_HINTS[niche]}`, '')
      : '';
  const tail = nicheTail + brandTail;
  const useRealistic = !ILLUSTRATED_STYLES.has(styleId);
  const baseIntro = promptTier(
    'Vertical Pinterest pin 1000x1500 px. ' + SCROLL_STOPPING_RULE_LONG + (useRealistic ? REALISTIC_PREFIX_LONG : ''),
    'Vertical 2:3 Pinterest pin. ' + SCROLL_STOPPING_RULE_SHORT + (useRealistic ? REALISTIC_PREFIX_SHORT : '')
  );
  const numSteps = typeof stepCount === 'number' ? stepCount : (styleId === 'step_cards_3' || styleId === 'grid_3_images' ? 3 : styleId === 'grid_4_images' ? 4 : styleId === 'timeline_infographic' ? 5 : null);

  switch (styleId) {
       case 'before_after':
      return (
        baseIntro +
        promptTier(
          `Split layout with a clear “Before” left half and “After” right half about "${keyword || topic}". ` +
            `Show the on-image main text "${headline}" across the top center. ` +
            `Label the left side "Before" and the right side "After" with short, readable labels. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the main text. ` : '') +
            `At the bottom, add small, readable source text "${source}".`,
          `Before | After split about "${keyword || topic}". Top text: "${headline}". ` +
            (subheadline ? `Sub: "${subheadline}". ` : '') +
            `Footer: "${source}".`
        ) +
        tail
      );
    case 'timeline_infographic': {
      const steps = numSteps || 5;
      const stepLabels = Array.from({ length: steps }, (_, i) => `Step ${i + 1}`).join(', ');
      return (
        baseIntro +
        promptTier(
          `Clean vertical infographic timeline with exactly ${steps} steps explaining "${keyword || topic}". ` +
            `Label steps as: ${stepLabels}. ` +
            `At the top of the pin, place the main title text "${headline}". ` +
            (subheadline ? `Optionally place a short subheadline "${subheadline}" just below the title. ` : '') +
            `Each step box has a very short label, and the background stays simple and low-contrast so the text is readable. ` +
            `At the bottom, include small source text "${source}".`,
          `${steps}-step vertical infographic for "${keyword || topic}". Title: "${headline}". ` +
            (subheadline ? `Subtitle: "${subheadline}". ` : '') +
            `Numbered steps ${stepLabels}, short labels only, flat simple background. Footer: "${source}".`
        ) +
        tail
      );
    }
    case 'grid_4_images':
      return (
        baseIntro +
        promptTier(
          `2×2 grid of four related photographs about "${keyword || topic}" with thin white gutters. ` +
            `Place the main headline "${headline}" in a banner at the top of the pin. ` +
            (subheadline ? `Optionally add a short subheadline "${subheadline}" under the headline. ` : '') +
            `At the very bottom, add small source text "${source}".`,
          `2×2 photo grid, "${keyword || topic}". Top banner: "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'offset_collage_3':
      return (
        baseIntro +
        promptTier(
          `Asymmetrical collage: one large hero photograph on the left and two smaller stacked photographs on the right, all about "${keyword || topic}". ` +
            `Place the main text "${headline}" over a solid or semi-transparent area in the top-right region so it is very readable. ` +
            (subheadline ? `Add a short supporting line "${subheadline}" below the headline. ` : '') +
            `Include small source text "${source}" near the bottom edge.`,
          `Collage: 1 large + 2 small photos, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'clean_appetizing':
      return (
        baseIntro +
        promptTier(
          `Soft neutral background with subtle texture. Simple, clear focal object or photograph that represents "${keyword || topic}". Clean, modern blog-style design with plenty of white space. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Clean food-blog style, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'curiosity_shock':
      return (
        baseIntro +
        promptTier(
          `Bold, dramatic, high-contrast image. Strong central subject that represents or relates to "${keyword || topic}" and creates shock and curiosity. The visual must be semantically relevant to the article topic—e.g. for tech/WordPress show a laptop, screen, or workspace; for food show ingredients or cooking. Dramatic lighting. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Dramatic high-contrast scene for "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'money_saving':
      return (
        baseIntro +
        promptTier(
          `Visual value motif: imagery suggesting saving time, money, or results. Clean layout with practical elements related to "${keyword || topic}". Bright but simple. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Value/savings theme, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'minimal_typography':
      return (
        baseIntro +
        promptTier(
          `Minimalist layout: pure white or light background, lots of whitespace. Single strong visual object representing "${keyword || topic}". High-contrast, typography-forward. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Minimal white space + one object, "${keyword || topic}". Big type: "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'question_style':
      return (
        baseIntro +
        promptTier(
          `Image that visually represents a question or dilemma about "${keyword || topic}", with subtle question-mark elements or split choices. The scene or subject must relate to the article topic. Clean background. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Question/dilemma visual, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'cozy_baking':
      return (
        baseIntro +
        promptTier(
          `Lifestyle context scene: someone interacting with "${keyword || topic}" in an appropriate everyday setting. The setting must match the topic—e.g. for tech/WordPress/digital topics show a laptop, screen, or workspace; for food/recipes show a kitchen; for wellness show a calm home setting. Warm natural light. Warm, inviting, lifestyle photography. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Warm lifestyle scene, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'viral_curiosity':
      return (
        baseIntro +
        promptTier(
          `Story-like composition: focused close-up of a hand holding something that represents "${keyword || topic}" (e.g. document, device, product). Slightly dramatic lighting. Mysterious, story-driven, personal experiment feel. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Story-style close-up, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'clumpy_fix':
      return (
        baseIntro +
        promptTier(
          `Practical, simple layout: light surface with a clear object representing "${keyword || topic}" and a small related element (tool, document). Minimal props, lots of breathing room. Non-dramatic. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Simple how-to flat lay, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'minimal_elegant':
      return (
        baseIntro +
        promptTier(
          `Soft beige or light gray background. Elegant overhead shot of a single, simple object that is semantically relevant to "${keyword || topic}"—e.g. for tech/WordPress show a laptop, tablet, or document; for food show ingredients or a dish; for wellness show a journal or plant. Delicate shadows. Minimal, premium feel. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Elegant minimal overhead, one object, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'step_cards_3':
      return (
        baseIntro +
        promptTier(
          `Three tall step cards stacked vertically, each with an icon or small image and short label (Step 1, Step 2, Step 3) for "${keyword || topic}". Simple flat or lightly textured background. ` +
            `At the top, place the main title text "${headline}". ` +
            (subheadline ? `Optionally place a short subheadline "${subheadline}" just below the title. ` : '') +
            `At the bottom, include small source text "${source}".`,
          `3 vertical step cards for "${keyword || topic}". Title "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'grid_3_images':
      return (
        baseIntro +
        promptTier(
          `Clean layout with a grid of three related photographs representing "${keyword || topic}" (e.g. three variations, steps, or examples). Thin white borders between panels. ` +
            `Place the main headline "${headline}" in a banner at the top. ` +
            (subheadline ? `Optionally add a short subheadline "${subheadline}" under the headline. ` : '') +
            `At the very bottom, add small source text "${source}".`,
          `3-panel photo grid, "${keyword || topic}". Top: "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'stacked_strips':
      return (
        baseIntro +
        promptTier(
          `Three horizontal photograph strips stacked vertically on one side. Each strip shows a different scene or detail related to "${keyword || topic}". Clean editorial feel. ` +
            `Place the main text "${headline}" over a solid or semi-transparent area. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" below the headline. ` : '') +
            `Include small source text "${source}" at the bottom.`,
          `3 horizontal photo strips, "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    case 'circle_cluster_4':
      return (
        baseIntro +
        promptTier(
          `Four circular photographs arranged around a central text area, each showing a different aspect of "${keyword || topic}". Clean light background. ` +
            `Place the main headline "${headline}" in the center. ` +
            (subheadline ? `Add a short subheadline "${subheadline}" below. ` : '') +
            `Add small source text "${source}" at the bottom of the pin.`,
          `4 circular photos around center text, "${keyword || topic}". Center: "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
    default:
      return (
        baseIntro +
        promptTier(
          `Eye-catching but not cluttered design about "${keyword || topic}". Use real-life photography or photorealistic imagery. ` +
            `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
            (subheadline ? `Optionally add a short subheadline "${subheadline}" under the headline. ` : '') +
            `Add small, readable source text "${source}" at the bottom of the pin.`,
          `Eye-catching pin about "${keyword || topic}". Headline "${headline}". ` +
            (subheadline ? `"${subheadline}". ` : '') +
            `Footer "${source}".`
        ) +
        tail
      );
  }
}

// POST /api/generate-image
// Accepts { title } for simple background-only, or { prompt } for full prompt including baked-in text (used by URL-to-Pin fallback)
app.post('/api/generate-image', async (req, res) => {
  console.log('Received request:', req.body);
  const { title, prompt: customPrompt } = req.body;
  if (!title && !customPrompt) {
    return res.status(400).json({ error: 'Title or prompt is required' });
  }

  if (!REPLICATE_API_TOKEN) {
    return res.status(500).json({ error: 'Replicate API token not set' });
  }

  try {
    const pinterestPrompt = customPrompt ||
      `Create an eye-catching, scroll-stopping Pinterest pin background for a blog post titled "${title || 'this topic'}". The image must be vertical (portrait layout), visually stunning, and use vibrant, modern colors with a clean, contemporary style. Soft lighting, shallow depth of field, high resolution, professional photographer style. Absolutely no text, words, or lettering—only visuals. The design should be suitable as a background for a Pinterest pin, with clear space for text overlay.`;
    // Call Replicate SDXL API
    const response = await fetch('https://api.replicate.com/v1/predictions', {
      method: 'POST',
      headers: {
        'Authorization': `Token ${REPLICATE_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        version: '7762fd07cf82c948538e41f63f77d685e02b063e37e496e96eefd46c929f9bdc',
        input: {
          prompt: pinterestPrompt,
          width: 1024,
          height: 1536,
        }
      }),
    });

    const data = await response.json();

    // Replicate returns a prediction object; poll until status is 'succeeded'
    let prediction = data;
    while (prediction.status !== 'succeeded' && prediction.status !== 'failed') {
      await new Promise(r => setTimeout(r, 1500));
      const pollRes = await fetch(`https://api.replicate.com/v1/predictions/${prediction.id}`, {
        headers: { 'Authorization': `Token ${REPLICATE_API_TOKEN}` }
      });
      prediction = await pollRes.json();
    }

    if (prediction.status === 'succeeded') {
      // SDXL returns an array of image URLs
      const replicateUrl = prediction.output[0];
      try {
        // Download the image as a buffer
        const imageRes = await fetch(replicateUrl);
        const arrayBuffer = await imageRes.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        // Determine file extension
        const fileExt = replicateUrl.split('.').pop().split('?')[0];
        const fileName = `ai-image-${Date.now()}-${Math.floor(Math.random()*10000)}.${fileExt}`;
        // Upload to Supabase Storage
        const { error: uploadError } = await supabaseAdmin.storage.from('ai-images').upload(fileName, buffer, {
          contentType: imageRes.headers.get('content-type') || 'image/png',
          upsert: true,
        });
        if (uploadError) {
          console.error('Error uploading to Supabase Storage:', uploadError);
          return res.json({ imageUrl: replicateUrl });
        }
        // Get public URL
        const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
        const publicUrl = publicUrlData?.publicUrl || replicateUrl;
        return res.json({ imageUrl: publicUrl });
      } catch (err) {
        console.error('Error downloading/uploading image:', err);
        return res.json({ imageUrl: replicateUrl });
      }
    } else {
      return res.status(500).json({ error: 'Image generation failed' });
    }
  } catch (err) {
    return res.status(500).json({ error: 'Error calling Replicate API', details: err.message });
  }
});

// --- Public tools (SEO) ---

const toolRateLimit = new Map();
function getClientIp(req) {
  const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return xf || req.socket?.remoteAddress || 'unknown';
}

function rateLimitTool(req, key, { windowMs = 60_000, max = 20 } = {}) {
  const ip = getClientIp(req);
  const now = Date.now();
  const k = `${key}::${ip}`;
  const prev = toolRateLimit.get(k) || { start: now, count: 0 };
  const next = now - prev.start > windowMs ? { start: now, count: 0 } : prev;
  next.count++;
  toolRateLimit.set(k, next);
  return next.count <= max;
}

function dedupeKeepOrder(arr) {
  const seen = new Set();
  const out = [];
  for (const x of arr || []) {
    const s = String(x || '').trim();
    if (!s) continue;
    const k = s.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(s);
  }
  return out;
}

function buildPinterestKeywordIdeas(seed, niche = '') {
  const s = String(seed || '').trim().replace(/\s+/g, ' ');
  const n = String(niche || '').trim().toLowerCase();
  const base = s;
  const year = new Date().getFullYear();
  const modifiers = [
    'ideas',
    'tips',
    'checklist',
    'template',
    'guide',
    'for beginners',
    'step by step',
    'mistakes',
    'vs',
    'best',
    'best {year}',
    'how to',
    'easy',
    'quick',
    'cheap',
    'budget',
  ];
  const ecommerce = ['etsy', 'shopify', 'amazon', 'gift', 'under $50', 'small business'];
  const food = ['meal prep', 'healthy', 'high protein', 'air fryer', 'dinner', 'snack'];
  const travel = ['itinerary', 'budget', 'packing list', 'weekend', 'family', 'solo'];
  const diy = ['diy', 'tutorial', 'before and after', 'ideas', 'beginner'];
  const affiliate = ['review', 'best', 'comparison', 'under $100', 'worth it'];

  const nicheMods =
    n === 'food'
      ? food
      : n === 'travel'
        ? travel
        : n === 'diy'
          ? diy
          : n === 'affiliate'
            ? affiliate
            : n === 'ecommerce'
              ? ecommerce
              : [];

  const replaceYear = (m) => m.replace(/\{year\}/g, String(year));
  const primary = [];
  for (const m of modifiers) {
    primary.push(`${base} ${replaceYear(m)}`);
  }
  for (const m of nicheMods) {
    primary.push(`${base} ${replaceYear(m)}`);
  }

  const questions = [
    `how to ${base}`,
    `why ${base} isn't working`,
    `how many ${base}`,
    `best time to post ${base}`,
    `${base} strategy`,
    `${base} checklist`,
  ];

  const longTail = [
    `${base} for small accounts`,
    `${base} without followers`,
    `${base} seo`,
    `${base} keywords`,
    `${base} content plan`,
  ];

  return {
    seed: base,
    primary: dedupeKeepOrder(primary).slice(0, 40),
    questions: dedupeKeepOrder(questions).slice(0, 20),
    longTail: dedupeKeepOrder(longTail).slice(0, 25),
  };
}

async function maybeAiExpandPinterestKeywords(seed, niche, openaiClient) {
  if (process.env.PINTEREST_KEYWORD_TOOL_AI === '0' || !process.env.OPENAI_API_KEY || !openaiClient) return null;
  const s = String(seed || '').trim();
  if (!s) return null;
  try {
    const year = new Date().getFullYear();
    const completion = await openaiClient.chat.completions.create({
      model: process.env.PINTEREST_KEYWORD_TOOL_MODEL || 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content:
            `Generate Pinterest keyword ideas for the seed phrase.\n` +
            `Seed: ${s}\n` +
            `${niche ? `Niche: ${String(niche)}\n` : ''}` +
            `Year: ${year}\n\n` +
            `Return JSON only with keys: primary, questions, longTail.\n` +
            `Rules:\n` +
            `- primary: 25-40 phrases, 2-6 words each, include intent modifiers (best, how to, ideas, checklist, template, for beginners).\n` +
            `- questions: 10-15 phrases.\n` +
            `- longTail: 15-25 phrases.\n` +
            `- No hashtags.\n` +
            `- No quotes.\n` +
            `- No duplicates.\n`,
        },
      ],
      max_tokens: 700,
      temperature: 0.6,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    const parsed = JSON.parse(jsonMatch[0]);
    const primary = dedupeKeepOrder(parsed.primary || []);
    const questions = dedupeKeepOrder(parsed.questions || []);
    const longTail = dedupeKeepOrder(parsed.longTail || []);
    if (!primary.length) return null;
    return {
      primary: primary.slice(0, 50),
      questions: questions.slice(0, 25),
      longTail: longTail.slice(0, 35),
    };
  } catch (e) {
    console.warn('maybeAiExpandPinterestKeywords:', e.message || e);
    return null;
  }
}

function normalizeNicheLabel(raw) {
  return String(raw || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[^\p{L}\p{N}\s&/+-]/gu, '');
}

function toHashtag(phrase) {
  const p = String(phrase || '').trim();
  if (!p) return '';
  // Pinterest hashtags are typically no spaces. Keep letters/numbers only.
  const compact = p
    .toLowerCase()
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/g)
    .join('');
  if (!compact) return '';
  return `#${compact}`;
}

function buildPinterestHashtagIdeas(topic, niche = '') {
  const t = String(topic || '').trim().replace(/\s+/g, ' ');
  const n = normalizeNicheLabel(niche).toLowerCase();
  const year = new Date().getFullYear();

  const base = [
    t,
    `${t} ideas`,
    `${t} tips`,
    `${t} checklist`,
    `${t} for beginners`,
    `how to ${t}`,
    `${t} {year}`,
    `best ${t}`,
    `${t} tutorial`,
    `${t} guide`,
    `${t} hacks`,
  ].map((x) => String(x).replace(/\{year\}/g, String(year)));

  const nicheBoost = n
    ? [
        `${n} tips`,
        `${n} ideas`,
        `${t} ${n}`,
        `${t} for ${n}`,
        `${n} inspiration`,
      ]
    : [];

  const suggested = dedupeKeepOrder([...base, ...nicheBoost].map(toHashtag)).filter(Boolean);

  // "Avoid" list is intentionally generic: overly broad, spammy, or irrelevant tags
  const avoid = dedupeKeepOrder(
    [
      '#love',
      '#instagood',
      '#photooftheday',
      '#beautiful',
      '#happy',
      '#cute',
      '#tbt',
      '#likeforlike',
      '#followme',
      '#viral',
      '#trending',
      '#fyp',
    ].map((x) => String(x).toLowerCase())
  );

  return {
    topic: t,
    suggested: suggested.slice(0, 30),
    avoid,
  };
}

function buildPinterestBioIdeas(niche, offer = '') {
  const n = normalizeNicheLabel(niche);
  const o = normalizeNicheLabel(offer);
  const keywords = dedupeKeepOrder(
    [
      n,
      o,
      `${n} tips`,
      `${n} ideas`,
      `${n} guide`,
      `${n} for beginners`,
      o ? `${o} tips` : '',
    ].filter(Boolean)
  ).slice(0, 12);

  const offerBit = o ? ` • ${o}` : '';
  const ctaLine = o ? `Grab the ${o} ↓` : 'Get new pin ideas ↓';

  const bios = dedupeKeepOrder([
    `${n} tips & ideas${offerBit}. ${ctaLine}`,
    `Helping you grow with ${n}.${offerBit} New pins weekly. ${ctaLine}`,
    `${n} made simple${offerBit}. Save this for later + follow for more.`,
    `${n} content + Pinterest SEO${offerBit}. ${ctaLine}`,
    `Practical ${n} for busy creators${offerBit}. ${ctaLine}`,
  ]).slice(0, 5);

  return { niche: n, offer: o, bios, ctaLine, keywords };
}

function buildPinterestBoardIdeas(niche, audience = '', offer = '') {
  const n = normalizeNicheLabel(niche);
  const a = normalizeNicheLabel(audience);
  const o = normalizeNicheLabel(offer);

  const baseBoards = [
    `${n} Tips`,
    `${n} Ideas`,
    `${n} for Beginners`,
    `Best ${n}`,
    `${n} Checklist`,
    `${n} Templates`,
    `${n} Mistakes to Avoid`,
    `${n} Before & After`,
    `${n} Resources`,
    `${n} Inspiration`,
  ];

  const audienceBoards = a
    ? [
        `${n} for ${a}`,
        `${a} ${n} Ideas`,
        `${n} for Busy ${a}`,
      ]
    : [];

  const offerBoards = o
    ? [
        `${o}`,
        `${o} Ideas`,
        `${o} Checklist`,
      ]
    : [];

  const names = dedupeKeepOrder([...baseBoards, ...audienceBoards, ...offerBoards]).slice(0, 16);

  const boards = names.map((name) => {
    const kw = dedupeKeepOrder([
      n,
      name.replace(/[^\p{L}\p{N}\s]/gu, ' ').replace(/\s+/g, ' ').trim(),
      a ? `${n} for ${a}` : '',
      o ? o : '',
      `${n} ideas`,
      `${n} tips`,
    ]).slice(0, 10);
    const descBits = [
      `Save ${n.toLowerCase()} ideas you can actually use.`,
      a ? `Made for ${a.toLowerCase()}.` : '',
      o ? `Includes ${o.toLowerCase()} and step-by-step pins.` : '',
    ].filter(Boolean);
    return {
      name,
      description: descBits.join(' '),
      keywords: kw,
    };
  });

  return { niche: n, audience: a, offer: o, boards };
}

async function maybeAiPinterestBio(niche, offer, openaiClient) {
  if (process.env.PINTEREST_BIO_TOOL_AI === '0' || !process.env.OPENAI_API_KEY || !openaiClient) return null;
  const n = normalizeNicheLabel(niche);
  if (!n) return null;
  try {
    const completion = await openaiClient.chat.completions.create({
      model: process.env.PINTEREST_BIO_TOOL_MODEL || 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content:
            `Write 5 Pinterest bios for a creator.\n` +
            `Niche: ${n}\n` +
            `${offer ? `Offer: ${normalizeNicheLabel(offer)}\n` : ''}` +
            `Return JSON only with keys: bios, ctaLine, keywords.\n` +
            `Rules:\n` +
            `- bios: exactly 5, each <= 140 characters.\n` +
            `- No emojis.\n` +
            `- ctaLine: short (<= 35 chars).\n` +
            `- keywords: 8-12 short phrases, no hashtags.\n`,
        },
      ],
      max_tokens: 420,
      temperature: 0.7,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    const parsed = JSON.parse(jsonMatch[0]);
    const bios = dedupeKeepOrder(parsed.bios || []).slice(0, 5);
    if (bios.length < 3) return null;
    return {
      bios: bios.slice(0, 5),
      ctaLine: String(parsed.ctaLine || '').trim(),
      keywords: dedupeKeepOrder(parsed.keywords || []).slice(0, 15),
    };
  } catch (e) {
    console.warn('maybeAiPinterestBio:', e.message || e);
    return null;
  }
}

async function maybeAiPinterestBoards(niche, audience, offer, openaiClient) {
  if (process.env.PINTEREST_BOARD_TOOL_AI === '0' || !process.env.OPENAI_API_KEY || !openaiClient) return null;
  const n = normalizeNicheLabel(niche);
  if (!n) return null;
  try {
    const completion = await openaiClient.chat.completions.create({
      model: process.env.PINTEREST_BOARD_TOOL_MODEL || 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content:
            `Generate Pinterest board ideas.\n` +
            `Niche: ${n}\n` +
            `${audience ? `Audience: ${normalizeNicheLabel(audience)}\n` : ''}` +
            `${offer ? `Offer: ${normalizeNicheLabel(offer)}\n` : ''}` +
            `Return JSON only with key: boards (array).\n` +
            `Each board must be an object: { name, description, keywords }.\n` +
            `Rules:\n` +
            `- boards: 10-14 items.\n` +
            `- name: 2-6 words.\n` +
            `- description: 1-2 short sentences.\n` +
            `- keywords: 6-10 short phrases, no hashtags.\n` +
            `- No duplicates.\n`,
        },
      ],
      max_tokens: 800,
      temperature: 0.7,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    const parsed = JSON.parse(jsonMatch[0]);
    const boards = Array.isArray(parsed.boards) ? parsed.boards : [];
    const cleaned = [];
    const seen = new Set();
    for (const b of boards) {
      const name = String(b?.name || '').trim();
      if (!name) continue;
      const k = name.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      cleaned.push({
        name,
        description: String(b?.description || '').trim(),
        keywords: dedupeKeepOrder(b?.keywords || []).slice(0, 12),
      });
    }
    if (cleaned.length < 6) return null;
    return { boards: cleaned.slice(0, 16) };
  } catch (e) {
    console.warn('maybeAiPinterestBoards:', e.message || e);
    return null;
  }
}

async function maybeAiPinterestHashtags(topic, niche, openaiClient) {
  if (process.env.PINTEREST_HASHTAG_TOOL_AI === '0' || !process.env.OPENAI_API_KEY || !openaiClient) return null;
  const t = String(topic || '').trim();
  if (t.length < 2) return null;
  try {
    const completion = await openaiClient.chat.completions.create({
      model: process.env.PINTEREST_HASHTAG_TOOL_MODEL || 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content:
            `Generate Pinterest hashtags for the topic.\n` +
            `Topic: ${t}\n` +
            `${niche ? `Niche: ${normalizeNicheLabel(niche)}\n` : ''}` +
            `Return JSON only with keys: suggested, avoid.\n` +
            `Rules:\n` +
            `- suggested: 20-30 hashtags, all starting with #, no spaces.\n` +
            `- avoid: 8-16 hashtags that are too generic/spammy.\n` +
            `- No duplicates.\n`,
        },
      ],
      max_tokens: 500,
      temperature: 0.6,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    const parsed = JSON.parse(jsonMatch[0]);
    const suggested = dedupeKeepOrder(parsed.suggested || [])
      .map((x) => String(x).trim())
      .filter((x) => x.startsWith('#'));
    if (suggested.length < 10) return null;
    const avoid = dedupeKeepOrder(parsed.avoid || [])
      .map((x) => String(x).trim().toLowerCase())
      .filter((x) => x.startsWith('#'));
    return { suggested: suggested.slice(0, 40), avoid: avoid.slice(0, 20) };
  } catch (e) {
    console.warn('maybeAiPinterestHashtags:', e.message || e);
    return null;
  }
}

app.post('/api/tools/pinterest-keywords', async (req, res) => {
  try {
    if (!rateLimitTool(req, 'pinterest-keywords', { windowMs: 60_000, max: 30 })) {
      return res.status(429).json({ error: 'Too many requests. Please try again in a minute.' });
    }
    const { seed, niche } = req.body || {};
    const s = String(seed || '').trim();
    if (s.length < 2) return res.status(400).json({ error: 'Enter a keyword (at least 2 characters).' });
    const base = buildPinterestKeywordIdeas(s, niche || '');
    const ai = await maybeAiExpandPinterestKeywords(s, niche || '', openai);
    const merged = {
      seed: base.seed,
      primary: dedupeKeepOrder([...(ai?.primary || []), ...base.primary]).slice(0, 60),
      questions: dedupeKeepOrder([...(ai?.questions || []), ...base.questions]).slice(0, 30),
      longTail: dedupeKeepOrder([...(ai?.longTail || []), ...base.longTail]).slice(0, 45),
    };
    return res.json({ ...merged, source: ai ? 'ai+heuristic' : 'heuristic' });
  } catch (e) {
    console.error('pinterest-keywords tool error:', e);
    return res.status(500).json({ error: 'Failed to generate keyword ideas.' });
  }
});

app.post('/api/tools/pinterest-board-generator', async (req, res) => {
  try {
    if (!rateLimitTool(req, 'pinterest-board-generator', { windowMs: 60_000, max: 25 })) {
      return res.status(429).json({ error: 'Too many requests. Please try again in a minute.' });
    }
    const { niche, audience, offer } = req.body || {};
    const n = normalizeNicheLabel(niche);
    if (n.length < 2) return res.status(400).json({ error: 'Enter a niche (at least 2 characters).' });
    const base = buildPinterestBoardIdeas(n, audience || '', offer || '');
    const ai = await maybeAiPinterestBoards(n, audience || '', offer || '', openai);
    const mergedBoards = dedupeKeepOrder([...(ai?.boards || []).map((b) => b?.name), ...base.boards.map((b) => b.name)])
      .map((name) => {
        const fromAi = (ai?.boards || []).find((b) => String(b?.name || '').trim().toLowerCase() === String(name).toLowerCase());
        const fromBase = base.boards.find((b) => String(b?.name || '').trim().toLowerCase() === String(name).toLowerCase());
        return fromAi || fromBase || { name, description: '', keywords: [] };
      })
      .slice(0, 16);
    return res.json({
      niche: base.niche,
      audience: base.audience,
      offer: base.offer,
      boards: mergedBoards,
      source: ai ? 'ai+heuristic' : 'heuristic',
    });
  } catch (e) {
    console.error('pinterest-board-generator tool error:', e);
    return res.status(500).json({ error: 'Failed to generate board ideas.' });
  }
});

app.post('/api/tools/pinterest-bio-generator', async (req, res) => {
  try {
    if (!rateLimitTool(req, 'pinterest-bio-generator', { windowMs: 60_000, max: 30 })) {
      return res.status(429).json({ error: 'Too many requests. Please try again in a minute.' });
    }
    const { niche, offer } = req.body || {};
    const n = normalizeNicheLabel(niche);
    if (n.length < 2) return res.status(400).json({ error: 'Enter a niche (at least 2 characters).' });
    const base = buildPinterestBioIdeas(n, offer || '');
    const ai = await maybeAiPinterestBio(n, offer || '', openai);
    const bios = dedupeKeepOrder([...(ai?.bios || []), ...base.bios]).slice(0, 5);
    const keywords = dedupeKeepOrder([...(ai?.keywords || []), ...base.keywords]).slice(0, 16);
    const ctaLine = String(ai?.ctaLine || base.ctaLine || '').trim();
    return res.json({
      niche: base.niche,
      offer: base.offer,
      bios,
      ctaLine,
      keywords,
      source: ai ? 'ai+heuristic' : 'heuristic',
    });
  } catch (e) {
    console.error('pinterest-bio-generator tool error:', e);
    return res.status(500).json({ error: 'Failed to generate bio ideas.' });
  }
});

app.post('/api/tools/pinterest-hashtag-generator', async (req, res) => {
  try {
    if (!rateLimitTool(req, 'pinterest-hashtag-generator', { windowMs: 60_000, max: 35 })) {
      return res.status(429).json({ error: 'Too many requests. Please try again in a minute.' });
    }
    const { topic, niche } = req.body || {};
    const t = String(topic || '').trim();
    if (t.length < 2) return res.status(400).json({ error: 'Enter a topic (at least 2 characters).' });
    const base = buildPinterestHashtagIdeas(t, niche || '');
    const ai = await maybeAiPinterestHashtags(t, niche || '', openai);
    const suggested = dedupeKeepOrder([...(ai?.suggested || []), ...base.suggested]).slice(0, 40);
    const avoid = dedupeKeepOrder([...(ai?.avoid || []), ...base.avoid]).slice(0, 25);
    return res.json({
      topic: base.topic,
      suggested,
      avoid,
      source: ai ? 'ai+heuristic' : 'heuristic',
    });
  } catch (e) {
    console.error('pinterest-hashtag-generator tool error:', e);
    return res.status(500).json({ error: 'Failed to generate hashtags.' });
  }
});

// --- URL → Pin helper endpoints ---

app.post('/api/urltopin/scrape', async (req, res) => {
  try {
    const { url, enrich } = req.body || {};
    if (!url) return res.status(400).json({ error: 'Missing url' });

    const html = await fetchArticleHtml(url);
    if (!html || html.length < 200) {
      return res.status(502).json({
        error:
          'Could not load this page. Many sites (including Medium) block automated requests. We retry with a browser when possible — if it still fails, try a different URL or paste your article on a blog you control.',
      });
    }
    const meta = extractMetaFromHtml(html, url);
    if (meta.title) {
      meta.title = await maybeShortenPageTitleForUrlToPin(url, meta.title, openai, meta.canonicalUrl);
    }
    const brandingGate = assessUrlBrandingGate(url);
    if (enrich) {
      try {
        meta.contentProfile = await enrichContentProfile(meta, openai);
      } catch (e) {
        console.warn('urltopin scrape enrich error:', e.message || e);
      }
    }
    return res.json({ ...meta, ...brandingGate });
  } catch (err) {
    console.error('urltopin scrape error:', err);
    return res.status(500).json({ error: err.message });
  }
});

app.post('/api/urltopin/plan-strategic', requireUser, async (req, res) => {
  try {
    const { url, articleData } = req.body || {};
    if (!url) {
      return res.status(400).json({ error: 'Missing url' });
    }
    const { base } = await fetchArticleBaseAndSummary(url, articleData || null);
    const contentProfile = await enrichContentProfile(base, openai);
    const plan = planStrategies(contentProfile, 10);
    const strategyCounts = {};
    plan.forEach((p) => { strategyCounts[p.strategy] = (strategyCounts[p.strategy] || 0) + 1; });
    const topStrategies = Object.entries(strategyCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([s]) => s.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()));
    return res.json({ plan, contentProfile, top_strategies: topStrategies });
  } catch (err) {
    console.error('urltopin plan-strategic error:', err);
    return res.status(500).json({ error: err.message });
  }
});

app.post('/api/urltopin/generate', requireUser, async (req, res) => {
  try {
    const {
      url,
      styles,
      articleData,
      brand,
      avoidText,
      mode,
      count,
      strategicSingle,
      imageSource = 'ai',
      userImageUrls: rawUserImageUrls,
      usePageReferenceImages: rawUsePageReferenceImages,
    } = req.body || {};
    const usePageReferenceImages = rawUsePageReferenceImages === true;
    let userImageUrls = rawUserImageUrls;
    if (typeof userImageUrls === 'string' && userImageUrls.trim()) {
      userImageUrls = [userImageUrls.trim()];
    } else if (!Array.isArray(userImageUrls)) {
      userImageUrls = [];
    } else {
      userImageUrls = userImageUrls.map((u) => (typeof u === 'string' ? u.trim() : u)).filter(Boolean);
    }
    const imageSourceNorm =
      typeof imageSource === 'string' ? imageSource.trim().toLowerCase() : 'ai';
    const useTextBased = imageSourceNorm === 'text' || imageSourceNorm === 'text_based';
    const useUserComposite =
      !useTextBased &&
      imageSourceNorm === 'user' &&
      userImageUrls.length > 0 &&
      userImageUrls.some((u) => u && String(u).trim());
    const isStrategic = mode === 'strategic';
    const isStrategicSingle = mode === 'strategic_single';

    let effectiveStyles = Array.isArray(styles) ? styles : [];
    if (isStrategicSingle && strategicSingle) {
      const { strategy, layoutId } = strategicSingle;
      if (strategy && layoutId) {
        effectiveStyles = [layoutId];
        req._strategicPlan = [{ strategy, goal: strategicSingle.goal || 'clicks', layoutId }];
        req._contentProfile = strategicSingle.contentProfile || {};
        req._strategicSingle = true;
      }
    } else if (isStrategic) {
      const fetched = await fetchArticleBaseAndSummary(url, articleData);
      const { base } = fetched;
      const contentProfile = await enrichContentProfile(base, openai);
      const plan = planStrategies(contentProfile, Math.min(count || 10, 10));
      effectiveStyles = plan.map((p) => p.layoutId);
      req._strategicPlan = plan;
      req._contentProfile = contentProfile;
      req._fetchedArticle = fetched;
    }

    if (!url || effectiveStyles.length === 0) {
      return res.status(400).json({
        error: isStrategic ? 'Missing url or articleData for strategic mode' : 'Missing url or styles',
      });
    }

    const brandingGate = assessUrlBrandingGate(url);
    if (brandingGate.requiresManualBrandOrCta && !String(brand?.brandName || '').trim()) {
      return res.status(400).json({
        error: 'branding_required',
        ...brandingGate,
      });
    }

    // Own-photo composites use a separate monthly cap (no image model). AI pins use pins_used.
    const pinsToGenerate = effectiveStyles.length;
    const aiPins = useUserComposite || useTextBased ? 0 : pinsToGenerate;
    const userPhotoPins = useUserComposite || useTextBased ? pinsToGenerate : 0;
    const usageResult = await applyPinQuotaDelta(req.user.id, {
      aiDelta: aiPins,
      userPhotoDelta: userPhotoPins,
    });
    if (!usageResult.allowed) {
      if (usageResult.limitKind === 'user_photo') {
        return res.status(402).json({
          error: 'user_photo_pin_limit_reached',
          message: `Your plan allows ${usageResult.planUserPhotoPinsLimit} own-photo pins per month. You have used ${usageResult.currentUserPhotoPinsUsed}, so creating ${userPhotoPins} more would exceed that limit.`,
          details: usageResult,
        });
      }
      return res.status(402).json({
        error: 'pin_limit_reached',
        message: `Your current plan allows ${usageResult.planPinsLimit} AI image pins per month. You have already used ${usageResult.currentUsed} this month, so generating ${aiPins} more would exceed your limit.`,
        details: usageResult,
      });
    }

    const textBasedNorm = normalizeTextBasedInput(req.body?.textBased);
    const requestRenderOptions =
      req.body?.renderOptions && typeof req.body.renderOptions === 'object' ? req.body.renderOptions : null;
    const bodyVariationSeed = Number(req.body?.variationSeed);

    const fastForFanOut = isStrategicSingle && !!articleData;
    const { base, articleSummary } =
      req._fetchedArticle || (await fetchArticleBaseAndSummary(url, articleData, fastForFanOut ? { fast: true } : null));
    const year = new Date().getFullYear();
    const domain =
      (base.linkDisplay || base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

    let nanoBananaReferenceInputs = [];
    let nanoBananaReferenceSource = null;
    const amazonCtxUrl = pickAmazonContextUrl(url, base.canonicalUrl);
    const refHtmlUrl = amazonCtxUrl || url;
    if (
      process.env.URLTOPIN_AMAZON_PRODUCT_IMAGES !== '0' &&
      !useTextBased &&
      !useUserComposite &&
      process.env.USE_DUMMY_IMAGES !== 'true' &&
      isAmazonProductPageForNanoReference(amazonCtxUrl)
    ) {
      try {
        const azHtml = await fetchArticleHtml(amazonCtxUrl);
        const candidates = extractAmazonProductImageUrlsFromHtml(azHtml, amazonCtxUrl);
        if (candidates.length > 0) {
          nanoBananaReferenceInputs = await mirrorAmazonImageUrlsForNanoBanana(candidates, req.user.id);
          if (nanoBananaReferenceInputs.length > 0) {
            nanoBananaReferenceSource = 'amazon_product';
            console.log(
              `urltopin: Nano Banana Amazon reference images: ${nanoBananaReferenceInputs.length} (${String(amazonCtxUrl).slice(0, 96)})`
            );
          }
        }
      } catch (e) {
        console.warn('urltopin Amazon product images for Nano:', e.message || e);
      }
    } else if (
      usePageReferenceImages &&
      process.env.URLTOPIN_PAGE_REFERENCE_IMAGES !== '0' &&
      !useTextBased &&
      !useUserComposite &&
      process.env.USE_DUMMY_IMAGES !== 'true'
    ) {
      try {
        const pageHtml = await fetchArticleHtml(refHtmlUrl);
        if (pageHtml && pageHtml.length > 200) {
          const candidates = extractGenericPageImageUrlsFromHtml(pageHtml, refHtmlUrl);
          if (candidates.length > 0) {
            nanoBananaReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana(candidates, req.user.id);
            if (nanoBananaReferenceInputs.length > 0) {
              nanoBananaReferenceSource = 'page';
              console.log(
                `urltopin: Nano Banana page reference images: ${nanoBananaReferenceInputs.length} (${String(refHtmlUrl).slice(0, 96)})`
              );
            }
          }
        }
      } catch (e) {
        console.warn('urltopin page reference images for Nano:', e.message || e);
      }
    }

    if (nanoBananaReferenceInputs.length > 0) {
      const remapped = remapStylesAvoidingInfographicsForAmazonRefs(effectiveStyles, req._strategicPlan || null);
      effectiveStyles = remapped.styles;
      if (remapped.plan) req._strategicPlan = remapped.plan;
    }

    const brandPrimary = brand?.primaryColor || null;
    const brandSecondary = brand?.secondaryColor || null;
    const brandAccent = brand?.accentColor || null;
    const brandName = brand?.brandName || null;
    const brandLogoUrl = brand?.logoUrl || null;

    const styleMeta = {
      clean_appetizing:
        'Clean, soft layout with a clear focal object related to the topic on a neutral background. Include a bold, legible Pinterest headline, a small subheadline, and small source text with the website URL at the bottom.',
      curiosity_shock:
        'Bold, dramatic, high-contrast image with a strong central subject that represents or relates to the article topic (e.g. laptop/screen for tech, ingredients for food). Creates shock and curiosity while staying semantically relevant. Use large, high-contrast headline text that feels urgent or surprising, plus bottom source text with the website URL.',
      money_saving:
        'Visual value or money-saving angle, using simple icons or motifs for money, time, and checkmarks alongside the main concept. Add punchy text that emphasizes saving time or money, and include small source text with the website URL at the bottom.',
      minimal_typography:
        'Minimal, elegant layout with lots of whitespace and a single strong visual object. Use clean, modern typography with a short, bold statement, and subtle bottom text showing the website URL.',
      question_style:
        'Image that visually represents a question or dilemma about the article topic, with subtle question-mark elements or split choices. The scene or subject must relate to the topic. Headline text should be a direct question that invites clicks, plus bottom source text with the website URL.',
      before_after:
        'Split “before vs after” layout with very clear visual contrast between the problem state and the improved state for this topic. Left side labelled “Before” shows confusion, mess or inefficiency; right side labelled “After” shows clarity, organization or success. Overlay short, readable text on each side and include small source text with the website URL at the bottom.',
      timeline_infographic:
        'Vertical infographic-style timeline made of 4–6 steps or milestones that walk the reader through the key stages of this topic (for example: Discover → Decide → Act → Maintain). Each step has a short label and simple icon. Arrange steps from top to bottom with clear arrows or connectors, and include a concise headline at the top plus small source text with the website URL at the bottom.',
      cozy_baking:
        'Lifestyle context scene where someone interacts with the topic in an appropriate everyday setting. The setting must match the topic—for tech/digital topics show a laptop or workspace; for food show a kitchen; for wellness show a calm home. Warm, welcoming lighting, friendly headline text, and subtle bottom text showing the website URL.',
      viral_curiosity:
        'Dramatic, story-like composition that feels like a personal experiment or confession. Use story-style text like “I tried X for Y days…” to drive curiosity, and add bottom source text with the website URL.',
      clumpy_fix:
        'Practical, how-to style where the visual clearly shows a “problem” version and a “fixed” or improved version of the same thing. Add clear how-to text that promises a simple fix or method, plus small bottom text with the website URL.',
      minimal_elegant:
        'Soft, premium, editorial-style image with a single object that clearly represents the article topic (e.g. laptop for tech, ingredients for food). Simple composition, elegant lighting, refined typography, discreet bottom text showing the website URL.',
      grid_3_images:
        'Layout where the pin is clearly made from three related images arranged in a clean collage or grid. Each image should show a different angle, example, or step for the topic, with thin spacing between them and a short headline and source text.',
      grid_4_images:
        'Layout where the pin is clearly made from four related images in a 2×2 grid. Each panel should show a different angle, variation, or step for the topic, with consistent gutters between panels and a short headline and source text.',
      stacked_strips:
        'Three horizontal image strips stacked vertically on one side of the pin, with a solid color text column on the other side. The text column holds a bold headline and small source URL, while the strips show three related scenes.',
      offset_collage_3:
        'Asymmetrical three-image collage with one dominant hero photo and two smaller supporting photos arranged to the side. Use overlapping cards or panels to create a dynamic layout, with a short headline and small source text.',
      circle_cluster_4:
        'Cluster of four circular photos arranged around a central text area. Each circle shows a different aspect of the topic, with a short headline in the center and small source text at the bottom.',
      step_cards_3:
        'Three tall step cards stacked vertically, each with an icon or small image and a very short label like Step 1, Step 2, Step 3. The cards should feel like a guided process, with a concise headline at the top and source URL at the bottom.',
    };

    const nicheVisualHints = {
      recipe:
        'Prioritize food, ingredients, kitchen scenes, storage containers, and real cooking or prep visuals rather than abstract icons.',
      finance:
        'Prioritize money-related visuals like bills, receipts, budgets, calculators, and simple charts instead of generic office imagery.',
      travel:
        'Use locations, landscapes, maps, luggage, and travel scenes that clearly signal destinations or journeys, not generic interiors.',
      self_improvement:
        'Show people in everyday life improving habits, routines, or mindset (journals, checklists, calm home scenes) rather than random tech imagery.',
      product_review:
        'Show the product or category clearly (packaging, close-ups, comparison layouts) so it is obvious what is being reviewed.',
    };

    const contentProfile = req._contentProfile || null;
    const niche = contentProfile?.niche || null;
    const stylePrompts = [];
    let strategicMetadataByIndex = [];
    let keyIdeas = [];
    const usedAngles = [];
    if ((isStrategic || isStrategicSingle) && req._strategicPlan) {
      const plan = req._strategicPlan;
      keyIdeas = await extractArticleKeyIdeas(articleSummary, openai);
      const usedOverlayByLayout = new Map(); // layoutId -> [{ headline, subheadline }, ...]
      const metaResults = [];
      for (let i = 0; i < plan.length; i++) {
        const p = plan[i];
        const angle = pickAngle(p.strategy, contentProfile, usedAngles);
        usedAngles.push(angle);
        const usedOverlayTexts = usedOverlayByLayout.get(p.layoutId) || [];
        const layoutOverlayGuidance = STYLE_ON_IMAGE_TEXT_GUIDANCE[p.layoutId] || null;
        const meta = await generateStrategicPinMetadata(
          {
            articleSummary,
            keyword,
            strategy: p.strategy,
            layoutId: p.layoutId,
            suggestedAngle: angle,
            keyIdeas,
            usedOverlayTexts,
            layoutOverlayGuidance,
          },
          openai
        );
        metaResults.push(meta);
        const used = usedOverlayByLayout.get(p.layoutId) || [];
        used.push({ headline: meta.overlay_headline || '', subheadline: meta.overlay_subheadline || '' });
        usedOverlayByLayout.set(p.layoutId, used);
      }
      strategicMetadataByIndex = plan.map((p, i) => ({
        ...metaResults[i],
        strategy: p.strategy,
        goal: p.goal,
        layoutId: p.layoutId,
      }));
    }

    const useDummyImages = process.env.USE_DUMMY_IMAGES === 'true';

    for (let i = 0; i < effectiveStyles.length; i++) {
      const id = effectiveStyles[i];
      const strategicMeta = strategicMetadataByIndex[i];
      if (useUserComposite) {
        const baseStyleDescription = styleMeta[id] || 'High quality, scroll-stopping Pinterest pin background.';
        const nicheHint = niche && nicheVisualHints[niche] ? ` Niche-specific visual guidance: ${nicheVisualHints[niche]}` : '';
        const strategyHint = strategicMeta?.image_prompt_hint ? ` Strategy visual (must respect this): ${strategicMeta.image_prompt_hint}.` : '';
        const styleDescription = baseStyleDescription + nicheHint + strategyHint;
        stylePrompts.push({
          id,
          label: id,
          prompt: `[user_photo_composite] ${styleDescription}`,
          index: i,
        });
        continue;
      }
      // AI and text-based pins: the string sent to Nano Banana is built later via buildOverlayImagePrompt
      // (AI) or a fixed tag (text). Skip the old GPT-4o-mini image-prompt pass — it was unused for generation
      // and added latency + cost per style.
      stylePrompts.push({
        id,
        label: id,
        prompt: useTextBased ? `[text_based_pin:${id}]` : `[ai_overlay:${id}]`,
        index: i,
      });
    }

    // Pre-generate title and description for all styles in parallel (so AI desc doesn't block after each image)
    const tokenHeader = req.headers.authorization || '';
    const metadataTimeoutMs = 20000;
    const styleMetadataByStyleId = new Map();

    if ((isStrategic || isStrategicSingle) && strategicMetadataByIndex.length > 0) {
      for (let i = 0; i < strategicMetadataByIndex.length; i++) {
        const meta = strategicMetadataByIndex[i];
        const layoutId = effectiveStyles[i];
        styleMetadataByStyleId.set(`${layoutId}::${i}`, {
          pinTitle: meta.title || topic,
          pinDescription: meta.description || base.description || '',
          hashtags: meta.hashtags || [],
          onImageHeadline: meta.overlay_headline || topic,
          onImageSubheadline: meta.overlay_subheadline || '',
          strategy: meta.strategy,
          goal: meta.goal,
          step_count: meta.step_count ?? null,
        });
      }
    } else {
      const usedOverlayByStyle = new Map();
      const metaKeyForManual = (sp) => (effectiveStyles.length > 1 && sp.index != null ? `${sp.id}::${sp.index}` : sp.id);
      for (let i = 0; i < stylePrompts.length; i++) {
        const sp = stylePrompts[i];
        const titlePrompt = `${topic}\n\nURL: ${url}\n\nStyle: ${sp.label}`;
        const descPrompt = `${topic}\n\nURL: ${url}\n\nDomain: ${domain}\n\nKeyword: ${keyword}\n\nStyle: ${sp.label}`;
        let pinTitle = topic;
        let pinDescription = base.description || '';
        let hashtags = [];
        let onImageHeadline = topic;
        let onImageSubheadline = '';
        try {
          const c1 = new AbortController();
          const c2 = new AbortController();
          const t1 = setTimeout(() => c1.abort(), metadataTimeoutMs);
          const t2 = setTimeout(() => c2.abort(), metadataTimeoutMs);
          const usedOverlayTexts = usedOverlayByStyle.get(sp.id) || [];
          const [titleRes, descRes, onImageResult] = await Promise.all([
            fetch(`${process.env.SELF_API_URL || 'http://localhost:' + PORT}/api/generate-field`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': tokenHeader },
              body: JSON.stringify({ content: titlePrompt, type: 'title' }),
              signal: c1.signal,
            }),
            fetch(`${process.env.SELF_API_URL || 'http://localhost:' + PORT}/api/generate-field`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': tokenHeader },
              body: JSON.stringify({ content: descPrompt, type: 'description' }),
              signal: c2.signal,
            }),
            generateStyleOnImageText({
              styleId: sp.id,
              topic,
              domain,
              keyword,
              year,
              description: base.description || '',
              avoidText: effectiveStyles.length === 1 && avoidText ? avoidText : null,
              usedOverlayTexts: effectiveStyles.length > 1 ? usedOverlayTexts : null,
            }),
          ]);
          clearTimeout(t1);
          clearTimeout(t2);
          if (titleRes.ok) {
            const titleJson = await titleRes.json();
            if (titleJson?.result) pinTitle = titleJson.result;
          }
          if (descRes.ok) {
            const descJson = await descRes.json();
            if (descJson?.result) {
              pinDescription = descJson.result;
              const tagMatches = pinDescription.match(/#[\w-]+/g);
              if (tagMatches) hashtags = tagMatches.slice(0, 10);
            }
          }
          if (onImageResult) {
            onImageHeadline = onImageResult.headline || topic;
            onImageSubheadline = onImageResult.subheadline || '';
          }
          const used = usedOverlayByStyle.get(sp.id) || [];
          used.push({ headline: onImageHeadline || '', subheadline: onImageSubheadline || '' });
          usedOverlayByStyle.set(sp.id, used);
        } catch (e) {
          console.warn('urltopin metadata generation error (style:', sp.label, '):', e.message || e);
        }
        styleMetadataByStyleId.set(metaKeyForManual(sp), { pinTitle, pinDescription, hashtags, onImageHeadline, onImageSubheadline });
      }
    }

    const brandForPrompt = {
      brandName: brandName || null,
      primaryColor: brandPrimary || null,
      secondaryColor: brandSecondary || null,
      accentColor: brandAccent || null,
      logoUrl: brandLogoUrl || null,
    };

    /** Bottom-of-pin line: user brand/CTA replaces raw URL when set (AI prompt + overlays stay consistent). */
    const pinFooterSourceLine = String(brandName || '').trim().slice(0, 80) || domain;

    const pinPromises = stylePrompts.map(async (sp) => {
      const metaKey = sp.index != null && ((isStrategic || isStrategicSingle) || effectiveStyles.length > 1) ? `${sp.id}::${sp.index}` : sp.id;
      const meta = styleMetadataByStyleId.get(metaKey) || styleMetadataByStyleId.get(sp.id) || {};
      const pinTitle = meta.pinTitle ?? topic;
      const pinDescription = (meta.pinDescription ?? base.description) || '';
      const hashtags = meta.hashtags ?? [];
      const onImageHeadline = meta.onImageHeadline ?? pinTitle;
      const onImageSubheadline = meta.onImageSubheadline ?? '';

      const overlayTextForPrompt = {
        headline: onImageHeadline,
        subheadline: onImageSubheadline,
        source: pinFooterSourceLine,
      };

      let imagePrompt = buildOverlayImagePrompt({
        styleId: sp.id,
        topic,
        domain,
        keyword,
        year,
        overlayText: overlayTextForPrompt,
        brand: brandForPrompt,
        stepCount: meta.step_count ?? null,
        niche: contentProfile?.niche || null,
      });
      if (useTextBased) {
        imagePrompt = `[text_based_pin] preset=${textBasedNorm.preset} primary=${textBasedNorm.primaryColor || 'default'} secondary=${textBasedNorm.secondaryColor || 'none'}`;
      } else {
        imagePrompt = appendNanoBananaAmazonUrlGarbageGuard(imagePrompt, amazonCtxUrl);
        if (nanoBananaReferenceInputs.length > 0) {
          if (nanoBananaReferenceSource === 'amazon_product') {
            imagePrompt +=
              ' ' +
              promptTier(
                'Attached reference image(s) show the real product from the Amazon listing. Use them as the primary hero subject: preserve packaging shape, brand marks, colors, and overall silhouette. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line. Integrate the product naturally; avoid duplicating it as a meaningless second copy unless the layout style requires a collage.',
                'Reference: use attached Amazon product photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.',
              );
          } else {
            imagePrompt +=
              ' ' +
              promptTier(
                'Attached reference image(s) come from the source page (hero or content photos). Use them as the primary visual subject when helpful: preserve recognizable subjects, colors, and composition; do not paste URL text or watermarks as new text. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
                'Reference: prefer attached page photos as the hero when they are strong; keep headline/sub/footer as specified.',
              );
          }
        }
      }

      const overlayText = {
        headline: onImageHeadline,
        subheadline: onImageSubheadline,
        source: pinFooterSourceLine,
      };

      let imageUrl = '';
      let userCompositeSourceUrl = null;
      const pinUserImageRaw =
        useUserComposite && userImageUrls.length
          ? userImageUrls[sp.index] ?? userImageUrls[sp.index % userImageUrls.length]
          : null;
      const pinUserImageUrl = pinUserImageRaw ? String(pinUserImageRaw).trim() : '';

      const textPinVariationSeed = Number.isFinite(bodyVariationSeed)
        ? bodyVariationSeed
        : typeof sp.index === 'number'
          ? sp.index
          : 0;

      if (useDummyImages) {
        imageUrl = `https://via.placeholder.com/1000x1500.png?text=${encodeURIComponent('Dev Pin')}`;
      } else if (useTextBased) {
        try {
          const png = await renderTextBasedPin({
            overlayText,
            brand: brandForPrompt,
            textBased: textBasedNorm,
            variationSeed: textPinVariationSeed,
            renderOptions: requestRenderOptions,
          });
          const fileName = `urltopin-text-${req.user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}-${sp.id}.png`;
          const { error: uploadError } = await supabaseAdmin.storage
            .from('ai-images')
            .upload(fileName, png, {
              contentType: 'image/png',
              upsert: true,
            });
          if (!uploadError) {
            const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
            const publicUrl = publicUrlData?.publicUrl;
            if (publicUrl) imageUrl = publicUrl;
          } else {
            console.warn('urltopin text-based upload error:', uploadError.message || uploadError);
          }
        } catch (e) {
          console.warn('urltopin text-based pin error (style:', sp.label, '):', e.message || e);
        }
      } else if (
        useUserComposite &&
        pinUserImageUrl &&
        isAllowedUserImageUrl(pinUserImageUrl, process.env.SUPABASE_URL)
      ) {
        userCompositeSourceUrl = pinUserImageUrl;
        try {
          const png = await buildUserPhotoPinBuffer(
            pinUserImageUrl,
            overlayText,
            brandForPrompt,
            requestRenderOptions
          );
          const fileName = `urltopin-user-${req.user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}-${sp.id}.png`;
          const { error: uploadError } = await supabaseAdmin.storage
            .from('ai-images')
            .upload(fileName, png, {
              contentType: 'image/png',
              upsert: true,
            });
          if (!uploadError) {
            const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
            const publicUrl = publicUrlData?.publicUrl;
            if (publicUrl) imageUrl = publicUrl;
          } else {
            console.warn('urltopin user composite upload error:', uploadError.message || uploadError);
          }
        } catch (e) {
          console.warn('urltopin user composite error (style:', sp.label, '):', e.message || e);
        }
      } else if (!useUserComposite) {
        try {
          const nanoOpts = nanoBananaReferenceInputs.length ? { imageInput: nanoBananaReferenceInputs } : {};
          const nanoUrl = await generateImageWithNanoBanana(imagePrompt, sp.label, nanoOpts);
          imageUrl = nanoUrl || '';
          if (!imageUrl) {
            console.warn('urltopin nano-banana first attempt returned no image (style:', sp.label, '), retrying once');
            const retryUrl = await generateImageWithNanoBanana(imagePrompt, sp.label, nanoOpts);
            imageUrl = retryUrl || '';
          }

          // If Nano Banana returned an image, persist it to Supabase Storage (ai-images)
          if (imageUrl) {
            try {
              const imageRes = await fetch(imageUrl);
              if (imageRes.ok) {
                const arrayBuffer = await imageRes.arrayBuffer();
                const buffer = Buffer.from(arrayBuffer);
                const fileExt = imageUrl.split('.').pop().split('?')[0] || 'png';
                const fileName = `urltopin-${req.user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}-${sp.id}.${fileExt}`;
                const { error: uploadError } = await supabaseAdmin.storage
                  .from('ai-images')
                  .upload(fileName, buffer, {
                    contentType: imageRes.headers.get('content-type') || 'image/png',
                    upsert: true,
                  });
                if (!uploadError) {
                  const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
                  const publicUrl = publicUrlData?.publicUrl;
                  if (publicUrl) {
                    imageUrl = publicUrl;
                  }
                } else {
                  console.warn('urltopin nano-banana upload error:', uploadError.message || uploadError);
                }
              } else {
                console.warn('urltopin nano-banana fetch image failed with status', imageRes.status);
              }
            } catch (uploadErr) {
              console.warn('urltopin nano-banana download/upload error:', uploadErr.message || uploadErr);
            }
          }
        } catch (e) {
          console.warn('urltopin nano-banana image generation error (style:', sp.label, '):', e.message || e);
        }

        if (!imageUrl) {
          try {
            const imgRes = await fetch(`${process.env.SELF_API_URL || 'http://localhost:' + PORT}/api/generate-image`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ prompt: imagePrompt, title: topic }),
            });
            if (imgRes.ok) {
              const imgJson = await imgRes.json();
              imageUrl = imgJson.imageUrl || '';
            }
          } catch (e) {
            console.warn('urltopin replicate image generation error:', e.message || e);
          }
        }
      }

      if (useTextBased && !imageUrl) {
        console.warn('urltopin text-based produced no image (style:', sp.label, ')');
      }
      if (useUserComposite && !imageUrl) {
        console.warn('urltopin user composite produced no image (style:', sp.label, ')');
      }

      const metaExtra = meta;
      const pinRecord = {
        styleId: sp.id,
        styleLabel: sp.label,
        imagePrompt,
        imageUrl,
        title: pinTitle,
        description: pinDescription,
        hashtags,
        link: url,
        overlayText,
        bakedInText: overlayTextForPrompt,
        ...(userCompositeSourceUrl && {
          userCompositeSourceUrl,
          imageGenerationMode: 'user_composite',
          ...(requestRenderOptions ? { renderOptions: requestRenderOptions } : {}),
        }),
        ...(useTextBased && {
          imageGenerationMode: 'text_based',
          textBased: textBasedNorm,
          variationSeed: textPinVariationSeed,
          ...(requestRenderOptions ? { renderOptions: requestRenderOptions } : {}),
        }),
        ...(!userCompositeSourceUrl &&
          !useUserComposite &&
          !useTextBased && {
            imageGenerationMode: 'ai',
            ...(nanoBananaReferenceInputs.length > 0 &&
              nanoBananaReferenceSource && {
                nanoBananaReferenceCount: nanoBananaReferenceInputs.length,
                nanoBananaReferenceSource,
              }),
          }),
        ...((isStrategic || isStrategicSingle) && metaExtra.strategy && {
          strategy: metaExtra.strategy,
          goal: metaExtra.goal,
          goalLabel:
            metaExtra.goal === 'clicks'
              ? 'High Click Potential'
              : metaExtra.goal === 'saves'
                ? 'Save-Friendly'
                : metaExtra.goal === 'engagement'
                  ? 'Engagement Focused'
                  : metaExtra.goal === 'trust'
                    ? 'Trust & Clarity'
                    : 'Experimental',
          ...(metaExtra.angle && { angle: metaExtra.angle }),
          ...(metaExtra.strategy && { reason: metaExtra.reason || getStrategyReason(metaExtra.strategy) }),
        }),
      };

      // Persist URL → Pin history for this user and also surface as a "generated" entry
      // in the scheduled_pins table so it appears in the main Scheduled Pins dashboard.
      try {
        const baseHistory = {
          user_id: req.user.id,
          source_url: url,
          article_title: base.title || null,
          article_domain: domain || null,
          style_id: sp.id,
          style_label: sp.label,
          image_url: imageUrl || null,
          pin_title: pinTitle || null,
          pin_description: pinDescription || null,
          pin_link: url,
        };

        // Fire-and-forget inserts so slow DB won't block the response
        supabaseAdmin
          .from('urltopin_history')
          .insert(baseHistory)
          .then(({ error }) => {
            if (error) {
              console.warn('urltopin_history insert error:', error.message || error);
            }
          })
          .catch((e) => {
            console.warn('urltopin_history insert failed:', e.message || e);
          });

        const originalPinData = {
          ...baseHistory,
          source: 'urltopin',
          overlayText,
          bakedInText: overlayTextForPrompt,
          ...(userCompositeSourceUrl && {
            userCompositeSourceUrl,
            imageGenerationMode: 'user_composite',
          }),
        };

        // Store in scheduled_pins with status 'generated' so they show under
        // Scheduled Pins dashboard and can be scheduled from there.
        supabaseAdmin
          .from('scheduled_pins')
          .insert({
            user_id: req.user.id,
            pinterest_account_id: null,
            title: pinTitle,
            description: pinDescription,
            image_url: imageUrl || null,
            board_id: '', // required by schema when not yet scheduled
            link: url,
            scheduled_for: null, // nullable when status is 'generated'; run migration to allow NULL
            timezone: null,
            is_recurring: false,
            recurrence_pattern: null,
            status: 'generated',
            original_pin_data: originalPinData,
          })
          .then(({ error }) => {
            if (error) {
              console.warn(
                'scheduled_pins insert (generated from urltopin) error:',
                error.message || error
              );
            }
          })
          .catch((e) => {
            console.warn(
              'scheduled_pins insert (generated from urltopin) failed:',
              e.message || e
            );
          });
      } catch (historyErr) {
        console.warn(
          'urltopin history/scheduled_pins insert threw synchronously:',
          historyErr.message || historyErr
        );
      }

      return pinRecord;
    });

    const pins = await Promise.all(pinPromises);
    let finalPins = pins;
    if (isStrategic || isStrategicSingle) {
      const diverse = checkDiversity(pins);
      if (diverse.length >= 10) {
        finalPins = diverse;
      }
      finalPins = rankPins(finalPins);
    }
    return res.json({ pins: finalPins });
  } catch (err) {
    console.error('urltopin generate error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// POST /api/urltopin/regenerate-metadata, regenerate only title or description (fast, no image)
app.post('/api/urltopin/regenerate-metadata', requireUser, async (req, res) => {
  try {
    const { url, articleData, styleId, type, currentTitle, currentDescription } = req.body || {};
    if (!url || !styleId || !type || (type !== 'title' && type !== 'description')) {
      return res.status(400).json({ error: 'Missing or invalid url, styleId, or type (must be "title" or "description")' });
    }
    const base = { ...extractMetaFromHtml('', url), ...(articleData || {}) };
    const derivedDisplay = buildLinkDisplayLabelFromUrl(url, 80);
    const derivedKw = deriveKeywordFromArticleUrl(url);
    base.linkDisplay = derivedDisplay || base.linkDisplay || '';
    base.keyword = derivedKw;
    if (base.title) {
      base.title = await maybeShortenPageTitleForUrlToPin(url, base.title, openai, base.canonicalUrl);
    }
    const domain =
      (base.linkDisplay || base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

    recordMetadataUsage(req.user.id, 1).catch((err) =>
      console.warn('recordMetadataUsage(urltopin/regenerate-metadata) error:', err?.message || err)
    );

    const contentBase = `${topic}\n\nURL: ${url}\n\nDomain: ${domain}\n\nKeyword: ${keyword}\n\nStyle: ${styleId}`;
    const currentValue = type === 'title' ? (currentTitle || '') : (currentDescription || '');
    const varietyHint = currentValue.trim()
      ? `\n\nThe current ${type} is: "${currentValue.slice(0, 200)}${currentValue.length > 200 ? '…' : ''}". Write a different alternative; do not repeat or closely copy the current one.`
      : '';

    const prompt = type === 'title'
      ? `Write a compelling Pinterest pin title (aim for 80-100 characters) for this content. Curiosity-driven, descriptive, use emotional triggers or questions. Only return the title, nothing else. Avoid quotes.${varietyHint}\n\n${contentBase}`
      : `Write an engaging Pinterest pin description (max 450 characters) for this content. Explain the benefit or insight. No URLs or "visit/click" CTAs. Include 4–6 relevant hashtags at the end. Only return the description.${varietyHint}\n\n${contentBase}`;

    const completion = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: type === 'title' ? 150 : 500,
      temperature: 0.85,
    });
    let result = (completion.choices[0].message?.content || '').trim();
    if (type === 'title') {
      result = result.replace(/["'`~@#$%^&*()_+=\[\]{}|;:<>\\/]+/g, '');
      if (result.length > 100) result = result.slice(0, 100);
      return res.json({ title: result });
    }
    return res.json({ description: sanitizeDescription(result) });
  } catch (err) {
    console.error('urltopin regenerate-metadata error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// Regenerate only the image for a given style using explicit overlay text
app.post('/api/urltopin/regenerate-image-with-text', requireUser, async (req, res) => {
  try {
    const {
      url,
      styleId,
      overlayText,
      articleData,
      brand,
      userImageUrl,
      renderOptions,
      variationSeed: rawVariationSeed,
      imageGenerationMode,
      textBased: rawTextBased,
      usePageReferenceImages: rawUsePageReferenceImages,
    } = req.body || {};
    const usePageReferenceImages = rawUsePageReferenceImages === true;
    if (!url || !styleId || !overlayText) {
      return res.status(400).json({ error: 'Missing url, styleId, or overlayText' });
    }

    const overlayForRender =
      overlayText && typeof overlayText === 'object'
        ? { ...overlayText, footerSourceOnly: true }
        : overlayText;

    const modeNorm = typeof imageGenerationMode === 'string' ? imageGenerationMode.trim().toLowerCase() : '';
    const textBasedNorm = normalizeTextBasedInput(rawTextBased);

    if (modeNorm === 'text_based') {
      const userQuota = await applyPinQuotaDelta(req.user.id, { userPhotoDelta: 1 });
      if (!userQuota.allowed) {
        return res.status(402).json({
          error: 'user_photo_pin_limit_reached',
          message: `Your plan allows ${userQuota.planUserPhotoPinsLimit} own-photo pins per month. You have used ${userQuota.currentUserPhotoPinsUsed}, so one more would exceed that limit.`,
          details: userQuota,
        });
      }
      try {
        const vs = Number(rawVariationSeed);
        const variationSeed = Number.isFinite(vs) ? vs : 0;
        const png = await renderTextBasedPin({
          overlayText: overlayForRender,
          brand,
          textBased: textBasedNorm,
          variationSeed,
          renderOptions: renderOptions && typeof renderOptions === 'object' ? renderOptions : null,
        });
        const fileName = `urltopin-text-${req.user.id}-${Date.now()}-regen-${styleId}.png`;
        const { error: uploadError } = await supabaseAdmin.storage
          .from('ai-images')
          .upload(fileName, png, { contentType: 'image/png', upsert: true });
        if (!uploadError) {
          const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
          const publicUrl = publicUrlData?.publicUrl;
          if (publicUrl) {
            return res.json({
              imageUrl: publicUrl,
              imagePrompt: `[text_based_pin] preset=${textBasedNorm.preset}`,
              imageGenerationMode: 'text_based',
              textBased: textBasedNorm,
              renderOptions: renderOptions && typeof renderOptions === 'object' ? renderOptions : null,
              variationSeed,
            });
          }
        } else {
          console.warn('urltopin regenerate text-based upload error:', uploadError.message || uploadError);
        }
      } catch (e) {
        console.warn('urltopin regenerate text-based error:', e.message || e);
      }
      await applyPinQuotaDelta(req.user.id, { userPhotoDelta: -1 });
      return res.status(500).json({ error: 'Failed to render text-based pin' });
    }

    const base = { ...extractMetaFromHtml('', url), ...(articleData || {}) };
    const derivedDisplay = buildLinkDisplayLabelFromUrl(url, 80);
    const derivedKw = deriveKeywordFromArticleUrl(url);
    base.linkDisplay = derivedDisplay || base.linkDisplay || '';
    base.keyword = derivedKw;
    if (base.title) {
      base.title = await maybeShortenPageTitleForUrlToPin(url, base.title, openai, base.canonicalUrl);
    }
    const year = new Date().getFullYear();
    const domain =
      (base.linkDisplay || base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

    let regenNanoReferenceInputs = [];
    let regenNanoReferenceSource = null;
    const amazonCtxUrl = pickAmazonContextUrl(url, base.canonicalUrl);
    const refHtmlUrl = amazonCtxUrl || url;
    if (
      process.env.URLTOPIN_AMAZON_PRODUCT_IMAGES !== '0' &&
      isAmazonProductPageForNanoReference(amazonCtxUrl)
    ) {
      try {
        const azHtml = await fetchArticleHtml(amazonCtxUrl);
        const candidates = extractAmazonProductImageUrlsFromHtml(azHtml, amazonCtxUrl);
        if (candidates.length > 0) {
          regenNanoReferenceInputs = await mirrorAmazonImageUrlsForNanoBanana(candidates, req.user.id);
          if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'amazon_product';
        }
      } catch (e) {
        console.warn('urltopin regenerate Amazon refs:', e.message || e);
      }
    } else if (
      usePageReferenceImages &&
      process.env.URLTOPIN_PAGE_REFERENCE_IMAGES !== '0'
    ) {
      try {
        const pageHtml = await fetchArticleHtml(refHtmlUrl);
        if (pageHtml && pageHtml.length > 200) {
          const candidates = extractGenericPageImageUrlsFromHtml(pageHtml, refHtmlUrl);
          if (candidates.length > 0) {
            regenNanoReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana(candidates, req.user.id);
            if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'page';
          }
        }
      } catch (e) {
        console.warn('urltopin regenerate page refs:', e.message || e);
      }
    }

    const nanoStyleId = replaceInfographicStyleIdForAmazonNanoRefs(
      styleId,
      regenNanoReferenceInputs.length > 0
    );

    let imagePrompt = buildOverlayImagePrompt({
      styleId: nanoStyleId,
      topic,
      domain,
      keyword,
      year,
      overlayText: overlayForRender,
      brand,
    });
    imagePrompt = appendNanoBananaAmazonUrlGarbageGuard(imagePrompt, amazonCtxUrl);

    const trimmedUserImg = userImageUrl && String(userImageUrl).trim();
    if (trimmedUserImg && isAllowedUserImageUrl(trimmedUserImg, process.env.SUPABASE_URL)) {
      const userQuota = await applyPinQuotaDelta(req.user.id, { userPhotoDelta: 1 });
      if (!userQuota.allowed) {
        return res.status(402).json({
          error: 'user_photo_pin_limit_reached',
          message: `Your plan allows ${userQuota.planUserPhotoPinsLimit} own-photo pins per month. You have used ${userQuota.currentUserPhotoPinsUsed}, so one more would exceed that limit.`,
          details: userQuota,
        });
      }
      try {
        const png = await buildUserPhotoPinBuffer(trimmedUserImg, overlayForRender, brand, renderOptions || null);
        const fileName = `urltopin-user-${req.user.id}-${Date.now()}-regen-${styleId}.png`;
        const { error: uploadError } = await supabaseAdmin.storage
          .from('ai-images')
          .upload(fileName, png, { contentType: 'image/png', upsert: true });
        if (!uploadError) {
          const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
          const publicUrl = publicUrlData?.publicUrl;
          if (publicUrl) {
            return res.json({
              imageUrl: publicUrl,
              imagePrompt: '[user_photo_pin]',
              userCompositeSourceUrl: trimmedUserImg,
              imageGenerationMode: 'user_composite',
              renderOptions: renderOptions || null,
            });
          }
        } else {
          console.warn('urltopin regenerate user composite upload error:', uploadError.message || uploadError);
        }
      } catch (e) {
        console.warn('urltopin regenerate user composite error:', e.message || e);
      }
      await applyPinQuotaDelta(req.user.id, { userPhotoDelta: -1 });
    }

    const aiQuota = await applyPinQuotaDelta(req.user.id, { aiDelta: 1 });
    if (!aiQuota.allowed) {
      return res.status(402).json({
        error: 'pin_limit_reached',
        message: `Your current plan allows ${aiQuota.planPinsLimit} AI image pins per month. You have already used ${aiQuota.currentUsed} this month, so generating one more would exceed your limit.`,
        details: aiQuota,
      });
    }

    let imagePromptForNano = imagePrompt;
    if (regenNanoReferenceInputs.length > 0) {
      if (regenNanoReferenceSource === 'amazon_product') {
        imagePromptForNano +=
          ' ' +
          promptTier(
            'Attached reference image(s) show the real product from the Amazon listing. Use them as the primary hero subject: preserve packaging shape, brand marks, colors, and overall silhouette. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
            'Reference: use attached Amazon product photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.',
          );
      } else {
        imagePromptForNano +=
          ' ' +
          promptTier(
            'Attached reference image(s) come from the source page (hero or content photos). Use them as the primary visual subject when helpful: preserve recognizable subjects, colors, and composition; do not paste URL text or watermarks as new text. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
            'Reference: prefer attached page photos as the hero when they are strong; keep headline/sub/footer as specified.',
          );
      }
    }

    const regenNanoOpts = regenNanoReferenceInputs.length ? { imageInput: regenNanoReferenceInputs } : {};

    let imageUrl = '';
    try {
      imageUrl = await generateImageWithNanoBanana(imagePromptForNano, nanoStyleId, regenNanoOpts);
      if (!imageUrl) {
        try {
          const imgRes = await fetch(`${process.env.SELF_API_URL || 'http://localhost:' + PORT}/api/generate-image`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ prompt: imagePromptForNano, title: topic }),
          });
          if (imgRes.ok) {
            const imgJson = await imgRes.json();
            imageUrl = imgJson.imageUrl || '';
          }
        } catch (e) {
          console.warn('urltopin regenerate-image-with-text fallback error:', e.message || e);
        }
      }
    } catch (e) {
      await applyPinQuotaDelta(req.user.id, { aiDelta: -1 });
      console.error('urltopin regenerate-image-with-text AI error:', e.message || e);
      return res.status(500).json({ error: e.message || 'Failed to generate image' });
    }

    if (!imageUrl) {
      await applyPinQuotaDelta(req.user.id, { aiDelta: -1 });
      return res.status(500).json({ error: 'Failed to generate image with the provided text' });
    }

    return res.json({
      imageUrl,
      imagePrompt: imagePromptForNano,
      ...(nanoStyleId !== styleId && { layoutIdUsed: nanoStyleId }),
      ...(regenNanoReferenceInputs.length > 0 &&
        regenNanoReferenceSource && {
          nanoBananaReferenceCount: regenNanoReferenceInputs.length,
          nanoBananaReferenceSource: regenNanoReferenceSource,
        }),
    });
  } catch (err) {
    console.error('urltopin regenerate-image-with-text error:', err);
    return res.status(500).json({ error: err.message });
  }
});

app.post('/api/urltopin/download-zip', requireUser, async (req, res) => {
  try {
    const { images } = req.body || {};
    if (!Array.isArray(images) || images.length === 0) {
      return res.status(400).json({ error: 'No images provided' });
    }

    const zip = new JSZip();

    for (const img of images) {
      const filename = img.filename || 'pin.png';
      try {
        const bake = img?.bake && typeof img.bake === 'object' ? img.bake : null;
        const bakeUserImageUrl = bake?.userImageUrl && String(bake.userImageUrl).trim();

        if (bakeUserImageUrl && isAllowedUserImageUrl(bakeUserImageUrl, process.env.SUPABASE_URL)) {
          const overlayText = bake?.overlayText && typeof bake.overlayText === 'object' ? bake.overlayText : {};
          const brand = bake?.brand && typeof bake.brand === 'object' ? bake.brand : null;
          const renderOptions = bake?.renderOptions && typeof bake.renderOptions === 'object' ? bake.renderOptions : null;
          const png = await compositeUserPhotoPin({
            sourceImageUrl: bakeUserImageUrl,
            overlayText,
            brand,
            renderOptions,
          });
          zip.file(filename, png);
          continue;
        }

        const bakeText = bake?.textBased && typeof bake.textBased === 'object';
        if (bakeText) {
          const overlayText = bake?.overlayText && typeof bake.overlayText === 'object' ? bake.overlayText : {};
          const brand = bake?.brand && typeof bake.brand === 'object' ? bake.brand : null;
          const renderOptions = bake?.renderOptions && typeof bake.renderOptions === 'object' ? bake.renderOptions : null;
          const textBased = normalizeTextBasedInput(bake.textBased);
          const vs = Number(bake?.variationSeed);
          const variationSeed = Number.isFinite(vs) ? vs : 0;
          try {
            const png = await renderTextBasedPin({
              overlayText,
              brand,
              textBased,
              variationSeed,
              renderOptions,
            });
            zip.file(filename, png);
          } catch (e) {
            console.warn('ZIP text-based bake failed:', e.message || e);
          }
          continue;
        }

        if (!img?.url) continue;
        const resp = await fetch(img.url);
        if (!resp.ok) continue;
        const arrayBuf = await resp.arrayBuffer();
        zip.file(filename, Buffer.from(arrayBuf));
      } catch (e) {
        console.warn('Failed to add image to ZIP:', filename, e.message || e);
      }
    }

    const content = await zip.generateAsync({ type: 'nodebuffer' });
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', 'attachment; filename="urltopin-pins.zip"');
    return res.send(content);
  } catch (err) {
    console.error('urltopin download-zip error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// --- Custom Templates CRUD (Supabase) ---
// Requires authenticated user (Bearer token)
async function requireUser(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !user) return res.status(401).json({ error: 'Unauthorized' });
  req.user = user;
  next();
}

// List templates for current user
app.get('/api/custom-templates', requireUser, async (req, res) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('custom_templates')
      .select('id,name,template_json,updated_at,created_at')
      .eq('user_id', req.user.id)
      .order('updated_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    const templates = (data || []).map(row => ({ id: row.id, name: row.name, ...row.template_json }));
    res.json({ templates });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Create or update a template
app.post('/api/custom-templates', requireUser, async (req, res) => {
  try {
    const tpl = req.body;
    if (!tpl || !tpl.id || !tpl.elements) {
      return res.status(400).json({ error: 'Invalid template payload' });
    }
    const row = {
      id: tpl.id,
      user_id: req.user.id,
      name: tpl.name || 'Untitled Template',
      template_json: tpl,
      updated_at: new Date().toISOString(),
    };
    const { error } = await supabaseAdmin
      .from('custom_templates')
      .upsert(row, { onConflict: 'id' });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Delete a template
app.delete('/api/custom-templates/:id', requireUser, async (req, res) => {
  try {
    const { id } = req.params;
    const { error } = await supabaseAdmin
      .from('custom_templates')
      .delete()
      .match({ id, user_id: req.user.id });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/export-pin (Puppeteer server-side rendering)
app.post('/api/export-pin', async (req, res) => {
  const { pinData, template, templateType, templateData } = req.body;
  console.log('[export-pin] Request received', { pinData: !!pinData, template });
  console.log('[export-pin] Template type:', typeof template);
  console.log('[export-pin] Template value:', template);
  console.log('[export-pin] Template meta:', { templateType, hasTemplateData: !!templateData });
  if (!pinData || !template) {
    console.error('[export-pin] Missing pinData or template');
    return res.status(400).json({ error: 'pinData and template are required' });
  }
  let browser;
  try {
    console.log('[export-pin] Launching Puppeteer...');
    browser = await puppeteer.launch({ 
      headless: 'new', 
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      timeout: 30000
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1000, height: 1500 });
    
    // Set up console log capture BEFORE navigating to the page
    page.on('console', msg => {
      const text = msg.text();
      console.log('[Puppeteer Console]', text);
      // Check for ExportPinPage loading
      if (text.includes('ExportPinPage component is loading')) {
        console.log('[export-pin] ✅ ExportPinPage component loaded successfully!');
      }
    });
    
    // Use environment variable for frontend base URL, fallback to localhost for local dev
    const FRONTEND_BASE_URL = process.env.FRONTEND_BASE_URL || 'http://localhost:3000';
    console.log('[export-pin] FRONTEND_BASE_URL from env:', process.env.FRONTEND_BASE_URL);
    console.log('[export-pin] Final FRONTEND_BASE_URL:', FRONTEND_BASE_URL);
    
    // Construct the URL with template parameters
    const params = new URLSearchParams();
    params.set('data', JSON.stringify(pinData));
    params.set('template', template);
    if (templateType) params.set('templateType', templateType);
    if (templateType === 'custom' && templateData) {
      // Pass full custom template JSON to the export page
      params.set('templateData', JSON.stringify(templateData));
    }
    const url = `${FRONTEND_BASE_URL}/export-pin?${params.toString()}`;
    console.log('[export-pin] Navigating to', url);
    console.log('[export-pin] Encoded template:', encodeURIComponent(template));
    
    // Add timeout and better error handling
    await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
    
    // Wait a bit more for React to hydrate
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // Capture page errors
    page.on('pageerror', error => {
      console.log('[Puppeteer Page Error]', error.message);
      console.log('[Puppeteer Page Error Stack]', error.stack);
    });
    
    // Capture network failures
    page.on('requestfailed', request => {
      console.log('[Puppeteer Request Failed]', request.url(), request.failure().errorText);
    });
    
    // Wait a bit for the page to fully load using setTimeout
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const pageTitle = await page.title();
    console.log('[export-pin] Page title:', pageTitle);
    
    // Get the page content to see if it's loading correctly
    const pageContent = await page.content();
    console.log('[export-pin] Page content length:', pageContent.length);
    
    // Check for React app indicators instead of component names
    const hasReactApp = pageContent.includes('react') || pageContent.includes('React');
    console.log('[export-pin] Page appears to be React app:', hasReactApp);
    
    // Debug: Check what's actually on the page
    const hasRootDiv = pageContent.includes('id="root"') || pageContent.includes('id=\'root\'');
    const hasScriptTags = pageContent.includes('<script');
    const hasCSV2Pin = pageContent.includes('CSV2Pin') || pageContent.includes('csv2pin');
    console.log('[export-pin] Page has root div:', hasRootDiv);
    console.log('[export-pin] Page has script tags:', hasScriptTags);
    console.log('[export-pin] Page mentions CSV2Pin:', hasCSV2Pin);
    
    // Show first 500 chars of page content for debugging
    console.log('[export-pin] Page content preview:', pageContent.substring(0, 500));
    
    // Test if JavaScript is working at all
    try {
      const jsTest = await page.evaluate(() => {
        return {
          hasWindow: typeof window !== 'undefined',
          hasDocument: typeof document !== 'undefined',
          hasReact: typeof window !== 'undefined' && window.React,
          hasReactDOM: typeof window !== 'undefined' && window.ReactDOM,
          userAgent: navigator.userAgent,
          scriptCount: document.querySelectorAll('script').length,
          scriptSources: Array.from(document.querySelectorAll('script')).map(s => s.src || 'inline').slice(0, 5)
        };
      });
      console.log('[export-pin] JavaScript execution test:', jsTest);
    } catch (e) {
      console.log('[export-pin] JavaScript execution failed:', e.message);
    }
    
    // Check if React is loading by waiting longer and checking again
    console.log('[export-pin] Waiting additional 5 seconds for React to load...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    try {
      const reactTest = await page.evaluate(() => {
        return {
          hasReact: typeof window !== 'undefined' && window.React,
          hasReactDOM: typeof window !== 'undefined' && window.ReactDOM,
          hasExportDebug: typeof window !== 'undefined' && window.exportDebug,
          rootElement: document.getElementById('root')?.innerHTML?.length || 0,
          isExportPageWorking: typeof window !== 'undefined' && window.exportDebug && window.exportDebug.templateKey
        };
      });
      console.log('[export-pin] React loading test after wait:', reactTest);
      
      if (reactTest.isExportPageWorking) {
        console.log('[export-pin] ✅ ExportPinPage is working correctly!');
      } else {
        console.log('[export-pin] ❌ ExportPinPage is not working properly');
      }
    } catch (e) {
      console.log('[export-pin] React test failed:', e.message);
    }
    
    const templateReceived = await page.evaluate(() => {
      if (window.exportDebug) {
        return window.exportDebug;
      }
      return null;
    });
    console.log('[export-pin] Template debug info:', templateReceived);
    
    // Check if the template component is rendering
    const templateComponent = await page.evaluate(() => {
      const templateDiv = document.querySelector('div');
      if (templateDiv) {
        return {
          className: templateDiv.className,
          style: templateDiv.style.cssText,
          children: templateDiv.children.length
        };
      }
      return null;
    });
    console.log('[export-pin] Template component info:', templateComponent);
    
    // Ensure all images are fully loaded before screenshot (important for multi-image templates)
    try {
      await page.waitForFunction(() => {
        const imgs = Array.from(document.images || []);
        if (imgs.length === 0) return true;
        return imgs.every((img) => img.complete && img.naturalWidth > 0);
      }, { timeout: 10000 });
      console.log('[export-pin] All images reported loaded.');
    } catch (e) {
      console.log('[export-pin] Proceeding without full image load confirmation:', e.message);
    }

    console.log('[export-pin] Taking screenshot...');
    const buffer = await page.screenshot({ type: 'png', fullPage: true });
    await browser.close();
    console.log('[export-pin] Screenshot taken and browser closed. Sending response.');
    res.set('Content-Type', 'image/png');
    res.send(buffer);
  } catch (err) {
    console.error('[export-pin] Error:', err, err.stack);
    if (browser) await browser.close();
    res.status(500).json({ error: 'Failed to export pin', details: err.message, stack: err.stack });
  }
});

// POST /api/generate-field (requires login; credits handled on frontend)
app.post('/api/generate-field', requireUser, async (req, res) => {
  const { content, type, style } = req.body; // type: 'title' or 'description'; style: optional hint
  if (!content || !type) return res.status(400).json({ error: 'Missing content or type' });

  // Soft-limit tracking for metadata usage (titles/descriptions)
  recordMetadataUsage(req.user.id, 1).catch((err) =>
    console.warn('recordMetadataUsage(generate-field) error:', err.message || err)
  );

  const isShortTitle = type === 'title' && style === 'short_50';
  const prompt = type === 'title'
    ? (
      isShortTitle
        ? `Write a concise Pinterest pin title (max 50 characters). Focus on the main keyword and benefit. No quotes or hashtags. Return only the title.\n${content}`
        : `Write a compelling Pinterest pin title (aim for 80-100 characters) for this content. The title should be curiosity-driven and make people want to click to learn more. Include emotional triggers, urgency, numbers, or questions where possible. Make it descriptive and specific rather than generic. Use engaging words that create intrigue. Only return the title, nothing else. Avoid quotes but you can use basic punctuation like periods, commas, exclamation points, and question marks:\n${content}`
    )
    : `Write an engaging Pinterest pin description (max 450 characters) for this content. The description should explain the benefit or insight the user will get by clicking. Avoid phrases like "+visit site+", "+click the link+", or adding URLs. Include 4–6 relevant hashtags at the end. Only return the description, nothing else:\n${content}`;
  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: type === 'title' ? 150 : 500,
      temperature: 0.7,
    });
    let result = completion.choices[0].message.content.trim();
    if (type === 'title') {
      // Remove only problematic characters, keep basic punctuation like . , ! ? -
      result = result.replace(/["'`~@#$%^&*()_+=\[\]{}|;:<>\\/]+/g, '');
      // Enforce 50 chars hard cap for short_50 style
      if (isShortTitle && result.length > 50) {
        const cut = result.lastIndexOf(' ', 49);
        result = (cut > 20 ? result.slice(0, cut) : result.slice(0, 50)).trim();
      }
      // Otherwise keep previous 100-char soft cap
      if (!isShortTitle && result.length > 100) {
        result = result.slice(0, 100);
      }
    }
    if (type === 'description') result = sanitizeDescription(result);
    res.json({ result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Analyze image with OpenAI Vision to extract text and generate metadata (requires login)
app.post('/api/analyze-image', requireUser, async (req, res) => {
  try {
    const { imageUrl, urlHint } = req.body;
    if (!imageUrl) return res.status(400).json({ error: 'Missing imageUrl' });

    // Soft-limit tracking for metadata usage (vision-based metadata)
    recordMetadataUsage(req.user.id, 1).catch((err) =>
      console.warn('recordMetadataUsage(analyze-image) error:', err.message || err)
    );

    const systemPrompt = `You are helping generate Pinterest pin metadata. First, read any visible text in the image (OCR). Then propose a compelling title (aim for 80-100 characters, maximum 100) and an engaging description (<=450 chars) suitable for Pinterest. The title should be curiosity-driven, descriptive, and use emotional triggers or questions to make people want to click. The description must include 4–6 relevant hashtags at the end. Do not include URLs or phrases like \"visit example.com\", \"click the link\", or similar calls to visit a site. If a destination URL context is provided, use it only to infer keywords, but never include the URL or a CTA. Return JSON with keys: extractedText, title, description. Do not include markdown, code fences, or commentary.`;

    const userPrompt = `Image URL: ${imageUrl}\n${urlHint ? `Destination URL (context): ${urlHint}` : ''}`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: systemPrompt },
        {
          role: 'user',
          content: [
            { type: 'text', text: userPrompt },
            { type: 'image_url', image_url: { url: imageUrl } }
          ]
        }
      ],
      temperature: 0.4,
      max_tokens: 600
    });

    const raw = response.choices?.[0]?.message?.content?.trim() || '';

    // Attempt to parse JSON; if model returned plain text, wrap it
    let extracted = { extractedText: '', title: '', description: '' };
    try {
      const maybe = JSON.parse(raw);
      extracted.extractedText = String(maybe.extractedText || '').slice(0, 2000);
      extracted.title = String(maybe.title || '').slice(0, 100);
      extracted.description = sanitizeDescription(String(maybe.description || ''));
    } catch (_) {
      // Fallback heuristic: split lines
      const lines = raw.split('\n').map(s => s.trim()).filter(Boolean);
      extracted.extractedText = lines.join(' ').slice(0, 2000);
      extracted.title = (lines[0] || '').slice(0, 100);
      extracted.description = sanitizeDescription(lines.slice(1).join(' '));
    }

    // Final cleanup for title - keep basic punctuation like . , ! ? -
    extracted.title = extracted.title.replace(/["'`~@#$%^&*()_+=\[\]{}|;:<>\\/]+/g, '').slice(0, 100);

    return res.json(extracted);
  } catch (err) {
    console.error('analyze-image error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// --- Dodo Payments checkout session for subscriptions ---

app.post('/api/dodo/create-checkout-session', requireUser, async (req, res) => {
  try {
    if (!DODO_API_KEY) {
      return res.status(500).json({ error: 'Dodo Payments API key not configured' });
    }

    const { planType, discountCode, couponCode, referralKey, affonsoReferral } = req.body || {};
    if (!planType) {
      return res.status(400).json({ error: 'Missing planType' });
    }

    const referralSlug = String(referralKey || '')
      .trim()
      .toLowerCase();

    const affonso_referral = String(affonsoReferral || '')
      .trim()
      .slice(0, 256);

    // Dodo checkout expects the human-readable discount *code* (e.g. LAUNCH20), not the dashboard id (dsc_...).
    // Priority: typed coupon → partner ?ref= slug map → global env default.
    const fromClient = String(discountCode || couponCode || '')
      .trim();
    const fromPartner = referralSlug ? resolvePartnerDiscountCode(referralSlug) : null;
    const fromEnv = String(process.env.DODO_CHECKOUT_DEFAULT_DISCOUNT_CODE || '')
      .trim();
    const discount_code = fromClient || fromPartner || fromEnv || undefined;

    // Map internal plan types to Dodo product IDs via environment variables
    const productMap = {
      free: process.env.DODO_PRODUCT_FREE_ID,
      starter: process.env.DODO_PRODUCT_STARTER_ID,
      creator: process.env.DODO_PRODUCT_CREATOR_ID,
      pro: process.env.DODO_PRODUCT_PRO_ID,
      agency: process.env.DODO_PRODUCT_AGENCY_ID,
    };

    const productId = productMap[planType];
    if (!productId) {
      return res.status(400).json({ error: `No Dodo product configured for planType "${planType}"` });
    }

  // Compute frontend base (strip any /app or deeper path)
  let frontendBase = process.env.FRONTEND_BASE_URL || 'http://localhost:3000';
  try {
    const u = new URL(frontendBase);
    frontendBase = `${u.protocol}//${u.host}`;
  } catch {
    frontendBase = frontendBase.replace(/\/app\/?$/, '');
  }

  const body = {
      // One subscription product in the cart; pricing & interval are defined in Dodo dashboard
      product_cart: [{ product_id: productId, quantity: 1 }],
      // Optional: pass through metadata so we can link back to Supabase user and plan in webhooks later
      metadata: {
        supabase_user_id: req.user.id,
        app_plan_type: planType,
        ...(referralSlug ? { referral_key: referralSlug } : {}),
        ...(affonso_referral ? { affonso_referral } : {}),
      },
    // Redirect back to app after success/failure
    return_url: `${frontendBase.replace(/\/$/, '')}/payment-success?plan=${encodeURIComponent(planType)}`,
    // Let customers enter or change a code on the hosted checkout; pre-apply when we have one.
    feature_flags: {
      allow_discount_code: true,
    },
    };

    if (discount_code) {
      body.discount_code = discount_code;
    }

    const resp = await fetch(`${DODO_BASE_URL}/checkouts`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DODO_API_KEY}`,
      },
      body: JSON.stringify(body),
    });

    const json = await resp.json().catch(() => ({}));

    if (!resp.ok) {
      console.error('Dodo create checkout session error:', resp.status, json);
      return res.status(500).json({
        error: 'failed_to_create_dodo_checkout',
        status: resp.status,
        details: json,
      });
    }

    const checkoutUrl = json.checkout_url || json.url || null;
    const sessionId = json.session_id || json.id || null;

    markPendingDodoActivation(req.user.id, planType, sessionId || null);
    console.log('🧾 Dodo checkout created', {
      userId: req.user.id,
      planType,
      sessionId: sessionId || null,
      hasCheckoutUrl: Boolean(checkoutUrl),
    });

    if (!checkoutUrl && !sessionId) {
      console.warn('Dodo checkout response missing checkout_url and session_id:', json);
    }

    return res.json({
      checkoutUrl,
      sessionId,
      raw: json,
    });
  } catch (err) {
    console.error('Dodo create checkout session unexpected error:', err);
    return res.status(500).json({ error: 'failed_to_create_dodo_checkout', message: err.message });
  }
});

// --- Account overview (plan + usage) ---

app.get('/api/account/usage', requireUser, async (req, res) => {
  try {
    const snapshot = await getCurrentUsageSnapshot(req.user.id);
    return res.json(snapshot);
  } catch (err) {
    console.error('account/usage error:', err);
    return res.status(500).json({ error: 'Failed to load account usage' });
  }
});

app.get('/api/account/plan', requireUser, async (req, res) => {
  try {
    const planType = await resolvePlanTypeForUser(req.user.id);
    return res.json({ planType });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to load plan' });
  }
});

// Simple endpoint to mark a plan as active for the current user
app.post('/api/account/activate-plan', requireUser, async (req, res) => {
  try {
    const { planType, checkoutSessionId } = req.body || {};
    if (!planType || !PLAN_PIN_LIMITS[planType]) {
      return res.status(400).json({ error: 'Invalid planType' });
    }

    const pending = consumePendingDodoActivation(req.user.id, planType);
    if (!pending.ok) {
      console.warn('activate-plan blocked (no verified pending checkout)', {
        userId: req.user.id,
        planType,
        reason: pending.reason,
        pendingPlanType: pending.pendingPlanType || null,
      });
      return res.status(409).json({
        error: 'No verified pending checkout for this plan. Please complete checkout again.',
        code: 'missing_verified_checkout',
        reason: pending.reason,
      });
    }

    const result = await applyPlanActivationForUser(req.user.id, planType, 'payment_success_fallback');
    if (!result.ok) {
      return res.status(500).json({ error: 'Failed to activate plan', details: result.error });
    }

    console.log('✅ activate-plan fallback succeeded', {
      userId: req.user.id,
      planType,
      checkoutSessionId: checkoutSessionId || null,
      via: 'payment-success',
    });
    return res.json({ ok: true, planType: result.planType, pinsLimit: result.pinsLimit });
  } catch (err) {
    console.error('activate-plan error:', err);
    return res.status(500).json({
      error: 'Failed to activate plan',
      details: err.message || String(err),
    });
  }
});

// Dodo webhook: primary source of subscription truth.
app.post('/api/dodo/webhook', async (req, res) => {
  try {
    const event = req.body || {};
    const eventType = String(event.type || event.event_type || event.name || '').toLowerCase();
    const dataObj = event?.data?.object || event?.data || event?.payload || {};
    const metadata = dataObj?.metadata || event?.metadata || {};
    const userId = String(metadata?.supabase_user_id || '').trim();
    const planType = String(metadata?.app_plan_type || metadata?.plan_type || '').trim();

    if (!eventType) return res.status(400).json({ error: 'Missing event type' });
    if (!userId) {
      console.warn('dodo webhook ignored: missing supabase_user_id metadata', { eventType });
      return res.json({ ok: true, ignored: true, reason: 'missing_user_metadata' });
    }

    const activateSignals = [
      'checkout.completed',
      'checkout.succeeded',
      'payment.succeeded',
      'subscription.created',
      'subscription.activated',
      'subscription.active',
      'subscription.renewed',
    ];
    const cancelSignals = [
      'subscription.canceled',
      'subscription.cancelled',
      'subscription.expired',
      'subscription.ended',
    ];

    if (activateSignals.some((sig) => eventType.includes(sig))) {
      if (!planType || !PLAN_PIN_LIMITS[planType]) {
        console.warn('dodo webhook activation ignored: missing/invalid plan metadata', {
          eventType,
          userId,
          planType,
        });
        return res.json({ ok: true, ignored: true, reason: 'missing_plan_metadata' });
      }
      const activated = await applyPlanActivationForUser(userId, planType, `dodo_webhook:${eventType}`);
      if (!activated.ok) {
        console.error('dodo webhook activation failed', { eventType, userId, planType, error: activated.error });
        return res.status(500).json({ error: 'Failed to activate plan from webhook' });
      }
      return res.json({ ok: true, action: 'activated', userId, planType });
    }

    if (cancelSignals.some((sig) => eventType.includes(sig))) {
      await supabaseAdmin
        .from('billing_subscriptions')
        .update({ status: 'cancelled', updated_at: new Date().toISOString() })
        .eq('user_id', userId)
        .eq('status', 'active');
      await supabaseAdmin
        .from('profiles')
        .update({ plan_type: 'free', is_pro: false, updated_at: new Date().toISOString() })
        .eq('id', userId);
      return res.json({ ok: true, action: 'cancelled', userId });
    }

    return res.json({ ok: true, ignored: true, reason: 'unhandled_event', eventType });
  } catch (err) {
    console.error('dodo webhook error:', err);
    return res.status(500).json({ error: 'Webhook processing failed' });
  }
});

// CSV save endpoint for all registered users
app.post('/api/csv', async (req, res) => {
  // Save CSV logic here (not implemented)
  res.json({ message: 'CSV saved (available to all registered users)' });
});

// --- Pinterest OAuth2 Integration ---
// Redirect user to Pinterest OAuth2
const PINTEREST_OAUTH_STATE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

app.get('/api/pinterest/login', (req, res) => {
  console.log('--- Pinterest OAuth Login Initiated ---');
  console.log('client_id:', process.env.PINTEREST_CLIENT_ID);
  console.log('redirect_uri:', process.env.PINTEREST_REDIRECT_URI);
  console.log('scope:', 'pins:write boards:read boards:write pins:read user_accounts:read');
  const reconnect = typeof req.query.reconnect === 'string' ? req.query.reconnect.trim() : '';
  let state = 'secureRandomState123'; // TODO: Use a real random state for security
  if (reconnect && PINTEREST_OAUTH_STATE_UUID.test(reconnect)) {
    state = `reconnect:${reconnect}`;
  }
  const params = new URLSearchParams({
    client_id: process.env.PINTEREST_CLIENT_ID,
    redirect_uri: process.env.PINTEREST_REDIRECT_URI,
    response_type: 'code',
    scope: 'pins:write boards:read boards:write pins:read user_accounts:read', // added user_accounts:read
    state,
  });
  const redirectUrl = `https://www.pinterest.com/oauth/?${params.toString()}`;
  console.log('Redirecting to:', redirectUrl);
  res.redirect(redirectUrl);
});


async function exchangePinterestCodeForToken(code, redirectUri) {
  console.log('redirectUri used in token exchange:', redirectUri);
  
  // Try Method 1: Basic Auth (Confidential Client approach)
  const basicAuth = Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString('base64');
  
  const params = new URLSearchParams({
    grant_type: 'authorization_code',
    code: code,
    redirect_uri: redirectUri
  });

  console.log('--- Exchanging Pinterest Code for Token (Method 1: Basic Auth) ---');
  console.log('Request body:', params.toString());

  try {
    let response = await fetch('https://api.pinterest.com/v5/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': `Basic ${basicAuth}`,
      },
      body: params.toString(),
    });

    let text = await response.text();
    let result;
    try { 
      result = JSON.parse(text); 
    } catch { 
      result = { error: 'Invalid JSON response', response: text }; 
    }

    console.log('Pinterest token endpoint response (Method 1):', response.status, result);

    // If Method 1 fails, try Method 2: Include credentials in body
    if (!response.ok && response.status === 400) {
      console.log('--- Trying Method 2: Credentials in body ---');
      
      const paramsWithCreds = new URLSearchParams({
        grant_type: 'authorization_code',
        code: code,
        client_id: process.env.PINTEREST_CLIENT_ID,
        client_secret: process.env.PINTEREST_CLIENT_SECRET,
        redirect_uri: redirectUri
      });

      response = await fetch('https://api.pinterest.com/v5/oauth/token', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json',
        },
        body: paramsWithCreds.toString(),
      });

      text = await response.text();
      try { 
        result = JSON.parse(text); 
      } catch { 
        result = { error: 'Invalid JSON response', response: text }; 
      }

      console.log('Pinterest token endpoint response (Method 2):', response.status, result);
    }

    if (!response.ok) {
      throw new Error(`Pinterest API error: ${response.status} - ${JSON.stringify(result)}`);
    }

    return result;
  } catch (error) {
    console.error('Error in exchangePinterestCodeForToken:', error);
    throw error;
  }
}

/** Seconds before access_token expiry to proactively refresh */
const PINTEREST_TOKEN_REFRESH_BUFFER_SEC = 300;

async function exchangePinterestRefreshToken(refreshToken) {
  if (!refreshToken || !process.env.PINTEREST_CLIENT_ID || !process.env.PINTEREST_CLIENT_SECRET) {
    throw new Error('Missing refresh token or Pinterest client credentials');
  }
  const basicAuth = Buffer.from(
    `${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`
  ).toString('base64');

  const params = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: refreshToken,
  });

  let response = await fetch('https://api.pinterest.com/v5/oauth/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
      Authorization: `Basic ${basicAuth}`,
    },
    body: params.toString(),
  });

  let text = await response.text();
  let result;
  try {
    result = JSON.parse(text);
  } catch {
    result = { error: 'Invalid JSON response', response: text };
  }

  if (!response.ok && response.status === 400) {
    const paramsWithCreds = new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      client_id: process.env.PINTEREST_CLIENT_ID,
      client_secret: process.env.PINTEREST_CLIENT_SECRET,
    });
    response = await fetch('https://api.pinterest.com/v5/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
      },
      body: paramsWithCreds.toString(),
    });
    text = await response.text();
    try {
      result = JSON.parse(text);
    } catch {
      result = { error: 'Invalid JSON response', response: text };
    }
  }

  if (!response.ok) {
    throw new Error(`Pinterest refresh error: ${response.status} - ${JSON.stringify(result)}`);
  }
  return result;
}

async function applyPinterestTokenResponseToAccount(accountId, tokenData) {
  if (!accountId || !tokenData?.access_token) return;
  const expiresAt =
    tokenData.expires_in != null
      ? new Date(Date.now() + Number(tokenData.expires_in) * 1000).toISOString()
      : null;
  const row = {
    access_token: tokenData.access_token,
    updated_at: new Date().toISOString(),
  };
  if (tokenData.refresh_token) row.refresh_token = tokenData.refresh_token;
  if (expiresAt) row.token_expires_at = expiresAt;

  const { error } = await supabaseAdmin.from('pinterest_accounts').update(row).eq('id', accountId);
  if (error) console.error('applyPinterestTokenResponseToAccount:', error.message || error);
}

function pinterestAccountNeedsProactiveRefresh(account) {
  if (!account?.refresh_token) return false;
  if (!account.token_expires_at) return false;
  const expMs = new Date(account.token_expires_at).getTime();
  if (Number.isNaN(expMs)) return false;
  return Date.now() > expMs - PINTEREST_TOKEN_REFRESH_BUFFER_SEC * 1000;
}

function pinterestResponseIsAuthFailure(status, pinData) {
  // 401 + invalid_token/unauthorized should trigger token refresh; 403 is often authorization/permissions.
  if (status === 401) return true;
  const msg = String(pinData?.message || pinData?.error || '').toLowerCase();
  return msg.includes('authentication') || msg.includes('unauthorized') || msg.includes('invalid_token');
}

function pinterestResponseIsPermissionFailure(status, pinData) {
  if (status !== 403) return false;
  const msg = String(pinData?.message || pinData?.error || '').toLowerCase();
  return (
    msg.includes('not permitted') ||
    msg.includes('forbidden') ||
    msg.includes('permission') ||
    msg.includes('access that resource')
  );
}

async function pinterestValidateBoardAccess(accessToken, boardId) {
  try {
    const res = await fetch(`https://api.pinterest.com/v5/boards/${encodeURIComponent(boardId)}`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    });

    let data = null;
    try {
      data = await res.json();
    } catch {
      // ignore JSON parse issues; Pinterest sometimes returns empty body
    }

    if (res.ok) return { ok: true, status: res.status, message: null };

    const msg = String(data?.message || data?.error || '').trim();
    return { ok: false, status: res.status, message: msg ? `${msg}.` : '' };
  } catch (e) {
    return { ok: false, status: null, message: (e?.message || 'Network error.') };
  }
}

/**
 * Returns a usable access token; refreshes OAuth token when expired/near expiry if refresh_token is stored.
 * @param {{ id: string, access_token?: string|null, refresh_token?: string|null, token_expires_at?: string|null }} account
 */
async function ensureValidPinterestAccessToken(account) {
  if (!account?.id) {
    return { accessToken: null, error: 'Invalid Pinterest account row' };
  }
  let accessToken = account.access_token || null;
  let refreshToken = account.refresh_token || null;

  if (pinterestAccountNeedsProactiveRefresh(account) && refreshToken) {
    try {
      const tokenData = await exchangePinterestRefreshToken(refreshToken);
      await applyPinterestTokenResponseToAccount(account.id, tokenData);
      accessToken = tokenData.access_token;
      refreshToken = tokenData.refresh_token || refreshToken;
    } catch (e) {
      console.error('Pinterest proactive token refresh failed:', e.message || e);
      return { accessToken: null, error: e.message || 'Token refresh failed' };
    }
  }

  if (!accessToken) {
    return { accessToken: null, error: 'No Pinterest access token' };
  }
  return { accessToken, refreshToken, error: null };
}

/**
 * After Pinterest returns auth error: try refresh once and return new access token (updates DB).
 */
async function pinterestRefreshAfterAuthFailure(account) {
  if (!account?.id || !account.refresh_token) return null;
  try {
    const tokenData = await exchangePinterestRefreshToken(account.refresh_token);
    await applyPinterestTokenResponseToAccount(account.id, tokenData);
    return tokenData.access_token || null;
  } catch (e) {
    console.error('Pinterest reactive token refresh failed:', e.message || e);
    return null;
  }
}


// Handle Pinterest OAuth2 callback
app.get('/api/pinterest/callback', async (req, res) => {
  const { code, state } = req.query;
  console.log('--- Pinterest OAuth Callback ---');
  console.log('Received code:', code);
  console.log('Received state:', state);
  if (!code) return res.status(400).send('Missing code');
  // Redirect to frontend with code for user association (use env FRONTEND_URL)
  const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:3000';
  const finish = new URL(`${FRONTEND_URL.replace(/\/$/, '')}/pinterest/finish`);
  finish.searchParams.set('code', code);
  if (state && typeof state === 'string') finish.searchParams.set('state', state);
  res.redirect(finish.toString());
});

app.post('/api/pinterest/oauth', async (req, res) => {
  const { code, redirectUri, reconnect_account_id: reconnectAccountIdBody } = req.body;
  const authHeader = req.headers.authorization;
  if (!code || !redirectUri) return res.status(400).json({ error: 'Missing code or redirectUri' });
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const reconnectRaw =
    typeof reconnectAccountIdBody === 'string' ? reconnectAccountIdBody.trim() : '';
  const reconnectAccountId =
    reconnectRaw && PINTEREST_OAUTH_STATE_UUID.test(reconnectRaw) ? reconnectRaw : null;

  // Log the values sent to Pinterest for debugging
  console.log({
    client_id: process.env.PINTEREST_CLIENT_ID,
    client_secret: process.env.PINTEREST_CLIENT_SECRET ? process.env.PINTEREST_CLIENT_SECRET.slice(0,3) + '...' + process.env.PINTEREST_CLIENT_SECRET.slice(-3) : undefined,
    redirect_uri: redirectUri,
    code,
    reconnect: !!reconnectAccountId,
  });
  try {
    if (reconnectAccountId) {
      const { data: owned, error: ownErr } = await supabaseAdmin
        .from('pinterest_accounts')
        .select('id')
        .eq('id', reconnectAccountId)
        .eq('user_id', user.id)
        .single();
      if (ownErr || !owned) {
        return res.status(404).json({ error: 'Pinterest account not found for reconnect' });
      }
    }

    const tokenData = await exchangePinterestCodeForToken(code, redirectUri);
    if (tokenData.access_token) {
      // Fetch account info for labeling
      let accountName = '';
      try {
        const accRes = await fetch('https://api.pinterest.com/v5/user_account', {
          headers: { 'Authorization': `Bearer ${tokenData.access_token}`, 'Accept': 'application/json' },
        });
        const acc = await accRes.json();
        accountName = acc?.username || acc?.profile?.username || 'Pinterest Account';
      } catch (e) {
        console.warn('Failed to fetch Pinterest account info:', e?.message || e);
      }
      const tokenExpiresAt =
        tokenData.expires_in != null
          ? new Date(Date.now() + Number(tokenData.expires_in) * 1000).toISOString()
          : null;

      if (reconnectAccountId) {
        const updateRow = {
          access_token: tokenData.access_token,
          account_name: accountName,
          refresh_token: tokenData.refresh_token || null,
          updated_at: new Date().toISOString(),
          ...(tokenExpiresAt ? { token_expires_at: tokenExpiresAt } : {}),
        };
        const { error: updErr } = await supabaseAdmin
          .from('pinterest_accounts')
          .update(updateRow)
          .eq('id', reconnectAccountId)
          .eq('user_id', user.id);
        if (updErr) {
          console.error('Error updating pinterest account on reconnect:', updErr);
          return res.status(500).json({ error: updErr.message || 'Failed to save tokens' });
        }
        return res.json({
          access_token: tokenData.access_token,
          account_name: accountName,
          reconnected: true,
        });
      }

      // Enforce plan limits (new link only)
      const { data: profile } = await supabaseAdmin
        .from('profiles')
        .select('plan_type')
        .eq('id', user.id)
        .single();
      const planType = profile?.plan_type || 'free';
      const { data: existing } = await supabaseAdmin
        .from('pinterest_accounts')
        .select('id')
        .eq('user_id', user.id);
      const count = Array.isArray(existing) ? existing.length : 0;
      const planLimits = { free: 1, starter: 1, creator: 3, pro: Infinity, agency: Infinity };
      const limit = planLimits[planType] ?? 1;
      if (count >= limit) {
        return res.status(403).json({ error: `Plan limit reached. Your plan (${planType}) allows ${limit === Infinity ? 'unlimited' : limit} account(s).` });
      }

      const insertRow = {
        user_id: user.id,
        access_token: tokenData.access_token,
        account_name: accountName,
        refresh_token: tokenData.refresh_token || null,
        ...(tokenExpiresAt ? { token_expires_at: tokenExpiresAt } : {}),
      };
      const { error: insertError } = await supabaseAdmin
        .from('pinterest_accounts')
        .insert(insertRow);
      if (insertError) {
        console.error('Error saving pinterest account:', insertError);
      }
      return res.json({ access_token: tokenData.access_token, account_name: accountName });
    } else {
      return res.status(400).json({ error: tokenData });
    }
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// List Pinterest accounts for current user
app.get('/api/pinterest/accounts', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });
  const { data, error } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, account_name, created_at')
    .eq('user_id', user.id)
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ accounts: data || [] });
});

// Delete / disconnect a Pinterest account for the current user
app.delete('/api/pinterest/accounts/:id', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];

  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const accountId = req.params.id;
  if (!accountId) return res.status(400).json({ error: 'Missing account id' });

  try {
    // Ensure the account belongs to this user
    const { data: account, error: fetchError } = await supabaseAdmin
      .from('pinterest_accounts')
      .select('id, user_id')
      .eq('id', accountId)
      .eq('user_id', user.id)
      .single();

    if (fetchError && fetchError.code !== 'PGRST116') {
      console.error('Error fetching pinterest account before delete:', fetchError);
      return res.status(500).json({ error: 'Failed to verify account ownership' });
    }

    if (!account) {
      return res.status(404).json({ error: 'Account not found' });
    }

    // Delete the account; scheduled_pins.pinterest_account_id has ON DELETE CASCADE
    const { error: deleteError } = await supabaseAdmin
      .from('pinterest_accounts')
      .delete()
      .eq('id', accountId)
      .eq('user_id', user.id);

    if (deleteError) {
      console.error('Error deleting pinterest account:', deleteError);
      return res.status(500).json({ error: 'Failed to delete account' });
    }

    return res.json({ success: true });
  } catch (err) {
    console.error('Unexpected error deleting pinterest account:', err);
    return res.status(500).json({ error: 'Unexpected error deleting account' });
  }
});

function extractAccountId(req) {
  return req.query.account_id || req.body?.account_id || null;
}

async function getPinterestAccessTokenForUser(userId, accountId) {
  if (!accountId) {
    // Fallback to single-token in profiles for backwards compatibility
    const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('pinterest_access_token')
      .eq('id', userId)
    .single();
    return profile?.pinterest_access_token || null;
  }
  const { data: account } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, access_token, refresh_token, token_expires_at')
    .eq('id', accountId)
    .eq('user_id', userId)
    .single();
  if (!account) return null;
  const { accessToken } = await ensureValidPinterestAccessToken(account);
  return accessToken || null;
}

app.get('/api/pinterest/boards', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const accountId = extractAccountId(req);
  const accessToken = await getPinterestAccessTokenForUser(user.id, accountId);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user.' });
  }

  // Fetch boards from Pinterest API
  const pinterestRes = await fetch('https://api.pinterest.com/v5/boards', {
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  });
  const boards = await pinterestRes.json();
  res.json({ boards: boards.items || boards.data || [] });
});

// Create a Pinterest board (name required; optional description, privacy PUBLIC/SECRET)
app.post('/api/pinterest/create-board', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { name, description, privacy, account_id } = req.body || {};
  if (!name || typeof name !== 'string' || !name.trim()) {
    return res.status(400).json({ error: 'Board name is required' });
  }

  const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user/account.' });
  }

  // Normalize privacy to Pinterest expected values: PUBLIC | SECRET
  let privacyValue = undefined;
  if (privacy) {
    const p = String(privacy).toUpperCase();
    if (p === 'PUBLIC' || p === 'SECRET') privacyValue = p; else privacyValue = undefined;
  }

  try {
    const body = {
      name: name.trim(),
      description: description ? String(description).slice(0, 250) : undefined,
      privacy: privacyValue,
    };
    const pinterestRes = await fetch('https://api.pinterest.com/v5/boards', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    const resp = await pinterestRes.json().catch(() => ({}));
    if (!pinterestRes.ok) {
      return res.status(pinterestRes.status).json({ error: resp || { message: 'Pinterest API error' } });
    }
    return res.json(resp);
  } catch (e) {
    return res.status(500).json({ error: e.message || 'Failed to create board' });
  }
});

app.post('/api/pinterest/create-pin', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  if (!(await enforcePaidSchedulingOrThrow(res, user.id))) return;

  const { image_url, title, description, board_id, link, account_id } = req.body;
  if (!image_url || !title || !description || !board_id) {
    return res.status(400).json({ error: 'Missing required fields.' });
  }

  const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user/account.' });
  }

  try {
    const requestBody = {
      board_id,
      title,
      description,
      media_source: {
        source_type: 'image_url',
        url: image_url,
      },
      link: link || undefined,
    };
    
    console.log('Pinterest API request:', {
      board_id,
      title: title.substring(0, 50) + '...',
      description: description.substring(0, 100) + '...',
      image_url: image_url.substring(0, 100) + '...',
      link: link || 'none'
    });
    
    const pinterestRes = await fetch('https://api.pinterest.com/v5/pins', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });

    const pinData = await pinterestRes.json();
    if (pinterestRes.ok) {
      return res.json(pinData);
    } else {
      console.error('Pinterest API error:', pinData);
      return res.status(400).json({ 
        error: {
          message: pinData.message || pinData.error || 'Pinterest API error',
          code: pinData.code || pinterestRes.status,
          details: pinData
        }
      });
    }
  } catch (error) {
    return res.status(400).json({ 
      error: { 
        code: 2, 
        message: `Pinterest API error: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Schedule a pin for later posting
app.post('/api/pinterest/schedule-pin', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  if (!(await enforcePaidSchedulingOrThrow(res, user.id))) return;

  const { 
    image_url, title, description, board_id, link, account_id,
    scheduled_for, timezone = 'UTC', is_recurring = false, recurrence_pattern,
    force_duplicate = false,
    bake
  } = req.body;

  // Validate required fields
  if ((!image_url && !bake) || !title || !description || !board_id || !scheduled_for) {
    return res.status(400).json({ error: 'Missing required fields: image_url (or bake), title, description, board_id, scheduled_for' });
  }

  // Validate scheduling time
  const scheduleDate = new Date(scheduled_for);
  const now = new Date();
  if (scheduleDate <= now) {
    return res.status(400).json({ error: 'Scheduled time must be in the future' });
  }

  // Validate not too far in future (1 year max)
  const oneYearFromNow = new Date();
  oneYearFromNow.setFullYear(oneYearFromNow.getFullYear() + 1);
  if (scheduleDate > oneYearFromNow) {
    return res.status(400).json({ error: 'Cannot schedule more than 1 year in advance' });
  }

  // Validate Pinterest account access
  const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user/account.' });
  }

  // Validate recurrence pattern if provided
  if (is_recurring && recurrence_pattern) {
    const pattern = typeof recurrence_pattern === 'string' 
      ? JSON.parse(recurrence_pattern) 
      : recurrence_pattern;
    
    if (!['daily', 'weekly', 'monthly'].includes(pattern.type)) {
      return res.status(400).json({ error: 'Invalid recurrence type. Must be daily, weekly, or monthly.' });
    }
    
    if (!pattern.interval || pattern.interval < 1 || pattern.interval > 30) {
      return res.status(400).json({ error: 'Invalid recurrence interval. Must be between 1 and 30.' });
    }
  }

  try {
    // Optional: bake a user-photo composite at scheduling time (avoids extra "regenerate" step).
    let finalImageUrl = image_url;
    if (bake && typeof bake === 'object') {
      const userImageUrl = bake.userImageUrl && String(bake.userImageUrl).trim();
      if (userImageUrl && isAllowedUserImageUrl(userImageUrl, process.env.SUPABASE_URL)) {
        const overlayText = bake.overlayText && typeof bake.overlayText === 'object' ? bake.overlayText : {};
        const brand = bake.brand && typeof bake.brand === 'object' ? bake.brand : null;
        const renderOptions = bake.renderOptions && typeof bake.renderOptions === 'object' ? bake.renderOptions : null;
        const png = await compositeUserPhotoPin({ sourceImageUrl: userImageUrl, overlayText, brand, renderOptions });
        const fileName = `pinterest-bake-${user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}.png`;
        const { error: uploadError } = await supabaseAdmin.storage
          .from('ai-images')
          .upload(fileName, png, { contentType: 'image/png', upsert: true });
        if (!uploadError) {
          const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
          const publicUrl = publicUrlData?.publicUrl;
          if (publicUrl) finalImageUrl = publicUrl;
        } else {
          console.warn('schedule-pin bake upload error:', uploadError.message || uploadError);
        }
      } else if (bake.textBased && typeof bake.textBased === 'object') {
        const overlayText = bake.overlayText && typeof bake.overlayText === 'object' ? bake.overlayText : {};
        const brand = bake.brand && typeof bake.brand === 'object' ? bake.brand : null;
        const renderOptions = bake.renderOptions && typeof bake.renderOptions === 'object' ? bake.renderOptions : null;
        const textBased = normalizeTextBasedInput(bake.textBased);
        const vs = Number(bake?.variationSeed);
        const variationSeed = Number.isFinite(vs) ? vs : 0;
        try {
          const png = await renderTextBasedPin({
            overlayText,
            brand,
            textBased,
            variationSeed,
            renderOptions,
          });
          const fileName = `pinterest-textbake-${user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}.png`;
          const { error: uploadError } = await supabaseAdmin.storage
            .from('ai-images')
            .upload(fileName, png, { contentType: 'image/png', upsert: true });
          if (!uploadError) {
            const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
            const publicUrl = publicUrlData?.publicUrl;
            if (publicUrl) finalImageUrl = publicUrl;
          } else {
            console.warn('schedule-pin text-based bake upload error:', uploadError.message || uploadError);
          }
        } catch (e) {
          console.warn('schedule-pin text-based bake error:', e.message || e);
        }
      }
    }

    // If not forcing duplicate, check whether this pin was already posted to this board
    if (!force_duplicate) {
      const { data: existing } = await supabaseAdmin
        .from('scheduled_pins')
        .select('id')
        .eq('user_id', user.id)
        .eq('board_id', board_id)
        .eq('status', 'posted')
        .eq('image_url', finalImageUrl)
        .limit(1);
      if (existing && existing.length > 0) {
        return res.status(409).json({
          already_posted_to_board: true,
          message: 'This pin was already posted to this board. Post again or skip.',
        });
      }
    }

    // Store original pin data for reference
    const originalPinData = {
      image_url: finalImageUrl, title, description, board_id, link, account_id,
      user_id: user.id, created_at: new Date().toISOString()
    };

    // Insert scheduled pin
    const { data: scheduledPin, error: insertError } = await supabaseAdmin
      .from('scheduled_pins')
      .insert({
        user_id: user.id,
        pinterest_account_id: account_id,
        title,
        description,
        image_url: finalImageUrl,
        board_id,
        link: link || '',
        scheduled_for: scheduleDate.toISOString(),
        timezone,
        is_recurring,
        recurrence_pattern: is_recurring ? recurrence_pattern : null,
        original_pin_data: originalPinData
      })
      .select()
      .single();

    if (insertError) {
      console.error('Error inserting scheduled pin:', insertError);
      return res.status(500).json({ error: 'Failed to schedule pin' });
    }

    return res.json({
      success: true,
      scheduled_pin: scheduledPin,
      message: `Pin scheduled for ${scheduleDate.toLocaleString()}`
    });

  } catch (error) {
    console.error('Error scheduling pin:', error);
    return res.status(500).json({ 
      error: { 
        message: `Failed to schedule pin: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Get user's scheduled pins
app.get('/api/pinterest/scheduled-pins', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { status, limit = 50, offset = 0, date_from, date_to, account_id } = req.query;

  try {
    let query = supabaseAdmin
      .from('scheduled_pins')
      .select(`
        *,
        pinterest_accounts(account_name)
      `)
      .eq('user_id', user.id)
      .is('deleted_at', null)
      // Put generated pins (scheduled_for=null) after real scheduled pins,
      // otherwise calendar view can fetch only null rows and appear empty.
      .order('scheduled_for', { ascending: true, nullsFirst: false })
      .order('created_at', { ascending: false });

    if (status) {
      query = query.eq('status', status);
    }
    // Generated pins have pinterest_account_id=null and scheduled_for=null - avoid filtering them out
    const isGeneratedOnly = status === 'generated';
    const isAllStatus = !status || status === 'all';
    if (account_id && !isGeneratedOnly) {
      query = query.eq('pinterest_account_id', account_id);
    }
    if (date_from || date_to) {
      const fromVal = date_from || '1970-01-01T00:00:00.000Z';
      const toEndOfDay = date_to
        ? new Date(date_to + 'T23:59:59.999Z').toISOString()
        : '2099-12-31T23:59:59.999Z';
      if (isGeneratedOnly) {
        // Generated pins have no scheduled_for - skip date filter
      } else if (isAllStatus) {
        // Include generated pins when viewing "All" with date filter
        query = query.or(`and(scheduled_for.gte.${fromVal},scheduled_for.lte.${toEndOfDay}),status.eq.generated`);
      } else {
        query = query.gte('scheduled_for', fromVal).lte('scheduled_for', toEndOfDay);
      }
    }

    const { data: scheduledPins, error: fetchError } = await query
      .range(parseInt(offset), parseInt(offset) + parseInt(limit) - 1);

    if (fetchError) {
      console.error('Error fetching scheduled pins:', fetchError);
      return res.status(500).json({ error: 'Failed to fetch scheduled pins' });
    }

    return res.json({
      scheduled_pins: scheduledPins,
      total: scheduledPins.length
    });

  } catch (error) {
    console.error('Error fetching scheduled pins:', error);
    return res.status(500).json({ 
      error: { 
        message: `Failed to fetch scheduled pins: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Preflight check upcoming scheduled pins for common permanent failures (missing account/board, board access)
app.get('/api/pinterest/scheduled-pins/preflight', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const windowHours = Math.min(24 * 14, Math.max(1, parseInt(req.query.window_hours || '72', 10))); // 1h..14d
  const limit = Math.min(200, Math.max(1, parseInt(req.query.limit || '100', 10)));

  try {
    const now = new Date();
    const until = new Date(now.getTime() + windowHours * 60 * 60 * 1000);

    const { data: pins, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('id,user_id,pinterest_account_id,board_id,title,scheduled_for,status,deleted_at')
      .eq('user_id', user.id)
      .eq('status', 'scheduled')
      .gte('scheduled_for', now.toISOString())
      .lte('scheduled_for', until.toISOString())
      .is('deleted_at', null)
      .order('scheduled_for', { ascending: true })
      .limit(limit);

    if (fetchError) {
      console.error('Preflight fetch scheduled pins error:', fetchError);
      return res.status(500).json({ error: 'Failed to fetch scheduled pins for preflight' });
    }

    const issues = [];
    const boardChecks = [];
    const seenKey = new Set();

    for (const pin of pins || []) {
      if (!pin.pinterest_account_id) {
        issues.push({
          pin_id: pin.id,
          type: 'missing_pinterest_account',
          message: 'No Pinterest account linked to this scheduled pin.',
          scheduled_for: pin.scheduled_for,
        });
        continue;
      }
      if (!pin.board_id) {
        issues.push({
          pin_id: pin.id,
          type: 'missing_board_id',
          message: 'No board selected for this scheduled pin.',
          scheduled_for: pin.scheduled_for,
        });
        continue;
      }

      const key = `${pin.pinterest_account_id}:${pin.board_id}`;
      if (!seenKey.has(key)) {
        seenKey.add(key);
        boardChecks.push({ pin_id: pin.id, account_id: pin.pinterest_account_id, board_id: pin.board_id });
      }
    }

    // Validate board access for a capped number of unique (account, board) pairs to avoid API spam
    const MAX_BOARD_CHECKS = 30;
    const toCheck = boardChecks.slice(0, MAX_BOARD_CHECKS);

    // Fetch account rows in batch
    const uniqueAccountIds = [...new Set(toCheck.map(x => x.account_id))];
    let accountsById = {};
    if (uniqueAccountIds.length > 0) {
      const { data: accRows, error: accErr } = await supabaseAdmin
        .from('pinterest_accounts')
        .select('id, access_token, refresh_token, token_expires_at')
        .in('id', uniqueAccountIds);
      if (accErr) {
        console.error('Preflight fetch pinterest accounts error:', accErr);
      } else {
        for (const a of accRows || []) accountsById[a.id] = a;
      }
    }

    for (const check of toCheck) {
      const accRow = accountsById[check.account_id];
      if (!accRow) {
        issues.push({
          pin_id: check.pin_id,
          type: 'pinterest_account_not_found',
          message: 'Pinterest account row not found (was it deleted/disconnected?).',
        });
        continue;
      }

      const { accessToken } = await ensureValidPinterestAccessToken(accRow);
      if (!accessToken) {
        issues.push({
          pin_id: check.pin_id,
          type: 'missing_access_token',
          message: 'No usable Pinterest access token for this account (reconnect required).',
        });
        continue;
      }

      const boardAccess = await pinterestValidateBoardAccess(accessToken, check.board_id);
      if (!boardAccess.ok) {
        issues.push({
          pin_id: check.pin_id,
          type: 'board_not_accessible',
          message: `Board not accessible for this connected account (status ${boardAccess.status || 'n/a'}). ${boardAccess.message || ''}`.trim(),
          board_id: check.board_id,
          pinterest_account_id: check.account_id,
        });
      }
    }

    return res.json({
      window_hours: windowHours,
      checked_pins: (pins || []).length,
      unique_board_checks: toCheck.length,
      capped_board_checks: boardChecks.length > MAX_BOARD_CHECKS,
      issues,
    });
  } catch (error) {
    console.error('Preflight scheduled pins error:', error);
    return res.status(500).json({ error: 'Failed to preflight scheduled pins', details: error.message });
  }
});

// Bulk-cancel pins stuck in the posting queue (due / failed / posting, or all non-posted waiting)
app.post('/api/pinterest/scheduled-pins/bulk-cancel', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const scope = req.body?.scope === 'all_waiting' ? 'all_waiting' : 'due';
  const nowIso = new Date().toISOString();

  try {
    let query = supabaseAdmin
      .from('scheduled_pins')
      .select('id, image_url')
      .eq('user_id', user.id)
      .is('deleted_at', null);

    if (scope === 'all_waiting') {
      query = query.in('status', ['scheduled', 'failed', 'posting']);
    } else {
      query = query.or(
        `and(status.eq.scheduled,scheduled_for.lte.${nowIso}),status.eq.failed,status.eq.posting`
      );
    }

    const { data: pins, error: fetchError } = await query;

    if (fetchError) {
      console.error('bulk-cancel fetch error:', fetchError);
      return res.status(500).json({ error: 'Failed to list pins to cancel' });
    }

    if (!pins || pins.length === 0) {
      return res.json({
        success: true,
        cancelled_count: 0,
        scope,
        message: 'No matching pins to cancel',
      });
    }

    const ids = pins.map((p) => p.id);
    const { error: updateError } = await supabaseAdmin
      .from('scheduled_pins')
      .update({
        status: 'cancelled',
        next_retry_at: null,
        updated_at: nowIso,
      })
      .in('id', ids)
      .eq('user_id', user.id)
      .is('deleted_at', null);

    if (updateError) {
      console.error('bulk-cancel update error:', updateError);
      return res.status(500).json({ error: 'Failed to cancel pins' });
    }

    const seenUrls = new Set();
    for (const pin of pins) {
      if (!pin.image_url || seenUrls.has(pin.image_url)) continue;
      seenUrls.add(pin.image_url);
      await supabaseAdmin
        .from('user_images')
        .update({
          is_scheduled: false,
          scheduled_for: null,
        })
        .eq('user_id', user.id)
        .eq('image_url', pin.image_url);
    }

    return res.json({
      success: true,
      cancelled_count: ids.length,
      scope,
    });
  } catch (error) {
    console.error('bulk-cancel error:', error);
    return res.status(500).json({ error: 'Bulk cancel failed', details: error.message });
  }
});

// Update a scheduled pin
app.put('/api/pinterest/scheduled-pins/:id', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { id } = req.params;
  const { 
    title, description, scheduled_for, timezone, status,
    is_recurring, recurrence_pattern 
  } = req.body;

  try {
    // If a free user attempts to schedule/unschedule via update, block scheduling-related updates.
    if (scheduled_for || status === 'scheduled') {
      if (!(await enforcePaidSchedulingOrThrow(res, user.id))) return;
    }

    // First check if pin exists and belongs to user (only active pins)
    const { data: existingPin, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('id', id)
      .eq('user_id', user.id)
      .is('deleted_at', null)
      .single();

    if (fetchError || !existingPin) {
      return res.status(404).json({ error: 'Scheduled pin not found' });
    }

    // Don't allow updates to posted pins
    if (existingPin.status === 'posted') {
      return res.status(400).json({ error: 'Cannot update a pin that has already been posted' });
    }

    // Validate new schedule time if provided
    if (scheduled_for) {
      const scheduleDate = new Date(scheduled_for);
      const now = new Date();
      if (scheduleDate <= now) {
        return res.status(400).json({ error: 'Scheduled time must be in the future' });
      }
    }

    // Build update object
    const updates = {};
    if (title) updates.title = title;
    if (description) updates.description = description;
    if (scheduled_for) updates.scheduled_for = new Date(scheduled_for).toISOString();
    if (timezone) updates.timezone = timezone;
    if (status) updates.status = status;
    if (typeof is_recurring === 'boolean') updates.is_recurring = is_recurring;
    if (recurrence_pattern) updates.recurrence_pattern = recurrence_pattern;

    // Putting a failed pin back on the calendar: clear error/retry state so the worker can try again
    if (
      scheduled_for &&
      updates.status === 'scheduled' &&
      existingPin.status === 'failed'
    ) {
      updates.error_message = null;
      updates.retry_count = 0;
      updates.next_retry_at = null;
    }

    const { data: updatedPin, error: updateError } = await supabaseAdmin
      .from('scheduled_pins')
      .update(updates)
      .eq('id', id)
      .eq('user_id', user.id)
      .select()
      .single();

    if (updateError) {
      console.error('Error updating scheduled pin:', updateError);
      return res.status(500).json({ error: 'Failed to update scheduled pin' });
    }

    return res.json({
      success: true,
      scheduled_pin: updatedPin,
      message: 'Scheduled pin updated successfully'
    });

  } catch (error) {
    console.error('Error updating scheduled pin:', error);
    return res.status(500).json({ 
      error: { 
        message: `Failed to update scheduled pin: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Post a specific scheduled pin immediately
app.post('/api/pinterest/scheduled-pins/:id/post-now', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  if (!(await enforcePaidSchedulingOrThrow(res, user.id))) return;

  const { id } = req.params;

  try {
    const { data: pin, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('id', id)
      .eq('user_id', user.id)
      .is('deleted_at', null)
      .single();

    if (fetchError || !pin) {
      return res.status(404).json({ error: 'Scheduled pin not found' });
    }

    if (pin.status === 'posted') {
      return res.status(400).json({ error: 'Pin already posted' });
    }
    if (pin.status === 'cancelled') {
      return res.status(400).json({ error: 'Cannot post a cancelled pin' });
    }

    // Ensure it's eligible to run now
    const nowIso = new Date().toISOString();
    const { error: prepErr } = await supabaseAdmin
      .from('scheduled_pins')
      .update({
        status: 'scheduled',
        scheduled_for: nowIso,
        next_retry_at: null,
        updated_at: nowIso,
      })
      .eq('id', id)
      .eq('user_id', user.id)
      .is('deleted_at', null);

    if (prepErr) {
      console.error('post-now prep update error:', prepErr);
      return res.status(500).json({ error: 'Failed to prepare pin for posting' });
    }

    // Re-fetch to get latest row and run the existing processing logic.
    const { data: freshPin, error: freshErr } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('id', id)
      .eq('user_id', user.id)
      .is('deleted_at', null)
      .single();

    if (freshErr || !freshPin) {
      return res.status(500).json({ error: 'Failed to load pin for posting' });
    }

    await processScheduledPin(freshPin);

    // Return updated state
    const { data: finalPin } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('id', id)
      .eq('user_id', user.id)
      .is('deleted_at', null)
      .single();

    return res.json({ success: true, scheduled_pin: finalPin });
  } catch (error) {
    console.error('post-now error:', error);
    return res.status(500).json({ error: 'Failed to post now', details: error.message });
  }
});

// Cancel/delete a scheduled pin
app.delete('/api/pinterest/scheduled-pins/:id', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { id } = req.params;

  try {
    // Check if pin exists and belongs to user (only active pins)
    const { data: existingPin, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('id', id)
      .eq('user_id', user.id)
      .is('deleted_at', null)
      .single();

    if (fetchError || !existingPin) {
      return res.status(404).json({ error: 'Scheduled pin not found' });
    }

    // Don't allow deletion of posted pins, just mark as cancelled
    if (existingPin.status === 'posted') {
      return res.status(400).json({ error: 'Cannot delete a pin that has already been posted' });
    }

    // If pin is scheduled or failed, we can safely delete it
    // If it's currently posting, mark as cancelled instead
    if (existingPin.status === 'posting') {
      const { error: updateError } = await supabaseAdmin
        .from('scheduled_pins')
        .update({ status: 'cancelled' })
        .eq('id', id)
        .eq('user_id', user.id);

      if (updateError) {
        console.error('Error cancelling scheduled pin:', updateError);
        return res.status(500).json({ error: 'Failed to cancel scheduled pin' });
      }

      // Also update the corresponding user_images record
      await supabaseAdmin
        .from('user_images')
        .update({ 
          is_scheduled: false,
          scheduled_for: null
        })
        .eq('user_id', user.id)
        .eq('image_url', existingPin.image_url);

      return res.json({
        success: true,
        message: 'Scheduled pin cancelled successfully'
      });
    } else {
      // Mark the pin as cancelled instead of deleting
      const { error: updateError } = await supabaseAdmin
        .from('scheduled_pins')
        .update({ 
          status: 'cancelled',
          updated_at: new Date().toISOString()
        })
        .eq('id', id)
        .eq('user_id', user.id);

      if (updateError) {
        console.error('Error cancelling scheduled pin:', updateError);
        return res.status(500).json({ error: 'Failed to cancel scheduled pin' });
      }

      // Also update the corresponding user_images record to remove scheduled status
      await supabaseAdmin
        .from('user_images')
        .update({ 
          is_scheduled: false,
          scheduled_for: null
        })
        .eq('user_id', user.id)
        .eq('image_url', existingPin.image_url);

      return res.json({
        success: true,
        message: 'Scheduled pin cancelled successfully'
      });
    }

  } catch (error) {
    console.error('Error deleting scheduled pin:', error);
    return res.status(500).json({ 
      error: { 
        message: `Failed to delete scheduled pin: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Debug endpoint to check Pinterest connection status
app.get('/api/pinterest/status', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { data: accounts, error: accError } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, account_name, access_token')
    .eq('user_id', user.id);
  const hasToken = Array.isArray(accounts) && accounts.some(a => !!a.access_token);
  let tokenValid = false;
  let pinterestError = null;

  if (hasToken && accounts[0]?.id) {
    try {
      const accessToken = await getPinterestAccessTokenForUser(user.id, accounts[0].id);
      if (!accessToken) {
        pinterestError = { message: 'No usable Pinterest access token' };
      } else {
        const testRes = await fetch('https://api.pinterest.com/v5/user_account', {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
          },
        });
        if (testRes.ok) tokenValid = true;
        else pinterestError = await testRes.json().catch(() => ({ message: testRes.statusText }));
      }
    } catch (error) {
      pinterestError = { message: error.message };
    }
  }

  res.json({
    user_id: user.id,
    has_accounts: hasToken,
    accounts_count: Array.isArray(accounts) ? accounts.length : 0,
    token_valid: tokenValid,
    pinterest_error: pinterestError,
    profile_error: accError
  });
});

// In-memory store for latest boards (for demo; use a DB for production)
let latestBoards = {};

// Endpoint for Make.com to POST boards
app.post('/api/pinterest-boards-result', express.json(), (req, res) => {
  latestBoards['default'] = req.body.boards || req.body; // Accept either { boards: [...] } or just an array
  res.json({ status: 'ok' });
});

// Endpoint for frontend to GET boards
app.get('/api/pinterest-boards', (req, res) => {
  res.json({ boards: latestBoards['default'] || [] });
});

app.get('/api/pinterest/boards', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const accountId = extractAccountId(req);
  const accessToken = await getPinterestAccessTokenForUser(user.id, accountId);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user.' });
  }

  // Fetch boards from Pinterest API
  const pinterestRes = await fetch('https://api.pinterest.com/v5/boards', {
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  });
  const boards = await pinterestRes.json();
  res.json({ boards: boards.items || boards.data || [] });
});


// Test endpoint
app.get('/api/test', (req, res) => {
  res.json({ message: 'Backend is working' });
});
// WordPress API endpoints
app.post('/api/wordpress/test-connection', async (req, res) => {
  const { siteUrl, username, appPassword } = req.body;
  
  if (!siteUrl || !username || !appPassword) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    // Clean up the site URL
    const cleanSiteUrl = siteUrl.replace(/\/$/, ''); // Remove trailing slash
    
    console.log('Testing connection to:', cleanSiteUrl);
    console.log('Username:', username);
    
    const auth = Buffer.from(`${username}:${appPassword}`).toString('base64');
    const response = await fetch(`${cleanSiteUrl}/wp-json/wp/v2/posts?per_page=1&status=publish`, {
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (response.ok) {
      const posts = await response.json();
      console.log('Connection test - posts found:', posts.length);
      res.json({ 
        success: true, 
        message: 'Connection successful',
        postsFound: posts.length
      });
    } else {
      const errorData = await response.json().catch(() => ({}));
      console.log('Connection test failed:', errorData);
      res.status(400).json({ 
        error: 'Invalid credentials or site URL',
        details: errorData
      });
    }
  } catch (error) {
    console.error('WordPress connection error:', error);
    res.status(500).json({ error: 'Connection failed. Please check your site URL and credentials.' });
  }
});

app.post('/api/wordpress/fetch-posts', async (req, res) => {
  const { siteUrl, username, appPassword } = req.body;
  
  if (!siteUrl || !username || !appPassword) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    const cleanSiteUrl = siteUrl.replace(/\/$/, '');
    const auth = Buffer.from(`${username}:${appPassword}`).toString('base64');
    
    console.log('Fetching posts from:', `${cleanSiteUrl}/wp-json/wp/v2/posts`);
    console.log('Using auth for user:', username);
    
    // First, let's test the basic posts endpoint
    const testResponse = await fetch(`${cleanSiteUrl}/wp-json/wp/v2/posts?per_page=1`, {
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      }
    });
    
    console.log('Test response status:', testResponse.status);
    console.log('Test response headers:', Object.fromEntries(testResponse.headers.entries()));
    
    if (!testResponse.ok) {
      const errorText = await testResponse.text();
      console.log('Test response error:', errorText);
      return res.status(400).json({ 
        error: 'Failed to fetch posts',
        details: { status: testResponse.status, body: errorText }
      });
    }
    
    // Now fetch posts with more details - try without status filter first
    const response = await fetch(`${cleanSiteUrl}/wp-json/wp/v2/posts?per_page=50&_embed`, {
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      return res.status(400).json({ 
        error: 'Failed to fetch posts',
        details: errorData
      });
    }
    
    const posts = await response.json();
    
    console.log('Raw posts response:', JSON.stringify(posts, null, 2));
    console.log('Number of posts found:', posts.length);
    console.log('Posts structure:', posts.length > 0 ? Object.keys(posts[0]) : 'No posts');
    
    // Filter to only published posts if we got any posts
    const publishedPosts = posts.filter(post => post.status === 'publish');
    console.log('Published posts found:', publishedPosts.length);
    
    // Transform WordPress posts to our format - use filtered posts
    const transformedPosts = publishedPosts.map(post => {
      // Extract featured image URL if available
      let featuredImageUrl = '';
      if (post._embedded && post._embedded['wp:featuredmedia'] && post._embedded['wp:featuredmedia'][0]) {
        featuredImageUrl = post._embedded['wp:featuredmedia'][0].source_url;
      }
      
      // Clean up content by removing HTML tags
      const cleanContent = post.content.rendered
        .replace(/<[^>]*>/g, '') // Remove HTML tags
        .replace(/\s+/g, ' ') // Replace multiple spaces with single space
        .trim();
      
      const cleanExcerpt = post.excerpt.rendered
        .replace(/<[^>]*>/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      
      return {
        id: post.id,
        title: post.title.rendered,
        content: cleanContent,
        excerpt: cleanExcerpt,
        featured_image_url: featuredImageUrl,
        link: post.link,
        date: post.date,
        author: post.author,
        categories: post.categories.join(', ')
      };
    });
    
    res.json({ posts: transformedPosts });
  } catch (error) {
    console.error('WordPress fetch posts error:', error);
    res.status(500).json({ error: 'Failed to fetch posts' });
  }
});

// Permanently delete a cancelled or posted pin (for cleanup)
app.delete('/api/pinterest/scheduled-pins/:id/permanent', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { id } = req.params;

  try {
    // Check if pin exists and belongs to user
    const { data: existingPin, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('*')
      .eq('id', id)
      .eq('user_id', user.id)
      .single();

    if (fetchError || !existingPin) {
      return res.status(404).json({ error: 'Scheduled pin not found' });
    }

    // Only allow permanent deletion of cancelled, posted, or generated pins
    if (!['cancelled', 'posted', 'generated'].includes(existingPin.status)) {
      return res.status(400).json({ error: 'Can only permanently delete cancelled, posted, or generated pins' });
    }

    // Soft delete the pin (preserve for analytics)
    const { error: deleteError } = await supabaseAdmin
      .from('scheduled_pins')
      .update({ 
        deleted_at: new Date().toISOString(),
        deleted_by: user.id 
      })
      .eq('id', id)
      .eq('user_id', user.id);

    if (deleteError) {
      console.error('Error permanently deleting scheduled pin:', deleteError);
      return res.status(500).json({ error: 'Failed to permanently delete scheduled pin' });
    }

    return res.json({
      success: true,
      message: 'Scheduled pin permanently deleted'
    });

  } catch (error) {
    console.error('Error permanently deleting scheduled pin:', error);
    return res.status(500).json({ 
      error: { 
        message: `Failed to permanently delete scheduled pin: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Fetch Pinterest analytics for a specific pin
app.get('/api/pinterest/pin-analytics/:pinId', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { pinId } = req.params;
  const { account_id } = req.query;

  try {
    const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
    if (!accessToken) {
      return res.status(400).json({ error: 'No Pinterest access token found' });
    }

    // Fetch analytics from Pinterest API with required date parameters
    const endDate = new Date().toISOString().split('T')[0]; // Today
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 30); // 30 days ago
    const startDateStr = startDate.toISOString().split('T')[0];
    
    const analyticsUrl = `https://api.pinterest.com/v5/pins/${pinId}/analytics?start_date=${startDateStr}&end_date=${endDate}&metric_types=IMPRESSION,OUTBOUND_CLICK,SAVE,PIN_CLICK,CLOSEUP`;
    
    const analyticsResponse = await fetch(analyticsUrl, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      }
    });

    if (!analyticsResponse.ok) {
      const errorData = await analyticsResponse.json();
      console.error('Pinterest Analytics API error:', errorData);
      return res.status(400).json({ 
        error: 'Failed to fetch Pinterest analytics',
        details: errorData
      });
    }

    const analyticsData = await analyticsResponse.json();
    
    // Pinterest API with date range returns metrics in this format:
    // {
    //   "all_time": {
    //     "IMPRESSION": 1234,
    //     "OUTBOUND_CLICK": 56,
    //     "SAVE": 78,
    //     "PIN_CLICK": 90,
    //     "CLOSEUP": 45
    //   }
    // }
    // OR for date ranges:
    // {
    //   "daily_metrics": [...],
    //   "summary": {
    //     "IMPRESSION": 1234,
    //     ...
    //   }
    // }

    const metrics = analyticsData.all_time || analyticsData.summary || analyticsData;
    const impressions = metrics.IMPRESSION || 0;
    const outboundClicks = metrics.OUTBOUND_CLICK || 0;
    const saves = metrics.SAVE || 0;
    const pinClicks = metrics.PIN_CLICK || 0;
    const closeupViews = metrics.CLOSEUP || 0;

    // Calculate engagement metrics
    const engagementRate = impressions > 0 ? ((saves + pinClicks) / impressions) * 100 : 0;
    const clickThroughRate = impressions > 0 ? (outboundClicks / impressions) * 100 : 0;
    const saveRate = impressions > 0 ? (saves / impressions) * 100 : 0;

    const processedMetrics = {
      impressions,
      outbound_clicks: outboundClicks,
      saves,
      pin_clicks: pinClicks,
      closeup_views: closeupViews,
      engagement_rate: Math.round(engagementRate * 100) / 100,
      click_through_rate: Math.round(clickThroughRate * 100) / 100,
      save_rate: Math.round(saveRate * 100) / 100,
      last_updated: new Date().toISOString()
    };

    return res.json({
      success: true,
      pin_id: pinId,
      metrics: processedMetrics,
      raw_data: analyticsData
    });

  } catch (error) {
    console.error('Error fetching Pinterest analytics:', error);
    return res.status(500).json({ 
      error: 'Failed to fetch Pinterest analytics',
      details: error.message 
    });
  }
});

// Sync Pinterest analytics for all user's posted pins
app.post('/api/pinterest/sync-analytics', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { account_id, force_sync = false } = req.body;
  
  console.log(`📊 Manual sync requested by user ${user.id} with force_sync: ${force_sync}`);

  try {
    const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
    if (!accessToken) {
      return res.status(400).json({ error: 'No Pinterest access token found' });
    }

    // Get posted pins with pagination when force_sync is requested
    let postedPins = [];
    let userImagePins = [];
    let fetchError = null;
    let userImagesError = null;

    if (force_sync) {
      const pageSize = 200; // fetch in batches to avoid memory spikes
      let from = 0;
      while (true) {
        let scheduledQuery = supabaseAdmin
          .from('scheduled_pins')
          .select('id, pinterest_pin_id, metrics_last_updated')
          .eq('user_id', user.id)
          .eq('status', 'posted')
          .not('pinterest_pin_id', 'is', null)
          .range(from, from + pageSize - 1);
        if (account_id) scheduledQuery = scheduledQuery.eq('pinterest_account_id', account_id);
        const { data, error } = await scheduledQuery;
        if (error) { fetchError = error; break; }
        if (!data || data.length === 0) break;
        postedPins = postedPins.concat(data);
        from += pageSize;
      }

      // user_images are not tied to an account; only include when syncing all accounts
      if (!account_id) {
        from = 0;
        while (true) {
          const { data, error } = await supabaseAdmin
            .from('user_images')
            .select('id, pinterest_pin_id, metrics_last_updated')
            .eq('user_id', user.id)
            .eq('pinterest_uploaded', true)
            .not('pinterest_pin_id', 'is', null)
            .range(from, from + pageSize - 1);
          if (error) { userImagesError = error; break; }
          if (!data || data.length === 0) break;
          userImagePins = userImagePins.concat(data);
          from += pageSize;
        }
      }
    } else {
      // Non-force path keeps a conservative limit to respect rate limits
      let postedQuery = supabaseAdmin
        .from('scheduled_pins')
        .select('id, pinterest_pin_id, metrics_last_updated')
        .eq('user_id', user.id)
        .eq('status', 'posted')
        .not('pinterest_pin_id', 'is', null)
        .limit(50);
      if (account_id) postedQuery = postedQuery.eq('pinterest_account_id', account_id);
      const postedResp = await postedQuery;
      postedPins = postedResp.data || [];
      fetchError = postedResp.error;

      if (!account_id) {
        const userImgResp = await supabaseAdmin
          .from('user_images')
          .select('id, pinterest_pin_id, metrics_last_updated')
          .eq('user_id', user.id)
          .eq('pinterest_uploaded', true)
          .not('pinterest_pin_id', 'is', null)
          .limit(50);
        userImagePins = userImgResp.data || [];
        userImagesError = userImgResp.error;
      }
    }

    console.log(`📊 Found ${postedPins?.length || 0} pins in scheduled_pins table`);
    console.log(`📊 Found ${userImagePins?.length || 0} pins in user_images table`);

    if (fetchError) {
      console.error('Error fetching posted pins:', fetchError);
      return res.status(500).json({ error: 'Failed to fetch posted pins' });
    }

    if (userImagesError) {
      console.error('Error fetching user image pins:', userImagesError);
    }

    // Combine both sources and deduplicate by pinterest_pin_id
    const allPins = [];
    const pinIdSet = new Set();

    // Add scheduled pins
    if (postedPins) {
      postedPins.forEach(pin => {
        if (!pinIdSet.has(pin.pinterest_pin_id)) {
          pinIdSet.add(pin.pinterest_pin_id);
          allPins.push({ ...pin, source: 'scheduled_pins' });
        }
      });
    }

    // Add user image pins (if not already added)
    if (userImagePins) {
      userImagePins.forEach(pin => {
        if (!pinIdSet.has(pin.pinterest_pin_id)) {
          pinIdSet.add(pin.pinterest_pin_id);
          allPins.push({ ...pin, source: 'user_images' });
        }
      });
    }

    console.log(`📊 Total unique pins to sync: ${allPins.length}`);
    console.log(`📊 Pinterest Pin IDs: ${Array.from(pinIdSet).join(', ')}`);

    if (allPins.length === 0) {
      return res.json({ 
        success: true, 
        message: 'No posted pins found to sync analytics for',
        synced_count: 0,
        total_pins: 0
      });
    }

    let syncedCount = 0;
    const errors = [];

    // Process pins in batches to respect rate limits
    for (const pin of allPins) {
      try {
        // Skip if metrics were updated recently (within last 24 hours) unless force sync
        if (!force_sync && pin.metrics_last_updated) {
          const lastUpdate = new Date(pin.metrics_last_updated);
          const twentyFourHoursAgo = new Date();
          twentyFourHoursAgo.setHours(twentyFourHoursAgo.getHours() - 24);
          
          if (lastUpdate > twentyFourHoursAgo) {
            console.log(`Skipping pin ${pin.pinterest_pin_id} - updated recently (use force sync to override)`);
            continue;
          }
        }
        
        if (force_sync) {
          console.log(`🔄 Force syncing pin ${pin.pinterest_pin_id}`);
        }

        // Fetch analytics for this pin with required date parameters
        // Pinterest API only allows data from the last 90 days
        const endDate = new Date().toISOString().split('T')[0]; // Today
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - 89); // 89 days ago (to be safe)
        const startDateStr = startDate.toISOString().split('T')[0];
        
        const analyticsUrl = `https://api.pinterest.com/v5/pins/${pin.pinterest_pin_id}/analytics?start_date=${startDateStr}&end_date=${endDate}&metric_types=IMPRESSION,OUTBOUND_CLICK,SAVE,PIN_CLICK,CLOSEUP`;
        
        console.log(`🔗 Analytics URL for pin ${pin.pinterest_pin_id}: ${analyticsUrl}`);
        
        const analyticsResponse = await fetch(analyticsUrl, {
          method: 'GET',
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
          }
        });

        if (analyticsResponse.ok) {
          const analyticsData = await analyticsResponse.json();
          console.log(`📊 Raw Pinterest API response for pin ${pin.pinterest_pin_id}:`, JSON.stringify(analyticsData, null, 2));
          
          // Handle different Pinterest API response formats
          let metrics = {};
          
          // Try different response structures Pinterest might use
          if (analyticsData.all_time) {
            metrics = analyticsData.all_time;
            console.log(`📊 Using all_time structure for pin ${pin.pinterest_pin_id}:`, metrics);
          } else if (analyticsData.summary) {
            metrics = analyticsData.summary;
            console.log(`📊 Using summary structure for pin ${pin.pinterest_pin_id}:`, metrics);
          } else if (analyticsData.all && analyticsData.all.summary_metrics) {
            metrics = analyticsData.all.summary_metrics;
            console.log(`📊 Using all.summary_metrics structure for pin ${pin.pinterest_pin_id}:`, metrics);
          } else if (analyticsData.all && analyticsData.all.daily_metrics) {
            // Try daily metrics if summary_metrics is empty
            const dailyMetrics = analyticsData.all.daily_metrics;
            if (Array.isArray(dailyMetrics) && dailyMetrics.length > 0) {
              // Sum up daily metrics
              metrics = dailyMetrics.reduce((acc, day) => {
                if (day.data_status === 'READY') {
                  acc.IMPRESSION = (acc.IMPRESSION || 0) + (day.metrics?.IMPRESSION || 0);
                  acc.SAVE = (acc.SAVE || 0) + (day.metrics?.SAVE || 0);
                  acc.PIN_CLICK = (acc.PIN_CLICK || 0) + (day.metrics?.PIN_CLICK || 0);
                  acc.OUTBOUND_CLICK = (acc.OUTBOUND_CLICK || 0) + (day.metrics?.OUTBOUND_CLICK || 0);
                  acc.CLOSEUP = (acc.CLOSEUP || 0) + (day.metrics?.CLOSEUP || 0);
                }
                return acc;
              }, {});
              console.log(`📊 Using summed daily_metrics for pin ${pin.pinterest_pin_id}:`, metrics);
            }
          } else if (analyticsData.all) {
            // Try the all object directly
            metrics = analyticsData.all;
            console.log(`📊 Using all structure directly for pin ${pin.pinterest_pin_id}:`, metrics);
          } else if (analyticsData.summary_metrics) {
            metrics = analyticsData.summary_metrics;
            console.log(`📊 Using summary_metrics structure for pin ${pin.pinterest_pin_id}:`, metrics);
          } else {
            metrics = analyticsData;
            console.log(`📊 Using root structure for pin ${pin.pinterest_pin_id}:`, metrics);
          }
          
          const impressions = metrics.IMPRESSION || 0;
          const outboundClicks = metrics.OUTBOUND_CLICK || 0;
          const saves = metrics.SAVE || 0;
          const pinClicks = metrics.PIN_CLICK || 0;
          const closeupViews = metrics.CLOSEUP || 0;
          
          console.log(`📊 Extracted metrics for pin ${pin.pinterest_pin_id}:`, {
            impressions, outboundClicks, saves, pinClicks, closeupViews, rawMetrics: metrics
          });

          // Calculate engagement metrics
          const engagementRate = impressions > 0 ? ((saves + pinClicks) / impressions) * 100 : 0;
          const clickThroughRate = impressions > 0 ? (outboundClicks / impressions) * 100 : 0;
          const saveRate = impressions > 0 ? (saves / impressions) * 100 : 0;

          // Update scheduled_pins table
          const updateData = {
            impressions,
            outbound_clicks: outboundClicks,
            saves,
            pin_clicks: pinClicks,
            closeup_views: closeupViews,
            engagement_rate: Math.round(engagementRate * 100) / 100,
            click_through_rate: Math.round(clickThroughRate * 100) / 100,
            save_rate: Math.round(saveRate * 100) / 100,
            metrics_last_updated: new Date().toISOString()
          };
          
          console.log(`📊 Updating tables for pin ${pin.pinterest_pin_id} (source: ${pin.source}) with data:`, updateData);
          
          // Update scheduled_pins table if pin came from there
          if (pin.source === 'scheduled_pins') {
            const { error: scheduledPinsError } = await supabaseAdmin
              .from('scheduled_pins')
              .update(updateData)
              .eq('id', pin.id);
              
            if (scheduledPinsError) {
              console.error(`❌ Error updating scheduled_pins for pin ${pin.pinterest_pin_id}:`, scheduledPinsError);
            } else {
              console.log(`✅ Successfully updated scheduled_pins for pin ${pin.pinterest_pin_id}`);
            }
          }

          // Always try to update user_images table (for both scheduled and direct uploads)
          console.log(`📊 Updating user_images for pin ${pin.pinterest_pin_id}`);
          
          const { error: userImagesError } = await supabaseAdmin
            .from('user_images')
            .update({
              impressions,
              outbound_clicks: outboundClicks,
              saves,
              pin_clicks: pinClicks,
              closeup_views: closeupViews,
              engagement_rate: Math.round(engagementRate * 100) / 100,
              click_through_rate: Math.round(clickThroughRate * 100) / 100,
              save_rate: Math.round(saveRate * 100) / 100,
              metrics_last_updated: new Date().toISOString()
            })
            .eq('pinterest_pin_id', pin.pinterest_pin_id)
            .eq('user_id', user.id);
            
          if (userImagesError) {
            console.error(`❌ Error updating user_images for pin ${pin.pinterest_pin_id}:`, userImagesError);
          } else {
            console.log(`✅ Successfully updated user_images for pin ${pin.pinterest_pin_id}`);
          }
          
          // If pin came from user_images but not scheduled_pins, also try to update scheduled_pins by pinterest_pin_id
          if (pin.source === 'user_images') {
            const { error: scheduledPinsError } = await supabaseAdmin
              .from('scheduled_pins')
              .update(updateData)
              .eq('pinterest_pin_id', pin.pinterest_pin_id)
              .eq('user_id', user.id);
              
            if (!scheduledPinsError) {
              console.log(`✅ Also updated scheduled_pins for pin ${pin.pinterest_pin_id} (matched by pinterest_pin_id)`);
            }
          }

          syncedCount++;
          console.log(`✅ Synced analytics for pin ${pin.pinterest_pin_id}`);
          
        } else {
          const errorData = await analyticsResponse.json().catch(() => ({}));
          const errorMsg = errorData.message || 'API error';
          errors.push(`Pin ${pin.pinterest_pin_id}: ${errorMsg}`);
          console.error(`❌ Failed to fetch analytics for pin ${pin.pinterest_pin_id}:`, errorData);
          
          // If it's a date range error, log helpful info
          if (errorMsg.includes('90 days')) {
            console.log(`📅 Note: Pinterest API only allows data from the last 90 days for pin ${pin.pinterest_pin_id}`);
          }
        }

        // Add delay between requests to respect rate limits
        await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second delay

      } catch (error) {
        errors.push(`Pin ${pin.pinterest_pin_id}: ${error.message}`);
        console.error(`❌ Error processing pin ${pin.pinterest_pin_id}:`, error);
      }
    }

    return res.json({
      success: true,
      message: `Analytics synced for ${syncedCount} pins`,
      synced_count: syncedCount,
      total_pins: allPins.length,
      scheduled_pins_found: postedPins?.length || 0,
      user_images_found: userImagePins?.length || 0,
      errors: errors.length > 0 ? errors : undefined
    });

  } catch (error) {
    console.error('Error syncing Pinterest analytics:', error);
    return res.status(500).json({ 
      error: 'Failed to sync Pinterest analytics',
      details: error.message 
    });
  }
});

// Test Pinterest Analytics API with any Pin ID (for debugging)
app.post('/api/pinterest/test-analytics/:pinId', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { pinId } = req.params;

  try {
    const accessToken = await getPinterestAccessTokenForUser(user.id, null);
    if (!accessToken) {
      return res.status(400).json({ error: 'No Pinterest access token found' });
    }

    console.log(`🧪 Testing analytics for Pinterest Pin ID: ${pinId}`);

    // Test multiple date ranges within Pinterest's 90-day limit
    const testRanges = [
      { name: '7 days', days: 7 },
      { name: '30 days', days: 30 },
      { name: '89 days', days: 89 } // Max allowed by Pinterest API
    ];

    const results = {};

    for (const range of testRanges) {
      const endDate = new Date().toISOString().split('T')[0];
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - range.days);
      const startDateStr = startDate.toISOString().split('T')[0];
      
      const analyticsUrl = `https://api.pinterest.com/v5/pins/${pinId}/analytics?start_date=${startDateStr}&end_date=${endDate}&metric_types=IMPRESSION,OUTBOUND_CLICK,SAVE,PIN_CLICK,CLOSEUP`;
      
      console.log(`🔗 Testing ${range.name}: ${analyticsUrl}`);
      console.log(`📅 Date range: ${startDateStr} to ${endDate}`);
      
      const analyticsResponse = await fetch(analyticsUrl, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        }
      });

      if (analyticsResponse.ok) {
        const analyticsData = await analyticsResponse.json();
        
        // Handle different Pinterest API response formats
        let metrics = {};
        console.log(`📊 Raw Pinterest API response for ${range.name}:`, JSON.stringify(analyticsData, null, 2));
        
        // Try different response structures Pinterest might use
        if (analyticsData.all_time) {
          metrics = analyticsData.all_time;
          console.log(`📊 Using all_time structure:`, metrics);
        } else if (analyticsData.summary) {
          metrics = analyticsData.summary;
          console.log(`📊 Using summary structure:`, metrics);
        } else if (analyticsData.all && analyticsData.all.summary_metrics) {
          metrics = analyticsData.all.summary_metrics;
          console.log(`📊 Using all.summary_metrics structure:`, metrics);
        } else if (analyticsData.all && analyticsData.all.daily_metrics) {
          // Try daily metrics if summary_metrics is empty
          const dailyMetrics = analyticsData.all.daily_metrics;
          if (Array.isArray(dailyMetrics) && dailyMetrics.length > 0) {
            // Sum up daily metrics
            metrics = dailyMetrics.reduce((acc, day) => {
              if (day.data_status === 'READY') {
                acc.IMPRESSION = (acc.IMPRESSION || 0) + (day.metrics?.IMPRESSION || 0);
                acc.SAVE = (acc.SAVE || 0) + (day.metrics?.SAVE || 0);
                acc.PIN_CLICK = (acc.PIN_CLICK || 0) + (day.metrics?.PIN_CLICK || 0);
                acc.OUTBOUND_CLICK = (acc.OUTBOUND_CLICK || 0) + (day.metrics?.OUTBOUND_CLICK || 0);
                acc.CLOSEUP = (acc.CLOSEUP || 0) + (day.metrics?.CLOSEUP || 0);
              }
              return acc;
            }, {});
            console.log(`📊 Using summed daily_metrics:`, metrics);
          }
        } else if (analyticsData.all) {
          // Try the all object directly
          metrics = analyticsData.all;
          console.log(`📊 Using all structure directly:`, metrics);
        } else if (analyticsData.summary_metrics) {
          metrics = analyticsData.summary_metrics;
          console.log(`📊 Using summary_metrics structure:`, metrics);
        } else {
          metrics = analyticsData;
          console.log(`📊 Using root structure:`, metrics);
        }

        const impressions = metrics.IMPRESSION || 0;
        const saves = metrics.SAVE || 0;
        const pinClicks = metrics.PIN_CLICK || 0;
        const outboundClicks = metrics.OUTBOUND_CLICK || 0;
        
        console.log(`📊 Extracted metrics for ${range.name}:`, {
          impressions, saves, pinClicks, outboundClicks, rawMetrics: metrics
        });

        results[range.name] = {
          impressions,
          saves,
          pin_clicks: pinClicks,
          outbound_clicks: outboundClicks,
          raw_response: analyticsData
        };
      } else {
        const errorText = await analyticsResponse.text();
        results[range.name] = {
          error: `API Error: ${analyticsResponse.status}`,
          details: errorText
        };
      }

      // Small delay between requests
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    return res.json({
      success: true,
      pin_id: pinId,
      pinterest_url: `https://pinterest.com/pin/${pinId}`,
      test_results: results
    });

  } catch (error) {
    console.error('Error testing Pinterest analytics:', error);
    return res.status(500).json({ 
      error: 'Failed to test Pinterest analytics',
      details: error.message 
    });
  }
});

// Reschedule spam-blocked pins with better spacing
app.post('/api/pinterest/reschedule-spam-blocked', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  try {
    await rescheduleSpamBlockedPins(user.id);
    
    return res.json({
      success: true,
      message: 'Spam-blocked pins have been rescheduled with better spacing'
    });
  } catch (error) {
    console.error('Error rescheduling spam-blocked pins:', error);
    return res.status(500).json({ 
      error: 'Failed to reschedule spam-blocked pins',
      details: error.message 
    });
  }
});

// Manual trigger endpoint for scheduled pins (for testing/debugging)
app.post('/api/pinterest/process-scheduled', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  console.log(`🔧 Manual trigger for scheduled pins by user: ${user.id}`);
  
  try {
    await processScheduledPins();
    res.json({ 
      success: true, 
      message: 'Scheduled pin processing triggered successfully' 
    });
  } catch (error) {
    console.error('Error in manual scheduled pin processing:', error);
    res.status(500).json({ 
      error: 'Failed to process scheduled pins',
      details: error.message 
    });
  }
});

// Graceful shutdown handling
process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received, shutting down gracefully');
  stopScheduler();
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('🛑 SIGINT received, shutting down gracefully');
  stopScheduler();
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`🚀 Backend listening on port ${PORT}`);
  
  // Start the scheduled pin processor
  startScheduler();
  startAnalyticsSync();
}); 