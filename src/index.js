import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';
import net from 'node:net';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import JSZip from 'jszip';
import sharp from 'sharp';
import {
  enrichContentProfile,
  planStrategies,
  normalizeWinnerContext,
  generateStrategicPinMetadata,
  extractArticleKeyIdeas,
  pickAngle,
  checkDiversity,
  rankPins,
  getStrategyReason,
  PIN_COPY_ANTI_CLICHE_INSTRUCTION,
  usesProductAffiliatePinMix,
} from './strategicPin.js';
import { compositeUserPhotoPin, isAllowedUserImageUrl } from './urltopinComposite.js';
import { renderTextBasedPin, normalizeTextBasedInput } from './urltopinTextBased.js';
import { initTrendsEngine, getTrendsCatalog, getTrendBySlug, startTrendsScheduler } from './trendsEngine.js';
import { analyzeWinningProduct, normalizeAmazonProduct } from './winningProductFinder.js';
import {
  sendPaymentFailedEmail,
  sendUpgradeNudgeEmail,
  nextPlanFor,
  sendWelcomeEmail,
  sendFirstPinEmail,
  isEmailEnabled,
} from './email.js';
dotenv.config();

const supabaseAdmin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

async function getTrendsPinterestAccessToken() {
  const envTok = String(process.env.PINTEREST_TRENDS_ACCESS_TOKEN || '').trim();
  if (envTok) return envTok;
  const { data: accounts, error } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, access_token, refresh_token, token_expires_at, updated_at')
    .not('access_token', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(10);
  if (error || !accounts?.length) return null;
  for (const account of accounts) {
    const { accessToken } = await ensureValidPinterestAccessToken(account);
    if (accessToken) return accessToken;
  }
  return null;
}

initTrendsEngine(openai, { getPinterestAccessToken: getTrendsPinterestAccessToken });

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

const DEFAULT_AFFILIATE_COMMISSION_RATE = Math.min(
  1,
  Math.max(0, Number(process.env.AFFILIATE_COMMISSION_RATE || '0.30') || 0.3)
);

/** Default recurring commission window per referred customer (months). 0 = first payment only. */
const DEFAULT_AFFILIATE_RECURRING_MONTHS = Math.max(
  0,
  Math.floor(Number(process.env.AFFILIATE_RECURRING_MONTHS || '12') || 12)
);

function getRecurringMonthsForAffiliate(affiliate) {
  const raw = affiliate?.recurring_months;
  if (raw !== null && raw !== undefined && raw !== '') {
    const n = Number(raw);
    if (Number.isFinite(n)) return Math.max(0, Math.floor(n));
  }
  return DEFAULT_AFFILIATE_RECURRING_MONTHS;
}

function parseAffiliateAdminEmails() {
  return String(process.env.AFFILIATE_ADMIN_EMAILS || process.env.ADMIN_EMAIL || '')
    .split(',')
    .map((e) => normalizeEmail(e))
    .filter(Boolean);
}

function isAffiliateAdminUser(user) {
  const email = normalizeEmail(user?.email);
  if (!email) return false;
  const admins = parseAffiliateAdminEmails();
  return admins.length > 0 && admins.includes(email);
}

function requireAffiliateAdmin(req, res, next) {
  if (!isAffiliateAdminUser(req.user)) {
    return res.status(403).json({ error: 'Admin access required' });
  }
  return next();
}

function normalizeAffiliateSlug(raw) {
  const s = String(raw || '').trim().toLowerCase();
  if (!s || !/^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$/.test(s)) return null;
  return s;
}

async function getActiveAffiliateBySlug(slug) {
  const s = normalizeAffiliateSlug(slug);
  if (!s) return null;
  try {
    const { data, error } = await supabaseAdmin
      .from('affiliates')
      .select('id, slug, email, user_id, commission_rate, status, payout_email, display_name, recurring_months')
      .eq('slug', s)
      .eq('status', 'active')
      .maybeSingle();
    if (error) {
      console.warn('getActiveAffiliateBySlug error:', error.message || error);
      return null;
    }
    return data || null;
  } catch (e) {
    console.warn('getActiveAffiliateBySlug unexpected:', e?.message || e);
    return null;
  }
}

async function resolveAffiliateSlugForUser(userId) {
  const uid = String(userId || '').trim();
  if (!uid) return null;
  try {
    const { data } = await supabaseAdmin
      .from('profiles')
      .select('referred_by_affiliate_slug')
      .eq('id', uid)
      .maybeSingle();
    return normalizeAffiliateSlug(data?.referred_by_affiliate_slug);
  } catch {
    return null;
  }
}

async function attachAffiliateReferralToUser(userId, slug) {
  const uid = String(userId || '').trim();
  const affiliate = await getActiveAffiliateBySlug(slug);
  if (!uid || !affiliate) return { ok: false, reason: 'invalid_slug' };
  if (affiliate.user_id && String(affiliate.user_id) === uid) {
    return { ok: false, reason: 'self_referral' };
  }
  try {
    const { data: profile } = await supabaseAdmin
      .from('profiles')
      .select('referred_by_affiliate_slug')
      .eq('id', uid)
      .maybeSingle();
    if (profile?.referred_by_affiliate_slug) {
      return { ok: true, alreadySet: true, slug: profile.referred_by_affiliate_slug };
    }
    const { error } = await supabaseAdmin
      .from('profiles')
      .update({
        referred_by_affiliate_slug: affiliate.slug,
        updated_at: new Date().toISOString(),
      })
      .eq('id', uid);
    if (error) return { ok: false, reason: 'update_failed', details: error.message };
    return { ok: true, slug: affiliate.slug };
  } catch (e) {
    return { ok: false, reason: 'unexpected', details: e?.message || String(e) };
  }
}

function extractDodoPaymentAmountCents(dataObj) {
  const o = dataObj || {};
  const candidates = [
    o.amount_cents,
    o.total_amount_cents,
    o.amount,
    o.total_amount,
    o.payment_amount,
    o.payment_amount_cents,
  ];
  for (const c of candidates) {
    const n = Number(c);
    if (!Number.isFinite(n) || n <= 0) continue;
    if (n < 1000 && !String(c).includes('cent')) return Math.round(n * 100);
    return Math.round(n);
  }
  return 0;
}

function extractDodoPaymentId(dataObj) {
  const o = dataObj || {};
  return String(o.payment_id || o.id || o.paymentId || '').trim() || null;
}

async function affiliateCommissionWindowAllows(affiliate, referredUserId) {
  const months = getRecurringMonthsForAffiliate(affiliate);
  const { data: rows } = await supabaseAdmin
    .from('affiliate_commissions')
    .select('id, created_at')
    .eq('affiliate_id', affiliate.id)
    .eq('referred_user_id', referredUserId)
    .order('created_at', { ascending: true })
    .limit(1);
  if (!rows?.length) return { ok: true, recurringMonths: months };
  if (months <= 0) return { ok: false, reason: 'first_payment_only' };
  const firstAt = new Date(rows[0].created_at);
  if (!Number.isFinite(firstAt.getTime())) return { ok: true, recurringMonths: months };
  const windowEnd = new Date(firstAt);
  windowEnd.setMonth(windowEnd.getMonth() + months);
  if (Date.now() > windowEnd.getTime()) {
    return { ok: false, reason: 'recurring_window_expired', recurringMonths: months };
  }
  return { ok: true, recurringMonths: months };
}

async function resolveBillingIntervalForAffiliateCommission(userId, opts = {}) {
  let interval = normalizeBillingInterval(opts.billingInterval || '');
  if (interval) return interval;
  const dodoSubId = String(opts.dodoSubscriptionId || '').trim();
  if (dodoSubId) {
    const local = await lookupLocalSubscriptionByDodoSubscriptionId(dodoSubId);
    interval = normalizeBillingInterval(local?.billing_interval || '');
    if (interval) return interval;
    if (DODO_API_KEY) {
      const dodoGet = await fetchDodoSubscriptionJson(dodoSubId);
      const productId = String(dodoGet.json?.product_id || dodoGet.json?.productId || '').trim();
      if (productId) {
        interval = inferBillingIntervalFromDodoProductId(productId);
        if (interval) return interval;
      }
    }
  }
  const uid = String(userId || '').trim();
  if (uid) {
    try {
      const { data: sub } = await supabaseAdmin
        .from('billing_subscriptions')
        .select('billing_interval')
        .eq('user_id', uid)
        .in('status', ['active', 'past_due'])
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      interval = normalizeBillingInterval(sub?.billing_interval || '');
    } catch {
      /* ignore */
    }
  }
  return interval || '';
}

/**
 * Record affiliate commission on paid subscription (first payment + renewals within window).
 * Commissions apply to monthly billing only (not annual plans).
 */
async function recordAffiliateCommissionOnPaidSubscription(userId, planType, opts = {}) {
  const uid = String(userId || '').trim();
  if (!uid || !planType || planType === 'free') return { ok: false, reason: 'invalid_input' };

  const billingInterval = await resolveBillingIntervalForAffiliateCommission(uid, opts);
  if (billingInterval !== 'month') {
    return { ok: false, reason: 'commission_monthly_plans_only', billingInterval: billingInterval || 'unknown' };
  }

  const slug =
    normalizeAffiliateSlug(opts.referralSlugFromMetadata) || (await resolveAffiliateSlugForUser(uid));
  if (!slug) return { ok: false, reason: 'no_referral' };

  const affiliate = await getActiveAffiliateBySlug(slug);
  if (!affiliate) return { ok: false, reason: 'inactive_affiliate' };
  if (affiliate.user_id && String(affiliate.user_id) === uid) {
    return { ok: false, reason: 'self_referral' };
  }

  const paymentId = opts.paymentId || null;

  try {
    if (paymentId) {
      const { data: dupPay } = await supabaseAdmin
        .from('affiliate_commissions')
        .select('id')
        .eq('payment_id', paymentId)
        .maybeSingle();
      if (dupPay?.id) return { ok: true, duplicate: true };
    }

    const windowCheck = await affiliateCommissionWindowAllows(affiliate, uid);
    if (!windowCheck.ok) return { ok: false, reason: windowCheck.reason };

    const rate = Math.min(
      1,
      Math.max(0, Number(affiliate.commission_rate) || DEFAULT_AFFILIATE_COMMISSION_RATE)
    );
    const amountCents = Math.max(0, Number(opts.amountCents || 0) || 0);
    const commissionCents = amountCents > 0 ? Math.round(amountCents * rate) : 0;

    const { error: insErr } = await supabaseAdmin.from('affiliate_commissions').insert({
      affiliate_id: affiliate.id,
      referred_user_id: uid,
      plan_type: planType,
      payment_id: paymentId,
      dodo_subscription_id: opts.dodoSubscriptionId || null,
      amount_cents: amountCents,
      commission_cents: commissionCents,
      currency: String(opts.currency || 'usd').toLowerCase(),
      status: 'pending',
      commission_kind: opts.commissionKind || 'subscription',
    });
    if (insErr) {
      if (insErr.code === '23505') return { ok: true, duplicate: true };
      console.warn('recordAffiliateCommission insert error:', insErr.message || insErr);
      return { ok: false, reason: 'insert_failed' };
    }
    console.log('affiliate: commission recorded', {
      affiliateSlug: affiliate.slug,
      userId: uid,
      planType,
      amountCents,
      commissionCents,
      recurringMonths: windowCheck.recurringMonths,
    });
    return { ok: true, affiliateSlug: affiliate.slug, commissionCents };
  } catch (e) {
    console.warn('recordAffiliateCommission unexpected:', e?.message || e);
    return { ok: false, reason: 'unexpected' };
  }
}

// --- Plan & usage helpers (pin_usage / metadata_usage) ---

const PLAN_PIN_LIMITS = {
  free: 10,
  starter: 60,
  creator: 150,
  pro: 450,
  agency: 1000,
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

// Free plan: 10 AI pins total for lifetime (not per month).
// We implement this by storing free-plan usage in a single "lifetime" bucket row in pin_usage.
// Keep it a valid YYYY-MM-DD date since pin_usage.year_month is stored as a date-like string.
const FREE_LIFETIME_YEAR_MONTH = '1970-01-01';

function currentYearMonthDate() {
  const now = new Date();
  // Use UTC month start to avoid timezone edge cases with Postgres date
  const monthStartUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  return monthStartUtc.toISOString().slice(0, 10); // 'YYYY-MM-DD'
}

function pinUsageBucketForPlan(planType) {
  const pt = String(planType || 'free').trim().toLowerCase();
  if (pt === 'free') return FREE_LIFETIME_YEAR_MONTH;
  return currentYearMonthDate();
}

async function getActiveSubscriptionForUser(userId) {
  try {
    const { data, error } = await supabaseAdmin
      .from('billing_subscriptions')
      .select(
        'id, plan_type, pins_limit_per_month, status, billing_interval, current_period_start, current_period_end, cancel_at_period_end, cancelled_at, dodo_subscription_id, usage_baseline_pins_used, usage_baseline_user_photo_pins_used'
      )
      .eq('user_id', userId)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1);

    if (error) {
      console.warn('billing_subscriptions fetch error:', error.message || error);
      return null;
    }

    if (!data || data.length === 0) return null;
    const sub = data[0];
    // If the paid period ended, treat as not active (and cleanup profile).
    try {
      if (sub?.current_period_end && new Date(sub.current_period_end).getTime() <= Date.now()) {
        await supabaseAdmin
          .from('billing_subscriptions')
          .update({ status: 'cancelled', updated_at: new Date().toISOString() })
          .eq('id', sub.id)
          .eq('user_id', userId);
        await supabaseAdmin
          .from('profiles')
          .update({ plan_type: 'free', is_pro: false, updated_at: new Date().toISOString() })
          .eq('id', userId);
        return null;
      }
    } catch {
      // ignore cleanup failures; fall through
    }
    return sub;
  } catch (err) {
    console.warn('getActiveSubscriptionForUser error:', err.message || err);
    return null;
  }
}

async function getLatestPastDueSubscriptionForUser(userId) {
  try {
    const { data, error } = await supabaseAdmin
      .from('billing_subscriptions')
      .select(
        'id, plan_type, pins_limit_per_month, status, billing_interval, current_period_start, current_period_end, cancel_at_period_end, cancelled_at, dodo_subscription_id, usage_baseline_pins_used, usage_baseline_user_photo_pins_used'
      )
      .eq('user_id', userId)
      .eq('status', 'past_due')
      .order('created_at', { ascending: false })
      .limit(1);
    if (error) return null;
    if (!data || data.length === 0) return null;
    return data[0];
  } catch {
    return null;
  }
}

function markPendingDodoActivation(userId, planType, sessionId = null) {
  if (!userId || !planType) return;
  pendingDodoActivations.set(String(userId), {
    planType: String(planType),
    billingInterval: 'month',
    sessionId: sessionId ? String(sessionId) : null,
    createdAt: Date.now(),
  });
}

function normalizeBillingInterval(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'year' || v === 'annual' || v === 'annually') return 'year';
  return 'month';
}

function normalizeEmail(raw) {
  const s = String(raw || '').trim().toLowerCase();
  return s || null;
}

function planRank(planType) {
  const p = String(planType || '').trim().toLowerCase();
  if (p === 'starter') return 1;
  if (p === 'creator') return 2;
  if (p === 'pro') return 3;
  if (p === 'agency') return 4;
  return 0; // free/unknown
}

function resolveDodoProductIdForPlan(planType, billingInterval) {
  const pt = String(planType || '').trim();
  const bi = normalizeBillingInterval(billingInterval);
  const productMapMonthly = {
    free: process.env.DODO_PRODUCT_FREE_ID,
    starter: process.env.DODO_PRODUCT_STARTER_ID,
    creator: process.env.DODO_PRODUCT_CREATOR_ID,
    pro: process.env.DODO_PRODUCT_PRO_ID,
    agency: process.env.DODO_PRODUCT_AGENCY_ID,
  };
  const productMapAnnual = {
    free: process.env.DODO_PRODUCT_FREE_ID,
    starter: process.env.DODO_PRODUCT_STARTER_ANNUAL_ID,
    creator: process.env.DODO_PRODUCT_CREATOR_ANNUAL_ID,
    pro: process.env.DODO_PRODUCT_PRO_ANNUAL_ID,
    agency: process.env.DODO_PRODUCT_AGENCY_ANNUAL_ID,
  };
  const map = bi === 'year' ? productMapAnnual : productMapMonthly;
  return map[pt] || null;
}

function inferBillingIntervalFromDodoProductId(productId) {
  const id = String(productId || '').trim();
  if (!id) return 'month';
  const annualIds = new Set(
    [
      process.env.DODO_PRODUCT_STARTER_ANNUAL_ID,
      process.env.DODO_PRODUCT_CREATOR_ANNUAL_ID,
      process.env.DODO_PRODUCT_PRO_ANNUAL_ID,
      process.env.DODO_PRODUCT_AGENCY_ANNUAL_ID,
    ]
      .map((x) => String(x || '').trim())
      .filter(Boolean)
  );
  if (annualIds.has(id)) return 'year';
  return 'month';
}

function markPendingDodoActivationWithInterval(userId, planType, billingInterval, sessionId = null) {
  if (!userId || !planType) return;
  pendingDodoActivations.set(String(userId), {
    planType: String(planType),
    billingInterval: normalizeBillingInterval(billingInterval),
    sessionId: sessionId ? String(sessionId) : null,
    createdAt: Date.now(),
  });
}

function consumePendingDodoActivation(userId, requestedPlanType, requestedInterval) {
  const key = String(userId || '');
  const row = pendingDodoActivations.get(key);
  if (!row) return { ok: false, reason: 'missing_pending_checkout' };
  const maxAgeMs = 24 * 60 * 60 * 1000;
  if (Date.now() - row.createdAt > maxAgeMs) {
    pendingDodoActivations.delete(key);
    return { ok: false, reason: 'pending_checkout_expired' };
  }
  if (String(row.planType) !== String(requestedPlanType)) {
    return { ok: false, reason: 'plan_mismatch_with_pending_checkout', pendingPlanType: row.planType };
  }
  const reqInt = normalizeBillingInterval(requestedInterval);
  const rowInt = normalizeBillingInterval(row.billingInterval);
  if (rowInt !== reqInt) {
    return {
      ok: false,
      reason: 'interval_mismatch_with_pending_checkout',
      pendingInterval: rowInt,
      requestedInterval: reqInt,
    };
  }
  pendingDodoActivations.delete(key);
  return { ok: true, pending: row };
}

async function applyPlanActivationForUser(
  userId,
  planType,
  source = 'unknown',
  opts = null
) {
  if (!planType || !PLAN_PIN_LIMITS[planType]) {
    return { ok: false, error: 'Invalid planType' };
  }

  const now = new Date();
  const billingInterval = normalizeBillingInterval(opts?.billingInterval || opts?.billing_interval || 'month');
  const periodStart = String(opts?.periodStart || now.toISOString());
  const periodEnd = String(opts?.periodEnd || (() => {
    const nextMonth = new Date(now);
    nextMonth.setMonth(nextMonth.getMonth() + 1);
    return nextMonth.toISOString();
  })());
  const pinsLimit = PLAN_PIN_LIMITS[planType];
  const dodoSubscriptionId = opts?.dodoSubscriptionId ? String(opts.dodoSubscriptionId) : null;

  // Safety: upgrades currently create a *new* Dodo subscription. If we don't cancel the old one,
  // the user can be billed twice (monthly→monthly, monthly→annual, annual→annual).
  // Capture the currently-active subscription ids BEFORE we mutate billing_subscriptions.
  let previousActiveDodoSubscriptionIds = [];
  try {
    const { data: activeRows } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('dodo_subscription_id')
      .eq('user_id', userId)
      .eq('status', 'active')
      .limit(10);
    previousActiveDodoSubscriptionIds = (activeRows || [])
      .map((r) => String(r?.dodo_subscription_id || '').trim())
      .filter(Boolean);
  } catch {
    previousActiveDodoSubscriptionIds = [];
  }

  // Baseline current usage so upgrades/renewals don't "steal" allowance from earlier periods.
  const yearMonth = currentYearMonthDate();
  let baselineAi = 0;
  let baselineUserPhoto = 0;
  try {
    const { data: usageRows } = await supabaseAdmin
      .from('pin_usage')
      .select('pins_used, user_photo_pins_used')
      .eq('user_id', userId)
      .eq('year_month', yearMonth)
      .limit(1);
    const row = usageRows && usageRows.length ? usageRows[0] : null;
    baselineAi = Math.max(0, Number(row?.pins_used ?? 0) || 0);
    baselineUserPhoto = Math.max(0, Number(row?.user_photo_pins_used ?? 0) || 0);
  } catch {
    baselineAi = 0;
    baselineUserPhoto = 0;
  }

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
      billing_interval: billingInterval,
      current_period_start: periodStart,
      current_period_end: periodEnd,
      cancel_at_period_end: false,
      cancelled_at: null,
      dodo_subscription_id: dodoSubscriptionId,
      usage_baseline_pins_used: baselineAi,
      usage_baseline_user_photo_pins_used: baselineUserPhoto,
    });

  if (insertError) {
    return { ok: false, error: insertError.message || String(insertError) };
  }

  // If we have a new subscription id, schedule cancellation of any previous active Dodo subscriptions
  // (exclude the new one; keep behavior safe when Dodo id is missing).
  if (DODO_API_KEY && dodoSubscriptionId) {
    const toCancel = [...new Set(previousActiveDodoSubscriptionIds)]
      .filter((id) => id && id !== dodoSubscriptionId);
    for (const oldId of toCancel) {
      try {
        const dodoResp = await fetch(
          `${DODO_BASE_URL}/subscriptions/${encodeURIComponent(oldId)}`,
          {
            method: 'PATCH',
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${DODO_API_KEY}`,
            },
            body: JSON.stringify({ cancel_at_next_billing_date: true }),
          }
        );
        const dodoJson = await dodoResp.json().catch(() => ({}));
        if (!dodoResp.ok) {
          console.warn('applyPlanActivationForUser: Dodo cancel old subscription failed', {
            status: dodoResp.status,
            details: dodoJson,
            oldSubId: oldId,
            userId,
            planType,
            source,
          });
        } else {
          console.log('✅ applyPlanActivationForUser: scheduled cancel in Dodo for old subscription', {
            userId,
            oldSubId: oldId,
            newSubId: dodoSubscriptionId,
            source,
          });
        }
      } catch (e) {
        console.warn('applyPlanActivationForUser: Dodo cancel old subscription error', {
          oldSubId,
          userId,
          planType,
          source,
          error: e?.message || e,
        });
      }
    }
  }

  // Activation can happen before the user ever signs in (so `profiles` may not exist yet).
  // Use upsert to avoid silently leaving the user looking like "free" in the UI.
  await supabaseAdmin
    .from('profiles')
    .upsert(
      {
        id: userId,
        plan_type: planType,
        is_pro: planType !== 'free',
        updated_at: now.toISOString(),
      },
      { onConflict: 'id' }
    );

  console.log('✅ plan activated', { userId, planType, source });
  return { ok: true, planType, pinsLimit };
}

/** For payment.* webhooks, `data.object.id` is often the payment id — never use it as subscription id. */
function extractDodoSubscriptionIdForWebhook(eventType, dataObj) {
  const t = String(eventType || '').toLowerCase();
  if (t.includes('payment.')) {
    const s = dataObj?.subscription_id ?? dataObj?.subscriptionId ?? dataObj?.subscription;
    if (typeof s === 'string' && s.trim()) return s.trim();
    if (s && typeof s === 'object' && s.id) return String(s.id).trim();
    return null;
  }
  return (
    String(dataObj?.subscription_id || dataObj?.id || dataObj?.subscription?.id || '').trim() || null
  );
}

async function lookupUserIdByDodoSubscriptionId(dodoSubId) {
  const id = String(dodoSubId || '').trim();
  if (!id) return '';
  try {
    const { data: row } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('user_id')
      .eq('dodo_subscription_id', id)
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    return row?.user_id ? String(row.user_id) : '';
  } catch {
    return '';
  }
}

async function lookupLocalSubscriptionByDodoSubscriptionId(dodoSubId) {
  const id = String(dodoSubId || '').trim();
  if (!id) return null;
  try {
    const { data: row } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, user_id, status, plan_type, dodo_subscription_id, billing_interval, current_period_start, current_period_end')
      .eq('dodo_subscription_id', id)
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    return row || null;
  } catch {
    return null;
  }
}

async function updateBillingSubscriptionsByTarget(patch, { dodoSubId, userId, activeOnly = true } = {}) {
  const subId = String(dodoSubId || '').trim();
  const uid = String(userId || '').trim();
  if (!subId && !uid) {
    return { data: null, error: new Error('missing_subscription_target') };
  }
  let q = supabaseAdmin.from('billing_subscriptions').update({
    ...patch,
    updated_at: patch?.updated_at || new Date().toISOString(),
  });
  if (subId) {
    q = q.eq('dodo_subscription_id', subId);
  } else {
    q = q.eq('user_id', uid);
  }
  if (activeOnly) {
    q = q.eq('status', 'active');
  }
  return q;
}

function dodoSubscriptionStatusAllowsPlanChange(statusRaw) {
  const st = String(statusRaw || '').trim().toLowerCase();
  if (!st) return true;
  return st === 'active' || st === 'trialing';
}

function formatDodoChangePlanUserError(status, details) {
  const code = String(details?.code || details?.error?.code || '').trim();
  const providerMessage = String(details?.message || details?.error?.message || '').trim();
  if (code === 'INACTIVE_SUBSCRIPTION_PLAN_CHANGE_NOT_SUPPORTED') {
    return 'Your subscription is inactive with the billing provider, so we cannot upgrade it in place. Please contact us through the Contact page on URL2Pin and we will help you restore billing or start a new subscription.';
  }
  if (status === 409) {
    return 'A plan change or cancellation is already pending on your subscription, so we cannot apply another upgrade right now. This often happens if cancel-at-period-end was scheduled earlier. Please contact us through the Contact page on URL2Pin and we will help you finish the upgrade.';
  }
  if (providerMessage) {
    return `We could not change your plan with the billing provider (${providerMessage}). Please contact us through the Contact page on URL2Pin if this continues.`;
  }
  return 'We could not change your plan with the billing provider. Please contact us through the Contact page on URL2Pin and we will help you finish the upgrade.';
}

async function fetchDodoSubscriptionJson(dodoSubId) {
  const subId = String(dodoSubId || '').trim();
  if (!subId || !DODO_API_KEY) return { ok: false, status: 0, json: null };
  try {
    const resp = await fetch(`${DODO_BASE_URL}/subscriptions/${encodeURIComponent(subId)}`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${DODO_API_KEY}` },
    });
    const json = await resp.json().catch(() => ({}));
    return { ok: resp.ok, status: resp.status, json };
  } catch (e) {
    console.warn('fetchDodoSubscriptionJson error:', e.message || e);
    return { ok: false, status: 0, json: null };
  }
}

function dodoSubscriptionIsInactiveStatus(statusRaw) {
  const st = String(statusRaw || '').trim().toLowerCase();
  if (!st) return false;
  return ['cancelled', 'canceled', 'ended', 'expired', 'inactive', 'on_hold', 'failed', 'past_due'].includes(st);
}

async function markLocalSubscriptionRowCancelled(rowId, userId, { clearCancelFlags = true } = {}) {
  if (!rowId || !userId) return;
  const patch = {
    status: 'cancelled',
    updated_at: new Date().toISOString(),
  };
  if (clearCancelFlags) {
    patch.cancel_at_period_end = false;
    patch.cancelled_at = null;
  }
  await supabaseAdmin
    .from('billing_subscriptions')
    .update(patch)
    .eq('id', rowId)
    .eq('user_id', userId);
}

async function syncLocalSubscriptionWithDodo(userId, localSub, dodoJson) {
  if (!localSub?.id || !userId) return localSub;
  const dodoStatus = String(dodoJson?.status || dodoJson?.subscription_status || '').trim().toLowerCase();
  if (!dodoSubscriptionIsInactiveStatus(dodoStatus)) return localSub;
  await markLocalSubscriptionRowCancelled(localSub.id, userId);
  const { data: stillActive } = await supabaseAdmin
    .from('billing_subscriptions')
    .select('id')
    .eq('user_id', userId)
    .eq('status', 'active')
    .limit(1);
  if (!stillActive || stillActive.length === 0) {
    await supabaseAdmin
      .from('profiles')
      .update({ plan_type: 'free', is_pro: false, updated_at: new Date().toISOString() })
      .eq('id', userId);
  }
  return null;
}

async function cancelScheduledDodoPlanChange(dodoSubId) {
  const subId = String(dodoSubId || '').trim();
  if (!subId || !DODO_API_KEY) return { ok: false, status: 0 };
  try {
    const resp = await fetch(
      `${DODO_BASE_URL}/subscriptions/${encodeURIComponent(subId)}/change-plan/scheduled`,
      {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${DODO_API_KEY}` },
      }
    );
    return { ok: resp.ok || resp.status === 404, status: resp.status };
  } catch (e) {
    console.warn('cancelScheduledDodoPlanChange error:', e.message || e);
    return { ok: false, status: 0 };
  }
}

async function resumeDodoSubscriptionIfScheduledCancel(dodoSubId) {
  const subId = String(dodoSubId || '').trim();
  if (!subId || !DODO_API_KEY) return { ok: false, status: 0, json: null };
  try {
    const resp = await fetch(`${DODO_BASE_URL}/subscriptions/${encodeURIComponent(subId)}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DODO_API_KEY}`,
      },
      body: JSON.stringify({ cancel_at_next_billing_date: false }),
    });
    const json = await resp.json().catch(() => ({}));
    return { ok: resp.ok, status: resp.status, json };
  } catch (e) {
    console.warn('resumeDodoSubscriptionIfScheduledCancel error:', e.message || e);
    return { ok: false, status: 0, json: null };
  }
}

async function clearLocalCancelAtPeriodEnd(localSubId, userId) {
  if (!localSubId || !userId) return;
  await supabaseAdmin
    .from('billing_subscriptions')
    .update({
      cancel_at_period_end: false,
      cancelled_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq('id', localSubId)
    .eq('user_id', userId)
    .eq('status', 'active');
}

async function ensureDodoSubscriptionReadyForPlanChange(dodoSubId, userId, localSub) {
  const subId = String(dodoSubId || '').trim();
  if (!subId || !DODO_API_KEY) {
    return { ok: false, code: 'missing_dodo_subscription', message: 'No active Dodo subscription found for this account.' };
  }

  await cancelScheduledDodoPlanChange(subId);

  const dodoGet = await fetchDodoSubscriptionJson(subId);
  if (!dodoGet.ok) {
    return {
      ok: false,
      code: 'dodo_subscription_lookup_failed',
      message: 'Failed to load subscription from billing provider.',
    };
  }

  let dodoJson = dodoGet.json || {};
  const cancelScheduled =
    dodoJson?.cancel_at_next_billing_date === true || dodoJson?.cancel_at_period_end === true;
  if (cancelScheduled || localSub?.cancel_at_period_end) {
    const resume = await resumeDodoSubscriptionIfScheduledCancel(subId);
    if (resume.ok) {
      await clearLocalCancelAtPeriodEnd(localSub?.id, userId);
      const refreshed = await fetchDodoSubscriptionJson(subId);
      if (refreshed.ok) dodoJson = refreshed.json || dodoJson;
    }
  }

  const dodoStatus = String(dodoJson?.status || dodoJson?.subscription_status || '').trim();
  if (!dodoSubscriptionStatusAllowsPlanChange(dodoStatus)) {
    if (localSub?.id) {
      await syncLocalSubscriptionWithDodo(userId, localSub, dodoJson);
    }
    return {
      ok: false,
      code: 'inactive_subscription',
      message: formatDodoChangePlanUserError(409, {
        code: 'INACTIVE_SUBSCRIPTION_PLAN_CHANGE_NOT_SUPPORTED',
        message: dodoStatus ? `Subscription status is ${dodoStatus}` : 'Subscription is inactive',
      }),
      provider_status: dodoStatus || null,
    };
  }

  return { ok: true, dodoJson };
}

async function getLatestPayingSubscriptionRow(userId) {
  const { data, error } = await supabaseAdmin
    .from('billing_subscriptions')
    .select(
      'id, plan_type, status, current_period_end, cancel_at_period_end, cancelled_at, dodo_subscription_id, billing_interval'
    )
    .eq('user_id', userId)
    .in('status', ['active', 'past_due'])
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) {
    console.warn('getLatestPayingSubscriptionRow error:', error.message || error);
    return null;
  }
  return data || null;
}

async function resolveBillingUpgradeAction(userId, targetPlanType, targetInterval, recurseDepth = 0) {
  const targetPlan = String(targetPlanType || '').trim();
  const targetInt = normalizeBillingInterval(targetInterval);
  if (!targetPlan || !PLAN_PIN_LIMITS[targetPlan]) {
    return { action: 'invalid_plan', error: 'Invalid planType' };
  }

  const localSub = await getLatestPayingSubscriptionRow(userId);
  if (!localSub) {
    return { action: 'checkout', reason: 'no_local_subscription' };
  }

  if (localSub.status === 'past_due') {
    const dodoSubId = String(localSub.dodo_subscription_id || '').trim();
    if (dodoSubId) {
      const dodoGet = await fetchDodoSubscriptionJson(dodoSubId);
      const dodoStatus = String(dodoGet.json?.status || dodoGet.json?.subscription_status || '').trim();
      if (dodoGet.ok && dodoSubscriptionStatusAllowsPlanChange(dodoStatus)) {
        await reactivatePastDueSubscriptionRow(userId, localSub);
        if (recurseDepth < 1) {
          return resolveBillingUpgradeAction(userId, targetPlan, targetInt, recurseDepth + 1);
        }
      }
    }
    await closePastDueSubscriptionRow(userId, localSub);
    return { action: 'checkout', reason: 'past_due_restarted' };
  }

  const dodoSubId = String(localSub.dodo_subscription_id || '').trim();
  if (!dodoSubId) {
    return {
      action: 'contact_support',
      code: 'active_without_dodo_id',
      message:
        'Your account shows an active plan but is not linked to billing yet. Please contact us through the Contact page on URL2Pin so we can fix this without double billing.',
      subscription: localSub,
    };
  }

  const dodoGet = await fetchDodoSubscriptionJson(dodoSubId);
  if (!dodoGet.ok) {
    return {
      action: 'contact_support',
      code: 'dodo_subscription_lookup_failed',
      message: 'We could not verify your billing subscription right now. Please try again in a moment or contact support.',
      subscription: localSub,
    };
  }

  const dodoJson = dodoGet.json || {};
  const dodoStatus = String(dodoJson?.status || dodoJson?.subscription_status || '').trim();
  if (!dodoSubscriptionStatusAllowsPlanChange(dodoStatus)) {
    await syncLocalSubscriptionWithDodo(userId, localSub, dodoJson);
    return {
      action: 'checkout',
      reason: 'dodo_subscription_inactive',
      provider_status: dodoStatus || null,
    };
  }

  const currentProductId = String(dodoJson?.product_id || dodoJson?.productId || '').trim();
  const currentInterval = inferBillingIntervalFromDodoProductId(currentProductId);
  const currentPlanType = String(localSub.plan_type || '').trim();
  const curRank = planRank(currentPlanType);
  const tgtRank = planRank(targetPlan);
  if (currentInterval === targetInt && tgtRank === curRank) {
    return { action: 'noop', message: 'Already on this plan.', subscription: localSub };
  }

  return {
    action: 'change_plan',
    subscription: localSub,
    current: { planType: currentPlanType, interval: currentInterval, provider_status: dodoStatus || null },
    target: { planType: targetPlan, interval: targetInt },
  };
}

function formatBillingClientMessage(payload) {
  if (!payload) return 'Something went wrong. Please try again or contact support.';
  if (typeof payload.message === 'string' && payload.message.trim()) return payload.message.trim();
  if (typeof payload.error === 'string' && payload.error.trim()) return payload.error.trim();
  return 'Something went wrong. Please try again or contact support.';
}

const persistentPendingCheckouts = new Map();
const PENDING_CHECKOUT_MAX_AGE_MS = 48 * 60 * 60 * 1000;

async function findDuplicateActiveSubscriptionOnOtherAccount(currentUserId, email) {
  const authedEmail = normalizeEmail(email);
  const uid = String(currentUserId || '').trim();
  if (!authedEmail || !uid) return null;
  try {
    const { data: otherProfiles } = await supabaseAdmin
      .from('profiles')
      .select('id, email')
      .ilike('email', authedEmail)
      .neq('id', uid)
      .limit(10);
    const otherUserIds = (otherProfiles || []).map((p) => String(p?.id || '').trim()).filter(Boolean);
    if (otherUserIds.length === 0) return null;
    const { data: otherActiveSubs } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, user_id, plan_type, status, dodo_subscription_id, billing_interval')
      .in('user_id', otherUserIds)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(2);
    if (!otherActiveSubs || otherActiveSubs.length !== 1) return null;
    return {
      otherUserId: String(otherActiveSubs[0].user_id || '').trim(),
      subscription: otherActiveSubs[0],
    };
  } catch (e) {
    console.warn('findDuplicateActiveSubscriptionOnOtherAccount error:', e?.message || e);
    return null;
  }
}

async function tryConsolidateDuplicateEmailSubscription(currentUserId, email) {
  const dup = await findDuplicateActiveSubscriptionOnOtherAccount(currentUserId, email);
  if (!dup?.subscription?.id || !dup.otherUserId) {
    return { ok: false, reason: 'no_duplicate' };
  }
  const now = new Date().toISOString();
  const sub = dup.subscription;
  const { error: moveErr } = await supabaseAdmin
    .from('billing_subscriptions')
    .update({ user_id: currentUserId, updated_at: now })
    .eq('id', sub.id)
    .eq('user_id', dup.otherUserId);
  if (moveErr) {
    console.warn('tryConsolidateDuplicateEmailSubscription move error:', moveErr.message || moveErr);
    return { ok: false, reason: 'move_failed' };
  }
  await supabaseAdmin
    .from('profiles')
    .update({ plan_type: 'free', is_pro: false, updated_at: now })
    .eq('id', dup.otherUserId);
  await supabaseAdmin
    .from('profiles')
    .update({
      plan_type: sub.plan_type || 'free',
      is_pro: String(sub.plan_type || 'free') !== 'free',
      updated_at: now,
    })
    .eq('id', currentUserId);
  return { ok: true, subscription: sub, otherUserId: dup.otherUserId };
}

async function reactivatePastDueSubscriptionRow(userId, localSub) {
  const planType = String(localSub?.plan_type || 'free').trim() || 'free';
  const now = new Date().toISOString();
  await supabaseAdmin
    .from('billing_subscriptions')
    .update({ status: 'active', updated_at: now })
    .eq('id', localSub.id)
    .eq('user_id', userId);
  await supabaseAdmin
    .from('profiles')
    .update({ plan_type: planType, is_pro: planType !== 'free', updated_at: now })
    .eq('id', userId);
}

async function closePastDueSubscriptionRow(userId, localSub) {
  if (!localSub?.id) return;
  const now = new Date().toISOString();
  await supabaseAdmin
    .from('billing_subscriptions')
    .update({
      status: 'cancelled',
      cancel_at_period_end: false,
      updated_at: now,
    })
    .eq('id', localSub.id)
    .eq('user_id', userId);
  const { data: stillActive } = await supabaseAdmin
    .from('billing_subscriptions')
    .select('id')
    .eq('user_id', userId)
    .eq('status', 'active')
    .limit(1);
  if (!stillActive || stillActive.length === 0) {
    await supabaseAdmin
      .from('profiles')
      .update({ plan_type: 'free', is_pro: false, updated_at: now })
      .eq('id', userId);
  }
}

async function clearFailedBillingForUser(userId) {
  const uid = String(userId || '').trim();
  if (!uid) return { ok: false, error: 'missing_user_id', closed: 0 };
  const now = new Date().toISOString();
  const { data: rows, error } = await supabaseAdmin
    .from('billing_subscriptions')
    .select('id, status, dodo_subscription_id')
    .eq('user_id', uid)
    .in('status', ['past_due', 'active'])
    .order('created_at', { ascending: false });
  if (error) {
    return { ok: false, error: error.message || 'fetch_failed', closed: 0 };
  }
  let closed = 0;
  for (const row of rows || []) {
    if (row.status === 'past_due') {
      await closePastDueSubscriptionRow(uid, row);
      closed += 1;
      continue;
    }
    if (row.status === 'active') {
      const dodoSubId = String(row.dodo_subscription_id || '').trim();
      if (!dodoSubId) {
        await supabaseAdmin
          .from('billing_subscriptions')
          .update({ status: 'cancelled', cancel_at_period_end: false, updated_at: now })
          .eq('id', row.id)
          .eq('user_id', uid);
        closed += 1;
        continue;
      }
      const dodoGet = await fetchDodoSubscriptionJson(dodoSubId);
      const dodoStatus = String(dodoGet.json?.status || dodoGet.json?.subscription_status || '').trim();
      if (!dodoGet.ok || dodoSubscriptionIsInactiveStatus(dodoStatus)) {
        await supabaseAdmin
          .from('billing_subscriptions')
          .update({ status: 'cancelled', cancel_at_period_end: false, updated_at: now })
          .eq('id', row.id)
          .eq('user_id', uid);
        closed += 1;
      }
    }
  }
  const { data: stillActive } = await supabaseAdmin
    .from('billing_subscriptions')
    .select('id')
    .eq('user_id', uid)
    .eq('status', 'active')
    .limit(1);
  if (!stillActive || stillActive.length === 0) {
    await supabaseAdmin
      .from('profiles')
      .update({ plan_type: 'free', is_pro: false, updated_at: now })
      .eq('id', uid);
  }
  return { ok: true, closed };
}

async function isDodoCheckoutSessionUnpaid(checkoutSessionId) {
  const raw = String(checkoutSessionId || '').trim();
  if (!raw || !DODO_API_KEY) return false;
  try {
    const resp = await fetch(`${DODO_BASE_URL}/checkouts/${encodeURIComponent(raw)}`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${DODO_API_KEY}` },
    });
    const json = await resp.json().catch(() => ({}));
    if (!resp.ok) return false;
    const paymentId = String(json?.payment_id ?? json?.paymentId ?? '').trim();
    if (paymentId) return false;
    const paymentStatus = String(json?.payment_status ?? json?.paymentStatus ?? '').toLowerCase();
    if (paymentStatus && ['succeeded', 'completed', 'paid'].includes(paymentStatus)) return false;
    return true;
  } catch {
    return false;
  }
}

async function savePendingCheckoutRecord(userId, planType, billingInterval, sessionId, checkoutUrl) {
  const uid = String(userId || '').trim();
  const session = String(sessionId || '').trim();
  if (!uid || !session) return;
  const interval = normalizeBillingInterval(billingInterval);
  const now = new Date().toISOString();
  const row = {
    user_id: uid,
    checkout_session_id: session,
    checkout_url: checkoutUrl || null,
    plan_type: String(planType || '').trim(),
    billing_interval: interval,
    status: 'open',
    updated_at: now,
  };
  const { error } = await supabaseAdmin
    .from('billing_pending_checkouts')
    .upsert(row, { onConflict: 'checkout_session_id' });
  if (error) {
    persistentPendingCheckouts.set(`${uid}:${row.plan_type}:${interval}`, {
      sessionId: session,
      checkoutUrl: checkoutUrl || null,
      createdAt: Date.now(),
    });
  }
}

async function markPendingCheckoutCompleted(checkoutSessionId) {
  const session = String(checkoutSessionId || '').trim();
  if (!session) return;
  const now = new Date().toISOString();
  const { error } = await supabaseAdmin
    .from('billing_pending_checkouts')
    .update({ status: 'completed', updated_at: now })
    .eq('checkout_session_id', session);
  if (error) {
    for (const [key, value] of persistentPendingCheckouts.entries()) {
      if (value?.sessionId === session) persistentPendingCheckouts.delete(key);
    }
  }
}

async function returnActivatePlanSuccess(res, checkoutSessionId, payload) {
  await markPendingCheckoutCompleted(checkoutSessionId);
  return res.json(payload);
}

async function findResumablePendingCheckout(userId, planType, billingInterval) {
  const uid = String(userId || '').trim();
  const plan = String(planType || '').trim();
  const interval = normalizeBillingInterval(billingInterval);
  if (!uid || !plan) return null;
  const maxAgeMs = PENDING_CHECKOUT_MAX_AGE_MS;
  let sessionId = '';
  let checkoutUrl = '';
  let createdAt = 0;
  try {
    const { data: rows } = await supabaseAdmin
      .from('billing_pending_checkouts')
      .select('checkout_session_id, checkout_url, created_at, status')
      .eq('user_id', uid)
      .eq('plan_type', plan)
      .eq('billing_interval', interval)
      .eq('status', 'open')
      .order('created_at', { ascending: false })
      .limit(1);
    const row = rows && rows.length ? rows[0] : null;
    if (row) {
      sessionId = String(row.checkout_session_id || '').trim();
      checkoutUrl = String(row.checkout_url || '').trim();
      createdAt = row.created_at ? new Date(row.created_at).getTime() : 0;
    }
  } catch {
    /* table may not exist yet */
  }
  if (!sessionId) {
    const mem = persistentPendingCheckouts.get(`${uid}:${plan}:${interval}`);
    if (mem) {
      sessionId = String(mem.sessionId || '').trim();
      checkoutUrl = String(mem.checkoutUrl || '').trim();
      createdAt = Number(mem.createdAt || 0);
    }
  }
  if (!sessionId || !createdAt || Date.now() - createdAt > maxAgeMs) return null;
  const unpaid = await isDodoCheckoutSessionUnpaid(sessionId);
  if (!unpaid) {
    await markPendingCheckoutCompleted(sessionId);
    return null;
  }
  return { sessionId, checkoutUrl: checkoutUrl || null };
}

/**
 * Renewal / payment failure: Dodo marks subscription on hold or failed charge.
 * Mark our row past_due and revoke paid access (profile free) if no other active sub.
 */
/**
 * Only grant paid access when Dodo confirms a successful charge or an active subscription.
 * Do NOT use checkout.* or subscription.created — they can fire before payment succeeds (card declined / abandoned checkout).
 */
function dodoWebhookEventConfirmsPaidSubscription(eventType, dataObj) {
  const t = String(eventType || '').toLowerCase();
  if (t.includes('payment.succeeded')) {
    const ps = String(dataObj?.status || '').toLowerCase();
    if (!ps) return true;
    return ps === 'succeeded' || ps === 'completed';
  }
  if (t.includes('subscription.renewed')) return true;
  if (t.includes('subscription.active') || t.includes('subscription.activated')) {
    const st = String(dataObj?.status || '').toLowerCase();
    if (!st) return true;
    return st === 'active' || st === 'trialing';
  }
  return false;
}

async function markBillingSubscriptionPastDueAndDowngradeProfile(userId, dodoSubId, logReason) {
  const uid = String(userId || '').trim();
  if (!uid) return { ok: false, error: 'missing_user_id' };
  const now = new Date().toISOString();
  const subId = String(dodoSubId || '').trim();

  let q = supabaseAdmin
    .from('billing_subscriptions')
    .update({ status: 'past_due', cancel_at_period_end: false, updated_at: now })
    .eq('user_id', uid)
    .eq('status', 'active');
  if (subId) {
    q = q.eq('dodo_subscription_id', subId);
  }
  const { error: updErr } = await q;
  if (updErr) {
    console.warn('markBillingSubscriptionPastDue: update error', { updErr, uid, subId, logReason });
  } else {
    console.warn('billing: subscription marked past_due', { userId: uid, dodoSubId: subId || null, logReason });
  }

  const { data: stillActive } = await supabaseAdmin
    .from('billing_subscriptions')
    .select('id')
    .eq('user_id', uid)
    .eq('status', 'active')
    .limit(1);
  if (!stillActive || stillActive.length === 0) {
    await supabaseAdmin
      .from('profiles')
      .update({ plan_type: 'free', is_pro: false, updated_at: now })
      .eq('id', uid);
  }
  return { ok: true };
}

/** Resolve a user's email: prefer profiles.email, fall back to the Supabase auth record. */
async function getUserEmailById(userId) {
  const uid = String(userId || '').trim();
  if (!uid) return '';
  try {
    const { data: profile } = await supabaseAdmin
      .from('profiles')
      .select('email')
      .eq('id', uid)
      .maybeSingle();
    const profileEmail = String(profile?.email || '').trim();
    if (profileEmail) return profileEmail;
  } catch {
    /* fall through to auth lookup */
  }
  try {
    const { data, error } = await supabaseAdmin.auth.admin.getUserById(uid);
    if (!error) return String(data?.user?.email || '').trim();
  } catch {
    /* ignore */
  }
  return '';
}

// Avoid emailing the same subscription repeatedly when Dodo fires payment.failed
// then subscription.on_hold/failed in quick succession.
const recentDunningEmails = new Map(); // dedupeKey -> timestamp(ms)
const DUNNING_EMAIL_DEDUPE_MS = 12 * 60 * 60 * 1000; // 12h

/**
 * Send a "your payment failed — update your card" email, deduped per subscription.
 * `emailHint` (from the webhook payload) is used first to avoid an extra lookup.
 */
async function triggerDunningEmail({ userId, dodoSubId, planType, emailHint, logReason }) {
  if (!isEmailEnabled()) return { ok: false, skipped: true, reason: 'email_disabled' };
  const uid = String(userId || '').trim();
  const dedupeKey = `${uid}:${String(dodoSubId || '').trim()}`;
  const last = recentDunningEmails.get(dedupeKey);
  const nowMs = Date.now();
  if (last && nowMs - last < DUNNING_EMAIL_DEDUPE_MS) {
    return { ok: false, skipped: true, reason: 'recently_emailed' };
  }

  let email = String(emailHint || '').trim();
  if (!email) email = await getUserEmailById(uid);
  if (!email) {
    console.warn('dunning: no email for user — skipping', { userId: uid, dodoSubId, logReason });
    return { ok: false, skipped: true, reason: 'no_email' };
  }

  let plan = String(planType || '').trim();
  if (!plan && dodoSubId) {
    try {
      const local = await lookupLocalSubscriptionByDodoSubscriptionId(dodoSubId);
      plan = String(local?.plan_type || '').trim();
    } catch {
      /* ignore */
    }
  }

  const result = await sendPaymentFailedEmail({ to: email, planType: plan });
  if (result.ok) {
    recentDunningEmails.set(dedupeKey, nowMs);
    console.log('dunning: payment-failed email sent', { userId: uid, dodoSubId, plan, logReason });
  }
  return result;
}

// At most one upgrade-nudge email per user per usage month (keyed below), to avoid spam.
const recentUpgradeNudges = new Map(); // `${userId}:${yearMonth}` -> timestamp(ms)
const UPGRADE_NUDGE_DEDUPE_MS = 26 * 24 * 60 * 60 * 1000; // ~ once per billing month

/**
 * Fire-and-forget expansion nudge when a user hits (or nears) their AI pin cap.
 * No-ops for top-tier (agency) and when email is disabled. Deduped per user/month.
 */
async function triggerUpgradeNudge({ userId, planType, used, limit, reason, yearMonth }) {
  try {
    if (!isEmailEnabled()) return;
    if (!nextPlanFor(planType)) return; // top tier — nothing to upsell
    const uid = String(userId || '').trim();
    if (!uid) return;
    const dedupeKey = `${uid}:${yearMonth || ''}`;
    const last = recentUpgradeNudges.get(dedupeKey);
    const nowMs = Date.now();
    if (last && nowMs - last < UPGRADE_NUDGE_DEDUPE_MS) return;
    // Reserve the slot before the await so concurrent pin requests don't double-send.
    recentUpgradeNudges.set(dedupeKey, nowMs);

    const email = await getUserEmailById(uid);
    if (!email) {
      recentUpgradeNudges.delete(dedupeKey);
      return;
    }
    const result = await sendUpgradeNudgeEmail({ to: email, currentPlan: planType, used, limit, reason });
    if (result?.ok) {
      console.log('upgrade nudge: email sent', { userId: uid, planType, reason, used, limit });
    } else {
      // Failed/skipped send shouldn't permanently suppress a future nudge.
      if (!result?.ok && result?.reason !== 'top_tier') recentUpgradeNudges.delete(dedupeKey);
    }
  } catch (e) {
    console.warn('upgrade nudge: error', { userId, planType, error: e?.message || e });
  }
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

/** Display value for usage API when local dev bypass is active (not enforced as a hard cap). */
const LOCALHOST_DEV_UNLIMITED_PINS = 999_999;

/** Local dev only: relax scheduling + pin quotas when the app/API request is from localhost. Never in production. */
function isLocalhostDevBypass(req) {
  if (process.env.NODE_ENV === 'production') return false;
  const flag = String(process.env.ALLOW_LOCALHOST_SCHEDULING || '').toLowerCase();
  if (flag === 'false' || flag === '0') return false;
  if (flag === 'true' || flag === '1') return true;
  if (req?.headers?.['x-url2pin-localhost-dev'] === '1') return true;
  if (!req) return false;
  const candidates = [
    req.headers?.origin,
    req.headers?.referer,
    req.headers?.host,
    req.headers?.['x-forwarded-host'],
  ];
  for (const raw of candidates) {
    const h = String(raw || '').toLowerCase();
    if (h.includes('localhost') || h.includes('127.0.0.1') || h.includes('[::1]')) return true;
  }
  return false;
}

function isLocalhostSchedulingBypass(req) {
  return isLocalhostDevBypass(req);
}

async function enforcePaidSchedulingOrThrow(res, userId, req) {
  if (isLocalhostDevBypass(req)) return true;
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
 * Manual product override supplied by the user when Amazon (or any page) can't be scraped.
 * Lets generation proceed from a user-typed title/description + optional uploaded image URLs.
 * @returns {{title:string, description:string, imageUrls:string[]} | null}
 */
function normalizeManualProductOverride(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const title = String(raw.title ?? '').trim().slice(0, 200);
  const description = String(raw.description ?? '').trim().slice(0, 600);
  let imageUrls = raw.imageUrls ?? raw.imageUrl ?? [];
  if (typeof imageUrls === 'string') imageUrls = imageUrls.trim() ? [imageUrls.trim()] : [];
  if (!Array.isArray(imageUrls)) imageUrls = [];
  imageUrls = imageUrls
    .map((u) => (typeof u === 'string' ? u.trim() : ''))
    .filter(Boolean)
    .slice(0, 4);
  if (!title && !description && imageUrls.length === 0) return null;
  return { title, description, imageUrls };
}

/**
 * @param {string} userId
 * @param {{ aiDelta?: number, userPhotoDelta?: number }} deltas, positive consume, negative refund
 */
async function applyPinQuotaDelta(userId, { aiDelta = 0, userPhotoDelta = 0 }, req = null) {
  const key = String(userId);
  const unlimitedPins = isLocalhostDevBypass(req);
  let promise = pinUsageLocks.get(key);
  const run = async () => {
    const sub = await getActiveSubscriptionForUser(userId);
    const planType = sub?.plan_type || 'free';
    const yearMonth = pinUsageBucketForPlan(planType);
    const planPinsLimit = unlimitedPins ? LOCALHOST_DEV_UNLIMITED_PINS : planAiPinsLimit(sub);
    const planUserPhotoPinsLimit = unlimitedPins
      ? LOCALHOST_DEV_UNLIMITED_PINS
      : resolveUserPhotoPinLimitForPlan(sub);
    const baselineAi = Math.max(0, Number(sub?.usage_baseline_pins_used ?? 0) || 0);
    const baselineUserPhoto = Math.max(0, Number(sub?.usage_baseline_user_photo_pins_used ?? 0) || 0);

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

      const effectiveCurrentAi = Math.max(0, currentAi - baselineAi);
      const effectiveCurrentUserPhoto = Math.max(0, currentUserPhoto - baselineUserPhoto);
      const tentativeAi = effectiveCurrentAi + aiDelta;
      const tentativeUserPhoto = effectiveCurrentUserPhoto + userPhotoDelta;

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
        if (!unlimitedPins) {
          void triggerUpgradeNudge({
            userId,
            planType,
            used: effectiveCurrentAi,
            limit: planPinsLimit,
            reason: 'limit_reached',
            yearMonth,
          });
        }
        return {
          allowed: false,
          limitKind: 'ai',
          planType,
          planPinsLimit,
          planUserPhotoPinsLimit,
          currentUsed: effectiveCurrentAi,
          wouldUseAi: aiDelta,
          currentUserPhotoPinsUsed: effectiveCurrentUserPhoto,
        };
      }

      if (userPhotoDelta > 0 && tentativeUserPhoto > planUserPhotoPinsLimit) {
        return {
          allowed: false,
          limitKind: 'user_photo',
          planType,
          planPinsLimit,
          planUserPhotoPinsLimit,
          currentUsed: effectiveCurrentAi,
          currentUserPhotoPinsUsed: effectiveCurrentUserPhoto,
          wouldUseUserPhoto: userPhotoDelta,
        };
      }

      // Store absolute counters, but enforce limits against the "effective" (baseline-adjusted) usage.
      const newAi = currentAi + aiDelta;
      const newUserPhoto = currentUserPhoto + userPhotoDelta;
      const usageNowIso = new Date().toISOString();

      const { error: upsertError } = await supabaseAdmin.from('pin_usage').upsert(
        {
          user_id: userId,
          year_month: yearMonth,
          pins_used: newAi,
          user_photo_pins_used: newUserPhoto,
          updated_at: usageNowIso,
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
        previousUsed: effectiveCurrentAi,
        newUsed: Math.max(0, newAi - baselineAi),
        previousUserPhotoPinsUsed: effectiveCurrentUserPhoto,
        newUserPhotoPinsUsed: Math.max(0, newUserPhoto - baselineUserPhoto),
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

async function getCurrentUsageSnapshot(userId, req = null) {
  const yearMonth = currentYearMonthDate();
  const localhostUnlimitedPins = isLocalhostDevBypass(req);

  // Subscription row for UI context (active preferred; else past_due for recovery banner)
  const subscription = (await getActiveSubscriptionForUser(userId)) || (await getLatestPastDueSubscriptionForUser(userId));
  const subscriptionActive = subscription?.status === 'active';
  const planType = subscriptionActive && subscription?.plan_type ? subscription.plan_type : 'free';
  const pinUsageBucket = pinUsageBucketForPlan(planType);
  const effectiveSubForLimits = subscriptionActive ? subscription : { plan_type: planType };
  const planPinsLimit = localhostUnlimitedPins
    ? LOCALHOST_DEV_UNLIMITED_PINS
    : planAiPinsLimit(effectiveSubForLimits);
  const planUserPhotoPinsLimit = localhostUnlimitedPins
    ? LOCALHOST_DEV_UNLIMITED_PINS
    : resolveUserPhotoPinLimitForPlan(effectiveSubForLimits);
  const baselineAi = subscriptionActive
    ? Math.max(0, Number(subscription?.usage_baseline_pins_used ?? 0) || 0)
    : 0;
  const baselineUserPhoto = subscriptionActive
    ? Math.max(0, Number(subscription?.usage_baseline_user_photo_pins_used ?? 0) || 0)
    : 0;

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
      .eq('year_month', pinUsageBucket)
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
  const effectivePinsUsed = Math.max(0, pinsUsed - baselineAi);
  const effectiveUserPhotoPinsUsed = Math.max(0, userPhotoPinsUsed - baselineUserPhoto);

  return {
    localhost_dev_unlimited_pins: localhostUnlimitedPins,
    user: {
      id: profile?.id || userId,
      email: profile?.email || null,
    },
    subscription: subscription
      ? {
          id: subscription.id || null,
          status: subscription.status || null,
          plan_type: subscription.plan_type || null,
          billing_interval: subscription.billing_interval || 'month',
          current_period_start: subscription.current_period_start || null,
          current_period_end: subscription.current_period_end || null,
          cancel_at_period_end: subscriptionActive && Boolean(subscription.cancel_at_period_end),
          cancelled_at: subscription.cancelled_at || null,
          dodo_subscription_id: subscription.dodo_subscription_id || null,
          previous_plan_type:
            !subscriptionActive && subscription.plan_type ? subscription.plan_type : null,
        }
      : null,
    plan: {
      type: planType,
      pins_limit_per_month: planPinsLimit,
      user_photo_pins_limit_per_month: planUserPhotoPinsLimit,
      metadata_limit_per_month: planMetaLimit,
    },
    usage: {
      year_month: yearMonth,
      pins_used: effectivePinsUsed,
      pins_remaining: Math.max(0, planPinsLimit - effectivePinsUsed),
      user_photo_pins_used: effectiveUserPhotoPinsUsed,
      user_photo_pins_remaining: Math.max(0, planUserPhotoPinsLimit - effectiveUserPhotoPinsUsed),
      metadata_calls: metadataCalls,
      metadata_soft_limit: planMetaLimit,
    },
  };
}

/**
 * GoTrue `getUser` uses fetch; on Render ↔ Supabase you can see transient TLS blips (ECONNRESET).
 * Retries a few times; callers should treat repeated failure as 503, not 401.
 */
function isTransientSupabaseNetworkError(err) {
  if (!err) return false;
  const cause = err.cause || err;
  const code = cause.code || err.code;
  if (
    code === 'ECONNRESET' ||
    code === 'ETIMEDOUT' ||
    code === 'ECONNREFUSED' ||
    code === 'ENOTFOUND' ||
    code === 'UND_ERR_SOCKET' ||
    code === 'EAI_AGAIN'
  ) {
    return true;
  }
  const msg = `${err.message || ''} ${cause.message || ''}`.toLowerCase();
  if (
    msg.includes('fetch failed') ||
    msg.includes('socket disconnected') ||
    msg.includes('tls connection') ||
    msg.includes('network error') ||
    msg.includes('aborted')
  ) {
    return true;
  }
  return false;
}

async function supabaseAuthGetUser(accessToken) {
  const maxAttempts = 3;
  const baseDelayMs = 400;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const out = await supabaseAdmin.auth.getUser(accessToken);
      const err = out?.error;
      if (!err) return out;
      lastErr = err;
      if (!isTransientSupabaseNetworkError(err)) return out;
    } catch (e) {
      lastErr = e;
      if (!isTransientSupabaseNetworkError(e)) {
        return { data: { user: null }, error: e };
      }
    }
    if (attempt < maxAttempts) {
      await new Promise((r) => setTimeout(r, baseDelayMs * attempt));
    }
  }
  console.warn('supabaseAuthGetUser: exhausted retries', lastErr?.message || lastErr);
  return { data: { user: null }, error: lastErr };
}

/** Returns authenticated user or null after sending 401 / 503 on `res`. */
function respondSupabaseAuth(res, user, err) {
  if (err && isTransientSupabaseNetworkError(err)) {
    res.status(503).json({
      error: 'Authentication service temporarily unavailable. Please retry in a moment.',
      code: 'auth_upstream_error',
    });
    return null;
  }
  if (err || !user) {
    res.status(401).json({
      error: 'Your session expired. Please sign in again.',
      code: 'session_expired',
    });
    return null;
  }
  return user;
}

async function requirePro(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return res.status(401).json({ error: 'Please sign in to continue.', code: 'auth_required' });
  }
  const token = authHeader.split(' ')[1];
  const { data: { user }, error } = await supabaseAuthGetUser(token);
  const authed = respondSupabaseAuth(res, user, error);
  if (!authed) return;
  const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('is_pro')
    .eq('id', authed.id)
    .single();
  if (!profile?.is_pro) {
    return res.status(403).json({ error: 'Pro membership required.' });
  }
  req.user = authed;
  next();
}

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

/** One-line access log for /api/trends* (local dev by default; set TRENDS_HTTP_LOG=0 to disable when NODE_ENV is unset). */
if (process.env.TRENDS_HTTP_LOG === '1' || (process.env.TRENDS_HTTP_LOG !== '0' && process.env.NODE_ENV !== 'production')) {
  app.use((req, res, next) => {
    const p = req.path || '';
    if (p.startsWith('/api/trends')) {
      console.log(`[trends-http] ${req.method} ${req.originalUrl || p}`);
    }
    next();
  });
}

/** Hostnames commonly used for URL shortening / tracking — path is required for identity, not just hostname. */
const URL_SHORTENER_HOSTNAMES = new Set([
  'a.co',
  'amzn.to',
  'amzn.eu',
  // Amazon affiliate / Genius-style short links (must expand so ASIN + Amazon flows run)
  'amzlink.to',
  'amznlink.to',
  // Walmart official share/affiliate short links (expand → walmart.com/ip/...)
  'walmrt.us',
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
  // Walmart Creator / Impact affiliate redirect domains (expand → walmart.com/ip/...)
  'goto.walmart.com',
  'linkst.walmart.com',
]);

function isCreatorAffiliatePlatformRedirectHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  // ShopMy quick links (go.shopmy.us/p-…) → retailer destination
  if (h === 'go.shopmy.us' || h === 'shopmy.us' || h.endsWith('.shopmy.us')) return true;
  // Mavely SmartLinks
  if (h === 'mavely.app' || h.endsWith('.mavely.app')) return true;
  if (h === 'mavely.app.link' || h.endsWith('.mavely.app.link')) return true;
  if (h === 'mavelyinfluencer.com' || h.endsWith('.mavelyinfluencer.com')) return true;
  return false;
}

function isBenableHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  return h === 'benable.com' || h.endsWith('.benable.com');
}

function isAffiliateTrackingRedirectHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (AFFILIATE_TRACKING_HOST_EXACT.has(h)) return true;
  if (isCreatorAffiliatePlatformRedirectHost(h)) return true;
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
  // Pinterest pin URLs behave like redirects (not the user's site identity).
  if (h === 'pinterest.com' || h.endsWith('.pinterest.com') || /^pinterest\.[a-z.]{2,}$/i.test(h)) return true;
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

function isEcommerceProductPath(pathname) {
  return /\/products?\//i.test(String(pathname || ''));
}

/**
 * Shopify/WooCommerce handles often end with -2, -7, etc. when the base handle was taken.
 * Strip those so "mary-jane-delfina-7" → "mary-jane-delfina" (not "mary jane delfina 7" on pins).
 */
function cleanProductSlugHandleForKeyword(slug) {
  let s = String(slug || '').trim();
  if (!s) return s;
  const parts = s.split('-').filter(Boolean);
  if (parts.length >= 4 && /^\d{1,3}$/.test(parts[parts.length - 1])) {
    parts.pop();
    return parts.join('-');
  }
  if (parts.length >= 3 && /^\d{1,3}$/.test(parts[parts.length - 1])) {
    const n = parseInt(parts[parts.length - 1], 10);
    if (n >= 2 && n <= 99) {
      parts.pop();
      return parts.join('-');
    }
  }
  return s;
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

    let last = parts[parts.length - 1] || '';
    if (!last) return '';

    if (isEcommerceProductPath(u.pathname || '')) {
      const prodIdx = parts.findIndex((p) => /^products?$/i.test(p));
      if (prodIdx >= 0 && parts[prodIdx + 1]) {
        last = cleanProductSlugHandleForKeyword(parts[prodIdx + 1]);
      } else {
        last = cleanProductSlugHandleForKeyword(last);
      }
    }

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

function titleLooksMeaningfulForKeywordSuppression(rawTitle) {
  const t = String(rawTitle || '').trim();
  if (t.length < 8) return false;
  const lower = t.toLowerCase();
  // Common generic titles that show up on shops / CMS templates.
  const generic = new Set([
    'home',
    'homepage',
    'index',
    'blog',
    'post',
    'article',
    'product',
    'products',
    'shop',
    'store',
    'my shop',
    'checkout',
    'cart',
    'search',
  ]);
  if (generic.has(lower)) return false;
  // If it's mostly punctuation/short tokens, treat as not meaningful.
  const letters = t.replace(/[^\p{L}]+/gu, '');
  if (letters.length < 6) return false;
  return true;
}

function looksLikeLatinSlugKeyword(rawKeyword) {
  const k = String(rawKeyword || '').trim();
  if (!k) return false;
  if (k.length < 6) return false;
  // Only ASCII letters/digits/hyphens/space/underscore.
  const asciiOnly = /^[a-z0-9 _-]+$/i.test(k);
  if (!asciiOnly) return false;
  const hyphens = (k.match(/-/g) || []).length;
  const digits = (k.match(/\d/g) || []).length;
  if (/^\d{3,}/.test(k)) return true;
  if (hyphens >= 3) return true;
  if (digits >= 4) return true;
  return false;
}

function titleHasNonLatinScript(rawTitle) {
  const t = String(rawTitle || '').trim();
  if (!t) return false;
  // If there are letters outside Latin, treat as non-Latin script.
  // This catches Greek, Cyrillic, Arabic, Hebrew, etc.
  const lettersOnly = t.replace(/[^\p{L}]+/gu, '');
  if (!lettersOnly) return false;
  // If any letter is NOT Latin script, return true.
  return /[^\p{Script=Latin}]/u.test(lettersOnly);
}

function isAmazonRelatedHost(host) {
  const h = normalizeUrlHostname(host);
  // Amazon short domains + regional amzn TLDs (amzn.asia, amzn.in, etc.)
  if (h === 'a.co' || h.startsWith('amzn.')) return true;
  if (h === 'amzn.to' || h.endsWith('.amzn.to')) return true;
  if (h === 'amzlink.to' || h.endsWith('.amzlink.to')) return true;
  if (h === 'amznlink.to' || h.endsWith('.amznlink.to')) return true;
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
 * Prefer any gate that requires a manual pin-footer line (original URL wins ties).
 * @returns {{ requiresManualBrandOrCta: boolean, brandingGateReason: string|null, brandingGateMessage: string|null }}
 */
function mergeBrandingGates(...gates) {
  for (const g of gates) {
    if (g?.requiresManualBrandOrCta) return g;
  }
  return { requiresManualBrandOrCta: false, brandingGateReason: null, brandingGateMessage: null };
}

/**
 * Product / affiliate landing URLs: Amazon, Walmart, ShopMy/Mavely, Benable.
 * Drives amazon_affiliate pin strategy mix (list/value/lifestyle — not curiosity-heavy infographics).
 * @param {...string} urlStrings raw input, resolved working URL, canonical, etc.
 * @returns {{ amazonLanding: boolean, walmartLanding: boolean, creatorAffiliateLanding: boolean, etsyLanding: boolean }}
 */
function detectProductAffiliateLandingFromUrls(...urlStrings) {
  const out = {
    amazonLanding: false,
    walmartLanding: false,
    creatorAffiliateLanding: false,
    etsyLanding: false,
  };
  for (const raw of urlStrings) {
    const s = String(raw || '').trim();
    if (!s) continue;
    try {
      const u = new URL(/^https?:\/\//i.test(s) ? s : `https://${s}`);
      const h = u.hostname;
      if (isAmazonRelatedHost(h)) out.amazonLanding = true;
      else if (isWalmartRelatedHost(h)) out.walmartLanding = true;
      else if (isCreatorAffiliatePlatformRedirectHost(h) || isBenableHost(h)) {
        out.creatorAffiliateLanding = true;
      } else if (isEtsyHost(h)) {
        out.etsyLanding = true;
      }
    } catch {
      /* ignore */
    }
  }
  return out;
}

/** Merge landing flags from base scrape or detectProductAffiliateLandingFromUrls into content profile. */
function mergeProductAffiliateLandingIntoProfile(profile, landingSource) {
  if (!profile || typeof profile !== 'object') profile = {};
  const landing =
    landingSource && typeof landingSource === 'object'
      ? {
          amazonLanding: !!landingSource.amazonLanding,
          walmartLanding: !!landingSource.walmartLanding,
          creatorAffiliateLanding: !!landingSource.creatorAffiliateLanding,
          etsyLanding: !!landingSource.etsyLanding,
        }
      : {
          amazonLanding: false,
          walmartLanding: false,
          creatorAffiliateLanding: false,
          etsyLanding: false,
        };
  return {
    ...profile,
    amazonLanding: profile.amazonLanding === true || landing.amazonLanding,
    walmartLanding: profile.walmartLanding === true || landing.walmartLanding,
    creatorAffiliateLanding:
      profile.creatorAffiliateLanding === true || landing.creatorAffiliateLanding,
    etsyLanding: profile.etsyLanding === true || landing.etsyLanding,
  };
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

    if (isCreatorAffiliatePlatformRedirectHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'creator_affiliate_link',
        brandingGateMessage:
          'This looks like a ShopMy or Mavely affiliate link. Pins should show your brand in the footer, not the network name. Enter your brand name or CTA in Pin footer (required) below (e.g. your site name or “Shop my picks”).',
      };
    }

    if (isBenableHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'benable_list',
        brandingGateMessage:
          'This looks like a Benable list link. Pins should show your brand in the footer, not “Benable.” Enter your brand name or CTA in Pin footer (required) below, and use this Benable URL as the pin destination.',
      };
    }

    if (isAffiliateTrackingRedirectHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'affiliate_tracking',
        brandingGateMessage:
          'This looks like an affiliate or network tracking link (not your own site). Pins should show your brand or CTA in the footer, not the tracking URL. Enter your brand name or CTA in Pin footer (required) below.',
      };
    }

    if (isEtsyHost(host)) {
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: 'marketplace',
        brandingGateMessage:
          'This looks like an Etsy shop or listing link. Pins should show your shop or brand in the footer, not “Etsy.” Enter your brand name or CTA in Pin footer (required) below (e.g. your shop name).',
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
          'This URL looks like a short or redirect link. Before generating pins, enter your brand name or CTA (that text is what we use in the pin footer).',
      };
    }

    if (isWalmartRelatedHost(host)) {
      const productish = /\/ip\//i.test(path) || /\/browse\//i.test(path) || /\/cp\//i.test(path);
      const reason = productish ? 'walmart_product_affiliate' : 'walmart_store';
      return {
        requiresManualBrandOrCta: true,
        brandingGateReason: reason,
        brandingGateMessage:
          reason === 'walmart_product_affiliate'
            ? 'This looks like a Walmart product or affiliate link. Pins should show your brand in the footer, not Walmart. Before generating, open Pin look & brand and add your brand name or CTA (e.g. your site name).'
            : 'This looks like a Walmart page. Before generating pins, add your brand name or CTA so the footer represents you, not the store.',
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
          ? 'This looks like an Amazon product or affiliate link. Pins should show your brand in the footer, not Amazon. Enter your brand name or CTA in Pin footer (required) below (e.g. your site name).'
          : 'This looks like an Amazon page. Enter your brand name or CTA in Pin footer (required) below so the footer represents you, not the store.';

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

// ---------------------------------------------------------------------------
// Walmart support — mirrors the Amazon affiliate product-image pipeline:
// detect Walmart product/affiliate links, harvest the product hero photo
// (HTML scrape first, RapidAPI fallback because Walmart blocks bots hard),
// and feed it to Nano Banana as a reference. Footer stays the user's brand/CTA.
// ---------------------------------------------------------------------------

/** Walmart's official share/affiliate short-link domains (resolve to a product page). */
function isWalmartShortLinkHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (h === 'walmrt.us' || h.endsWith('.walmrt.us')) return true;
  if (h === 'goto.walmart.com' || h.endsWith('.goto.walmart.com')) return true;
  if (h === 'linkst.walmart.com' || h.endsWith('.linkst.walmart.com')) return true;
  return false;
}

function isWalmartRelatedHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (isWalmartShortLinkHost(h)) return true;
  if (h === 'walmart.com' || h.endsWith('.walmart.com')) return true;
  return false;
}

/** Numeric item id from https://www.walmart.com/ip/<slug>/<itemId> (or /ip/<itemId>). */
function extractWalmartItemIdFromUrl(urlString) {
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return '';
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (!isWalmartRelatedHost(u.hostname)) return '';
    const path = u.pathname || '';
    // Bot interstitial encodes the intended product path in ?url= (base64).
    if (/\/blocked/i.test(path)) {
      const enc = u.searchParams.get('url');
      if (enc) {
        try {
          const decoded = Buffer.from(decodeURIComponent(enc), 'base64').toString('utf8');
          const mBlocked = decoded.match(/\/ip\/(?:[^/]+\/)?(\d{5,15})/i);
          if (mBlocked) return mBlocked[1];
        } catch {
          /* ignore */
        }
      }
    }
    let m = path.match(/\/ip\/(?:[^/]+\/)?(\d{5,15})(?:\/|$)/i);
    if (m) return m[1];
    m = String(u.search || '').match(/[?&](?:item_id|itemId|selectedItemId|prodsku)=(\d{5,15})/i);
    if (m) return m[1];
    return '';
  } catch {
    return '';
  }
}

/** True when we should call walmart-data RapidAPI (short links, /ip/, or bot-blocked interstitial). */
function isWalmartUrlEligibleForApi(urlString) {
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return false;
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (isWalmartShortLinkHost(u.hostname)) return true;
    if (!isWalmartRelatedHost(u.hostname)) return false;
    if (/\/blocked/i.test(u.pathname || '')) return !!extractWalmartItemIdFromUrl(raw);
    return /\/ip\//i.test(u.pathname || '') || !!extractWalmartItemIdFromUrl(raw);
  } catch {
    return false;
  }
}

/** Resolve any Walmart share/affiliate/blocked URL to a canonical https://www.walmart.com/ip/<id>. */
async function resolveWalmartProductUrlForApi(urlString) {
  const raw = String(urlString || '').trim();
  if (!raw || !isWalmartUrlEligibleForApi(raw)) return '';
  const id = extractWalmartItemIdFromUrl(raw);
  if (id) return `https://www.walmart.com/ip/${id}`;
  if (isWalmartShortLinkHost(new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`).hostname)) {
    return await resolveWalmartShortLinkToProductUrl(raw);
  }
  return '';
}

/** Product / affiliate pages where we try to attach Walmart product images to Nano Banana. */
function isWalmartProductPageForNanoReference(urlString) {
  return isWalmartUrlEligibleForApi(urlString);
}

function pickWalmartContextUrl(inputUrl, canonicalUrl) {
  const rawInput = String(inputUrl || '').trim();
  const rawCanon = String(canonicalUrl || '').trim();
  try {
    if (rawCanon) {
      const cu = new URL(rawCanon);
      if (isWalmartRelatedHost(cu.hostname)) return rawCanon;
    }
  } catch {
    // ignore
  }
  return rawInput;
}

/** Walmart product photos live on i5.walmartimages.com (and regional *.walmartimages.com). */
function isAllowedWalmartCdnImageUrl(urlString) {
  try {
    const u = new URL(urlString);
    if (!/^https:/i.test(u.protocol)) return false;
    return u.hostname.toLowerCase().endsWith('walmartimages.com');
  } catch {
    return false;
  }
}

function normalizeWalmartImageUrlString(raw) {
  if (!raw || typeof raw !== 'string') return null;
  let s = raw.trim();
  if (s.startsWith('//')) s = `https:${s}`;
  // Walmart embeds image URLs inside JSON with escaped slashes.
  s = s.replace(/\\u002f/gi, '/').replace(/\\\//g, '/');
  try {
    const p = new URL(s);
    if (!isAllowedWalmartCdnImageUrl(p.href)) return null;
    // Drop non-photo assets (Walmart mixes in interactive-video.svg etc.).
    if (/\.svg(\?|$)/i.test(p.pathname || '')) return null;
    // Strip sizing query (?odnHeight=117&odnWidth=117) so we mirror the full-res asset.
    return `${p.origin}${p.pathname}`;
  } catch {
    return null;
  }
}

/**
 * Resolve a Walmart share/affiliate short link to a real product URL. The provider can't
 * scrape walmrt.us / goto.walmart.com directly, but the destination item id is embedded in
 * the goto.walmart.com redirect (?u=... / ?prodsku=...). Returns '' if it can't resolve.
 */
async function resolveWalmartShortLinkToProductUrl(shortUrl) {
  try {
    let current = String(shortUrl || '').trim();
    if (!current) return '';
    for (let hop = 0; hop < 4; hop++) {
      let u;
      try {
        u = new URL(current);
      } catch {
        return '';
      }
      const host = normalizeUrlHostname(u.hostname);
      // Already a usable product URL.
      if ((host === 'walmart.com' || host.endsWith('.walmart.com')) && /\/ip\//i.test(u.pathname || '')) {
        return current;
      }
      // goto.walmart.com carries the destination in ?u= and the item id in ?prodsku=.
      if (host === 'goto.walmart.com' || host.endsWith('.goto.walmart.com')) {
        const uParam = u.searchParams.get('u');
        if (uParam) {
          let dest = uParam;
          try {
            dest = decodeURIComponent(uParam);
          } catch {
            /* use raw */
          }
          const idFromDest = extractWalmartItemIdFromUrl(dest) || (dest.match(/\/ip\/(\d{5,15})/i)?.[1] || '');
          if (idFromDest) return `https://www.walmart.com/ip/${idFromDest}`;
          if (/\/ip\//i.test(dest)) return dest;
        }
        const sku = u.searchParams.get('prodsku');
        if (sku && /^\d{5,15}$/.test(sku)) return `https://www.walmart.com/ip/${sku}`;
      }
      // Otherwise follow one redirect hop and re-inspect.
      const resp = await fetchWithTimeout(current, { redirect: 'manual' }, 12000).catch(() => null);
      const loc = resp && resp.headers ? resp.headers.get('location') : null;
      if (!loc) return '';
      try {
        current = new URL(loc, current).href;
      } catch {
        return '';
      }
    }
    return '';
  } catch {
    return '';
  }
}

/** Recursively pull Product images out of a JSON-LD node using a CDN-specific normalizer. */
function collectLdJsonProductImagesWith(node, out, seen, normalizeFn) {
  if (node == null) return;
  if (Array.isArray(node)) {
    node.forEach((x) => collectLdJsonProductImagesWith(x, out, seen, normalizeFn));
    return;
  }
  if (typeof node !== 'object') return;
  if (node['@graph']) collectLdJsonProductImagesWith(node['@graph'], out, seen, normalizeFn);
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
      const n = raw ? normalizeFn(raw) : null;
      if (n && !seen.has(n)) {
        seen.add(n);
        out.push(n);
      }
    }
  }
  for (const key of Object.keys(node)) {
    if (key === '@context' || key === '@type' || key === '@id' || key === '@graph') continue;
    collectLdJsonProductImagesWith(node[key], out, seen, normalizeFn);
  }
}

function extractWalmartProductImageUrlsFromHtml(html, pageUrl = '') {
  if (!html || typeof html !== 'string') return [];
  const ordered = [];
  const seen = new Set();
  const push = (raw) => {
    const n = normalizeWalmartImageUrlString(raw);
    if (!n || seen.has(n)) return;
    seen.add(n);
    ordered.push(n);
  };
  let m;
  const ogRe = /<meta[^>]+property=["']og:image["'][^>]*content=["']([^"']+)["'][^>]*>/gi;
  while ((m = ogRe.exec(html)) !== null) push(m[1]);
  const ogReAlt = /<meta[^>]+content=["']([^"']+)["'][^>]*property=["']og:image["'][^>]*>/gi;
  while ((m = ogReAlt.exec(html)) !== null) push(m[1]);
  const twRe = /<meta[^>]+name=["']twitter:image["'][^>]*content=["']([^"']+)["'][^>]*>/gi;
  while ((m = twRe.exec(html)) !== null) push(m[1]);

  const scriptRe = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  while ((m = scriptRe.exec(html)) !== null) {
    try {
      const jsonText = m[1].trim();
      if (!jsonText) continue;
      const out = [];
      const seenLd = new Set();
      collectLdJsonProductImagesWith(JSON.parse(jsonText), out, seenLd, normalizeWalmartImageUrlString);
      out.forEach(push);
    } catch {
      /* ignore */
    }
  }

  // Gallery URLs embedded in __NEXT_DATA__ / inline JSON (escaped or plain).
  const cdnRe = /https?:(?:\\u002f|\\\/|\/){2}i5\.walmartimages\.com(?:\\u002f|\\\/|\/)[^"'\\\s)]+/gi;
  let c;
  while ((c = cdnRe.exec(html)) !== null && ordered.length < 12) push(c[0]);

  return ordered.slice(0, 12);
}

/** Walmart bot/consent interstitials surface as generic/empty titles. */
function looksLikeBlockedWalmartTitle(title) {
  const t = String(title || '').trim().toLowerCase();
  if (!t) return true;
  if (t === 'walmart.com' || t.startsWith('walmart.com')) return true;
  if (t.includes('robot or human')) return true;
  if (t.includes('access denied')) return true;
  if (t.includes('activate and hold')) return true;
  if (t.includes('are you a human')) return true;
  return false;
}

// Short-lived cache so strategic fan-out doesn't hammer RapidAPI for the same item.
const WALMART_RAPIDAPI_CACHE_TTL_MS = 2 * 60 * 1000;
const walmartRapidApiCache = new Map();
const walmartRapidApiInFlight = new Map();

function deepCollectWalmartImageUrls(node, out, seen, cap = 12) {
  if (node == null || out.length >= cap) return;
  if (typeof node === 'string') {
    if (/walmartimages\.com/i.test(node)) {
      const n = normalizeWalmartImageUrlString(node);
      if (n && !seen.has(n)) {
        seen.add(n);
        out.push(n);
      }
    }
    return;
  }
  if (Array.isArray(node)) {
    for (const x of node) deepCollectWalmartImageUrls(x, out, seen, cap);
    return;
  }
  if (typeof node === 'object') {
    for (const k of Object.keys(node)) deepCollectWalmartImageUrls(node[k], out, seen, cap);
  }
}

/** Accept any https(s) image URL the provider returns (not just walmartimages CDN). */
function normalizeProviderImageUrlString(raw) {
  if (!raw || typeof raw !== 'string') return null;
  let s = raw.trim();
  if (!s) return null;
  if (s.startsWith('//')) s = `https:${s}`;
  s = s.replace(/\\u002f/gi, '/').replace(/\\\//g, '/');
  try {
    const u = new URL(s);
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
    if (/\.svg(\?|$)/i.test(u.pathname || '')) return null;
    return u.href;
  } catch {
    return null;
  }
}

/**
 * RapidAPI Walmart product data — locked to "Walmart Data" by mahmudulhasandev:
 *   https://rapidapi.com/mahmudulhasandev/api/walmart-data
 *   GET https://walmart-data.p.rapidapi.com/product-details.php?url=<full product URL>
 *   → { original_status, pc_status, url, body: { title, price, currency, images:[...], ratings, reviewsCount } }
 *
 * Only RAPIDAPI_KEY is required (host/endpoint default to this provider). To swap providers later,
 * set RAPIDAPI_WALMART_HOST and RAPIDAPI_WALMART_URL_TEMPLATE ({host},{itemId},{url} placeholders).
 * Returns { title, description, images:[urls] } or null.
 */
async function fetchWalmartProductDataViaRapidApi({ itemId, url }) {
  let cacheKey = '';
  try {
    const key = String(process.env.RAPIDAPI_KEY || '').trim();
    const host =
      String(process.env.RAPIDAPI_WALMART_HOST || '').trim() || 'walmart-data.p.rapidapi.com';
    if (!key) {
      console.warn('[walmart] RAPIDAPI_KEY is not set in this environment — falling back to scrape (will hit Walmart bot wall).');
      return null;
    }
    const id = String(itemId || '').trim();
    let link = String(url || '').trim();
    // This provider keys off the full product URL; bail if we have neither.
    if (!id && !link) return null;

    // Resolve share/affiliate/blocked URLs to a canonical walmart.com/ip/<id> the provider accepts.
    if (link && isWalmartUrlEligibleForApi(link)) {
      const resolved = await resolveWalmartProductUrlForApi(link);
      if (resolved) link = resolved;
    }
    // If we only have an item id, synthesize a product URL the provider accepts.
    if (!link && id) link = `https://www.walmart.com/ip/${id}`;

    cacheKey = `${host}::${link || id}`;
    const now = Date.now();
    const cached = walmartRapidApiCache.get(cacheKey);
    if (cached && cached.data && now - cached.ts < WALMART_RAPIDAPI_CACHE_TTL_MS) {
      return cached.data;
    }
    const inflight = walmartRapidApiInFlight.get(cacheKey);
    if (inflight) return await inflight;

    const template =
      String(process.env.RAPIDAPI_WALMART_URL_TEMPLATE || '').trim() ||
      'https://{host}/product-details.php?url={url}';
    const endpoint = template
      .replace(/\{host\}/g, host)
      .replace(/\{itemId\}/g, encodeURIComponent(id))
      .replace(/\{url\}/g, encodeURIComponent(link));

    const p = (async () => {
      const resp = await fetchWithTimeout(
        endpoint,
        {
          headers: {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host,
            Accept: 'application/json',
          },
        },
        45000 // this provider scrapes live; product pages can take ~30s.
      );
      if (!resp.ok) {
        const snippet = await resp.text().catch(() => '');
        console.warn(
          `[walmart] RapidAPI ${resp.status} for ${host} — ${snippet.slice(0, 160)} (key …${key.slice(-4)})`
        );
        return null;
      }
      const json = await resp.json().catch(() => null);
      if (!json || typeof json !== 'object') return null;
      if (json.pc_status && json.pc_status >= 400) {
        console.warn(`[walmart] provider could not scrape ${link} — pc_status ${json.pc_status} ${String(json.error || '').slice(0, 120)}`);
      }

      // walmart-data wraps everything in `body`; tolerate flatter shapes too.
      const body = json.body && typeof json.body === 'object' ? json.body : json;

      const images = [];
      const seen = new Set();
      if (Array.isArray(body.images)) {
        for (const im of body.images) {
          const raw = typeof im === 'string' ? im : im && typeof im === 'object' ? im.url || im.src : '';
          // Prefer the Walmart CDN normalizer (strips ?odnHeight sizing → full-res, drops svg).
          const n = normalizeWalmartImageUrlString(raw) || normalizeProviderImageUrlString(raw);
          if (n && !seen.has(n)) {
            seen.add(n);
            images.push(n);
          }
          if (images.length >= 12) break;
        }
      }
      // Fallback: deep-scan for walmartimages CDN URLs if the provider shape changes.
      if (images.length === 0) {
        deepCollectWalmartImageUrls(json, images, seen, 12);
      }

      const title = String(
        body.title || body.name || body.productName || json.title || ''
      ).trim();
      const description = String(
        body.description || body.shortDescription || body.short_description || ''
      )
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

      const data = { title, description, images };
      console.log(
        `[walmart] RapidAPI ok — title:"${title.slice(0, 60)}" images:${images.length} (${String(link).slice(0, 80)})`
      );
      walmartRapidApiCache.set(cacheKey, { ts: Date.now(), data });
      return data;
    })();
    walmartRapidApiInFlight.set(cacheKey, p);
    return await p;
  } catch (e) {
    console.warn('fetchWalmartProductDataViaRapidApi:', e.message || e);
    return null;
  } finally {
    if (cacheKey) {
      try {
        walmartRapidApiInFlight.delete(cacheKey);
      } catch {
        /* ignore */
      }
    }
  }
}

/**
 * Harvest Walmart product reference images (mirrored to Supabase → public URLs) for Nano Banana.
 * HTML scrape first, RapidAPI fallback when Walmart blocks the scrape.
 * Returns { images: string[], rapid: {title,description,images}|null }.
 */
async function harvestWalmartReferenceImages(walmartUrl, userId, preRapid = null) {
  const result = { images: [], rapid: preRapid || null };
  try {
    let candidates = [];
    if (preRapid && Array.isArray(preRapid.images) && preRapid.images.length > 0) {
      candidates = preRapid.images;
    }
    if (candidates.length === 0) {
      try {
        const html = await fetchArticleHtml(walmartUrl);
        candidates = extractWalmartProductImageUrlsFromHtml(html, walmartUrl);
      } catch {
        /* ignore */
      }
    }
    if (candidates.length === 0) {
      const rapid = await fetchWalmartProductDataViaRapidApi({
        itemId: extractWalmartItemIdFromUrl(walmartUrl),
        url: walmartUrl,
      });
      result.rapid = rapid;
      if (rapid && Array.isArray(rapid.images)) candidates = rapid.images;
    }
    if (candidates.length > 0) {
      result.images = await mirrorGenericPageImageUrlsForNanoBanana(candidates.slice(0, 3), userId);
    }
  } catch (e) {
    console.warn('harvestWalmartReferenceImages:', e.message || e);
  }
  return result;
}

/** Nano Banana + reference images often 422 or look wrong on infographic-style layouts. */
function isInfographicLikeStyleId(styleId) {
  const s = String(styleId || '').trim().toLowerCase();
  if (!s) return false;
  // Explicit known styles
  if (s === 'timeline_infographic' || s === 'step_cards_3') return true;
  // Generic guard for any future "infographic" layouts
  if (s.includes('infographic')) return true;
  return false;
}

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
    if (!isInfographicLikeStyleId(styles[i])) continue;
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
  if (!isInfographicLikeStyleId(styleId)) return styleId;
  return AMAZON_REF_NON_INFOGRAPHIC_FALLBACKS[0];
}

/** Storage path prefix for anonymous free-preview reference image mirrors (no auth). */
const PREVIEW_ANON_STORAGE_USER_ID = 'preview-anonymous';

/** Reuse mirrored ref images across strategic_single fan-out (same URL, many pins). */
const NANO_REF_HARVEST_CACHE_TTL_MS = Math.max(
  60_000,
  parseInt(process.env.URLTOPIN_REF_HARVEST_CACHE_TTL_MS || '600000', 10) || 600000
);
const nanoRefHarvestCache = new Map();

function nanoRefHarvestCacheKey(userId, url, manualProduct) {
  const manualSig =
    manualProduct?.imageUrls?.length > 0 ? manualProduct.imageUrls.slice(0, 3).join('|') : '';
  return `${String(userId || '')}::${String(url || '').trim().toLowerCase()}::${manualSig}`;
}

function getNanoRefHarvestFromCache(key) {
  const hit = nanoRefHarvestCache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) {
    nanoRefHarvestCache.delete(key);
    return null;
  }
  return { images: [...hit.images], source: hit.source };
}

function setNanoRefHarvestCache(key, images, source) {
  if (!key || !Array.isArray(images) || images.length === 0) return;
  nanoRefHarvestCache.set(key, {
    images: [...images],
    source: source || null,
    expiresAt: Date.now() + NANO_REF_HARVEST_CACHE_TTL_MS,
  });
  if (nanoRefHarvestCache.size > 200) {
    const oldest = nanoRefHarvestCache.keys().next().value;
    if (oldest) nanoRefHarvestCache.delete(oldest);
  }
}

function appendNanoBananaReferencePromptSuffix(imagePrompt, referenceSource) {
  if (!imagePrompt || !referenceSource) return imagePrompt;
  if (referenceSource === 'amazon_product') {
    return (
      `${imagePrompt} ` +
      promptTier(
        'Attached reference image(s) show the real product from the Amazon listing. Use them as the primary hero subject: preserve packaging shape, brand marks, colors, and overall silhouette. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line. Integrate the product naturally; avoid duplicating it as a meaningless second copy unless the layout style requires a collage.',
        'Reference: use attached Amazon product photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.'
      )
    );
  }
  if (referenceSource === 'walmart_product') {
    return (
      `${imagePrompt} ` +
      promptTier(
        'Attached reference image(s) show the real product from the Walmart listing. Use them as the primary hero subject: preserve packaging shape, brand marks, colors, and overall silhouette. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line. Integrate the product naturally; avoid duplicating it as a meaningless second copy unless the layout style requires a collage.',
        'Reference: use attached Walmart product photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.'
      )
    );
  }
  if (referenceSource === 'etsy_product') {
    return (
      `${imagePrompt} ` +
      promptTier(
        'Attached reference image(s) are from the Etsy listing (product photos). Use them as the primary hero subject: preserve jewelry/product shape, materials, colors, and overall look. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
        'Reference: use attached Etsy listing photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.'
      )
    );
  }
  return (
    `${imagePrompt} ` +
    promptTier(
      'Attached reference image(s) come from the source page (hero or content photos). Use them as the primary visual subject when helpful: preserve recognizable subjects, colors, and composition; do not paste URL text or watermarks as new text. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
      'Reference: prefer attached page photos as the hero when they are strong; keep headline/sub/footer as specified.'
    )
  );
}

/**
 * Product / page reference images for Nano Banana (generate + free preview).
 * @returns {{ images: string[], source: string|null }}
 */
async function harvestNanoBananaReferenceImagesForUrlToPin({
  userId,
  workingUrl,
  base,
  usePageReferenceImages = true,
  manualProduct = null,
}) {
  const result = { images: [], source: null };
  if (!userId || process.env.USE_DUMMY_IMAGES === 'true') return result;

  const amazonCtxUrl = pickAmazonContextUrl(workingUrl, base?.canonicalUrl);
  const refCacheKey = nanoRefHarvestCacheKey(userId, amazonCtxUrl || workingUrl, manualProduct);
  const cached = getNanoRefHarvestFromCache(refCacheKey);
  if (cached) {
    console.log(
      `urltopin: Nano Banana reference images (cached): ${cached.images.length} (${String(amazonCtxUrl || workingUrl).slice(0, 96)})`
    );
    return { images: cached.images, source: cached.source };
  }
  const walmartCtxUrl = pickWalmartContextUrl(workingUrl, base?.canonicalUrl);
  const refHtmlUrl = amazonCtxUrl || workingUrl;
  const amazonRapid = base?.amazon_rapidapi_data || null;
  let walmartRapid = base?.walmart_rapidapi_data || null;

  const rapidEtsyUrls = Array.isArray(base?.etsy_rapidapi_image_urls) ? base.etsy_rapidapi_image_urls : [];
  if (rapidEtsyUrls.length > 0) {
    try {
      result.images = await mirrorGenericPageImageUrlsForNanoBanana(rapidEtsyUrls.slice(0, 3), userId);
      if (result.images.length > 0) result.source = 'etsy_product';
    } catch (e) {
      console.warn('harvestNanoBananaReferenceImagesForUrlToPin Etsy RapidAPI:', e.message || e);
    }
  }
  if (result.images.length === 0) {
    const etsyThumb = String(base?.etsy_oembed_thumbnail || '').trim();
    if (etsyThumb) {
      try {
        result.images = await mirrorGenericPageImageUrlsForNanoBanana([etsyThumb], userId);
        if (result.images.length > 0) result.source = 'page';
      } catch (e) {
        console.warn('harvestNanoBananaReferenceImagesForUrlToPin Etsy oEmbed:', e.message || e);
      }
    }
  }

  if (
    result.images.length === 0 &&
    process.env.URLTOPIN_AMAZON_PRODUCT_IMAGES !== '0' &&
    isAmazonProductPageForNanoReference(amazonCtxUrl)
  ) {
    try {
      if (amazonRapid && Array.isArray(amazonRapid.images) && amazonRapid.images.length > 0) {
        const candidates = amazonRapid.images
          .map((im) => (im && typeof im === 'object' ? im.hi_res || im.image || im.large || '' : ''))
          .filter(Boolean);
        if (candidates.length > 0) {
          result.images = await mirrorAmazonImageUrlsForNanoBanana(candidates, userId);
          if (result.images.length > 0) result.source = 'amazon_product';
        }
      }
      if (result.images.length === 0) {
        const azHtml = await fetchArticleHtml(amazonCtxUrl);
        let candidates = extractAmazonProductImageUrlsFromHtml(azHtml, amazonCtxUrl);
        if (candidates.length === 0 || detectAmazonBotOrConsentPage(azHtml)) {
          const widgetImg = await fetchAmazonAsinWidgetImageUrl(amazonCtxUrl);
          if (widgetImg) candidates = [widgetImg];
        }
        if (candidates.length > 0) {
          result.images = await mirrorAmazonImageUrlsForNanoBanana(candidates, userId);
          if (result.images.length > 0) result.source = 'amazon_product';
        }
      }
    } catch (e) {
      console.warn('harvestNanoBananaReferenceImagesForUrlToPin Amazon:', e.message || e);
    }
  }

  if (
    result.images.length === 0 &&
    process.env.URLTOPIN_WALMART_PRODUCT_IMAGES !== '0' &&
    isWalmartProductPageForNanoReference(walmartCtxUrl)
  ) {
    try {
      const harvested = await harvestWalmartReferenceImages(walmartCtxUrl, userId, walmartRapid);
      result.images = harvested.images;
      if (harvested.images.length > 0) result.source = 'walmart_product';
    } catch (e) {
      console.warn('harvestNanoBananaReferenceImagesForUrlToPin Walmart:', e.message || e);
    }
  }

  if (
    result.images.length === 0 &&
    usePageReferenceImages &&
    process.env.URLTOPIN_PAGE_REFERENCE_IMAGES !== '0'
  ) {
    try {
      let pageHtml = '';
      if (!isEtsyListingPageUrl(refHtmlUrl)) {
        pageHtml = await fetchArticleHtml(refHtmlUrl);
      }
      if (pageHtml && pageHtml.length > 200) {
        const candidates = extractGenericPageImageUrlsFromHtml(pageHtml, refHtmlUrl);
        if (candidates.length > 0) {
          result.images = await mirrorGenericPageImageUrlsForNanoBanana(candidates, userId);
          if (result.images.length > 0) result.source = 'page';
        }
      }
    } catch (e) {
      console.warn('harvestNanoBananaReferenceImagesForUrlToPin page refs:', e.message || e);
    }
  }

  if (result.images.length > 0) {
    setNanoRefHarvestCache(refCacheKey, result.images, result.source);
  }
  return result;
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

/** Numeric listing id from `https://www.etsy.com/listing/123/slug` (any Etsy host). */
function extractEtsyListingIdFromUrl(urlString) {
  try {
    const raw = String(urlString || '').trim();
    if (!raw) return '';
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (!isEtsyHost(u.hostname)) return '';
    const m = (u.pathname || '').match(/\/listing\/(\d{6,})(?:\/|$)/i);
    return m ? m[1] : '';
  } catch {
    return '';
  }
}

/** True for `/listing/{id}/…` on Etsy — listing HTML is not fetchable server-side (403); use RapidAPI + oEmbed only. */
function isEtsyListingPageUrl(urlString) {
  return !!extractEtsyListingIdFromUrl(urlString);
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

function detectAmazonBotOrConsentPage(html) {
  if (!html || typeof html !== 'string') return false;
  const s = html.slice(0, 90000).toLowerCase();
  // Common Amazon interstitial / bot / consent patterns.
  if (s.includes('click the button below to continue shopping')) return true;
  if (s.includes('type the characters you see in this image')) return true;
  if (s.includes('enter the characters you see')) return true;
  if (s.includes('sorry, we just need to make sure you\'re not a robot')) return true;
  if (s.includes('robot check')) return true;
  if (s.includes('validatecaptcha')) return true;
  if (s.includes('captcha')) return true;
  return false;
}

function looksLikeGenericAmazonTitle(title) {
  const t = String(title || '').trim().toLowerCase();
  if (!t) return true;
  if (t === 'amazon.com') return true;
  if (t === 'amazon') return true;
  if (t.startsWith('amazon.com') && !t.includes(':')) return true;
  if (t.includes('continue shopping')) return true;
  if (t.includes('robot check')) return true;
  if (t.includes('captcha')) return true;
  return false;
}

function resolveAmazonMarketplaceCodeFromHost(hostname) {
  const h = String(hostname || '').toLowerCase();
  // Minimal mapping; extend as needed.
  if (h.endsWith('.co.uk')) return 'GB';
  if (h.endsWith('.de')) return 'DE';
  if (h.endsWith('.fr')) return 'FR';
  if (h.endsWith('.it')) return 'IT';
  if (h.endsWith('.es')) return 'ES';
  if (h.endsWith('.ca')) return 'CA';
  if (h.endsWith('.com.au')) return 'AU';
  if (h.endsWith('.co.jp') || h.endsWith('.jp')) return 'JP';
  if (h.endsWith('.in')) return 'IN';
  // Default for amazon.com
  return 'US';
}

function resolveRapidApiAmazonMarketplaceFromHost(hostname) {
  const h = String(hostname || '').toLowerCase();
  // RapidAPI example uses marketplace=com|co.uk|de etc.
  if (h.endsWith('.co.uk')) return 'co.uk';
  if (h.endsWith('.com.au')) return 'com.au';
  const m = h.match(/amazon\.([a-z.]{2,})$/i);
  if (m && m[1]) return m[1].toLowerCase();
  return 'com';
}

// Short-lived cache to avoid hammering RapidAPI when URL→Pin strategic mode fans out multiple calls.
const AMAZON_RAPIDAPI_CACHE_TTL_MS = 2 * 60 * 1000;
const amazonRapidApiCache = new Map(); // key -> { ts, data }
const amazonRapidApiInFlight = new Map(); // key -> Promise<data|null>

async function fetchAmazonProductDataViaRapidApi({ asin, marketplace, language }) {
  let cacheKey = '';
  try {
    const key = String(process.env.RAPIDAPI_KEY || '').trim();
    const host = String(process.env.RAPIDAPI_AMAZON_HOST || '').trim() ||
      'real-time-amazon-data-the-most-complete.p.rapidapi.com';
    if (!key || !asin) return null;

    const mp = String(marketplace || 'com').trim();
    const lang = String(language || 'en').trim();
    cacheKey = `${host}::${mp}::${lang}::${String(asin).toUpperCase()}`;
    const now = Date.now();
    const cached = amazonRapidApiCache.get(cacheKey);
    if (cached && cached.data && now - cached.ts < AMAZON_RAPIDAPI_CACHE_TTL_MS) {
      return cached.data;
    }
    const inflight = amazonRapidApiInFlight.get(cacheKey);
    if (inflight) {
      return await inflight;
    }
    const url =
      `https://${host}/product-details?asin=${encodeURIComponent(asin)}&marketplace=${encodeURIComponent(mp)}&language=${encodeURIComponent(lang)}`;

    const p = (async () => {
      const resp = await fetchWithTimeout(
        url,
        {
          headers: {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host,
            Accept: 'application/json',
          },
        },
        20000
      );
      if (!resp.ok) return null;
      const json = await resp.json().catch(() => null);
      if (!json || json.status !== true || !json.data) return null;
      amazonRapidApiCache.set(cacheKey, { ts: Date.now(), data: json.data });
      return json.data;
    })();
    amazonRapidApiInFlight.set(cacheKey, p);
    const data = await p;
    return data;
  } catch (e) {
    console.warn('fetchAmazonProductDataViaRapidApi:', e.message || e);
    return null;
  } finally {
    if (cacheKey) {
      try {
        amazonRapidApiInFlight.delete(cacheKey);
      } catch {
        /* ignore */
      }
    }
  }
}

function buildAmazonRapidApiSummary(data) {
  if (!data || typeof data !== 'object') return '';
  const parts = [];
  const title = typeof data.title === 'string' ? data.title.trim() : '';
  const desc = typeof data.description === 'string' ? data.description.trim() : '';
  const brand =
    data.tech_specs && typeof data.tech_specs === 'object' && typeof data.tech_specs.brand_name === 'string'
      ? data.tech_specs.brand_name.trim()
      : '';
  const bullets = Array.isArray(data.bullet_points)
    ? data.bullet_points.map((b) => String(b || '').trim()).filter(Boolean)
    : [];
  const cats = Array.isArray(data.category_path)
    ? data.category_path.map((c) => (c && typeof c === 'object' && c.name ? String(c.name).trim() : '')).filter(Boolean)
    : [];

  if (title) parts.push(`Product: ${title}`);
  if (brand) parts.push(`Brand: ${brand}`);
  if (cats.length) parts.push(`Category: ${cats.slice(-3).join(' > ')}`);
  if (desc) parts.push(`Description: ${desc}`);
  if (bullets.length) parts.push(`Key points: ${bullets.slice(0, 8).join(' • ')}`);

  const out = parts.join('\n');
  return out.slice(0, 1400);
}

async function fetchAmazonAsinWidgetImageUrl(amazonUrlString) {
  try {
    const asin = extractAmazonAsinFromUrl(amazonUrlString);
    if (!asin) return '';
    const u = new URL(String(amazonUrlString || '').trim());
    const mp = resolveAmazonMarketplaceCodeFromHost(u.hostname);
    const widgetHost = mp === 'US' ? 'ws-na.amazon-adsystem.com' : 'ws-eu.amazon-adsystem.com';
    const widgetUrl =
      `https://${widgetHost}/widgets/q?` +
      `_encoding=UTF8&MarketPlace=${encodeURIComponent(mp)}` +
      `&ASIN=${encodeURIComponent(asin)}` +
      `&ServiceVersion=20070822&ID=AsinImage&WS=1&Format=_SL500_`;

    const resp = await fetchWithTimeout(
      widgetUrl,
      { redirect: 'follow', headers: { ...URL_SCRAPE_HEADERS, Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8' } },
      20000
    );
    if (!resp.ok) return '';
    const ct = (resp.headers.get('content-type') || '').toLowerCase();
    if (!ct.startsWith('image/')) return '';
    // Final response URL is usually the actual CDN image URL.
    const finalUrl = resp.url || '';
    return isAllowedAmazonCdnImageUrl(finalUrl) ? finalUrl : '';
  } catch {
    return '';
  }
}

async function fetchEtsyOembed(listingUrl) {
  try {
    const raw = String(listingUrl || '').trim();
    if (!raw) return null;
    const u = new URL(raw);
    if (!isEtsyHost(u.hostname)) return null;
    const oembedUrl = `https://www.etsy.com/oembed?url=${encodeURIComponent(u.href)}&format=json`;
    const resp = await fetchWithTimeout(
      oembedUrl,
      { headers: { ...URL_SCRAPE_HEADERS, Accept: 'application/json' } },
      20000
    );
    if (!resp.ok) return null;
    const json = await resp.json().catch(() => null);
    if (!json || typeof json !== 'object') return null;
    return {
      title: typeof json.title === 'string' ? json.title.trim() : '',
      thumbnail_url: typeof json.thumbnail_url === 'string' ? json.thumbnail_url.trim() : '',
      provider_name: typeof json.provider_name === 'string' ? json.provider_name.trim() : '',
    };
  } catch (e) {
    console.warn('fetchEtsyOembed:', e.message || e);
    return null;
  }
}

function decodeHtmlEntitiesBasic(s) {
  return String(s || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#0*39;/g, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#(\d+);/g, (_, n) => {
      const c = Number.parseInt(n, 10);
      return Number.isFinite(c) && c >= 32 && c !== 127 ? String.fromCharCode(c) : _;
    })
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => {
      const c = Number.parseInt(h, 16);
      return Number.isFinite(c) && c >= 32 && c !== 127 ? String.fromCharCode(c) : _;
    });
}

function collectEtsyRapidApiImageUrls(data) {
  if (!data || typeof data !== 'object') return [];
  const out = [];
  const seen = new Set();
  const push = (s) => {
    const u = String(s || '').trim();
    if (!u || seen.has(u)) return;
    if (!isAllowedGenericReferenceImageUrl(u)) return;
    seen.add(u);
    out.push(u);
  };
  if (typeof data.mainImage === 'string') push(data.mainImage);
  if (Array.isArray(data.images)) {
    for (const im of data.images) {
      if (typeof im === 'string') push(im);
    }
  }
  return out.slice(0, 14);
}

// Same TTL pattern as Amazon RapidAPI (strategic fan-out / parallel styles).
const ETSY_RAPIDAPI_CACHE_TTL_MS = 2 * 60 * 1000;
const etsyRapidApiCache = new Map();
const etsyRapidApiInFlight = new Map();

/**
 * Etsy listing product payload from RapidAPI (`etsy-api2` style: `{ data: { title, description, images, ... } }`).
 * Uses `RAPIDAPI_KEY` and optional `RAPIDAPI_ETSY_HOST` (default: etsy-api2.p.rapidapi.com).
 */
async function fetchEtsyProductDataViaRapidApi(listingId) {
  let cacheKey = '';
  try {
    const key = String(process.env.RAPIDAPI_KEY || '').trim();
    const host =
      String(process.env.RAPIDAPI_ETSY_HOST || '').trim() || 'etsy-api2.p.rapidapi.com';
    const id = String(listingId || '').trim();
    if (!key || !id || !/^\d+$/.test(id)) return null;

    cacheKey = `${host}::etsy::${id}`;
    const now = Date.now();
    const cached = etsyRapidApiCache.get(cacheKey);
    if (cached && cached.data && now - cached.ts < ETSY_RAPIDAPI_CACHE_TTL_MS) {
      return cached.data;
    }
    const inflight = etsyRapidApiInFlight.get(cacheKey);
    if (inflight) {
      return await inflight;
    }

    const url = `https://${host}/product/description?listingId=${encodeURIComponent(id)}`;

    const p = (async () => {
      const resp = await fetchWithTimeout(
        url,
        {
          headers: {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host,
            Accept: 'application/json',
          },
        },
        20000
      );
      if (!resp.ok) return null;
      const json = await resp.json().catch(() => null);
      const data = json && typeof json === 'object' ? json.data : null;
      if (!data || typeof data !== 'object') return null;
      etsyRapidApiCache.set(cacheKey, { ts: Date.now(), data });
      return data;
    })();
    etsyRapidApiInFlight.set(cacheKey, p);
    return await p;
  } catch (e) {
    console.warn('fetchEtsyProductDataViaRapidApi:', e.message || e);
    return null;
  } finally {
    if (cacheKey) {
      try {
        etsyRapidApiInFlight.delete(cacheKey);
      } catch {
        /* ignore */
      }
    }
  }
}

/**
 * Enrich `base` for Etsy listing URLs: RapidAPI when `RAPIDAPI_KEY` + listing id, else oEmbed for title/thumb gaps.
 * Sets `etsy_rapidapi_image_urls` for Nano Banana mirroring when RapidAPI returns images.
 */
async function enrichEtsyListingBaseFromApis(base, pageUrlString) {
  try {
    const raw = String(pageUrlString || '').trim();
    if (!raw) return;
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (!isEtsyHost(u.hostname)) return;

    const listingId = extractEtsyListingIdFromUrl(raw);
    let rapid = null;
    if (listingId && String(process.env.RAPIDAPI_KEY || '').trim()) {
      rapid = await fetchEtsyProductDataViaRapidApi(listingId);
    }

    if (rapid && typeof rapid === 'object') {
      const t = typeof rapid.title === 'string' ? decodeHtmlEntitiesBasic(rapid.title.trim()) : '';
      if (t) {
        const cur = String(base.title || '').trim();
        if (cur.length < 3) {
          base.title = t.slice(0, 180);
        } else if (cur.length < 14) {
          base.title = t.slice(0, 180);
        }
      }
      const d = typeof rapid.description === 'string' ? decodeHtmlEntitiesBasic(rapid.description.trim()) : '';
      if (d) {
        const curD = String(base.description || '').trim();
        if (curD.length < 40) {
          base.description = d.slice(0, 450);
        }
      }
      const imgs = collectEtsyRapidApiImageUrls(rapid);
      if (imgs.length) {
        base.etsy_rapidapi_image_urls = imgs;
        base.etsy_oembed_thumbnail = imgs[0];
      }
      base.etsy_rapidapi_listing_id = listingId;
    }

    if (!base.title || String(base.title).trim().length < 3) {
      const oe = await fetchEtsyOembed(u.href);
      if (oe) {
        if (!base.title && oe.title) base.title = oe.title.slice(0, 180);
        if (oe.thumbnail_url && !base.etsy_oembed_thumbnail) base.etsy_oembed_thumbnail = oe.thumbnail_url;
      }
    }
  } catch (e) {
    console.warn('enrichEtsyListingBaseFromApis:', e.message || e);
  }
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
    if (
      /(product-images?|\/products\/|item-images?|\/uploads\/|\/media\/|featured-image|hero-image)/i.test(
        path
      )
    ) {
      s += 28;
    }
    if (meta.isMainImage) s += 22;
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
    const idM = /\bid\s*=\s*["']([^"']+)["']/i.exec(tag);
    const classM = /\bclass\s*=\s*["']([^"']+)["']/i.exec(tag);
    const width = wM ? parseInt(wM[1], 10) : 0;
    const height = hM ? parseInt(hM[1], 10) : 0;
    const isMainImage =
      (idM && /(main|img|hero|product)/i.test(idM[1])) ||
      (classM && /(hero|product|featured|main-img|primary)/i.test(classM[1]));
    const fromSet = srcsetM ? parseSrcsetLargestUrl(srcsetM[1], pageUrl) : null;
    if (fromSet) {
      pushGenericCandidate(bucket, fromSet, pageUrl, { baseScore: 12, width, height, isMainImage });
    } else if (srcM) {
      pushGenericCandidate(bucket, srcM[1], pageUrl, { baseScore: 8, width, height, isMainImage });
    }
  }

  const byUrl = new Map();
  for (const c of bucket) {
    const prev = byUrl.get(c.url);
    if (!prev || c.score > prev) byUrl.set(c.url, c.score);
  }
  let ranked = [...byUrl.entries()]
    .map(([url, score]) => ({ url, score }))
    .filter((x) => x.score >= PAGE_REF_MIN_SCORE)
    .sort((a, b) => b.score - a.score);
  if (ranked.length === 0 && byUrl.size > 0) {
    const fallback = [...byUrl.entries()]
      .map(([url, score]) => ({ url, score }))
      .sort((a, b) => b.score - a.score)[0];
    if (fallback && fallback.score >= 8) {
      ranked = [fallback];
    }
  }
  return ranked.slice(0, PAGE_REF_MAX_CANDIDATES).map((x) => x.url);
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

function isPinterestOutboundHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  if (h === 'pin.it') return true;
  if (h === 'pinterest.com' || h.endsWith('.pinterest.com') || /^pinterest\.[a-z.]{2,}$/i.test(h)) return true;
  return false;
}

function isPrivateOrReservedIp(ip) {
  if (!ip) return false;
  const v = net.isIP(ip);
  if (v === 4) {
    const [a, b] = ip.split('.').map((x) => Number.parseInt(x, 10));
    if (!Number.isFinite(a) || !Number.isFinite(b)) return true;
    if (a === 10) return true;
    if (a === 127) return true;
    if (a === 0) return true;
    if (a === 169 && b === 254) return true;
    if (a === 192 && b === 168) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 100 && b >= 64 && b <= 127) return true; // CGNAT
    if (a === 192 && b === 0 && Number.parseInt(ip.split('.')[2] || '0', 10) === 2) return true; // 192.0.2.0/24 doc
    return false;
  }
  if (v === 6) {
    const s = ip.toLowerCase();
    if (s === '::1') return true;
    if (s.startsWith('fe80:')) return true; // link-local
    if (s.startsWith('fc') || s.startsWith('fd')) return true; // ULA
    if (s.startsWith('::ffff:')) {
      const m = s.match(/^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$/);
      if (m) return isPrivateOrReservedIp(m[1]);
    }
    return false;
  }
  return false;
}

function assertSafePublicHttpUrl(u) {
  if (!(u instanceof URL)) throw new Error('invalid_url');
  if (u.username || u.password) throw new Error('url_credentials_not_allowed');
  if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error('url_protocol_not_allowed');
  const host = normalizeUrlHostname(u.hostname);
  if (!host) throw new Error('url_host_missing');
  if (host === 'localhost' || host.endsWith('.localhost')) throw new Error('url_host_blocked');
  if (host.endsWith('.local') || host.endsWith('.internal')) throw new Error('url_host_blocked');
  if (host === 'metadata.google.internal' || host.endsWith('.metadata.google.internal')) throw new Error('url_host_blocked');
  const ipVer = net.isIP(host);
  if (ipVer && isPrivateOrReservedIp(host)) throw new Error('url_ip_blocked');
  return true;
}

function shouldResolveOutboundUrlForUrlToPin(hostname) {
  const h = normalizeUrlHostname(hostname);
  if (!h) return false;
  if (isPinterestOutboundHost(h)) return false;
  if (isLikelyUrlShortenerHost(h)) return true;
  if (isCreatorAffiliatePlatformRedirectHost(h)) return true;
  if (isAffiliateTrackingRedirectHost(h)) return true;
  return false;
}

function isTemuRelatedHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  return h === 'temu.com' || h.endsWith('.temu.com');
}

function isTargetRelatedHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  return h === 'target.com' || h.endsWith('.target.com');
}

/** Merchant sites affiliate hops often land on before we have a product detail page. */
function isKnownAffiliateDestinationMerchantHost(host) {
  const h = normalizeUrlHostname(host);
  if (!h) return false;
  return (
    isAmazonRelatedHost(h) ||
    isWalmartRelatedHost(h) ||
    isEtsyHost(h) ||
    isTemuRelatedHost(h) ||
    isTargetRelatedHost(h)
  );
}

/**
 * True when the URL looks like a product detail page (not store home / tracking landing).
 * Used after affiliate redirect expansion so scrape + page refs target real products.
 */
function isProductDetailPageUrl(urlString) {
  const raw = String(urlString || '').trim();
  if (!raw) return false;
  try {
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    const host = u.hostname;
    const path = u.pathname || '';

    if (isAmazonRelatedHost(host)) {
      return isAmazonProductPageForNanoReference(raw) || !!extractAmazonAsinFromUrl(raw);
    }
    if (isWalmartRelatedHost(host)) {
      return isWalmartProductPageForNanoReference(raw);
    }
    if (isEtsyHost(host)) {
      return isEtsyListingPageUrl(raw);
    }
    if (isTemuRelatedHost(host)) {
      if (/\/-g-\d{5,}/i.test(path) || /-g-\d{5,}\.html/i.test(path)) return true;
      if (/\/goods\.html/i.test(path) && u.searchParams.get('goods_id')) return true;
      const segs = path.split('/').filter(Boolean);
      if (segs.length >= 2 && /^[a-z0-9][-a-z0-9._]*-g-\d{5,}/i.test(segs[segs.length - 1])) return true;
      return false;
    }
    if (isTargetRelatedHost(host)) {
      if (/\/p\//i.test(path)) return true;
      if (/\/-\/A-\d+/i.test(path) || /\/A-\d+/i.test(path)) return true;
      return false;
    }
    if (isKnownAffiliateDestinationMerchantHost(host)) {
      return false;
    }
    return isEcommerceProductPath(path);
  } catch {
    return false;
  }
}

/** Merchant host but homepage, category hub, or tracking landing — not a PDP. */
function isMerchantNonPdpLandingUrl(urlString) {
  try {
    const u = new URL(String(urlString || '').trim());
    if (!isKnownAffiliateDestinationMerchantHost(u.hostname)) return false;
    return !isProductDetailPageUrl(urlString);
  } catch {
    return false;
  }
}

function shouldKeepResolvingOutboundUrl(urlString) {
  if (isProductDetailPageUrl(urlString)) return false;
  try {
    const u = new URL(String(urlString || '').trim());
    if (shouldResolveOutboundUrlForUrlToPin(u.hostname)) return true;
    if (isMerchantNonPdpLandingUrl(urlString)) return true;
    return false;
  } catch {
    return false;
  }
}

function extractMetaRefreshTargetFromHtml(html) {
  const s = String(html || '');
  if (!s) return '';
  const m = s.match(/<meta[^>]+http-equiv=["']refresh["'][^>]*>/i);
  if (!m) return '';
  const tag = m[0];
  const c = tag.match(/content=["']([^"']+)["']/i);
  if (!c) return '';
  const content = c[1];
  const urlPart = content.split(/;/i).map((p) => p.trim()).find((p) => /^url=/i.test(p));
  if (!urlPart) return '';
  return urlPart.replace(/^url=/i, '').trim();
}

/** Canonical / og:url from interstitial or merchant landing HTML. */
function extractCanonicalOrOgUrlFromHtml(html, pageUrl) {
  const s = String(html || '');
  if (!s) return '';
  const patterns = [
    /<link[^>]+rel=["']canonical["'][^>]*href=["']([^"']+)["'][^>]*>/i,
    /<link[^>]+href=["']([^"']+)["'][^>]*rel=["']canonical["'][^>]*>/i,
    /<meta[^>]+property=["']og:url["'][^>]*content=["']([^"']+)["'][^>]*>/i,
    /<meta[^>]+content=["']([^"']+)["'][^>]*property=["']og:url["'][^>]*>/i,
  ];
  for (const re of patterns) {
    const m = s.match(re);
    if (m && m[1]) {
      try {
        const abs = new URL(m[1].trim(), pageUrl).href;
        assertSafePublicHttpUrl(new URL(abs));
        return abs;
      } catch {
        /* try next */
      }
    }
  }
  return '';
}

/**
 * When expansion stops on temu.com/?… or target.com/ (not a PDP), follow redirects /
 * parse canonical / meta refresh once more before accepting the landing URL.
 */
async function tryDeepenMerchantLandingUrl(pageUrlString, timeoutMs, headers) {
  const start = String(pageUrlString || '').trim();
  if (!start || !isMerchantNonPdpLandingUrl(start)) return null;

  let current;
  try {
    current = new URL(start);
    assertSafePublicHttpUrl(current);
  } catch {
    return null;
  }

  const maxFollow = 5;
  let lastHtml = '';

  for (let i = 0; i < maxFollow; i++) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    let resp;
    try {
      resp = await fetch(current.href, {
        method: 'GET',
        redirect: 'manual',
        headers,
        signal: ctrl.signal,
      });
    } catch {
      clearTimeout(t);
      break;
    } finally {
      clearTimeout(t);
    }

    if (resp.status >= 300 && resp.status < 400) {
      const loc = resp.headers.get('location') || resp.headers.get('Location');
      if (!loc) break;
      try {
        current = new URL(loc, current.href);
        assertSafePublicHttpUrl(current);
      } catch {
        break;
      }
      if (isProductDetailPageUrl(current.href)) return current.href;
      continue;
    }

    if (!resp.ok) break;

    try {
      const buf = await resp.arrayBuffer();
      lastHtml = Buffer.from(buf).slice(0, 65536).toString('utf8');
    } catch {
      lastHtml = '';
    }

    if (isProductDetailPageUrl(current.href)) return current.href;

    const metaRefresh = extractMetaRefreshTargetFromHtml(lastHtml);
    if (metaRefresh) {
      try {
        const next = new URL(metaRefresh, current.href);
        assertSafePublicHttpUrl(next);
        current = next;
        if (isProductDetailPageUrl(current.href)) return current.href;
        continue;
      } catch {
        /* fall through */
      }
    }

    const canonical = extractCanonicalOrOgUrlFromHtml(lastHtml, current.href);
    if (canonical && canonical !== current.href) {
      try {
        current = new URL(canonical);
        assertSafePublicHttpUrl(current);
        if (isProductDetailPageUrl(current.href)) return current.href;
      } catch {
        /* ignore */
      }
    }
    break;
  }

  return isProductDetailPageUrl(current.href) ? current.href : null;
}

/**
 * Expand short / affiliate redirect URLs to a final http(s) destination (bounded hops).
 * Prefers a product detail page (PDP) over merchant home / tracking landings.
 */
async function resolveOutboundUrlForUrlToPin(rawUrlString) {
  const raw = String(rawUrlString || '').trim();
  if (!raw) return raw;
  let current;
  try {
    current = new URL(raw);
  } catch {
    return raw;
  }
  if (!shouldKeepResolvingOutboundUrl(current.href) && !shouldResolveOutboundUrlForUrlToPin(current.hostname)) {
    return raw;
  }

  const maxHops = 6;
  const timeoutMs = 9000;
  const headers = {
    ...URL_SCRAPE_HEADERS,
    Accept: '*/*',
  };

  try {
    assertSafePublicHttpUrl(current);
  } catch {
    return raw;
  }

  let bestPdpUrl = isProductDetailPageUrl(current.href) ? current.href : null;

  for (let hop = 0; hop < maxHops; hop++) {
    if (isProductDetailPageUrl(current.href)) {
      bestPdpUrl = current.href;
      break;
    }
    if (!shouldKeepResolvingOutboundUrl(current.href)) {
      break;
    }

    let headResp = null;
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        headResp = await fetch(current.href, {
          method: 'HEAD',
          redirect: 'manual',
          headers,
          signal: ctrl.signal,
        });
      } finally {
        clearTimeout(t);
      }
    } catch {
      headResp = null;
    }

    if (headResp) {
      const loc = headResp.headers.get('location') || headResp.headers.get('Location');
      if (headResp.status >= 300 && headResp.status < 400 && loc) {
        let next;
        try {
          next = new URL(loc, current.href);
        } catch {
          return bestPdpUrl || raw;
        }
        try {
          assertSafePublicHttpUrl(next);
        } catch {
          return bestPdpUrl || raw;
        }
        current = next;
        if (isProductDetailPageUrl(current.href)) bestPdpUrl = current.href;
        continue;
      }
    }

    if (hop < maxHops - 1) {
      const ctrl2 = new AbortController();
      const t2 = setTimeout(() => ctrl2.abort(), timeoutMs);
      let resp2;
      try {
        resp2 = await fetch(current.href, {
          method: 'GET',
          redirect: 'manual',
          headers,
          signal: ctrl2.signal,
        });
      } catch {
        clearTimeout(t2);
        break;
      } finally {
        clearTimeout(t2);
      }
      if (resp2.status >= 300 && resp2.status < 400) {
        const loc2 = resp2.headers.get('location') || resp2.headers.get('Location');
        if (loc2) {
          let next2;
          try {
            next2 = new URL(loc2, current.href);
          } catch {
            break;
          }
          try {
            assertSafePublicHttpUrl(next2);
          } catch {
            break;
          }
          current = next2;
          if (isProductDetailPageUrl(current.href)) bestPdpUrl = current.href;
          continue;
        }
      }
      if (resp2.ok) {
        try {
          const buf = await resp2.arrayBuffer();
          const slice = Buffer.from(buf).slice(0, 65536).toString('utf8');
          let advanced = false;
          const target = extractMetaRefreshTargetFromHtml(slice);
          if (target) {
            try {
              const next3 = new URL(target, current.href);
              assertSafePublicHttpUrl(next3);
              current = next3;
              advanced = true;
              if (isProductDetailPageUrl(current.href)) bestPdpUrl = current.href;
            } catch {
              /* ignore */
            }
          }
          if (!advanced) {
            const canonical = extractCanonicalOrOgUrlFromHtml(slice, current.href);
            if (canonical && canonical !== current.href) {
              try {
                current = new URL(canonical);
                assertSafePublicHttpUrl(current);
                advanced = true;
                if (isProductDetailPageUrl(current.href)) bestPdpUrl = current.href;
              } catch {
                /* ignore */
              }
            }
          }
          if (advanced) continue;
        } catch {
          /* ignore */
        }
      }
    }

    break;
  }

  if (!bestPdpUrl && isMerchantNonPdpLandingUrl(current.href)) {
    try {
      const deepened = await tryDeepenMerchantLandingUrl(current.href, timeoutMs, headers);
      if (deepened && isProductDetailPageUrl(deepened)) {
        bestPdpUrl = deepened;
      }
    } catch (e) {
      console.warn('resolveOutboundUrl tryDeepenMerchantLandingUrl:', e.message || e);
    }
  }

  if (bestPdpUrl) return bestPdpUrl;

  try {
    return current.href;
  } catch {
    return raw;
  }
}

/**
 * Load public article HTML: fetch with browser headers first.
 * - Amazon: single fetch only — if blocked/consent or non-OK, return '' so URL→Pin can use RapidAPI (no Puppeteer).
 * - Other sites: on 403/401/503/429 or network error, retry with Puppeteer (e.g. Medium).
 */
async function fetchArticleHtml(url) {
  let hostIsAmazon = false;
  let hostIsWalmart = false;
  let skipPuppeteerFallback = false;
  try {
    const u = new URL(String(url || '').trim());
    hostIsAmazon = isAmazonRelatedHost(u.hostname);
    hostIsWalmart = isWalmartRelatedHost(u.hostname);
    skipPuppeteerFallback =
      isAffiliateTrackingRedirectHost(u.hostname) || isCreatorAffiliatePlatformRedirectHost(u.hostname);
  } catch {
    hostIsAmazon = false;
    hostIsWalmart = false;
    skipPuppeteerFallback = false;
  }

  try {
    const resp = await fetch(url, { redirect: 'follow', headers: URL_SCRAPE_HEADERS });
    if (resp.ok) {
      const html = await resp.text();
      if (hostIsAmazon && detectAmazonBotOrConsentPage(html)) {
        return '';
      }
      if (hostIsWalmart && /robot or human|activate and hold the button/i.test(html)) {
        return '';
      }
      return html;
    }
    const status = resp.status;
    console.warn('fetchArticleHtml non-OK:', String(url).slice(0, 96), status);
    if (hostIsAmazon || hostIsWalmart) {
      return '';
    }
    if (
      !skipPuppeteerFallback &&
      (status === 403 || status === 401 || status === 503 || status === 429)
    ) {
      const puppetHtml = await fetchArticleHtmlViaPuppeteer(url);
      if (puppetHtml) return puppetHtml;
    }
    return '';
  } catch (e) {
    console.warn('fetchArticleHtml error:', e.message || e);
    if (hostIsAmazon || hostIsWalmart) {
      return '';
    }
    if (skipPuppeteerFallback) {
      return '';
    }
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

/** When RapidAPI returns a usable listing, skip slow HTML + Puppeteer for Etsy PDPs. */
async function tryEtsyRapidPrefetchForUrl(workingUrl) {
  const key = String(process.env.RAPIDAPI_KEY || '').trim();
  if (!key) return null;
  try {
    const raw = String(workingUrl || '').trim();
    if (!raw) return null;
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (!isEtsyHost(u.hostname)) return null;
    const listingId = extractEtsyListingIdFromUrl(raw);
    if (!listingId) return null;
    return await fetchEtsyProductDataViaRapidApi(listingId);
  } catch {
    return null;
  }
}

/** When RapidAPI returns a usable product, skip slow / blocked Amazon HTML fetches. */
async function tryAmazonRapidPrefetchForUrl(workingUrl) {
  const key = String(process.env.RAPIDAPI_KEY || '').trim();
  if (!key) return null;
  try {
    const raw = String(workingUrl || '').trim();
    if (!raw) return null;
    if (!isAmazonProductPageForNanoReference(raw)) return null;
    const u = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    if (!isAmazonRelatedHost(u.hostname)) return null;
    const asin = extractAmazonAsinFromUrl(raw);
    if (!asin) return null;
    return await fetchAmazonProductDataViaRapidApi({
      asin,
      marketplace: resolveRapidApiAmazonMarketplaceFromHost(u.hostname),
      language: 'en',
    });
  } catch {
    return null;
  }
}

/**
 * When RapidAPI returns a usable product, skip our own (always bot-walled) Walmart HTML fetch
 * and Puppeteer entirely — the provider gives title + description + images directly.
 */
async function tryWalmartRapidPrefetchForUrl(workingUrl, originalUrl = '') {
  const key = String(process.env.RAPIDAPI_KEY || '').trim();
  if (!key) {
    console.warn('[walmart] prefetch skipped — RAPIDAPI_KEY not set in this environment');
    return null;
  }
  if (process.env.URLTOPIN_WALMART_PRODUCT_IMAGES === '0') return null;
  try {
    const candidates = [...new Set([String(originalUrl || '').trim(), String(workingUrl || '').trim()].filter(Boolean))];
    for (const candidate of candidates) {
      if (!isWalmartUrlEligibleForApi(candidate)) continue;
      const productUrl = await resolveWalmartProductUrlForApi(candidate);
      if (!productUrl) {
        console.warn(`[walmart] could not resolve product URL from ${candidate.slice(0, 96)}`);
        continue;
      }
      console.log(`[walmart] prefetch → ${productUrl.slice(0, 96)}`);
      const data = await fetchWalmartProductDataViaRapidApi({
        itemId: extractWalmartItemIdFromUrl(productUrl),
        url: productUrl,
      });
      if (data && data.title) return data;
    }
    return null;
  } catch (e) {
    console.warn('[walmart] prefetch error:', e.message || e);
    return null;
  }
}

/**
 * Fetch full article HTML and build richer base metadata + summary.
 * Falls back gracefully to meta tags only if fetch or parsing fails.
 */
async function fetchArticleBaseAndSummary(url, clientArticleData, opts = null) {
  const fast = !!opts?.fast;
  const preResolved = String(opts?.preResolvedUrl || '').trim();
  const outputLanguage = String(opts?.outputLanguage || '').trim().toLowerCase();
  let workingUrl = String(url || '').trim();
  if (preResolved) {
    workingUrl = preResolved;
  } else {
    try {
      const expanded = await resolveOutboundUrlForUrlToPin(workingUrl);
      if (expanded && expanded !== workingUrl) workingUrl = expanded;
    } catch (e) {
      console.warn('fetchArticleBaseAndSummary resolveOutboundUrl error:', e.message || e);
    }
  }
  const hasClientMeta =
    clientArticleData &&
    typeof clientArticleData === 'object' &&
    (clientArticleData.title || clientArticleData.description || clientArticleData.domain);

  let etsyRapidEarly = null;
  let amazonRapidEarly = null;
  let walmartRapidEarly = null;
  if (String(process.env.RAPIDAPI_KEY || '').trim()) {
    etsyRapidEarly = await tryEtsyRapidPrefetchForUrl(workingUrl);
    if (!etsyRapidEarly) {
      amazonRapidEarly = await tryAmazonRapidPrefetchForUrl(workingUrl);
    }
    if (!etsyRapidEarly && !amazonRapidEarly) {
      walmartRapidEarly = await tryWalmartRapidPrefetchForUrl(workingUrl, url);
    }
  }
  const etsyRapidTitleOk =
    etsyRapidEarly &&
    typeof etsyRapidEarly.title === 'string' &&
    etsyRapidEarly.title.trim().length >= 3;
  const amazonRapidTitleOk =
    amazonRapidEarly &&
    typeof amazonRapidEarly.title === 'string' &&
    amazonRapidEarly.title.trim().length >= 3;
  const walmartRapidTitleOk =
    walmartRapidEarly &&
    typeof walmartRapidEarly.title === 'string' &&
    walmartRapidEarly.title.trim().length >= 3;
  // Amazon: skip HTML when RapidAPI returned a real title. Etsy listing pages: always skip HTML (403 from Etsy;
  // title/thumb come from RapidAPI + oEmbed in enrichEtsyListingBaseFromApis). All other URLs still use
  // fetchArticleHtml (+ Puppeteer on hard blocks).
  const skipEtsyListingHtml = isEtsyListingPageUrl(workingUrl);
  const skipMerchantHtmlScrape =
    !!(etsyRapidTitleOk || amazonRapidTitleOk || walmartRapidTitleOk) || skipEtsyListingHtml;

  // In "fast" mode (used for strategic_single fan-out requests), avoid refetching full HTML.
  // We already scraped client-side and pass basic metadata; the slight summary quality drop is
  // worth the latency win and reduces user abandonment.
  // For Amazon PDPs with RapidAPI title, or any Etsy listing URL, skip HTML entirely (no Puppeteer).
  let html = '';
  if (!fast || !hasClientMeta) {
    if (!skipMerchantHtmlScrape) {
      try {
        html = await fetchArticleHtml(workingUrl);
      } catch (e) {
        console.warn('fetchArticleBaseAndSummary fetch error:', e.message || e);
      }
    }
  }

  const metaFromHtml = extractMetaFromHtml(html || '', workingUrl);
  const base = {
    ...metaFromHtml,
    ...(clientArticleData || {}),
  };
  // Canonical article URL always wins for display + keyword (fixes short links & opaque path slugs).
  const derivedDisplay = buildLinkDisplayLabelFromUrl(workingUrl, 80);
  const derivedKw = deriveKeywordFromArticleUrl(workingUrl);
  base.linkDisplay = derivedDisplay || base.linkDisplay || '';
  base.keyword = derivedKw;
  const rawInputUrl = String(url || '').trim();
  Object.assign(base, mergeBrandingGates(assessUrlBrandingGate(rawInputUrl), assessUrlBrandingGate(workingUrl)));

  if (amazonRapidEarly && amazonRapidTitleOk) {
    const t = String(amazonRapidEarly.title || '').trim();
    if (t) base.title = t.slice(0, 300);
    const d = typeof amazonRapidEarly.description === 'string' ? amazonRapidEarly.description.trim() : '';
    if (d && (!base.description || String(base.description).trim().length < 30)) {
      base.description = d.slice(0, 450);
    }
    base.amazon_blocked = false;
    base.amazon_rapidapi_data = amazonRapidEarly;
  }

  if (walmartRapidEarly && walmartRapidTitleOk) {
    const t = String(walmartRapidEarly.title || '').trim();
    if (t) base.title = t.slice(0, 300);
    const d = typeof walmartRapidEarly.description === 'string' ? walmartRapidEarly.description.trim() : '';
    if (d && (!base.description || String(base.description).trim().length < 30)) {
      base.description = d.slice(0, 450);
    }
    base.walmart_rapidapi_data = walmartRapidEarly;
  }

  // Never surface Walmart's bot-challenge page title when RapidAPI didn't recover the product.
  if (looksLikeBlockedWalmartTitle(base.title)) {
    base.title = '';
  }

  // Amazon fallback: if scraping yields a generic / blocked title, flag it so callers can abort
  // BEFORE spending credits (Nano Banana) and BEFORE deducting quota.
  if (!amazonRapidEarly) {
    try {
      const u = new URL(String(workingUrl || '').trim());
      if (isAmazonRelatedHost(u.hostname) && looksLikeGenericAmazonTitle(base.title)) {
        base.amazon_blocked = true;
        // Keep title empty so downstream copywriters don't anchor on "Amazon product".
        base.title = '';
      }
    } catch {
      /* ignore */
    }
  }

  // Etsy: RapidAPI listing payload when configured; else oEmbed for missing title/thumbnail.
  await enrichEtsyListingBaseFromApis(base, workingUrl);

  if (base.title) {
    base.title = await maybeShortenPageTitleForUrlToPin(workingUrl, base.title, openai, base.canonicalUrl);
  }

  // If we already have a meaningful page title (esp. non-Latin languages like Greek),
  // avoid using a Latin/Greeklish URL slug as the "keyword" in prompts.
  // Keep slug keyword as fallback for ecommerce pages when title is missing/generic.
  try {
    const u = new URL(String(workingUrl || '').trim());
    const isAmazon = isAmazonRelatedHost(u.hostname);
    const meaningfulTitle = titleLooksMeaningfulForKeywordSuppression(base.title);
    const wantNonEnglish = !!(outputLanguage && outputLanguage !== 'auto' && outputLanguage !== 'en');
    const nonLatinTitle = titleHasNonLatinScript(base.title);
    const isProductPath = isEcommerceProductPath(u.pathname || '');
    if (
      !isAmazon &&
      meaningfulTitle &&
      (isProductPath ||
        ((wantNonEnglish || nonLatinTitle) && looksLikeLatinSlugKeyword(base.keyword)))
    ) {
      base.keyword = '';
    }
  } catch {
    /* ignore */
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

  let articleSummary = summaryParts.join('. ').slice(0, 1200);
  if (amazonRapidEarly && amazonRapidTitleOk && skipMerchantHtmlScrape) {
    const rs = buildAmazonRapidApiSummary(amazonRapidEarly);
    if (rs) articleSummary = rs.slice(0, 1200);
  }

  Object.assign(base, detectProductAffiliateLandingFromUrls(rawInputUrl, workingUrl, base.canonicalUrl));

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
    const altFromScheduled = (() => {
      try {
        const v = pin?.original_pin_data?.alt_text;
        return typeof v === 'string' ? v.trim() : '';
      } catch {
        return '';
      }
    })();
    const requestBody = {
      board_id: pin.board_id,
      title: pin.title,
      description: pin.description,
      ...(altFromScheduled ? { alt_text: altFromScheduled } : {}),
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
let refImageCleanupInterval;
let onboardingEmailInterval;

async function cleanupOldUrlToPinReferenceImages() {
  // Deletes only our temporary reference images mirrored into Storage for Nano Banana.
  if (!supabaseAdmin?.storage) return;
  const bucket = 'ai-images';
  const ttlHours = Math.max(1, parseInt(process.env.URLTOPIN_REF_IMAGE_TTL_HOURS || '48', 10) || 48);
  const cutoffMs = Date.now() - ttlHours * 60 * 60 * 1000;
  const prefixes = ['amazon-ref-', 'page-ref-'];
  const batchSize = 80;

  try {
    let offset = 0;
    let totalCandidates = 0;
    let totalDeleted = 0;
    for (;;) {
      const { data: items, error } = await supabaseAdmin.storage
        .from(bucket)
        .list('', { limit: 1000, offset, sortBy: { column: 'created_at', order: 'asc' } });
      if (error) {
        console.warn('ref image cleanup: list error:', error.message || error);
        return;
      }
      if (!items || items.length === 0) break;

      const toDelete = [];
      for (const it of items) {
        const name = String(it?.name || '');
        if (!prefixes.some((p) => name.startsWith(p))) continue;
        const createdRaw = it?.created_at || it?.updated_at || null;
        const createdMs = createdRaw ? new Date(createdRaw).getTime() : NaN;
        if (!Number.isFinite(createdMs) || createdMs > cutoffMs) continue;
        totalCandidates += 1;
        toDelete.push(name);
      }

      for (let i = 0; i < toDelete.length; i += batchSize) {
        const chunk = toDelete.slice(i, i + batchSize);
        const { error: delErr } = await supabaseAdmin.storage.from(bucket).remove(chunk);
        if (delErr) {
          console.warn('ref image cleanup: remove error:', delErr.message || delErr);
        } else {
          totalDeleted += chunk.length;
        }
      }

      if (items.length < 1000) break;
      offset += items.length;
    }

    if (totalCandidates > 0) {
      console.log(`🧹 Cleaned up ${totalDeleted}/${totalCandidates} URL→Pin reference images older than ${ttlHours}h`);
    }
  } catch (e) {
    console.warn('ref image cleanup: unexpected error:', e?.message || e);
  }
}

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

function startRefImageCleanup() {
  if (refImageCleanupInterval) clearInterval(refImageCleanupInterval);
  // Run every 6 hours; also run once shortly after startup.
  refImageCleanupInterval = setInterval(cleanupOldUrlToPinReferenceImages, 6 * 60 * 60 * 1000);
  setTimeout(cleanupOldUrlToPinReferenceImages, 60 * 1000);
  console.log('🧹 URL→Pin reference image cleanup started (runs every 6 hours)');
}

/**
 * Claim a lifecycle email for a user so it's only sent once (idempotent across
 * restarts). Returns true if WE claimed it (i.e., not previously sent).
 * Returns false if already sent, on error, or if the email_events table is missing
 * (so onboarding degrades gracefully to "send nothing" until the table exists).
 */
async function claimEmailEvent(userId, emailKey) {
  const uid = String(userId || '').trim();
  const key = String(emailKey || '').trim();
  if (!uid || !key) return false;
  try {
    const { data, error } = await supabaseAdmin
      .from('email_events')
      .upsert({ user_id: uid, email_key: key }, { onConflict: 'user_id,email_key', ignoreDuplicates: true })
      .select('user_id');
    if (error) {
      console.warn('claimEmailEvent error (skipping send):', error.message || error);
      return false;
    }
    return Array.isArray(data) && data.length > 0;
  } catch (e) {
    console.warn('claimEmailEvent exception (skipping send):', e?.message || e);
    return false;
  }
}

/** Set of user_ids that have generated at least one pin (AI or own-photo), any month. */
async function fetchActivatedUserIds() {
  const activated = new Set();
  try {
    const rows = await founderFetchAll('pin_usage', 'user_id, pins_used, user_photo_pins_used');
    for (const r of rows) {
      const used = (Number(r?.pins_used) || 0) + (Number(r?.user_photo_pins_used) || 0);
      if (used > 0 && r?.user_id) activated.add(String(r.user_id));
    }
  } catch (e) {
    console.warn('fetchActivatedUserIds error:', e?.message || e);
  }
  return activated;
}

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * Lifecycle onboarding emails. Age-windowed so we only ever email *new* signups
 * (never the existing backlog), and idempotent via email_events:
 *   - welcome:               age < 2 days
 *   - activation_first_pin:  1–3 days old AND no pin generated yet
 */
async function processOnboardingEmails() {
  if (!isEmailEnabled()) return;
  let users;
  try {
    users = await founderFetchAuthUsers();
  } catch (e) {
    console.warn('processOnboardingEmails: failed to list users:', e?.message || e);
    return;
  }
  if (!Array.isArray(users) || users.length === 0) return;

  const activated = await fetchActivatedUserIds();
  const now = Date.now();
  let sent = 0;
  const MAX_PER_RUN = 100; // safety valve against accidental blasts

  for (const u of users) {
    if (sent >= MAX_PER_RUN) break;
    const email = String(u?.email || '').trim();
    const uid = String(u?.id || '').trim();
    const createdAt = u?.created_at ? new Date(u.created_at).getTime() : NaN;
    if (!email || !uid || !Number.isFinite(createdAt)) continue;
    const ageDays = (now - createdAt) / DAY_MS;
    if (ageDays < 0) continue;

    try {
      if (ageDays < 2) {
        if (await claimEmailEvent(uid, 'welcome')) {
          const r = await sendWelcomeEmail({ to: email });
          if (r?.ok) { sent += 1; console.log('onboarding: welcome sent', { uid }); }
        }
      }
      if (ageDays >= 1 && ageDays < 3 && !activated.has(uid)) {
        if (await claimEmailEvent(uid, 'activation_first_pin')) {
          const r = await sendFirstPinEmail({ to: email });
          if (r?.ok) { sent += 1; console.log('onboarding: first-pin sent', { uid }); }
        }
      }
    } catch (e) {
      console.warn('processOnboardingEmails: per-user error', { uid, error: e?.message || e });
    }
  }
  if (sent > 0) console.log(`📧 Onboarding emails: ${sent} sent this run`);
}

function startOnboardingEmails() {
  if (onboardingEmailInterval) clearInterval(onboardingEmailInterval);
  if (!isEmailEnabled()) {
    console.log('📧 Onboarding emails disabled (no RESEND_API_KEY)');
    return;
  }
  // Hourly is plenty: age-windows are 1–5 days wide, so timing is forgiving.
  onboardingEmailInterval = setInterval(processOnboardingEmails, 60 * 60 * 1000);
  setTimeout(processOnboardingEmails, 90 * 1000); // once shortly after startup
  console.log('📧 Onboarding email processor started (runs hourly)');
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
  if (refImageCleanupInterval) {
    clearInterval(refImageCleanupInterval);
    refImageCleanupInterval = null;
    console.log('🧹 URL→Pin reference image cleanup stopped');
  }
  if (onboardingEmailInterval) {
    clearInterval(onboardingEmailInterval);
    onboardingEmailInterval = null;
    console.log('📧 Onboarding email processor stopped');
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

const AFFILIATE_DISCLOSURE_MAX_CUSTOM_LEN = 40;

/** Parse affiliate disclosure from request body. Default off — unchanged behavior when omitted. */
function normalizeAffiliateDisclosureRequest(body) {
  const raw = String(body?.affiliateDisclosure ?? 'off').trim().toLowerCase();
  const mode =
    raw === 'ad' || raw === 'affiliate' || raw === 'custom' || raw === 'off' ? raw : 'off';
  let custom = String(body?.affiliateDisclosureCustom ?? '').trim();
  if (custom.length > AFFILIATE_DISCLOSURE_MAX_CUSTOM_LEN) {
    custom = custom.slice(0, AFFILIATE_DISCLOSURE_MAX_CUSTOM_LEN);
  }
  if (mode === 'off') return { mode: 'off', tag: null };
  if (mode === 'ad') return { mode: 'ad', tag: '#ad' };
  if (mode === 'affiliate') return { mode: 'affiliate', tag: '#affiliate' };
  if (mode === 'custom') {
    if (!custom) return { mode: 'off', tag: null };
    const firstToken = custom.split(/\s+/)[0];
    let tag = firstToken.startsWith('#') ? firstToken : `#${firstToken}`;
    tag = tag.replace(/[^#A-Za-z0-9_-]/g, '');
    if (tag.length < 2 || tag === '#') return { mode: 'off', tag: null };
    return { mode: 'custom', tag: tag.slice(0, AFFILIATE_DISCLOSURE_MAX_CUSTOM_LEN + 1) };
  }
  return { mode: 'off', tag: null };
}

/**
 * When disclosure is on, append one hashtag as the last hashtag in the description. No-op when off.
 * @param {string} description
 * @param {{ mode: string, tag: string | null }} disclosure
 * @param {{ maxLength?: number }} [opts] — if set, trim earlier text so result fits (keeps disclosure tag).
 */
function appendAffiliateDisclosureToDescription(description, disclosure, opts = {}) {
  if (!disclosure || disclosure.mode === 'off' || !disclosure.tag) {
    return String(description ?? '');
  }
  const tag = String(disclosure.tag).trim();
  if (!tag) return String(description ?? '');
  let desc = String(description ?? '').replace(/\s+$/u, '');
  const allTags = desc.match(/#[\w-]+/g);
  if (allTags && allTags[allTags.length - 1].toLowerCase() === tag.toLowerCase()) {
    return desc;
  }
  const suffix = desc ? ` ${tag}` : tag;
  let out = `${desc}${suffix}`;
  const maxLen = opts.maxLength;
  if (maxLen && out.length > maxLen) {
    const room = maxLen - suffix.length;
    if (room < 1) return tag.slice(0, maxLen);
    let trimmed = desc.slice(0, room).replace(/\s+$/u, '');
    const lastSpace = trimmed.lastIndexOf(' ');
    if (lastSpace > 10) trimmed = trimmed.slice(0, lastSpace).replace(/\s+$/u, '');
    out = trimmed ? `${trimmed}${suffix}` : tag;
    if (out.length > maxLen) {
      trimmed = desc.slice(0, Math.max(0, maxLen - suffix.length)).replace(/\s+$/u, '');
      const ls = trimmed.lastIndexOf(' ');
      if (ls > 10) trimmed = trimmed.slice(0, ls).replace(/\s+$/u, '');
      out = trimmed ? `${trimmed}${suffix}` : tag;
    }
  }
  return out;
}

function hashtagsFromPinDescription(description) {
  const m = String(description || '').match(/#[\w-]+/g);
  return m ? m.slice(0, 10) : [];
}

async function generatePinterestAltText(
  { title, description, overlayText, styleLabel, linkDisplay, nanoBananaPrompt, outputLanguage, strictLanguage },
  openaiClient
) {
  try {
    const t = String(title || '').trim();
    const d = String(description || '').trim();
    const headline = String(overlayText?.headline || '').trim();
    const sub = String(overlayText?.subheadline || '').trim();
    const ld = String(linkDisplay || '').trim();
    const nb = String(nanoBananaPrompt || '').trim();
    if (!t && !headline) return '';

    const lang = String(outputLanguage || '').trim().toLowerCase();
    const languageLine =
      lang && lang !== 'auto'
        ? `\n\nLANGUAGE REQUIREMENT: Write the alt text in ${lang.toUpperCase()} only. Do not use English.\n`
        : '';

    const prompt =
      `Write Pinterest image alt text for accessibility.\n` +
      `Describe what someone would see if they could not see the image.\n` +
      `Rules:\n` +
      `- Exactly 1 sentence.\n` +
      `- Max 240 characters.\n` +
      `- Describe the visual content (subject, setting, objects, colors/style, composition).\n` +
      `- Do NOT quote or restate any headline/subheadline/overlay text.\n` +
      `- Do NOT mention "headline" or "subheadline".\n` +
      `- Avoid marketing language.\n` +
      `- No hashtags. No URLs. No CTAs. No emojis.\n` +
      languageLine +
      `\n` +
      (nb
        ? `IMAGE PROMPT (source of truth for what the image contains):\n<<<${nb.slice(0, 2200)}>>>\n\n`
        : '') +
      `Context (not to be quoted):\n` +
      `- Pin title: ${t.slice(0, 120)}\n` +
      `- Pin description (context only): ${d.slice(0, 220)}\n` +
      `- Style label: ${String(styleLabel || '').slice(0, 80)}\n` +
      `- Source label: ${ld.slice(0, 80)}\n` +
      (headline || sub
        ? `- Note: the image may include a text overlay (do not repeat its words).\n`
        : '') +
      `\nReturn only the alt text sentence.`;

    const completion = await openaiClient.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 120,
      temperature: 0.2,
    });
    let out = String(completion.choices?.[0]?.message?.content || '')
      .replace(/\s+/g, ' ')
      .trim();
    out = out.replace(/^["'`]+|["'`]+$/g, '').trim();
    out = out.replace(/https?:\/\/\S+/g, '').replace(/#[A-Za-z0-9_]+/g, '').trim();
    // If the model still describes the overlay as "headline/subheadline/reads:", scrub that phrasing.
    out = out
      .replace(/\b(featuring|with|showing)\s+(the\s+)?(headline|subheadline)\b[^.]*\.?/gi, '')
      .replace(/\b(headline|subheadline)\b\s*(reads|says)?\s*[:\-–—]?\s*["'`][^"'`]{3,}["'`]/gi, '')
      .replace(/\b(text\s+overlay)\b\s*(reads|says)?\s*[:\-–—]?\s*["'`][^"'`]{3,}["'`]/gi, 'text overlay')
      .replace(/\s{2,}/g, ' ')
      .trim();
    if (out.length > 240) out = out.slice(0, 240).trim();

    if (strictLanguage && lang && lang !== 'auto' && out) {
      const detectLang = async (text) => {
        const tt = String(text || '').trim();
        if (!tt) return 'unknown';
        try {
          const completion = await openaiClient.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
              {
                role: 'user',
                content:
                  'Detect the primary language of the text.\n' +
                  'Return JSON only with key "lang" as an ISO 639-1 code when possible (e.g. "en", "el", "sk"), or "unknown".\n' +
                  `Text:\n<<<${tt.slice(0, 600)}>>>`,
              },
            ],
            max_tokens: 30,
            temperature: 0,
          });
          const raw = completion.choices?.[0]?.message?.content?.trim() || '';
          const m = raw.match(/\{[\s\S]*\}/);
          if (!m) return 'unknown';
          const parsed = JSON.parse(m[0]);
          const v = String(parsed.lang || '').trim().toLowerCase();
          return v || 'unknown';
        } catch {
          return 'unknown';
        }
      };

      const detected = await detectLang(out);
      if (detected !== 'unknown' && detected !== lang) {
        const retryPrompt =
          prompt +
          `\nCRITICAL: Output MUST be in ${lang.toUpperCase()} only. If you cannot comply, output an empty string.\n`;
        const retry = await openaiClient.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [{ role: 'user', content: retryPrompt }],
          max_tokens: 120,
          temperature: 0.2,
        });
        let next = String(retry.choices?.[0]?.message?.content || '').replace(/\s+/g, ' ').trim();
        next = next.replace(/^["'`]+|["'`]+$/g, '').trim();
        next = next.replace(/https?:\/\/\S+/g, '').replace(/#[A-Za-z0-9_]+/g, '').trim();
        next = next
          .replace(/\b(featuring|with|showing)\s+(the\s+)?(headline|subheadline)\b[^.]*\.?/gi, '')
          .replace(/\b(headline|subheadline)\b\s*(reads|says)?\s*[:\-–—]?\s*["'`][^"'`]{3,}["'`]/gi, '')
          .replace(/\b(text\s+overlay)\b\s*(reads|says)?\s*[:\-–—]?\s*["'`][^"'`]{3,}["'`]/gi, 'text overlay')
          .replace(/\s{2,}/g, ' ')
          .trim();
        if (next.length > 240) next = next.slice(0, 240).trim();
        if (next) out = next;
      }
    }

    return out;
  } catch (e) {
    console.warn('generatePinterestAltText error:', e.message || e);
    return '';
  }
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

const NANO_BANANA_MAX_CONCURRENT = Math.max(
  1,
  parseInt(process.env.NANO_BANANA_MAX_CONCURRENT || '3', 10) || 3
);
let nanoBananaSlotsInUse = 0;
const nanoBananaSlotWaiters = [];

function acquireNanoBananaSlot() {
  if (nanoBananaSlotsInUse < NANO_BANANA_MAX_CONCURRENT) {
    nanoBananaSlotsInUse += 1;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    nanoBananaSlotWaiters.push(() => {
      nanoBananaSlotsInUse += 1;
      resolve();
    });
  });
}

function releaseNanoBananaSlot() {
  nanoBananaSlotsInUse = Math.max(0, nanoBananaSlotsInUse - 1);
  const next = nanoBananaSlotWaiters.shift();
  if (next) next();
}

async function generateImageWithNanoBanana(prompt, logLabel = '', options = {}) {
  await acquireNanoBananaSlot();
  try {
    return await generateImageWithNanoBananaInner(prompt, logLabel, options);
  } finally {
    releaseNanoBananaSlot();
  }
}

async function generateImageWithNanoBananaInner(prompt, logLabel = '', options = {}) {
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
          // Don't abandon when the provider is simply queued (waiting/pending/queued).
          // Those states can legitimately remain constant for minutes during high load.
          const queueStates = new Set(['waiting', 'wait', 'pending', 'queuing', 'queueing', 'queued', 'submitted', 'created']);
          const canBeAbandoned = !queueStates.has(stateNorm);
          if (stuckSameStatePolls > 0 && canBeAbandoned && sameProgressStateCount >= stuckSameStatePolls) {
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

/** Download a provider temp URL (e.g. Kie tempfile.aiquickdraw.com) into Supabase ai-images. */
async function persistProviderImageUrlToAiImages(sourceUrl, fileStem, logLabel = '') {
  if (!sourceUrl || !fileStem) return sourceUrl || '';
  try {
    const imageRes = await fetch(sourceUrl);
    if (!imageRes.ok) {
      console.warn(
        'persistProviderImageUrlToAiImages: fetch failed' +
          (logLabel ? ` (${logLabel})` : '') +
          ` status=${imageRes.status}`
      );
      return sourceUrl;
    }
    const buffer = Buffer.from(await imageRes.arrayBuffer());
    const fileExt = (sourceUrl.split('.').pop() || 'png').split('?')[0] || 'png';
    const fileName = `${fileStem}.${fileExt}`;
    const { error: uploadError } = await supabaseAdmin.storage.from('ai-images').upload(fileName, buffer, {
      contentType: imageRes.headers.get('content-type') || 'image/png',
      upsert: true,
    });
    if (uploadError) {
      console.warn(
        'persistProviderImageUrlToAiImages: upload error' +
          (logLabel ? ` (${logLabel})` : '') +
          ':',
        uploadError.message || uploadError
      );
      return sourceUrl;
    }
    const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
    return publicUrlData?.publicUrl || sourceUrl;
  } catch (e) {
    console.warn(
      'persistProviderImageUrlToAiImages' + (logLabel ? ` (${logLabel})` : '') + ':',
      e.message || e
    );
    return sourceUrl;
  }
}

async function withSoftTimeout(promise, timeoutMs) {
  const ms = Number(timeoutMs);
  if (!Number.isFinite(ms) || ms <= 0) return await promise;
  let t;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => {
        t = setTimeout(() => resolve(null), ms);
      }),
    ]);
  } finally {
    if (t) clearTimeout(t);
  }
}

const STYLE_ON_IMAGE_TEXT_GUIDANCE = {
  curiosity_shock:
    'Generate one bold, original curiosity headline about the topic (max 60 chars). Rotate devices: myth vs reality, unexpected downside, "why X fails", contrarian angle — not the same hook shape every time. Subheadline: short teaser (max 50 chars), new wording per pin.',
  question_style:
    'Generate a direct question headline people want answered (max 60 chars). Vary question type (trust, timing, comparison, "is it worth it", "what happens if") — do not default to the same question frame every time. Subheadline: promise or credibility line (max 50 chars), fresh phrasing.',
  viral_curiosity:
    'Generate a story- or experiment-style headline (max 60 chars): time-boxed try, habit change, surprising result — vary the narrative. Subheadline (max 50 chars): different each time.',
  money_saving:
    'Generate a value headline about saving time, money, or effort (max 60 chars). Subheadline: concrete payoff (max 50 chars). Avoid repeating the same "waste vs smart way" pairing across pins.',
  minimal_typography:
    'Generate a short, high-impact headline (max 50 chars). Subheadline: one crisp supporting line — must not reuse the same subhead as other pins in the batch.',
  cozy_baking:
    'Generate a warm, practical headline (max 60 chars). Subheadline: friendly helper line with varied wording.',
  clean_appetizing:
    'Generate a clear, inviting headline (max 60 chars). Subheadline: soft guide line (scope, year, or "what you\'ll learn") — vary phrasing.',
  clumpy_fix:
    'Generate a simple how-to or fix headline (max 60 chars). Subheadline: clarity promise; vary wording, not a fixed tagline.',
  minimal_elegant:
    'Generate an elegant, minimal headline (max 50 chars). Subheadline: very short or empty.',
  before_after:
    'Generate a before/after or transformation headline (max 60 chars). Subheadline: contrast or outcome line with varied phrasing.',
  timeline_infographic:
    'Generate a step-by-step or roadmap headline (max 60 chars). Subheadline: guide framing — avoid repeating the same roadmap/steps cliché every time.',
  grid_3_images: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  grid_4_images: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  stacked_strips: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  offset_collage_3: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  circle_cluster_4: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
  step_cards_3: 'Generate a short bold headline about the topic. Max 60 chars. Subheadline: supporting line. Adapt to the article.',
};

async function generateStyleOnImageText({
  styleId,
  topic,
  domain,
  keyword,
  year,
  description,
  avoidText,
  usedOverlayTexts,
  outputLanguage,
  strictLanguage,
}) {
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
  const lang = String(outputLanguage || '').trim().toLowerCase();
  const langNote =
    lang && lang !== 'auto'
      ? `\n\nLANGUAGE REQUIREMENT: Write BOTH headline and subheadline in ${lang.toUpperCase()} only. Do not use English.\n`
      : '';
  const content =
    `Article/topic: ${topic}\n` +
    `${keyword ? `Keyword: ${keyword}\n` : ''}` +
    `Domain: ${domain}\n` +
    `Year: ${year}\n` +
    `${description ? `Context: ${description.slice(0, 200)}\n` : ''}` +
    `Style: ${styleId}\n\n` +
    `${guidance}${avoidNote}` +
    `${langNote}\n` +
    `${PIN_COPY_ANTI_CLICHE_INSTRUCTION}\n\nReturn JSON only: {"headline":"...","subheadline":"..."}. No markdown.`;
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
  amazon_affiliate:
    'Amazon / shopping pins: hero the real product (shape, packaging, context of use); honest worth-it vibe; no fake prices, star ratings, or Amazon UI chrome as invented text.',
};

/**
 * Auto-generate a short, catchy headline for a multi-product pin from the product titles.
 * Falls back to a sensible default if the AI call fails.
 */
async function generateMultiProductHeadline({ mode, items, outputLanguage }, openaiClient) {
  const safeItems = Array.isArray(items) ? items.filter((it) => it && it.title) : [];
  const titles = safeItems.map((it, i) => `${i + 1}. ${String(it.title).slice(0, 90)}`).join('\n');
  const count = Math.max(1, safeItems.length);
  const langLine =
    outputLanguage && outputLanguage !== 'auto'
      ? `Write the headline in this language (ISO code): ${outputLanguage}.\n`
      : '';
  const fallback =
    mode === 'comparison'
      ? (safeItems.length === 2
          ? `${String(safeItems[0].title).split(/[\s,–-]/)[0]} vs ${String(safeItems[1].title).split(/[\s,–-]/)[0]}: Which Wins?`
          : 'Which One Should You Buy?')
      : `${count} Top Picks Worth Buying`;
  if (!openaiClient || !safeItems.length) return fallback.slice(0, 70);
  const instruction =
    mode === 'comparison'
      ? `Write ONE punchy Pinterest pin headline (max 8 words) comparing these two products. Use a "X vs Y" angle ending in a question or hook. Do not mention any quantity or count. No quotes, no emojis.\n`
      : `Write ONE punchy Pinterest pin headline (max 8 words) for a product roundup / gift guide of the items below. ` +
        `There are EXACTLY ${count} product${count === 1 ? '' : 's'}. If the headline includes a number, that number MUST be ${count} — never use any other number. No quotes, no emojis.\n`;
  try {
    const completion = await openaiClient.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: `${instruction}${langLine}Products:\n${titles}` }],
      max_tokens: 40,
      temperature: 0.8,
    });
    let h = String(completion.choices?.[0]?.message?.content || '').trim();
    h = h.replace(/^["'`\s]+|["'`\s]+$/g, '').replace(/[\r\n]+/g, ' ').slice(0, 80);
    if (mode !== 'comparison') h = correctHeadlineCount(h, count);
    return h || fallback.slice(0, 70);
  } catch (e) {
    console.warn('multi-product headline generation error:', e.message || e);
    return fallback.slice(0, 70);
  }
}

/**
 * Safety net: a roundup headline must never claim a count different from the real product count.
 * Fixes a leading number (digit or spelled-out, e.g. "5 Best…" / "Five Best…") to match `count`.
 * Only the leading number is touched so mid-headline values like prices ("$100") are left alone.
 */
function correctHeadlineCount(text, count) {
  let t = String(text || '');
  const words = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'];
  const mDigit = t.match(/^\s*(\d+)\b/);
  if (mDigit) {
    if (Number(mDigit[1]) !== count) t = t.replace(/^\s*\d+\b/, String(count));
    return t.trim();
  }
  const mWord = t.match(/^\s*([A-Za-z]+)\b/);
  if (mWord) {
    const idx = words.indexOf(mWord[1].toLowerCase());
    if (idx > 0 && idx !== count) t = t.replace(/^\s*[A-Za-z]+\b/, String(count));
  }
  return t.trim();
}

/**
 * Nano Banana prompt for multi-product affiliate pins (roundup grid or A-vs-B comparison).
 * The provided product photos are passed as reference images (imageInput) in the SAME order as `items`,
 * and the prompt is written to keep numbering exact and to treat similar/same-brand products as distinct cards.
 */
function buildMultiProductPinPrompt({ mode, headline, items, footer, brand }) {
  const safeItems = Array.isArray(items) ? items.filter((it) => it && (it.title || it.imageUrl)) : [];
  const n = Math.max(1, safeItems.length);
  const brandColorParts = [];
  if (brand?.primaryColor) brandColorParts.push(`primary ${brand.primaryColor}`);
  if (brand?.secondaryColor) brandColorParts.push(`secondary ${brand.secondaryColor}`);
  if (brand?.accentColor) brandColorParts.push(`accent ${brand.accentColor}`);
  const paletteHint = brandColorParts.length
    ? ` Use this brand color palette for the header band, number badges, and accents: ${brandColorParts.join(', ')}.`
    : '';
  const footerLine = String(footer || '').trim();
  const footerHint = footerLine ? ` At the very bottom, add a small footer band with the text "${footerLine}".` : '';

  // Shared rules that protect numbering + product fidelity (most important per product requirements).
  const fidelityRules =
    ` CRITICAL RULES: (1) Use the supplied reference photos as the ACTUAL products, in the SAME order given. ` +
    `(2) Show EXACTLY ${n} product${n === 1 ? '' : 's'} — never add, drop, merge, or duplicate a product. ` +
    `(3) Some products may look very similar or be from the same brand — still render them as separate, distinct cards in order; do not collapse them into one. ` +
    `(4) Each product gets its own clean white card with rounded corners and a soft shadow, one product photo per card. ` +
    `(5) Keep all text crisp, correctly spelled, and high-contrast.`;

  if (mode === 'comparison') {
    const a = safeItems[0] || {};
    const b = safeItems[1] || {};
    return (
      'Create a vertical 1000x1500 px (2:3) Pinterest pin: a clean, modern side-by-side PRODUCT COMPARISON graphic. ' +
      `A bold header band across the top shows the title "${String(headline || 'Which one should you buy?').slice(0, 90)}". ` +
      'Below the header, split the pin into TWO equal columns. ' +
      `LEFT column = product A: photo of "${String(a.title || 'Option A').slice(0, 80)}" on a card, with a circular "A" badge and the product name as a short label beneath it. ` +
      `RIGHT column = product B: photo of "${String(b.title || 'Option B').slice(0, 80)}" on a card, with a circular "B" badge and the product name as a short label beneath it. ` +
      'Place one bold circular "VS" badge in the exact center between the two columns. ' +
      'Lots of white space, big readable typography, scroll-stopping but uncluttered.' +
      fidelityRules +
      paletteHint +
      footerHint
    );
  }

  // roundup / gift guide — numbered list of cards
  const numberedList = safeItems
    .map((it, i) => `Card ${i + 1} = "${String(it.title || `Product ${i + 1}`).slice(0, 80)}"`)
    .join('; ');
  return (
    `Create a vertical 1000x1500 px (2:3) Pinterest pin: a clean, modern PRODUCT ROUNDUP / gift-guide graphic with EXACTLY ${n} numbered cards stacked vertically. ` +
    `A bold header band across the top shows the title "${String(headline || `${n} Top Picks`).slice(0, 90)}". ` +
    `Below the header, list the ${n} products as ${n} separate rounded cards, top to bottom, in this exact order: ${numberedList}. ` +
    `Each card has a LARGE, clearly visible NUMBER BADGE in the top-left corner showing its position as a single digit, numbered strictly 1, 2, 3 … ${n} from top to bottom with no skipped, repeated, or out-of-order numbers. ` +
    `Inside each card: the product photo on the left and the product name as a short label on the right. ` +
    'Generous spacing between cards, bright, high readability, Pinterest-friendly.' +
    fidelityRules +
    paletteHint +
    footerHint
  );
}

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

function normalizeSchedulerBoardPinSamples(rawPins) {
  if (!Array.isArray(rawPins)) return [];
  const out = [];
  for (const item of rawPins.slice(0, 10)) {
    const p = item && typeof item === 'object' ? item : {};
    const title = String(p.title || '').trim();
    const description = String(p.description || p.excerpt || '').trim();
    const link = String(p.link || p.url || '').trim();
    if (!title && !description) continue;
    out.push({
      title: title.slice(0, 120),
      description: description.slice(0, 280),
      link: link.slice(0, 200),
    });
  }
  return out;
}

function inferTopicFromPinSamples(samples) {
  const blob = samples
    .map((s) => `${s.title} ${s.description}`)
    .join(' ')
    .toLowerCase()
    .replace(/[^\w\s]/g, ' ');
  const stop = new Set([
    'the', 'and', 'for', 'with', 'your', 'this', 'that', 'from', 'into', 'about', 'best', 'how', 'what',
    'why', 'when', 'tips', 'ideas', 'guide', 'easy', 'simple', 'free', 'pin', 'pins',
  ]);
  const freq = new Map();
  for (const w of blob.split(/\s+/)) {
    if (w.length < 4 || stop.has(w)) continue;
    freq.set(w, (freq.get(w) || 0) + 1);
  }
  const ranked = [...freq.entries()].sort((a, b) => b[1] - a[1]);
  if (ranked.length >= 2) {
    return `${ranked[0][0]} ${ranked[1][0]}`.replace(/\b\w/g, (c) => c.toUpperCase());
  }
  if (ranked.length === 1) {
    return ranked[0][0].replace(/\b\w/g, (c) => c.toUpperCase());
  }
  const firstTitle = samples[0]?.title || '';
  const words = firstTitle.replace(/[^\w\s]/g, ' ').split(/\s+/).filter((w) => w.length > 2);
  return words.slice(0, 3).join(' ') || 'Inspiration';
}

function buildSchedulerBoardSuggestionHeuristic(samples, variationSeed = 0) {
  const topic = inferTopicFromPinSamples(samples);
  const seed = Number(variationSeed) || 0;
  const nameTemplates = [
    `${topic} Ideas`,
    `Best ${topic}`,
    `${topic} Tips & Tricks`,
    `${topic} Inspiration`,
    `${topic} Saves`,
    `${topic} for Pinterest`,
  ];
  const name = nameTemplates[Math.abs(seed) % nameTemplates.length].slice(0, 50);
  const descSamples = samples.map((s) => s.description).filter(Boolean);
  const descTemplates = [
    `Save the best ${topic.toLowerCase()} ideas, tips, and inspiration — curated from your latest pins.`,
    `A focused board for ${topic.toLowerCase()} content worth saving and sharing on Pinterest.`,
    `Hand-picked ${topic.toLowerCase()} pins to help you plan, save, and post with confidence.`,
    `Everything ${topic.toLowerCase()} in one place: practical ideas you can use in your next pins.`,
  ];
  const description = (
    descSamples[seed % descSamples.length] ||
    descTemplates[Math.abs(seed) % descTemplates.length]
  ).slice(0, 250);
  return { name, description, topic };
}

async function maybeAiSchedulerBoardCopy(samples, variationSeed, openaiClient) {
  if (process.env.SCHEDULER_BOARD_SUGGEST_AI === '0' || !process.env.OPENAI_API_KEY || !openaiClient) {
    return null;
  }
  try {
    const seed = Number(variationSeed) || 0;
    const completion = await openaiClient.chat.completions.create({
      model: process.env.SCHEDULER_BOARD_SUGGEST_MODEL || process.env.PINTEREST_BOARD_TOOL_MODEL || 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content:
            `Create ONE Pinterest board name and description for pins the user is scheduling.\n` +
            `Pin samples (JSON): ${JSON.stringify(samples)}\n` +
            `variation_seed: ${seed} (if > 0, pick a noticeably different angle than a generic board)\n` +
            `Return JSON only: { "name": string, "description": string }\n` +
            `Rules:\n` +
            `- name: 2-6 words, clear niche/topic, no emoji, max 50 characters\n` +
            `- description: 1-2 sentences, helpful for Pinterest search, max 220 characters, no emoji\n` +
            `- Must match the theme of the pin samples\n` +
            `- Do not mention "AI" or "generated"\n`,
        },
      ],
      max_tokens: 220,
      temperature: seed > 0 ? 0.85 : 0.65,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    const parsed = JSON.parse(jsonMatch[0]);
    const name = String(parsed?.name || '').trim().slice(0, 50);
    const description = String(parsed?.description || '').trim().slice(0, 250);
    if (!name) return null;
    return { name, description };
  } catch (e) {
    console.warn('maybeAiSchedulerBoardCopy:', e.message || e);
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

    const rawUrl = String(url || '').trim();
    // Shared pipeline with generate: short-link resolve; RapidAPI-first for Etsy, Amazon PDPs,
    // and Walmart product/affiliate links when `RAPIDAPI_KEY` is set.
    const { base, articleSummary } = await fetchArticleBaseAndSummary(rawUrl, null, null);

    const hasScrapeContent =
      (base.title && String(base.title).trim().length >= 3) ||
      (base.description && String(base.description).trim().length >= 20);

    if (!hasScrapeContent) {
      return res.status(502).json({
        error:
          'Could not load this page. Many sites (including Medium) block automated requests. We retry with a browser when possible — if it still fails, try a different URL or paste your article on a blog you control.',
      });
    }

    const meta = {
      title: base.title || '',
      description: base.description || '',
      canonicalUrl: base.canonicalUrl || '',
      domain: base.domain || '',
      keyword: base.keyword || '',
      linkDisplay: base.linkDisplay || '',
      requiresManualBrandOrCta: !!base.requiresManualBrandOrCta,
      brandingGateReason: base.brandingGateReason ?? null,
      brandingGateMessage: base.brandingGateMessage ?? null,
    };
    if (base.amazonLanding) meta.amazonLanding = true;
    if (base.walmartLanding) meta.walmartLanding = true;
    if (base.creatorAffiliateLanding) meta.creatorAffiliateLanding = true;
    if (base.etsyLanding) meta.etsyLanding = true;
    if (articleSummary && String(articleSummary).trim().length > 0) {
      meta.articleSummary = articleSummary;
    }
    if (base.etsy_oembed_thumbnail) meta.etsy_oembed_thumbnail = base.etsy_oembed_thumbnail;
    if (Array.isArray(base.etsy_rapidapi_image_urls) && base.etsy_rapidapi_image_urls.length > 0) {
      meta.etsy_rapidapi_image_urls = base.etsy_rapidapi_image_urls;
    }
    if (enrich) {
      try {
        meta.contentProfile = await enrichContentProfile(meta, openai);
      } catch (e) {
        console.warn('urltopin scrape enrich error:', e.message || e);
      }
    }
    return res.json(meta);
  } catch (err) {
    console.error('urltopin scrape error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// --- Free anonymous "first pin" preview (no auth) -------------------------
// Cold SEO/Pinterest traffic hits a signup wall before seeing any value. This
// endpoint generates ONE real pin from the visitor's URL so they get the "aha"
// moment, then the frontend shows the signup wall to unlock the rest. It is
// deliberately isolated from /generate: no auth, no quota writes, no scheduling
// or export, exactly one pin, watermarked, and strictly rate-limited so it can
// never run up the image bill.
// Default: one free preview per IP (long window ≈ "once per visitor"), not per day.
// The free plan is only 10 pins lifetime, so a single taste is enough to earn the
// signup; more would just cost image credits with no extra conversion. NOTE: the
// limiter is in-memory, so this resets on each backend restart/deploy (which also
// conveniently auto-unblocks shared/NAT IPs over time).
const FREE_PREVIEW_PER_IP_WINDOW_MS = Math.max(
  60_000,
  parseInt(process.env.FREE_PREVIEW_PER_IP_WINDOW_MS || String(365 * 24 * 60 * 60 * 1000), 10) ||
    365 * 24 * 60 * 60 * 1000
);
const FREE_PREVIEW_PER_IP_MAX = Math.max(1, parseInt(process.env.FREE_PREVIEW_PER_IP_MAX || '1', 10) || 1);
const FREE_PREVIEW_GLOBAL_DAILY_MAX = Math.max(
  1,
  parseInt(process.env.FREE_PREVIEW_GLOBAL_DAILY_MAX || '150', 10) || 150
);
let freePreviewGlobalDay = '';
let freePreviewGlobalCount = 0;
// Global circuit-breaker so a distributed/botnet attack can't exhaust the image budget.
function tryConsumeGlobalFreePreview() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== freePreviewGlobalDay) {
    freePreviewGlobalDay = today;
    freePreviewGlobalCount = 0;
  }
  if (freePreviewGlobalCount >= FREE_PREVIEW_GLOBAL_DAILY_MAX) return false;
  freePreviewGlobalCount += 1;
  return true;
}
function refundGlobalFreePreview() {
  if (freePreviewGlobalCount > 0) freePreviewGlobalCount -= 1;
}

// Overlays a repeated diagonal watermark + bottom banner onto the preview image
// so it cannot be passed off as a finished pin. Signup yields the clean version.
async function watermarkPreviewImage(inputBuffer) {
  const img = sharp(inputBuffer, { failOn: 'none' });
  const meta = await img.metadata();
  const w = meta.width || 1024;
  const h = meta.height || 1536;
  const diagFont = Math.round(w * 0.07);
  const rows = [];
  const stepY = Math.round(diagFont * 3.4);
  const stepX = Math.round(diagFont * 9);
  for (let y = Math.round(h * 0.12); y < h; y += stepY) {
    for (let x = -Math.round(w * 0.3); x < w; x += stepX) {
      rows.push(
        `<text x="${x}" y="${y}" transform="rotate(-28 ${x} ${y})" class="wm">URL2Pin</text>`
      );
    }
  }
  const bannerH = Math.round(h * 0.085);
  const bannerFont = Math.round(w * 0.042);
  const svg = `<svg width="${w}" height="${h}" xmlns="http://www.w3.org/2000/svg">
    <style>
      .wm { fill:#ffffff; fill-opacity:0.30; font-family:Arial, Helvetica, sans-serif; font-weight:800; font-size:${diagFont}px; }
      .banner { fill:#ffffff; font-family:Arial, Helvetica, sans-serif; font-weight:700; font-size:${bannerFont}px; }
    </style>
    ${rows.join('')}
    <rect x="0" y="${h - bannerH}" width="${w}" height="${bannerH}" fill="#000000" fill-opacity="0.55"/>
    <text x="${Math.round(w / 2)}" y="${h - Math.round(bannerH * 0.32)}" text-anchor="middle" class="banner">Free preview — sign up to remove</text>
  </svg>`;
  return img
    .composite([{ input: Buffer.from(svg), top: 0, left: 0 }])
    .jpeg({ quality: 82 })
    .toBuffer();
}

function goalLabelForPreview(goal) {
  switch (goal) {
    case 'clicks':
      return 'High Click Potential';
    case 'saves':
      return 'Save-Friendly';
    case 'engagement':
      return 'Engagement Focused';
    case 'trust':
      return 'Trust & Clarity';
    default:
      return 'Experimental';
  }
}

app.post('/api/urltopin/preview', async (req, res) => {
  let consumedGlobal = false;
  // Local dev only: skip the per-IP + global caps so the preview can be tested
  // repeatedly. Never active in production (see isLocalhostDevBypass).
  const devBypassLimits = isLocalhostDevBypass(req);
  try {
    // 1. Strict per-IP limit (default: 1 free preview per IP per 24h).
    if (
      !devBypassLimits &&
      !rateLimitTool(req, 'free-preview', {
        windowMs: FREE_PREVIEW_PER_IP_WINDOW_MS,
        max: FREE_PREVIEW_PER_IP_MAX,
      })
    ) {
      return res.status(429).json({
        error: 'preview_limit_reached',
        message:
          'You’ve used your free preview pin. Sign up free to generate the full set — up to 10 pins per URL, no watermark, with download. Schedule to Pinterest unlocks on paid plans.',
      });
    }

    // 2. Global daily circuit-breaker (protects the image budget).
    if (!devBypassLimits) {
      if (!tryConsumeGlobalFreePreview()) {
        return res.status(429).json({
          error: 'preview_capacity',
          message: 'Free previews are at capacity right now. Sign up free to generate your pins instantly.',
        });
      }
      consumedGlobal = true;
    }

    const { url, articleData, outputLanguage: rawOutputLanguage, brand } = req.body || {};
    const rawUrl = String(url || '').trim();
    if (!rawUrl) {
      if (consumedGlobal) refundGlobalFreePreview();
      return res.status(400).json({ error: 'Missing url' });
    }
    const outputLanguage = String(rawOutputLanguage || 'auto').trim().toLowerCase() || 'auto';

    let workingUrl = rawUrl;
    try {
      const expanded = await resolveOutboundUrlForUrlToPin(rawUrl);
      if (expanded) workingUrl = expanded;
    } catch {
      /* ignore */
    }

    const brandingGate = assessUrlBrandingGate(workingUrl);
    const brandName = String(brand?.brandName || '').trim() || null;
    if (brandingGate.requiresManualBrandOrCta && !brandName) {
      if (consumedGlobal) refundGlobalFreePreview();
      return res.status(400).json({
        error: 'branding_required',
        ...brandingGate,
      });
    }

    // 3. Scrape (same pipeline as /scrape and /generate).
    const { base, articleSummary } = await fetchArticleBaseAndSummary(rawUrl, articleData || null, {
      outputLanguage,
      preResolvedUrl: workingUrl,
    });
    const hasScrapeContent =
      (base.title && String(base.title).trim().length >= 3) ||
      (base.description && String(base.description).trim().length >= 20);
    if (!hasScrapeContent) {
      if (consumedGlobal) refundGlobalFreePreview();
      return res.status(502).json({
        error: 'scrape_failed',
        message:
          'We couldn’t read this page (some sites block automated access). Try a different URL — a blog post or product page works best.',
      });
    }

    // 4. Enrich + plan exactly ONE strategy.
    let contentProfile = await enrichContentProfile(base, openai);
    contentProfile = mergeProductAffiliateLandingIntoProfile(contentProfile, base);
    const plan = planStrategies(contentProfile, 1);
    const p0 = plan && plan[0];
    if (!p0) {
      if (consumedGlobal) refundGlobalFreePreview();
      return res.status(502).json({ error: 'plan_failed', message: 'Could not plan a pin for this page.' });
    }

    const refHarvest = await harvestNanoBananaReferenceImagesForUrlToPin({
      userId: PREVIEW_ANON_STORAGE_USER_ID,
      workingUrl,
      base,
    });
    const layoutId = replaceInfographicStyleIdForAmazonNanoRefs(
      p0.layoutId,
      refHarvest.images.length > 0
    );
    const p = { ...p0, layoutId };

    // 5. Metadata for the single pin (mirrors the strategic path in /generate).
    const keyword = base.keyword || '';
    const keyIdeas = await extractArticleKeyIdeas(articleSummary, openai);
    const angle = pickAngle(p.strategy, contentProfile, []);
    const layoutOverlayGuidance = STYLE_ON_IMAGE_TEXT_GUIDANCE[p.layoutId] || null;
    const meta = await generateStrategicPinMetadata(
      {
        articleSummary,
        keyword,
        strategy: p.strategy,
        layoutId: p.layoutId,
        suggestedAngle: angle,
        keyIdeas,
        usedOverlayTexts: [],
        layoutOverlayGuidance,
        outputLanguage,
        strictLanguage: false,
      },
      openai
    );

    const topic = contentProfile?.topic || base.title || keyword || 'this topic';
    const domain = base.domain || '';
    const pinFooterSourceLine = brandName || domain;
    const overlayText = {
      headline: meta.overlay_headline || topic,
      subheadline: meta.overlay_subheadline || '',
      source: pinFooterSourceLine,
    };

    // 6. Build the image prompt and generate ONE image (product refs when available).
    let imagePrompt = buildOverlayImagePrompt({
      styleId: p.layoutId,
      topic,
      domain,
      keyword,
      year: new Date().getFullYear(),
      overlayText,
      brand: brandName ? { brandName } : null,
      stepCount: meta.step_count ?? null,
      niche: usesProductAffiliatePinMix(contentProfile) ? 'amazon_affiliate' : contentProfile?.niche || null,
    });
    imagePrompt = appendNanoBananaAmazonUrlGarbageGuard(imagePrompt, workingUrl);
    imagePrompt = appendNanoBananaReferencePromptSuffix(imagePrompt, refHarvest.source);

    let rawImageUrl = '';
    if (process.env.USE_DUMMY_IMAGES === 'true') {
      rawImageUrl = 'https://via.placeholder.com/1024x1536.png?text=URL2Pin+Preview';
    } else {
      const providerSoftTimeoutMs = Math.max(
        30_000,
        parseInt(process.env.URLTOPIN_IMAGE_PROVIDER_SOFT_TIMEOUT_MS || '360000', 10) || 360000
      );
      const nanoOpts = refHarvest.images.length ? { imageInput: refHarvest.images } : {};
      rawImageUrl =
        (await withSoftTimeout(
          generateImageWithNanoBanana(imagePrompt, `preview-${p.layoutId}`, nanoOpts),
          providerSoftTimeoutMs
        )) || '';
    }

    if (!rawImageUrl) {
      if (consumedGlobal) refundGlobalFreePreview();
      return res.status(502).json({
        error: 'preview_image_failed',
        message: 'We couldn’t generate the preview just now. Please try again in a moment.',
      });
    }

    // 7. Watermark the image. Download → composite → return as a data URL so the
    //    full-resolution clean image is never exposed pre-signup. If anything
    //    fails, fall back to the raw image URL (never break the aha moment).
    let previewImage = rawImageUrl;
    let watermarked = false;
    try {
      const imgRes = await fetch(rawImageUrl);
      if (imgRes.ok) {
        const buf = Buffer.from(await imgRes.arrayBuffer());
        const wmBuf = await watermarkPreviewImage(buf);
        previewImage = `data:image/jpeg;base64,${wmBuf.toString('base64')}`;
        watermarked = true;
      }
    } catch (e) {
      console.warn('urltopin preview watermark error:', e.message || e);
    }

    return res.json({
      preview: true,
      watermarked,
      totalAvailable: 10,
      pin: {
        styleId: p.layoutId,
        imageUrl: previewImage,
        title: meta.title || topic,
        description: meta.description || base.description || '',
        overlayText,
        strategy: p.strategy,
        goal: p.goal,
        goalLabel: goalLabelForPreview(p.goal),
        link: rawUrl,
      },
    });
  } catch (err) {
    if (consumedGlobal) refundGlobalFreePreview();
    console.error('urltopin preview error:', err);
    return res.status(500).json({
      error: 'preview_failed',
      message: 'Something went wrong generating your preview. Please try again.',
    });
  }
});

app.post('/api/urltopin/plan-strategic', requireUser, async (req, res) => {
  try {
    const {
      url,
      articleData,
      count: rawCount,
      pinsPerUrl: rawPinsPerUrl,
      outputLanguage: rawOutputLanguage,
      winnerContext: rawWinnerContext,
      manualProduct: rawManualProduct,
    } = req.body || {};
    if (!url) {
      return res.status(400).json({ error: 'Missing url' });
    }
    const winnerContext = normalizeWinnerContext(rawWinnerContext);
    const manualProduct = normalizeManualProductOverride(rawManualProduct);
    const requestedCount = Number.parseInt(rawPinsPerUrl ?? rawCount ?? 10, 10);
    const count = [2, 3, 5, 10].includes(requestedCount) ? requestedCount : 10;
    const outputLanguage = String(rawOutputLanguage || '').trim().toLowerCase();
    const { base } = await fetchArticleBaseAndSummary(url, articleData || null, { outputLanguage });
    if (manualProduct?.title) {
      base.title = manualProduct.title;
      if (manualProduct.description) base.description = manualProduct.description.slice(0, 450);
      base.amazon_blocked = false;
    }
    let contentProfile = await enrichContentProfile(base, openai, winnerContext);
    contentProfile = mergeProductAffiliateLandingIntoProfile(contentProfile, base);
    const plan = planStrategies(contentProfile, count, winnerContext);
    const strategyCounts = {};
    plan.forEach((p) => { strategyCounts[p.strategy] = (strategyCounts[p.strategy] || 0) + 1; });
    const topStrategies = Object.entries(strategyCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([s]) => s.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()));
    return res.json({
      plan,
      contentProfile,
      top_strategies: topStrategies,
      ...(winnerContext ? { clone_mode: true } : {}),
    });
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
      outputLanguage: rawOutputLanguage,
      strictLanguage: rawStrictLanguage,
      imageSource = 'ai',
      userImageUrls: rawUserImageUrls,
      usePageReferenceImages: rawUsePageReferenceImages,
      metadataOnly: rawMetadataOnly,
      winnerContext: rawWinnerContext,
      manualProduct: rawManualProduct,
    } = req.body || {};
    const winnerContext = normalizeWinnerContext(rawWinnerContext);
    const manualProduct = normalizeManualProductOverride(rawManualProduct);
    const outputLanguage = String(rawOutputLanguage || 'auto').trim().toLowerCase() || 'auto';
    const strictLanguage = rawStrictLanguage === true || rawStrictLanguage === 'true';
    const metadataOnly = rawMetadataOnly === true || rawMetadataOnly === 'true';
    const affiliateDisclosure = normalizeAffiliateDisclosureRequest(req.body || {});
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

    const rawUrl = String(url || '').trim();
    let effectiveUrl = rawUrl;
    if (rawUrl) {
      try {
        const expanded = await resolveOutboundUrlForUrlToPin(rawUrl);
        if (expanded) effectiveUrl = expanded;
      } catch (e) {
        console.warn('urltopin generate resolveOutboundUrl error:', e.message || e);
      }
    }

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
      const fetched = await fetchArticleBaseAndSummary(rawUrl, articleData, {
        preResolvedUrl: effectiveUrl,
        outputLanguage,
      });
      const { base } = fetched;
      let contentProfile = await enrichContentProfile(base, openai, winnerContext);
      contentProfile = mergeProductAffiliateLandingIntoProfile(contentProfile, base);
      const plan = planStrategies(contentProfile, Math.min(count || 10, 10), winnerContext);
      effectiveStyles = plan.map((p) => p.layoutId);
      req._strategicPlan = plan;
      req._contentProfile = contentProfile;
      req._fetchedArticle = fetched;
    }

    if (!rawUrl || effectiveStyles.length === 0) {
      return res.status(400).json({
        error: isStrategic ? 'Missing url or articleData for strategic mode' : 'Missing url or styles',
      });
    }

    const brandingGate = assessUrlBrandingGate(effectiveUrl);
    if (brandingGate.requiresManualBrandOrCta && !String(brand?.brandName || '').trim()) {
      return res.status(400).json({
        error: 'branding_required',
        ...brandingGate,
      });
    }

    const fastForFanOut = isStrategicSingle && !!articleData;
    const fetchedBase =
      req._fetchedArticle ||
      (await fetchArticleBaseAndSummary(rawUrl, articleData, {
        ...(fastForFanOut ? { fast: true } : {}),
        preResolvedUrl: effectiveUrl,
        outputLanguage,
      }));
    const base = fetchedBase.base;
    let articleSummary = fetchedBase.articleSummary;

    // Amazon: if scraping is blocked, try paid product-data API BEFORE spending quota / Nano Banana.
    const amazonCtxUrl = pickAmazonContextUrl(effectiveUrl, base.canonicalUrl);
    let amazonRapid = base.amazon_rapidapi_data || null;
    if (base?.amazon_blocked && isAmazonProductPageForNanoReference(amazonCtxUrl)) {
      const asin = extractAmazonAsinFromUrl(amazonCtxUrl);
      if (asin && !amazonRapid) {
        try {
          const host = new URL(amazonCtxUrl).hostname;
          amazonRapid = await fetchAmazonProductDataViaRapidApi({
            asin,
            marketplace: resolveRapidApiAmazonMarketplaceFromHost(host),
            language: 'en',
          });
        } catch {
          amazonRapid = null;
        }
      }
      if (!amazonRapid && manualProduct?.title) {
        // User supplied the product details manually (recovery path) — proceed with those.
        base.title = manualProduct.title;
        if (manualProduct.description) {
          base.description = manualProduct.description.slice(0, 450);
        }
        const manualSummaryParts = [manualProduct.title, manualProduct.description].filter(Boolean);
        if (manualSummaryParts.length) {
          articleSummary = manualSummaryParts.join('. ').slice(0, 600);
        }
        base.amazon_blocked = false;
        base.manual_product_override = true;
      } else if (!amazonRapid) {
        return res.status(409).json({
          error: 'amazon_blocked',
          message:
            'Amazon blocked automated access to this product page, so we can’t reliably read the product title/content right now. Enter the product title (and optionally upload a product photo) to build pins anyway, try again later, or use a non-Amazon landing page (your blog post / product page) for this pin.',
          recoverable: true,
        });
      }
      if (!base.title && typeof amazonRapid.title === 'string' && amazonRapid.title.trim()) {
        base.title = amazonRapid.title.trim();
      }
      if (
        (!base.description || looksLikeGenericAmazonTitle(base.description)) &&
        typeof amazonRapid.description === 'string' &&
        amazonRapid.description.trim()
      ) {
        base.description = amazonRapid.description.trim().slice(0, 450);
      }
      const rapidSummary = buildAmazonRapidApiSummary(amazonRapid);
      if (rapidSummary) {
        articleSummary = rapidSummary;
      }
      base.amazon_blocked = false;
    }

    // Walmart: blocks bots aggressively, so when scraping returns a bot/empty title,
    // recover the product title/description (and reuse images) from RapidAPI before spending quota.
    const walmartCtxUrl = pickWalmartContextUrl(effectiveUrl, base.canonicalUrl);
    let walmartRapid = base.walmart_rapidapi_data || null;
    if (
      isWalmartProductPageForNanoReference(walmartCtxUrl) &&
      !walmartRapid &&
      (!base.title || looksLikeBlockedWalmartTitle(base.title))
    ) {
      try {
        walmartRapid = await fetchWalmartProductDataViaRapidApi({
          itemId: extractWalmartItemIdFromUrl(walmartCtxUrl),
          url: walmartCtxUrl,
        });
      } catch {
        walmartRapid = null;
      }
      if (walmartRapid) {
        if ((!base.title || looksLikeBlockedWalmartTitle(base.title)) && walmartRapid.title) {
          base.title = walmartRapid.title;
        }
        if (!base.description && walmartRapid.description) {
          base.description = walmartRapid.description.slice(0, 450);
        }
        if (!articleSummary && walmartRapid.description) {
          articleSummary = walmartRapid.description.slice(0, 600);
        }
      } else if (manualProduct?.title) {
        base.title = manualProduct.title;
        if (manualProduct.description) base.description = manualProduct.description.slice(0, 450);
      }
    }

    // Own-photo composites use a separate monthly cap (no image model). AI pins use pins_used.
    const pinsToGenerate = effectiveStyles.length;
    const aiPins = metadataOnly || useUserComposite || useTextBased ? 0 : pinsToGenerate;
    const userPhotoPins = metadataOnly ? 0 : useUserComposite || useTextBased ? pinsToGenerate : 0;
    const usageResult = await applyPinQuotaDelta(
      req.user.id,
      {
        aiDelta: aiPins,
        userPhotoDelta: userPhotoPins,
      },
      req
    );
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
    const year = new Date().getFullYear();
    const domain =
      (base.linkDisplay || base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

    let nanoBananaReferenceInputs = [];
    let nanoBananaReferenceSource = null;
    const refHtmlUrl = amazonCtxUrl || effectiveUrl;
    const refHarvestCacheKey = nanoRefHarvestCacheKey(
      req.user.id,
      amazonCtxUrl || effectiveUrl,
      manualProduct
    );
    if (!metadataOnly && !useTextBased && !useUserComposite) {
      const cachedRefs = getNanoRefHarvestFromCache(refHarvestCacheKey);
      if (cachedRefs) {
        nanoBananaReferenceInputs = cachedRefs.images;
        nanoBananaReferenceSource = cachedRefs.source;
        console.log(
          `urltopin: Nano Banana reference images (cached): ${cachedRefs.images.length} (${String(refHtmlUrl).slice(0, 96)})`
        );
      }
    }
    if (!metadataOnly && !useTextBased && !useUserComposite && nanoBananaReferenceInputs.length === 0) {
      // Etsy: prefer RapidAPI listing images for Nano Banana; fallback to oEmbed thumbnail.
      const rapidEtsyUrls = Array.isArray(base?.etsy_rapidapi_image_urls) ? base.etsy_rapidapi_image_urls : [];
      if (rapidEtsyUrls.length > 0) {
        try {
          nanoBananaReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana(
            rapidEtsyUrls.slice(0, 3),
            req.user.id
          );
          if (nanoBananaReferenceInputs.length > 0) {
            nanoBananaReferenceSource = 'etsy_product';
          }
        } catch (e) {
          console.warn('urltopin Etsy RapidAPI images mirror error:', e.message || e);
        }
      }
      if (nanoBananaReferenceInputs.length === 0) {
        const etsyThumb = String(base?.etsy_oembed_thumbnail || '').trim();
        if (etsyThumb) {
          try {
            nanoBananaReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana([etsyThumb], req.user.id);
            if (nanoBananaReferenceInputs.length > 0) {
              nanoBananaReferenceSource = 'page';
            }
          } catch (e) {
            console.warn('urltopin Etsy oEmbed thumbnail mirror error:', e.message || e);
          }
        }
      }
    }
    // Manual recovery: user uploaded a product photo because the page couldn't be scraped.
    if (
      !metadataOnly &&
      !useTextBased &&
      !useUserComposite &&
      process.env.USE_DUMMY_IMAGES !== 'true' &&
      nanoBananaReferenceInputs.length === 0 &&
      manualProduct?.imageUrls?.length
    ) {
      try {
        nanoBananaReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana(
          manualProduct.imageUrls.slice(0, 3),
          req.user.id
        );
        if (nanoBananaReferenceInputs.length > 0) {
          nanoBananaReferenceSource = 'manual_product';
        }
      } catch (e) {
        console.warn('urltopin manual product images mirror error:', e.message || e);
      }
    }
    if (
      !metadataOnly &&
      process.env.URLTOPIN_AMAZON_PRODUCT_IMAGES !== '0' &&
      !useTextBased &&
      !useUserComposite &&
      process.env.USE_DUMMY_IMAGES !== 'true' &&
      nanoBananaReferenceInputs.length === 0 &&
      isAmazonProductPageForNanoReference(amazonCtxUrl)
    ) {
      try {
        // If we already have paid API data (because scraping was blocked), use its images directly.
        if (amazonRapid && Array.isArray(amazonRapid.images) && amazonRapid.images.length > 0) {
          const candidates = amazonRapid.images
            .map((im) => (im && typeof im === 'object' ? (im.hi_res || im.image || im.large || '') : ''))
            .filter(Boolean);
          if (candidates.length > 0) {
            nanoBananaReferenceInputs = await mirrorAmazonImageUrlsForNanoBanana(candidates, req.user.id);
            if (nanoBananaReferenceInputs.length > 0) {
              nanoBananaReferenceSource = 'amazon_product';
              console.log(
                `urltopin: Nano Banana Amazon reference images (RapidAPI): ${nanoBananaReferenceInputs.length} (${String(amazonCtxUrl).slice(0, 96)})`
              );
            }
          }
        }
        if (nanoBananaReferenceInputs.length > 0) {
          // already populated from paid API
        } else {
        const azHtml = await fetchArticleHtml(amazonCtxUrl);
        let candidates = extractAmazonProductImageUrlsFromHtml(azHtml, amazonCtxUrl);
        if (candidates.length === 0 || detectAmazonBotOrConsentPage(azHtml)) {
          const widgetImg = await fetchAmazonAsinWidgetImageUrl(amazonCtxUrl);
          if (widgetImg) candidates = [widgetImg];
        }
        if (candidates.length > 0) {
          nanoBananaReferenceInputs = await mirrorAmazonImageUrlsForNanoBanana(candidates, req.user.id);
          if (nanoBananaReferenceInputs.length > 0) {
            nanoBananaReferenceSource = 'amazon_product';
            console.log(
              `urltopin: Nano Banana Amazon reference images: ${nanoBananaReferenceInputs.length} (${String(amazonCtxUrl).slice(0, 96)})`
            );
          }
        }
        }
      } catch (e) {
        console.warn('urltopin Amazon product images for Nano:', e.message || e);
      }
      if (nanoBananaReferenceInputs.length === 0) {
        console.warn(
          '[urltopin] Amazon product page: no reference images attached this run (pins will use AI-only visuals).',
          {
            url: String(amazonCtxUrl).slice(0, 120),
            rapidApi: !!amazonRapid,
            rapidImageCount: Array.isArray(amazonRapid?.images) ? amazonRapid.images.length : 0,
            amazonBlocked: !!base?.amazon_blocked,
          }
        );
      }
    } else if (
      !metadataOnly &&
      process.env.URLTOPIN_WALMART_PRODUCT_IMAGES !== '0' &&
      !useTextBased &&
      !useUserComposite &&
      process.env.USE_DUMMY_IMAGES !== 'true' &&
      nanoBananaReferenceInputs.length === 0 &&
      isWalmartProductPageForNanoReference(walmartCtxUrl)
    ) {
      try {
        const harvested = await harvestWalmartReferenceImages(
          walmartCtxUrl,
          req.user.id,
          walmartRapid
        );
        nanoBananaReferenceInputs = harvested.images;
        if (harvested.rapid) walmartRapid = harvested.rapid;
        if (nanoBananaReferenceInputs.length > 0) {
          nanoBananaReferenceSource = 'walmart_product';
          console.log(
            `urltopin: Nano Banana Walmart reference images: ${nanoBananaReferenceInputs.length} (${String(walmartCtxUrl).slice(0, 96)})`
          );
        } else {
          console.warn(
            '[urltopin] Walmart product page: no reference images attached this run (pins will use AI-only visuals).',
            { url: String(walmartCtxUrl).slice(0, 120), rapidApi: !!walmartRapid }
          );
        }
      } catch (e) {
        console.warn('urltopin Walmart product images for Nano:', e.message || e);
      }
    } else if (
      !metadataOnly &&
      usePageReferenceImages &&
      process.env.URLTOPIN_PAGE_REFERENCE_IMAGES !== '0' &&
      !useTextBased &&
      !useUserComposite &&
      process.env.USE_DUMMY_IMAGES !== 'true'
    ) {
      try {
        // Etsy listing HTML 403s server-side; refs already come from RapidAPI / oEmbed on the main URL path.
        let pageHtml = '';
        if (!isEtsyListingPageUrl(refHtmlUrl)) {
          pageHtml = await fetchArticleHtml(refHtmlUrl);
        }
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
      setNanoRefHarvestCache(refHarvestCacheKey, nanoBananaReferenceInputs, nanoBananaReferenceSource);
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
      amazon_affiliate:
        'Amazon / shopping: clear product hero, honest worth-it vibe; no fake prices, ratings, or Amazon UI as invented text.',
    };

    const contentProfile = req._contentProfile || null;
    const niche = contentProfile?.niche || null;
    const nicheForVisualHints = usesProductAffiliatePinMix(contentProfile) ? 'amazon_affiliate' : niche;
    const stylePrompts = [];
    let strategicMetadataByIndex = [];
    let keyIdeas = [];
    const usedAngles = [];
    if ((isStrategic || isStrategicSingle) && req._strategicPlan) {
      const plan = req._strategicPlan;
      keyIdeas = await extractArticleKeyIdeas(articleSummary, openai);
      const usedOverlayByLayout = new Map(); // layoutId -> [{ headline, subheadline }, ...]
      const metaResults = [];
      const priorPinCopy = [];
      if (winnerContext) {
        priorPinCopy.push({
          title: winnerContext.title,
          overlay_headline: winnerContext.overlayHeadline,
          overlay_subheadline: winnerContext.overlaySubheadline || '',
        });
      }
      const winnerCtxForMeta =
        req.body?.winnerContext != null ? winnerContext : null;
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
            priorPinCopy: priorPinCopy.length ? [...priorPinCopy] : undefined,
            layoutOverlayGuidance,
            outputLanguage,
            strictLanguage,
            winnerContext: winnerCtxForMeta,
          },
          openai
        );
        metaResults.push(meta);
        priorPinCopy.push({
          title: meta.title || '',
          overlay_headline: meta.overlay_headline || '',
          overlay_subheadline: meta.overlay_subheadline || '',
        });
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
        const nicheHint =
          nicheForVisualHints && nicheVisualHints[nicheForVisualHints]
            ? ` Niche-specific visual guidance: ${nicheVisualHints[nicheForVisualHints]}`
            : '';
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
          ...(meta.angle && { angle: meta.angle }),
          ...(meta.reason && { reason: meta.reason }),
        });
      }
    } else {
      const usedOverlayByStyle = new Map();
      const metaKeyForManual = (sp) => (effectiveStyles.length > 1 && sp.index != null ? `${sp.id}::${sp.index}` : sp.id);
      for (let i = 0; i < stylePrompts.length; i++) {
        const sp = stylePrompts[i];
        const titlePrompt = `${topic}\n\nURL: ${effectiveUrl}\n\nStyle: ${sp.label}`;
        const descPrompt = `${topic}\n\nURL: ${effectiveUrl}\n\nDomain: ${domain}\n\nKeyword: ${keyword}\n\nStyle: ${sp.label}`;
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
              body: JSON.stringify({
                content: titlePrompt,
                type: 'title',
                outputLanguage,
                strictLanguage,
              }),
              signal: c1.signal,
            }),
            fetch(`${process.env.SELF_API_URL || 'http://localhost:' + PORT}/api/generate-field`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': tokenHeader },
              body: JSON.stringify({
                content: descPrompt,
                type: 'description',
                outputLanguage,
                strictLanguage,
              }),
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
              outputLanguage,
              strictLanguage,
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
        niche: usesProductAffiliatePinMix(contentProfile) ? 'amazon_affiliate' : contentProfile?.niche || null,
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
          } else if (nanoBananaReferenceSource === 'walmart_product') {
            imagePrompt +=
              ' ' +
              promptTier(
                'Attached reference image(s) show the real product from the Walmart listing. Use them as the primary hero subject: preserve packaging shape, brand marks, colors, and overall silhouette. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line. Integrate the product naturally; avoid duplicating it as a meaningless second copy unless the layout style requires a collage.',
                'Reference: use attached Walmart product photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.',
              );
          } else if (nanoBananaReferenceSource === 'etsy_product') {
            imagePrompt +=
              ' ' +
              promptTier(
                'Attached reference image(s) are from the Etsy listing (product photos). Use them as the primary hero subject: preserve jewelry/product shape, materials, colors, and overall look. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
                'Reference: use attached Etsy listing photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.',
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

      if (metadataOnly) {
        const metaExtra = meta;
        const descriptionForPin = appendAffiliateDisclosureToDescription(
          pinDescription,
          affiliateDisclosure
        );
        const hashtagsForPin =
          affiliateDisclosure.mode === 'off'
            ? hashtags
            : hashtagsFromPinDescription(descriptionForPin);
        return {
          styleId: sp.id,
          styleLabel: sp.label,
          imagePrompt,
          imageUrl: '',
          title: pinTitle,
          description: descriptionForPin,
          altText: '',
          hashtags: hashtagsForPin,
          link: url,
          overlayText,
          bakedInText: overlayTextForPrompt,
          metadataOnly: true,
          imageGenerationMode: 'metadata_only',
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
      }

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
          const providerSoftTimeoutMs =
            Math.max(30_000, parseInt(process.env.URLTOPIN_IMAGE_PROVIDER_SOFT_TIMEOUT_MS || '360000', 10) || 360000);
          const nanoUrl = await withSoftTimeout(
            generateImageWithNanoBanana(imagePrompt, sp.label, nanoOpts),
            providerSoftTimeoutMs
          );
          imageUrl = nanoUrl || '';
          if (!imageUrl) {
            console.warn('urltopin nano-banana first attempt returned no image (style:', sp.label, '), retrying once');
            const retryUrl = await withSoftTimeout(
              generateImageWithNanoBanana(imagePrompt, sp.label, nanoOpts),
              providerSoftTimeoutMs
            );
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
      const altText = await generatePinterestAltText(
        {
          title: pinTitle,
          description: pinDescription,
          overlayText,
          styleLabel: sp.label,
          linkDisplay: domain || base.linkDisplay || '',
          nanoBananaPrompt: imagePrompt,
          outputLanguage,
          strictLanguage,
        },
        openai
      );
      const descriptionForPin = appendAffiliateDisclosureToDescription(
        pinDescription,
        affiliateDisclosure
      );
      const hashtagsForPin =
        affiliateDisclosure.mode === 'off'
          ? hashtags
          : hashtagsFromPinDescription(descriptionForPin);
      const pinRecord = {
        styleId: sp.id,
        styleLabel: sp.label,
        imagePrompt,
        imageUrl,
        ...(imageUrl
          ? {}
          : {
              image_error: 'image_provider_unavailable_or_queued',
              image_error_message:
                'Image provider is busy or unavailable. Please keep this tab open and retry later, or switch to Text-based pins / My photos.',
            }),
        title: pinTitle,
        description: descriptionForPin,
        altText,
        hashtags: hashtagsForPin,
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
          pin_description: descriptionForPin || null,
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
          ...(altText ? { alt_text: altText } : {}),
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
            description: descriptionForPin,
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

    // If some pins fail to produce an image (provider queue / timeout), refund quota so users
    // are not charged for pins they can't see/use.
    if (!metadataOnly) {
      const failedAi = pins.filter((p) => p?.imageGenerationMode === 'ai' && !String(p?.imageUrl || '').trim()).length;
      const failedUserPhoto = pins.filter(
        (p) => p?.imageGenerationMode === 'user_composite' && !String(p?.imageUrl || '').trim()
      ).length;
      const failedTextBased = pins.filter(
        (p) => p?.imageGenerationMode === 'text_based' && !String(p?.imageUrl || '').trim()
      ).length;

      // Only AI pins affect aiDelta; user composites + text based share the user_photo bucket.
      // (Text-based pins are counted as user_photo pins in this codebase.)
      const refundAi = Math.max(0, failedAi);
      const refundUserPhoto = Math.max(0, failedUserPhoto + failedTextBased);
      if (refundAi > 0 || refundUserPhoto > 0) {
        try {
          await applyPinQuotaDelta(
            req.user.id,
            { aiDelta: refundAi ? -refundAi : 0, userPhotoDelta: refundUserPhoto ? -refundUserPhoto : 0 },
            req
          );
        } catch (e) {
          console.warn('urltopin: quota refund failed:', e?.message || e);
        }
      }
    }

    let finalPins = pins;
    if (isStrategic || isStrategicSingle) {
      const diverse = checkDiversity(pins);
      if (diverse.length >= 10) {
        finalPins = diverse;
      }
      finalPins = rankPins(finalPins);
    }
    return res.json(metadataOnly ? { pins: finalPins, metadataOnly: true } : { pins: finalPins });
  } catch (err) {
    console.error('urltopin generate error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// POST /api/urltopin/regenerate-metadata, regenerate only title or description (fast, no image)
app.post('/api/urltopin/regenerate-metadata', requireUser, async (req, res) => {
  try {
    const {
      url,
      articleData,
      styleId,
      type,
      currentTitle,
      currentDescription,
      outputLanguage: rawOutputLanguage,
      strictLanguage: rawStrictLanguage,
    } = req.body || {};
    if (!url || !styleId || !type || (type !== 'title' && type !== 'description')) {
      return res.status(400).json({ error: 'Missing or invalid url, styleId, or type (must be "title" or "description")' });
    }
    const outputLanguage = String(rawOutputLanguage || 'auto').trim().toLowerCase() || 'auto';
    const strictLanguage = rawStrictLanguage === true || rawStrictLanguage === 'true';
    const affiliateDisclosure = normalizeAffiliateDisclosureRequest(req.body || {});
    const rawUrl = String(url || '').trim();
    let effectiveUrl = rawUrl;
    try {
      const expanded = await resolveOutboundUrlForUrlToPin(rawUrl);
      if (expanded) effectiveUrl = expanded;
    } catch (e) {
      console.warn('urltopin regenerate-metadata resolveOutboundUrl error:', e.message || e);
    }
    const { base } = await fetchArticleBaseAndSummary(rawUrl, articleData || null, {
      preResolvedUrl: effectiveUrl,
    });
    const domain =
      (base.linkDisplay || base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

    recordMetadataUsage(req.user.id, 1).catch((err) =>
      console.warn('recordMetadataUsage(urltopin/regenerate-metadata) error:', err?.message || err)
    );

    const contentBase = `${topic}\n\nURL: ${effectiveUrl}\n\nDomain: ${domain}\n\nKeyword: ${keyword}\n\nStyle: ${styleId}`;
    const currentValue = type === 'title' ? (currentTitle || '') : (currentDescription || '');
    const varietyHint = currentValue.trim()
      ? `\n\nThe current ${type} is: "${currentValue.slice(0, 200)}${currentValue.length > 200 ? '…' : ''}". Write a different alternative; do not repeat or closely copy the current one.`
      : '';

    const prompt = type === 'title'
      ? `Write a compelling Pinterest pin title (aim for 80-100 characters) for this content. Curiosity-driven, descriptive, use emotional triggers or questions. Only return the title, nothing else. Avoid quotes.${varietyHint}\n\n${contentBase}`
      : `Write an engaging Pinterest pin description (max 450 characters) for this content. Explain the benefit or insight. No URLs or "visit/click" CTAs. Include 4–6 relevant hashtags at the end. Only return the description.${varietyHint}\n\n${contentBase}`;

    const languageLine =
      outputLanguage && outputLanguage !== 'auto'
        ? `\n\nLANGUAGE REQUIREMENT: Write the output in ${outputLanguage.toUpperCase()} only. Do not use English.\n`
        : '';

    const detectLang = async (text) => {
      const t = String(text || '').trim();
      if (!t) return 'unknown';
      try {
        const completion = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [
            {
              role: 'user',
              content:
                'Detect the primary language of the text.\n' +
                'Return JSON only with key "lang" as an ISO 639-1 code when possible (e.g. "en", "sk", "cs"), or "unknown".\n' +
                `Text:\n<<<${t.slice(0, 900)}>>>`,
            },
          ],
          max_tokens: 30,
          temperature: 0,
        });
        const raw = completion.choices?.[0]?.message?.content?.trim() || '';
        const m = raw.match(/\{[\s\S]*\}/);
        if (!m) return 'unknown';
        const parsed = JSON.parse(m[0]);
        const out = String(parsed.lang || '').trim().toLowerCase();
        return out || 'unknown';
      } catch {
        return 'unknown';
      }
    };

    const runOnce = async (forceHard = false) => {
      const hardLine =
        outputLanguage && outputLanguage !== 'auto'
          ? forceHard
            ? `\n\nCRITICAL: Output MUST be in ${outputLanguage.toUpperCase()} only. If you cannot comply, output an empty string.\n`
            : languageLine
          : '';
      const effectivePrompt =
        hardLine && !prompt.includes(hardLine) ? `${prompt}${hardLine}` : prompt;
      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: effectivePrompt }],
        max_tokens: type === 'title' ? 150 : 500,
        temperature: 0.85,
      });
      return String(completion.choices?.[0]?.message?.content || '').trim();
    };

    let result = await runOnce(false);
    if (type === 'title') {
      result = result.replace(/["'`~@#$%^&*()_+=\[\]{}|;:<>\\/]+/g, '');
      if (result.length > 100) result = result.slice(0, 100);
      if (strictLanguage && outputLanguage !== 'auto' && result) {
        const detected = await detectLang(result);
        if (detected !== 'unknown' && detected !== outputLanguage) {
          const retry = await runOnce(true);
          let next = String(retry || '').trim().replace(/["'`~@#$%^&*()_+=\[\]{}|;:<>\\/]+/g, '');
          if (next.length > 100) next = next.slice(0, 100);
          if (next) result = next;
        }
      }
      return res.json({ title: result });
    }
    let desc = sanitizeDescription(result);
    if (strictLanguage && outputLanguage !== 'auto' && desc) {
      const detected = await detectLang(desc);
      if (detected !== 'unknown' && detected !== outputLanguage) {
        const retry = await runOnce(true);
        const next = sanitizeDescription(retry);
        if (next) desc = next;
      }
    }
    desc = appendAffiliateDisclosureToDescription(desc, affiliateDisclosure, { maxLength: 450 });
    return res.json({ description: desc });
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
    if (!url || !styleId) {
      return res.status(400).json({ error: 'Missing url or styleId' });
    }
    const rawUrl = String(url || '').trim();
    let effectiveUrl = rawUrl;
    try {
      const expanded = await resolveOutboundUrlForUrlToPin(rawUrl);
      if (expanded) effectiveUrl = expanded;
    } catch (e) {
      console.warn('urltopin regenerate-image resolveOutboundUrl error:', e.message || e);
    }

    const overlayForRender =
      overlayText && typeof overlayText === 'object'
        ? { ...overlayText, footerSourceOnly: true }
        : overlayText;

    const modeNorm = typeof imageGenerationMode === 'string' ? imageGenerationMode.trim().toLowerCase() : '';
    const textBasedNorm = normalizeTextBasedInput(rawTextBased);
    const trimmedUserImgEarly = userImageUrl && String(userImageUrl).trim();
    const overlayNorm =
      overlayText && typeof overlayText === 'object'
        ? overlayText
        : { headline: '', subheadline: '', source: '' };
    if (
      modeNorm !== 'text_based' &&
      !String(overlayNorm.headline || '').trim() &&
      !trimmedUserImgEarly
    ) {
      return res.status(400).json({ error: 'Missing on-image headline text for regenerate' });
    }

    if (modeNorm === 'text_based') {
      const userQuota = await applyPinQuotaDelta(req.user.id, { userPhotoDelta: 1 }, req);
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
      await applyPinQuotaDelta(req.user.id, { userPhotoDelta: -1 }, req);
      return res.status(500).json({ error: 'Failed to render text-based pin' });
    }

    const { base } = await fetchArticleBaseAndSummary(rawUrl, articleData || null, {
      preResolvedUrl: effectiveUrl,
    });
    const year = new Date().getFullYear();
    const domain =
      (base.linkDisplay || base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    let topic = base.title || 'Does Brown Sugar Expire?';

    let regenNanoReferenceInputs = [];
    let regenNanoReferenceSource = null;
    const amazonCtxUrl = pickAmazonContextUrl(effectiveUrl, base.canonicalUrl);
    const walmartCtxUrl = pickWalmartContextUrl(effectiveUrl, base.canonicalUrl);
    const refHtmlUrl = amazonCtxUrl || effectiveUrl;
    // Etsy: RapidAPI listing images when present; else oEmbed thumbnail.
    if (isEtsyHost(new URL(effectiveUrl).hostname)) {
      const rapidEtsyUrls = Array.isArray(base?.etsy_rapidapi_image_urls) ? base.etsy_rapidapi_image_urls : [];
      if (rapidEtsyUrls.length > 0) {
        try {
          regenNanoReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana(
            rapidEtsyUrls.slice(0, 3),
            req.user.id
          );
          if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'etsy_product';
        } catch (e) {
          console.warn('urltopin regenerate Etsy RapidAPI images mirror error:', e.message || e);
        }
      }
      if (regenNanoReferenceInputs.length === 0) {
        const etsyThumb = String(base?.etsy_oembed_thumbnail || '').trim();
        if (etsyThumb) {
          try {
            regenNanoReferenceInputs = await mirrorGenericPageImageUrlsForNanoBanana([etsyThumb], req.user.id);
            if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'page';
          } catch (e) {
            console.warn('urltopin regenerate Etsy oEmbed thumbnail mirror error:', e.message || e);
          }
        }
      }
    }
    if (
      process.env.URLTOPIN_AMAZON_PRODUCT_IMAGES !== '0' &&
      isAmazonProductPageForNanoReference(amazonCtxUrl)
    ) {
      try {
        // Prefer RapidAPI for Amazon (single fetch); it provides both title + images.
        let amazonRapid = base.amazon_rapidapi_data || null;
        const asin = extractAmazonAsinFromUrl(amazonCtxUrl);
        if (asin && !amazonRapid) {
          try {
            const host = new URL(amazonCtxUrl).hostname;
            amazonRapid = await fetchAmazonProductDataViaRapidApi({
              asin,
              marketplace: resolveRapidApiAmazonMarketplaceFromHost(host),
              language: 'en',
            });
          } catch {
            amazonRapid = null;
          }
        }

        if (amazonRapid) {
          if (typeof amazonRapid.title === 'string' && amazonRapid.title.trim()) {
            topic = amazonRapid.title.trim();
          }
          if (typeof amazonRapid.description === 'string' && amazonRapid.description.trim()) {
            base.description = amazonRapid.description.trim().slice(0, 450);
          }
          const candidates = Array.isArray(amazonRapid.images)
            ? amazonRapid.images
                .map((im) => (im && typeof im === 'object' ? (im.hi_res || im.image || im.large || '') : ''))
                .filter(Boolean)
            : [];
          if (candidates.length > 0) {
            regenNanoReferenceInputs = await mirrorAmazonImageUrlsForNanoBanana(candidates, req.user.id);
            if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'amazon_product';
          }
        } else {
          // Fallback: try HTML-derived images (may be blocked) then widget image.
          const azHtml = await fetchArticleHtml(amazonCtxUrl);
          let candidates = extractAmazonProductImageUrlsFromHtml(azHtml, amazonCtxUrl);
          if (candidates.length === 0 || detectAmazonBotOrConsentPage(azHtml)) {
            const widgetImg = await fetchAmazonAsinWidgetImageUrl(amazonCtxUrl);
            if (widgetImg) candidates = [widgetImg];
          }
          if (candidates.length > 0) {
            regenNanoReferenceInputs = await mirrorAmazonImageUrlsForNanoBanana(candidates, req.user.id);
            if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'amazon_product';
          }
        }
      } catch (e) {
        console.warn('urltopin regenerate Amazon refs:', e.message || e);
      }
    } else if (
      process.env.URLTOPIN_WALMART_PRODUCT_IMAGES !== '0' &&
      regenNanoReferenceInputs.length === 0 &&
      isWalmartProductPageForNanoReference(walmartCtxUrl)
    ) {
      try {
        const walmartRapid =
          base.walmart_rapidapi_data ||
          (await fetchWalmartProductDataViaRapidApi({
            itemId: extractWalmartItemIdFromUrl(walmartCtxUrl),
            url: walmartCtxUrl,
          }));
        if (walmartRapid && walmartRapid.title && looksLikeBlockedWalmartTitle(topic)) {
          topic = walmartRapid.title;
        }
        const harvested = await harvestWalmartReferenceImages(
          walmartCtxUrl,
          req.user.id,
          walmartRapid
        );
        regenNanoReferenceInputs = harvested.images;
        if (regenNanoReferenceInputs.length > 0) regenNanoReferenceSource = 'walmart_product';
      } catch (e) {
        console.warn('urltopin regenerate Walmart refs:', e.message || e);
      }
    } else if (
      usePageReferenceImages &&
      process.env.URLTOPIN_PAGE_REFERENCE_IMAGES !== '0'
    ) {
      try {
        let pageHtml = '';
        if (!isEtsyListingPageUrl(refHtmlUrl)) {
          pageHtml = await fetchArticleHtml(refHtmlUrl);
        }
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

    let regenNicheHint = null;
    const regenLanding = detectProductAffiliateLandingFromUrls(rawUrl, effectiveUrl, base?.canonicalUrl);
    if (usesProductAffiliatePinMix(regenLanding)) {
      regenNicheHint = 'amazon_affiliate';
    }

    let imagePrompt = buildOverlayImagePrompt({
      styleId: nanoStyleId,
      topic,
      domain,
      keyword,
      year,
      overlayText: overlayForRender,
      brand,
      niche: regenNicheHint,
    });
    imagePrompt = appendNanoBananaAmazonUrlGarbageGuard(imagePrompt, amazonCtxUrl);

    const trimmedUserImg = userImageUrl && String(userImageUrl).trim();
    if (trimmedUserImg && isAllowedUserImageUrl(trimmedUserImg, process.env.SUPABASE_URL)) {
      const userQuota = await applyPinQuotaDelta(req.user.id, { userPhotoDelta: 1 }, req);
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
      await applyPinQuotaDelta(req.user.id, { userPhotoDelta: -1 }, req);
    }

    const aiQuota = await applyPinQuotaDelta(req.user.id, { aiDelta: 1 }, req);
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
      } else if (regenNanoReferenceSource === 'walmart_product') {
        imagePromptForNano +=
          ' ' +
          promptTier(
            'Attached reference image(s) show the real product from the Walmart listing. Use them as the primary hero subject: preserve packaging shape, brand marks, colors, and overall silhouette. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
            'Reference: use attached Walmart product photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.',
          );
      } else if (regenNanoReferenceSource === 'etsy_product') {
        imagePromptForNano +=
          ' ' +
          promptTier(
            'Attached reference image(s) are from the Etsy listing (product photos). Use them as the primary hero subject: preserve jewelry/product shape, materials, colors, and overall look. Compose a vertical 2:3 Pinterest pin in the requested style with the specified on-image headline, subheadline, and small footer line.',
            'Reference: use attached Etsy listing photo(s) as the main hero; match the real product; keep headline/sub/footer as specified.',
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
      const providerSoftTimeoutMs =
        Math.max(30_000, parseInt(process.env.URLTOPIN_IMAGE_PROVIDER_SOFT_TIMEOUT_MS || '360000', 10) || 360000);
      imageUrl = await withSoftTimeout(
        generateImageWithNanoBanana(imagePromptForNano, nanoStyleId, regenNanoOpts),
        providerSoftTimeoutMs
      );
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
      await applyPinQuotaDelta(req.user.id, { aiDelta: -1 }, req);
      console.error('urltopin regenerate-image-with-text AI error:', e.message || e);
      return res.status(500).json({ error: e.message || 'Failed to generate image' });
    }

    if (!imageUrl) {
      await applyPinQuotaDelta(req.user.id, { aiDelta: -1 }, req);
      return res.status(500).json({ error: 'Failed to generate image with the provided text' });
    }

    imageUrl = await persistProviderImageUrlToAiImages(
      imageUrl,
      `urltopin-regen-${req.user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}-${styleId}`,
      'regenerate-image-with-text'
    );

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
  if (!authHeader) {
    return res.status(401).json({ error: 'Please sign in to continue.', code: 'auth_required' });
  }
  const token = authHeader.split(' ')[1];
  const { data: { user }, error } = await supabaseAuthGetUser(token);
  const authed = respondSupabaseAuth(res, user, error);
  if (!authed) return;
  req.user = authed;
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

/** Normalize a brand kit payload to a small, safe shape we persist. */
function sanitizeBrandKit(raw) {
  const obj = raw && typeof raw === 'object' ? raw : {};
  const str = (v, max) => {
    const s = String(v ?? '').trim();
    return s ? s.slice(0, max) : '';
  };
  return {
    brandName: str(obj.brandName, 80),
    logoUrl: str(obj.logoUrl, 1000),
    primaryColor: str(obj.primaryColor, 32),
    secondaryColor: str(obj.secondaryColor, 32),
    accentColor: str(obj.accentColor, 32),
  };
}

// Brand kit persistence (stored on profiles.brand_kit jsonb).
// Gracefully no-ops if the column is missing so the frontend can fall back to localStorage.
app.get('/api/account/brand-kit', requireUser, async (req, res) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('profiles')
      .select('brand_kit')
      .eq('id', req.user.id)
      .maybeSingle();
    if (error) {
      console.warn('brand-kit fetch error (column may be missing):', error.message || error);
      return res.json({ ok: true, brandKit: null, persisted: false });
    }
    return res.json({ ok: true, brandKit: data?.brand_kit || null, persisted: true });
  } catch (e) {
    console.warn('brand-kit fetch unexpected error:', e?.message || e);
    return res.json({ ok: true, brandKit: null, persisted: false });
  }
});

app.put('/api/account/brand-kit', requireUser, async (req, res) => {
  try {
    const brandKit = sanitizeBrandKit(req.body?.brandKit ?? req.body);
    const { error } = await supabaseAdmin
      .from('profiles')
      .update({ brand_kit: brandKit, updated_at: new Date().toISOString() })
      .eq('id', req.user.id);
    if (error) {
      console.warn('brand-kit save error (column may be missing):', error.message || error);
      return res.json({ ok: true, brandKit, persisted: false });
    }
    return res.json({ ok: true, brandKit, persisted: true });
  } catch (e) {
    console.warn('brand-kit save unexpected error:', e?.message || e);
    return res.json({ ok: true, brandKit: null, persisted: false });
  }
});

/**
 * Fetch a product's title + primary image for a single URL (Amazon or any product page).
 * Used by the multi-product (roundup / comparison) pin builder so the frontend can preview items.
 */
app.post('/api/urltopin/product-info', requireUser, async (req, res) => {
  try {
    const rawUrl = String(req.body?.url || '').trim();
    if (!rawUrl) return res.status(400).json({ error: 'Missing url' });
    let effectiveUrl = rawUrl;
    try {
      const expanded = await resolveOutboundUrlForUrlToPin(rawUrl);
      if (expanded) effectiveUrl = expanded;
    } catch {
      /* keep raw */
    }
    const { base } = await fetchArticleBaseAndSummary(rawUrl, null, { fast: true, preResolvedUrl: effectiveUrl });
    let title = String(base?.title || '').trim();
    let imageUrl = '';

    // Amazon: prefer product image (scrape, then widget, then RapidAPI if blocked).
    const amazonCtxUrl = pickAmazonContextUrl(effectiveUrl, base?.canonicalUrl);
    if (isAmazonProductPageForNanoReference(amazonCtxUrl)) {
      try {
        const azHtml = await fetchArticleHtml(amazonCtxUrl);
        let candidates = extractAmazonProductImageUrlsFromHtml(azHtml, amazonCtxUrl);
        if (candidates.length === 0 || detectAmazonBotOrConsentPage(azHtml)) {
          const widgetImg = await fetchAmazonAsinWidgetImageUrl(amazonCtxUrl);
          if (widgetImg) candidates = [widgetImg];
        }
        if (candidates.length > 0) imageUrl = candidates[0];
      } catch {
        /* ignore */
      }
      if ((!title || !imageUrl) && base?.amazon_blocked) {
        const asin = extractAmazonAsinFromUrl(amazonCtxUrl);
        if (asin) {
          try {
            const host = new URL(amazonCtxUrl).hostname;
            const rapid = await fetchAmazonProductDataViaRapidApi({
              asin,
              marketplace: resolveRapidApiAmazonMarketplaceFromHost(host),
              language: 'en',
            });
            if (rapid) {
              if (!title && rapid.title) title = String(rapid.title).trim();
              if (!imageUrl && Array.isArray(rapid.images) && rapid.images.length) {
                const im = rapid.images[0];
                imageUrl = (im && typeof im === 'object' ? (im.hi_res || im.image || im.large) : im) || '';
              }
            }
          } catch {
            /* ignore */
          }
        }
      }
    }
    // Walmart: scrape product image, then RapidAPI fallback (and title) when blocked.
    const walmartCtxUrl = pickWalmartContextUrl(effectiveUrl, base?.canonicalUrl);
    if (!imageUrl && isWalmartProductPageForNanoReference(walmartCtxUrl)) {
      try {
        const wHtml = await fetchArticleHtml(walmartCtxUrl);
        const candidates = extractWalmartProductImageUrlsFromHtml(wHtml, walmartCtxUrl);
        if (candidates.length > 0) imageUrl = candidates[0];
      } catch {
        /* ignore */
      }
      if (!title || looksLikeBlockedWalmartTitle(title) || !imageUrl) {
        const rapid = await fetchWalmartProductDataViaRapidApi({
          itemId: extractWalmartItemIdFromUrl(walmartCtxUrl),
          url: walmartCtxUrl,
        });
        if (rapid) {
          if ((!title || looksLikeBlockedWalmartTitle(title)) && rapid.title) {
            title = String(rapid.title).trim();
          }
          if (!imageUrl && Array.isArray(rapid.images) && rapid.images.length) {
            imageUrl = rapid.images[0];
          }
        }
      }
    }
    if (!imageUrl) {
      // Non-Amazon (or no product image yet): pull og:image / twitter:image from the page.
      try {
        const html = await fetchArticleHtml(effectiveUrl);
        const og =
          html.match(/<meta[^>]+property=["']og:image["'][^>]*content=["']([^"']+)["']/i) ||
          html.match(/<meta[^>]+name=["']twitter:image["'][^>]*content=["']([^"']+)["']/i);
        if (og && og[1]) {
          try {
            imageUrl = new URL(og[1].trim(), effectiveUrl).href;
          } catch {
            imageUrl = og[1].trim();
          }
        }
      } catch {
        /* image stays empty (optional) */
      }
    }

    return res.json({
      ok: true,
      url: effectiveUrl,
      title: title || '',
      imageUrl: imageUrl || '',
      amazonBlocked: !!base?.amazon_blocked,
    });
  } catch (err) {
    console.error('urltopin product-info error:', err);
    return res.status(500).json({ error: err.message });
  }
});

const winningProductAnalyzeTimestamps = new Map(); // userId -> number[]

async function fetchUserProductPinStats(userId, asin) {
  const uid = String(userId || '').trim();
  const id = String(asin || '').trim().toUpperCase();
  if (!uid || !id) {
    return { pinsCreated: 0, pinsPosted: 0, impressions: 0, outboundClicks: 0, saves: 0 };
  }
  try {
    const { count: historyCount, error: historyError } = await supabaseAdmin
      .from('urltopin_history')
      .select('id', { count: 'exact', head: true })
      .eq('user_id', uid)
      .or(`source_url.ilike.%${id}%,pin_link.ilike.%${id}%`);

    const { data: pinRows, error: pinsError } = await supabaseAdmin
      .from('scheduled_pins')
      .select('status, impressions, outbound_clicks, saves, pinterest_pin_id')
      .eq('user_id', uid)
      .ilike('link', `%${id}%`);

    if (historyError) console.warn('winning-product history count:', historyError.message || historyError);
    if (pinsError) console.warn('winning-product pin stats:', pinsError.message || pinsError);

    const rows = Array.isArray(pinRows) ? pinRows : [];
    const posted = rows.filter((p) => p.status === 'posted' || p.pinterest_pin_id);
    const totals = posted.reduce(
      (acc, p) => ({
        impressions: acc.impressions + (Number(p.impressions) || 0),
        outboundClicks: acc.outboundClicks + (Number(p.outbound_clicks) || 0),
        saves: acc.saves + (Number(p.saves) || 0),
      }),
      { impressions: 0, outboundClicks: 0, saves: 0 }
    );

    return {
      pinsCreated: Number(historyCount) || rows.length || 0,
      pinsPosted: posted.length,
      ...totals,
    };
  } catch (e) {
    console.warn('winning-product pin stats:', e?.message || e);
    return { pinsCreated: 0, pinsPosted: 0, impressions: 0, outboundClicks: 0, saves: 0 };
  }
}

/**
 * Winning Product Finder — score an Amazon product's Pinterest opportunity before pin creation.
 * Body: { url: string }
 */
async function buildWinningProductReport(rawUrl, { userId = null } = {}) {
  const trimmed = String(rawUrl || '').trim();
  if (!trimmed) {
    const err = new Error('Paste an Amazon product URL.');
    err.status = 400;
    throw err;
  }

  let effectiveUrl = trimmed;
  try {
    const expanded = await resolveOutboundUrlForUrlToPin(trimmed);
    if (expanded) effectiveUrl = expanded;
  } catch {
    /* keep raw */
  }

  const amazonCtxUrl = pickAmazonContextUrl(effectiveUrl, null);
  if (!isAmazonProductPageForNanoReference(amazonCtxUrl)) {
    const err = new Error('Supported: Amazon product URLs (including amzn.to links).');
    err.status = 400;
    throw err;
  }

  const asin = extractAmazonAsinFromUrl(amazonCtxUrl);
  if (!asin) {
    const err = new Error('Could not extract an Amazon ASIN from that URL.');
    err.status = 400;
    throw err;
  }

  const rapid = await fetchAmazonProductDataViaRapidApi({
    asin,
    marketplace: resolveRapidApiAmazonMarketplaceFromHost(new URL(amazonCtxUrl).hostname),
    language: 'en',
  });
  if (!rapid) {
    const err = new Error('Could not fetch product data from Amazon. Check the link and try again.');
    err.status = 502;
    throw err;
  }

  const product = normalizeAmazonProduct(rapid, amazonCtxUrl);
  if (!product) {
    const err = new Error('Amazon returned incomplete product data for this link.');
    err.status = 502;
    throw err;
  }

  const uid = userId ? String(userId).trim() : '';
  const userPinHistory = uid ? await fetchUserProductPinStats(uid, asin) : { pinsCreated: 0 };
  const getPinterestAccessToken = uid
    ? async () => getPinterestAccessTokenForUser(uid, null)
    : getTrendsPinterestAccessToken;

  const report = await analyzeWinningProduct(product, {
    getPinterestAccessToken,
    userPinHistory,
  });

  return { ok: true, ...report };
}

app.post('/api/urltopin/winning-product-analyze', requireUser, async (req, res) => {
  try {
    const uid = String(req.user?.id || '').trim();
    const now = Date.now();
    const recent = (winningProductAnalyzeTimestamps.get(uid) || []).filter((t) => now - t < 60_000);
    if (recent.length >= 12) {
      return res.status(429).json({ error: 'Too many analyses. Please wait a minute and try again.' });
    }
    recent.push(now);
    winningProductAnalyzeTimestamps.set(uid, recent);

    const report = await buildWinningProductReport(req.body?.url, { userId: uid });
    return res.json(report);
  } catch (err) {
    console.error('winning-product-analyze error:', err);
    return res.status(err.status || 500).json({ error: err.message || 'Analysis failed.' });
  }
});

app.post('/api/tools/pin-worth-checker', async (req, res) => {
  try {
    if (!rateLimitTool(req, 'pin-worth-checker', { windowMs: 60_000, max: 12 })) {
      return res.status(429).json({ error: 'Too many checks. Please wait a minute and try again.' });
    }
    const report = await buildWinningProductReport(req.body?.url);
    return res.json(report);
  } catch (err) {
    console.error('pin-worth-checker tool error:', err);
    return res.status(err.status || 500).json({ error: err.message || 'Analysis failed.' });
  }
});

/**
 * Build a single multi-product affiliate pin: a roundup / gift-guide grid, or an A-vs-B comparison.
 * Body: { mode:'roundup'|'comparison', headline, link, items:[{title,imageUrl,link}], brand, outputLanguage }
 */
app.post('/api/urltopin/generate-multi-product', requireUser, async (req, res) => {
  try {
    const mode = req.body?.mode === 'comparison' ? 'comparison' : 'roundup';
    let headline = String(req.body?.headline || '').trim().slice(0, 120);
    const destinationUrl = String(req.body?.link || req.body?.destinationUrl || '').trim();
    const outputLanguage = String(req.body?.outputLanguage || 'auto').trim().toLowerCase() || 'auto';
    const strictLanguage = req.body?.strictLanguage === true || req.body?.strictLanguage === 'true';
    const affiliateDisclosure = normalizeAffiliateDisclosureRequest(req.body || {});
    const brand = req.body?.brand && typeof req.body.brand === 'object' ? req.body.brand : {};

    let items = Array.isArray(req.body?.items) ? req.body.items : [];
    // Preserve the exact order the user supplied — numbering is drawn from this order, so it must not be reordered.
    items = items
      .map((it) => ({
        title: String(it?.title || '').trim().slice(0, 120),
        imageUrl: String(it?.imageUrl || '').trim(),
        link: String(it?.link || '').trim(),
      }))
      .filter((it) => it.title || it.imageUrl);

    if (!destinationUrl) return res.status(400).json({ error: 'Missing link (where the pin should send people)' });
    if (mode === 'comparison' && items.length !== 2) {
      return res.status(400).json({ error: 'Comparison pins need exactly 2 products.' });
    }
    if (mode === 'roundup' && (items.length < 2 || items.length > 6)) {
      return res.status(400).json({ error: 'Roundup pins need between 2 and 6 products.' });
    }
    // Every product needs a title (it's the on-pin label). Fall back to a numbered placeholder so the layout stays valid.
    items = items.map((it, i) => ({
      ...it,
      title: it.title || (mode === 'comparison' ? `Option ${i === 0 ? 'A' : 'B'}` : `Pick ${i + 1}`),
    }));

    // Headline is optional — when omitted, let the AI craft one from the products.
    if (!headline) {
      headline = await generateMultiProductHeadline({ mode, items, outputLanguage }, openai);
    }

    // One AI image pin consumed.
    const usageResult = await applyPinQuotaDelta(req.user.id, { aiDelta: 1 }, req);
    if (!usageResult.allowed) {
      return res.status(402).json({
        error: 'pin_limit_reached',
        message: `Your current plan allows ${usageResult.planPinsLimit} AI image pins per month. You have already used ${usageResult.currentUsed} this month.`,
        details: usageResult,
      });
    }

    // Mirror product photos to Supabase, then pass them to Nano Banana as ordered reference images so the
    // generated composite uses the real products in the same order as `items` (keeps numbering aligned).
    let referenceInputs = [];
    const photoUrls = items.map((it) => it.imageUrl).filter(Boolean);
    if (photoUrls.length && process.env.USE_DUMMY_IMAGES !== 'true') {
      try {
        referenceInputs = await mirrorGenericPageImageUrlsForNanoBanana(photoUrls.slice(0, 6), req.user.id);
      } catch (e) {
        console.warn('multi-product image mirror error:', e.message || e);
      }
    }

    const footer = String(brand?.brandName || '').trim().slice(0, 80);
    const prompt = buildMultiProductPinPrompt({ mode, headline, items, footer, brand });

    let imageUrl = '';
    try {
      const providerSoftTimeoutMs = Math.max(
        30_000,
        parseInt(process.env.URLTOPIN_IMAGE_PROVIDER_SOFT_TIMEOUT_MS || '360000', 10) || 360000
      );
      const nanoOpts = referenceInputs.length ? { imageInput: referenceInputs } : {};
      let nanoUrl = await withSoftTimeout(generateImageWithNanoBanana(prompt, `multi_${mode}`, nanoOpts), providerSoftTimeoutMs);
      if (!nanoUrl) {
        nanoUrl = await withSoftTimeout(generateImageWithNanoBanana(prompt, `multi_${mode}`, nanoOpts), providerSoftTimeoutMs);
      }
      imageUrl = nanoUrl || '';
      if (imageUrl) {
        try {
          const imageRes = await fetch(imageUrl);
          if (imageRes.ok) {
            const buffer = Buffer.from(await imageRes.arrayBuffer());
            const fileExt = (imageUrl.split('.').pop() || 'png').split('?')[0] || 'png';
            const fileName = `urltopin-multi-${req.user.id}-${Date.now()}-${Math.floor(Math.random() * 10000)}.${fileExt}`;
            const { error: uploadError } = await supabaseAdmin.storage
              .from('ai-images')
              .upload(fileName, buffer, {
                contentType: imageRes.headers.get('content-type') || 'image/png',
                upsert: true,
              });
            if (!uploadError) {
              const { data: pub } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
              if (pub?.publicUrl) imageUrl = pub.publicUrl;
            }
          }
        } catch (e) {
          console.warn('multi-product re-host error:', e.message || e);
        }
      }
    } catch (e) {
      await applyPinQuotaDelta(req.user.id, { aiDelta: -1 }, req);
      console.error('multi-product image generation error:', e.message || e);
      return res.status(500).json({ error: e.message || 'Failed to generate image' });
    }

    if (!imageUrl) {
      await applyPinQuotaDelta(req.user.id, { aiDelta: -1 }, req);
      return res.status(503).json({
        error: 'image_provider_unavailable',
        message: 'Image provider is busy right now. Please try again in a moment.',
      });
    }

    // Title + description via the shared generate-field endpoint.
    const tokenHeader = req.headers.authorization || '';
    const productList = items.map((it, i) => `${i + 1}. ${it.title || `Product ${i + 1}`}`).join('\n');
    const contentForCopy =
      (mode === 'comparison'
        ? `Comparison pin: "${headline}". Comparing:\n${productList}`
        : `Product roundup / gift guide pin: "${headline}". Featuring:\n${productList}`) +
      `\n\nDestination: ${destinationUrl}`;
    let pinTitle = headline;
    let pinDescription = '';
    try {
      const selfBase = process.env.SELF_API_URL || 'http://localhost:' + PORT;
      const [titleRes, descRes] = await Promise.all([
        fetch(`${selfBase}/api/generate-field`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: tokenHeader },
          body: JSON.stringify({ content: contentForCopy, type: 'title', outputLanguage, strictLanguage }),
        }),
        fetch(`${selfBase}/api/generate-field`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: tokenHeader },
          body: JSON.stringify({ content: contentForCopy, type: 'description', outputLanguage, strictLanguage }),
        }),
      ]);
      if (titleRes.ok) {
        const j = await titleRes.json();
        if (j?.result) pinTitle = j.result;
      }
      if (descRes.ok) {
        const j = await descRes.json();
        if (j?.result) pinDescription = j.result;
      }
    } catch (e) {
      console.warn('multi-product metadata error:', e.message || e);
    }

    const descriptionForPin = appendAffiliateDisclosureToDescription(pinDescription, affiliateDisclosure);
    const hashtags = hashtagsFromPinDescription(descriptionForPin);
    let altText = '';
    try {
      altText = await generatePinterestAltText(
        {
          title: pinTitle,
          description: pinDescription,
          overlayText: { headline },
          styleLabel: mode === 'comparison' ? 'Comparison' : 'Roundup',
          linkDisplay: destinationUrl,
          nanoBananaPrompt: prompt,
          outputLanguage,
          strictLanguage,
        },
        openai
      );
    } catch {
      /* alt text is best-effort */
    }

    const pinRecord = {
      styleId: mode === 'comparison' ? 'comparison_split' : 'roundup_grid',
      styleLabel: mode === 'comparison' ? 'Comparison' : 'Roundup',
      imagePrompt: prompt,
      imageUrl,
      title: pinTitle,
      description: descriptionForPin,
      altText,
      hashtags,
      link: destinationUrl,
      overlayText: { headline },
      imageGenerationMode: 'ai',
      multiProduct: { mode, items },
      ...(referenceInputs.length > 0 && {
        nanoBananaReferenceCount: referenceInputs.length,
        nanoBananaReferenceSource: 'multi_product',
      }),
    };

    // Persist so it shows in the dashboard and can be scheduled (same shape as single pins).
    try {
      const baseHistory = {
        user_id: req.user.id,
        source_url: destinationUrl,
        article_title: headline,
        article_domain: (() => {
          try {
            return new URL(destinationUrl).hostname;
          } catch {
            return null;
          }
        })(),
        style_id: pinRecord.styleId,
        style_label: pinRecord.styleLabel,
        image_url: imageUrl,
        pin_title: pinTitle,
        pin_description: descriptionForPin,
        pin_link: destinationUrl,
      };
      supabaseAdmin.from('urltopin_history').insert(baseHistory).then(({ error }) => {
        if (error) console.warn('urltopin_history (multi) insert error:', error.message || error);
      }).catch(() => {});
      supabaseAdmin
        .from('scheduled_pins')
        .insert({
          user_id: req.user.id,
          pinterest_account_id: null,
          title: pinTitle,
          description: descriptionForPin,
          image_url: imageUrl,
          board_id: '',
          link: destinationUrl,
          scheduled_for: null,
          timezone: null,
          is_recurring: false,
          recurrence_pattern: null,
          status: 'generated',
          original_pin_data: { ...baseHistory, source: 'urltopin', overlayText: { headline }, alt_text: altText, multiProduct: { mode, items } },
        })
        .then(({ error }) => {
          if (error) console.warn('scheduled_pins (multi) insert error:', error.message || error);
        })
        .catch(() => {});
    } catch (e) {
      console.warn('multi-product persist threw:', e.message || e);
    }

    return res.json({ pins: [pinRecord] });
  } catch (err) {
    console.error('urltopin generate-multi-product error:', err);
    return res.status(500).json({ error: err.message });
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
  const { content, type, style, outputLanguage: rawOutputLanguage, strictLanguage: rawStrictLanguage } = req.body; // type: 'title' or 'description'; style: optional hint
  if (!content || !type) return res.status(400).json({ error: 'Missing content or type' });

  // Soft-limit tracking for metadata usage (titles/descriptions)
  recordMetadataUsage(req.user.id, 1).catch((err) =>
    console.warn('recordMetadataUsage(generate-field) error:', err.message || err)
  );

  const isShortTitle = type === 'title' && style === 'short_50';
  const outputLanguage = String(rawOutputLanguage || 'auto').trim().toLowerCase() || 'auto';
  const strictLanguage = rawStrictLanguage === true || rawStrictLanguage === 'true';
  const languageLine =
    outputLanguage && outputLanguage !== 'auto'
      ? `\n\nLANGUAGE REQUIREMENT: Write the output in ${outputLanguage.toUpperCase()} only. Do not use English.\n`
      : '';
  const prompt = type === 'title'
    ? (
      isShortTitle
        ? `Write a concise Pinterest pin title (max 50 characters). Focus on the main keyword and benefit. No quotes or hashtags. Return only the title.${languageLine}\n${content}`
        : `Write a compelling Pinterest pin title (aim for 80-100 characters) for this content. The title should be curiosity-driven and make people want to click to learn more. Include emotional triggers, urgency, numbers, or questions where possible. Make it descriptive and specific rather than generic. Use engaging words that create intrigue. Only return the title, nothing else. Avoid quotes but you can use basic punctuation like periods, commas, exclamation points, and question marks:${languageLine}\n${content}`
    )
    : `Write an engaging Pinterest pin description (max 450 characters) for this content. The description should explain the benefit or insight the user will get by clicking. Avoid phrases like "+visit site+", "+click the link+", or adding URLs. Include 4–6 relevant hashtags at the end. Only return the description, nothing else:${languageLine}\n${content}`;
  try {
    const detectLang = async (text) => {
      const t = String(text || '').trim();
      if (!t) return 'unknown';
      try {
        const completion = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [
            {
              role: 'user',
              content:
                'Detect the primary language of the text.\n' +
                'Return JSON only with key "lang" as an ISO 639-1 code when possible (e.g. "en", "sk", "cs"), or "unknown".\n' +
                `Text:\n<<<${t.slice(0, 900)}>>>`,
            },
          ],
          max_tokens: 30,
          temperature: 0,
        });
        const raw = completion.choices?.[0]?.message?.content?.trim() || '';
        const m = raw.match(/\{[\s\S]*\}/);
        if (!m) return 'unknown';
        const parsed = JSON.parse(m[0]);
        const out = String(parsed.lang || '').trim().toLowerCase();
        return out || 'unknown';
      } catch {
        return 'unknown';
      }
    };

    const runOnce = async (forceLanguageHarder = false) => {
      const hardLine =
        outputLanguage && outputLanguage !== 'auto'
          ? forceLanguageHarder
            ? `\n\nCRITICAL: Output MUST be in ${outputLanguage.toUpperCase()} only. If you cannot comply, output an empty string.\n`
            : languageLine
          : '';
      const effectivePrompt =
        forceLanguageHarder && hardLine && !prompt.includes(hardLine)
          ? prompt.replace(languageLine, hardLine)
          : prompt;
      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: effectivePrompt }],
      max_tokens: type === 'title' ? 150 : 500,
      temperature: 0.7,
      });
      return completion.choices?.[0]?.message?.content?.trim() || '';
    };

    let result = await runOnce(false);
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

    if (strictLanguage && outputLanguage !== 'auto' && result) {
      const detected = await detectLang(result);
      if (detected !== 'unknown' && detected !== outputLanguage) {
        const retry = await runOnce(true);
        let next = String(retry || '').trim();
        if (type === 'title') {
          next = next.replace(/["'`~@#$%^&*()_+=\[\]{}|;:<>\\/]+/g, '');
          if (isShortTitle && next.length > 50) {
            const cut = next.lastIndexOf(' ', 49);
            next = (cut > 20 ? next.slice(0, cut) : next.slice(0, 50)).trim();
          }
          if (!isShortTitle && next.length > 100) next = next.slice(0, 100);
        }
        if (type === 'description') next = sanitizeDescription(next);
        if (next) result = next;
      }
    }

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

/**
 * Resolve Dodo subscription id after checkout (for billing_subscriptions.dodo_subscription_id).
 * Per Dodo OpenAPI: GET /checkouts/{id} returns GetCheckoutSessionsStatus (payment_id, not subscription_id);
 * GET /payments/{payment_id} returns PaymentResponse.subscription_id for subscription checkouts.
 * @param {string} checkoutSessionId from create-checkout response (session_id / id)
 * @returns {Promise<string|null>}
 */
async function fetchDodoSubscriptionIdFromCheckoutSession(checkoutSessionId) {
  const raw = String(checkoutSessionId || '').trim();
  if (!raw || !DODO_API_KEY) return null;
  try {
    const resp = await fetch(`${DODO_BASE_URL}/checkouts/${encodeURIComponent(raw)}`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${DODO_API_KEY}`,
      },
    });
    const json = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      console.warn('Dodo GET checkout failed', {
        status: resp.status,
        checkoutSessionId: raw,
        details: json,
      });
      return null;
    }

    const paymentId = String(json?.payment_id ?? json?.paymentId ?? '').trim();
    if (paymentId) {
      const payResp = await fetch(`${DODO_BASE_URL}/payments/${encodeURIComponent(paymentId)}`, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${DODO_API_KEY}`,
        },
      });
      const payJson = await payResp.json().catch(() => ({}));
      if (!payResp.ok) {
        console.warn('Dodo GET payment failed', {
          status: payResp.status,
          checkoutSessionId: raw,
          paymentId,
          details: payJson,
        });
      } else {
        const sub = payJson?.subscription_id ?? payJson?.subscriptionId ?? null;
        const id = sub ? String(sub).trim() : '';
        if (id) return id;
        console.warn('Dodo payment has no subscription_id', {
          checkoutSessionId: raw,
          paymentId,
          payment_status: payJson?.status,
        });
      }
    } else {
      console.warn('Dodo checkout has no payment_id yet', {
        checkoutSessionId: raw,
        payment_status: json?.payment_status ?? json?.paymentStatus,
      });
    }

    return null;
  } catch (e) {
    console.warn('Dodo GET checkout error:', e.message || e);
    return null;
  }
}

async function fetchDodoSubscriptionIdFromCheckoutSessionWithRetry(checkoutSessionId, maxWaitMs = 20000) {
  const started = Date.now();
  while (Date.now() - started < maxWaitMs) {
    const id = await fetchDodoSubscriptionIdFromCheckoutSession(checkoutSessionId);
    if (id) return id;
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
  return null;
}

app.post('/api/dodo/create-checkout-session', requireUser, async (req, res) => {
  try {
    if (!DODO_API_KEY) {
      return res.status(500).json({ error: 'Dodo Payments API key not configured' });
    }

    const { planType, billingInterval: rawBillingInterval, discountCode, couponCode, referralKey } = req.body || {};
    if (!planType) {
      return res.status(400).json({ error: 'Missing planType' });
    }
    const billingInterval = normalizeBillingInterval(rawBillingInterval);

    const authedEmail = normalizeEmail(req.user?.email);
    if (authedEmail) {
      const consolidated = await tryConsolidateDuplicateEmailSubscription(req.user.id, authedEmail);
      if (consolidated.ok) {
        console.log('create-checkout-session: consolidated duplicate-email subscription', {
          userId: req.user.id,
          fromUserId: consolidated.otherUserId,
        });
      }
    }

    const ownActive = await getLatestPayingSubscriptionRow(req.user.id);
    let reactivatedPastDue = false;
    if (ownActive?.status === 'past_due') {
      const pastDodoId = String(ownActive.dodo_subscription_id || '').trim();
      if (pastDodoId) {
        const dodoGet = await fetchDodoSubscriptionJson(pastDodoId);
        const dodoStatus = String(dodoGet.json?.status || dodoGet.json?.subscription_status || '').trim();
        if (dodoGet.ok && dodoSubscriptionStatusAllowsPlanChange(dodoStatus)) {
          await reactivatePastDueSubscriptionRow(req.user.id, ownActive);
          reactivatedPastDue = true;
        }
      }
      if (!reactivatedPastDue) {
        await closePastDueSubscriptionRow(req.user.id, ownActive);
      }
    }

    const ownActiveAfterPastDue = reactivatedPastDue
      ? await getLatestPayingSubscriptionRow(req.user.id)
      : ownActive?.status === 'past_due'
        ? null
        : ownActive;
    if (ownActiveAfterPastDue?.status === 'active') {
      const ownDodoId = String(ownActiveAfterPastDue.dodo_subscription_id || '').trim();
      if (ownDodoId) {
        const dodoGet = await fetchDodoSubscriptionJson(ownDodoId);
        const dodoStatus = String(dodoGet.json?.status || dodoGet.json?.subscription_status || '').trim();
        if (dodoGet.ok && dodoSubscriptionStatusAllowsPlanChange(dodoStatus)) {
          return res.status(409).json({
            error: 'active_subscription_requires_change_plan',
            code: 'active_subscription_requires_change_plan',
            message:
              'You already have an active subscription. Use plan change instead of starting a new checkout to avoid double billing.',
          });
        }
        if (dodoGet.ok && dodoSubscriptionIsInactiveStatus(dodoStatus)) {
          await syncLocalSubscriptionWithDodo(req.user.id, ownActiveAfterPastDue, dodoGet.json);
        }
      } else {
        return res.status(409).json({
          error: 'active_subscription_unlinked',
          code: 'active_subscription_unlinked',
          message:
            'Your account already has an active plan on file but it is not linked to billing. Please contact us through the Contact page on URL2Pin before starting a new checkout.',
        });
      }
    }

    const resumable = await findResumablePendingCheckout(req.user.id, planType, billingInterval);
    if (resumable?.checkoutUrl) {
      markPendingDodoActivationWithInterval(req.user.id, planType, billingInterval, resumable.sessionId || null);
      return res.json({
        checkoutUrl: resumable.checkoutUrl,
        sessionId: resumable.sessionId,
        resumed: true,
      });
    }

    // Safety: prevent duplicate active subscriptions across multiple Supabase accounts for the same email.
    if (authedEmail) {
      try {
        const { data: otherProfiles } = await supabaseAdmin
          .from('profiles')
          .select('id, email')
          .ilike('email', authedEmail)
          .neq('id', req.user.id)
          .limit(10);

        const otherUserIds = (otherProfiles || []).map((p) => String(p?.id || '').trim()).filter(Boolean);
        if (otherUserIds.length > 0) {
          const { data: otherActiveSubs } = await supabaseAdmin
            .from('billing_subscriptions')
            .select('id, user_id, plan_type, status, dodo_subscription_id')
            .in('user_id', otherUserIds)
            .eq('status', 'active')
            .limit(1);

          if (otherActiveSubs && otherActiveSubs.length > 0) {
            const consolidatedAgain = await tryConsolidateDuplicateEmailSubscription(req.user.id, authedEmail);
            if (!consolidatedAgain.ok) {
              return res.status(409).json({
                error: 'duplicate_account_detected',
                code: 'duplicate_account_detected',
                message:
                  'We found another account with an active subscription for this email. Sign in to that account, or contact us through the Contact page on URL2Pin and we will merge your accounts.',
              });
            }
          }
        }
      } catch (e) {
        console.warn('create-checkout-session: duplicate-account check failed (continuing)', e?.message || e);
      }
    }

    const referralSlug = normalizeAffiliateSlug(referralKey) || '';

    if (referralSlug && req.user?.id) {
      await attachAffiliateReferralToUser(req.user.id, referralSlug);
    }

    // Dodo checkout expects the human-readable discount *code* (e.g. LAUNCH20), not the dashboard id (dsc_...).
    // Priority: typed coupon → partner ?ref= slug map → global env default.
    const fromClient = String(discountCode || couponCode || '')
      .trim();
    const fromPartner = referralSlug ? resolvePartnerDiscountCode(referralSlug) : null;
    const fromEnv = String(process.env.DODO_CHECKOUT_DEFAULT_DISCOUNT_CODE || '')
      .trim();
    const discount_code = fromClient || fromPartner || fromEnv || undefined;

    // Map internal plan types to Dodo product IDs via environment variables
    const productMapMonthly = {
      free: process.env.DODO_PRODUCT_FREE_ID,
      starter: process.env.DODO_PRODUCT_STARTER_ID,
      creator: process.env.DODO_PRODUCT_CREATOR_ID,
      pro: process.env.DODO_PRODUCT_PRO_ID,
      agency: process.env.DODO_PRODUCT_AGENCY_ID,
    };
    const productMapAnnual = {
      free: process.env.DODO_PRODUCT_FREE_ID,
      starter: process.env.DODO_PRODUCT_STARTER_ANNUAL_ID,
      creator: process.env.DODO_PRODUCT_CREATOR_ANNUAL_ID,
      pro: process.env.DODO_PRODUCT_PRO_ANNUAL_ID,
      agency: process.env.DODO_PRODUCT_AGENCY_ANNUAL_ID,
    };

    const productId = (billingInterval === 'year' ? productMapAnnual : productMapMonthly)[planType];
    if (!productId) {
      return res.status(400).json({
        error: `No Dodo product configured for planType "${planType}" (${billingInterval})`,
      });
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
        app_billing_interval: billingInterval,
        ...(referralSlug ? { referral_key: referralSlug } : {}),
      },
    // Redirect back to app after success/failure
    return_url: `${frontendBase.replace(/\/$/, '')}/payment-success?plan=${encodeURIComponent(planType)}&interval=${encodeURIComponent(billingInterval)}`,
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

    markPendingDodoActivationWithInterval(req.user.id, planType, billingInterval, sessionId || null);
    await savePendingCheckoutRecord(req.user.id, planType, billingInterval, sessionId, checkoutUrl);
    console.log('🧾 Dodo checkout created', {
      userId: req.user.id,
      planType,
      billingInterval,
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
    const snapshot = await getCurrentUsageSnapshot(req.user.id, req);
    return res.json(snapshot);
  } catch (err) {
    console.error('account/usage error:', err);
    return res.status(500).json({ error: 'Failed to load account usage' });
  }
});

app.post('/api/referral/attach', requireUser, async (req, res) => {
  try {
    const slug = normalizeAffiliateSlug(req.body?.slug);
    if (!slug) return res.status(400).json({ error: 'Invalid referral slug' });
    const result = await attachAffiliateReferralToUser(req.user.id, slug);
    if (!result.ok && result.reason === 'self_referral') {
      return res.status(400).json({ error: 'You cannot use your own referral link.' });
    }
    if (!result.ok) return res.status(400).json({ error: 'Invalid or inactive referral link.' });
    return res.json({ ok: true, slug: result.slug, alreadySet: Boolean(result.alreadySet) });
  } catch (err) {
    console.error('referral/attach error:', err);
    return res.status(500).json({ error: 'Failed to attach referral' });
  }
});

async function findAffiliateRowForUser(userId, email) {
  const { data: byUser } = await supabaseAdmin
    .from('affiliates')
    .select('id, slug, email, display_name, commission_rate, status, payout_email, user_id, recurring_months')
    .eq('user_id', userId)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (byUser) return byUser;
  if (!email) return null;
  const { data: byEmail } = await supabaseAdmin
    .from('affiliates')
    .select('id, slug, email, display_name, commission_rate, status, payout_email, user_id, recurring_months')
    .ilike('email', email)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (byEmail && !byEmail.user_id) {
    await supabaseAdmin
      .from('affiliates')
      .update({ user_id: userId, updated_at: new Date().toISOString() })
      .eq('id', byEmail.id);
    byEmail.user_id = userId;
  }
  return byEmail || null;
}

app.post('/api/affiliate/apply', requireUser, async (req, res) => {
  try {
    const email = normalizeEmail(req.user?.email);
    if (!email) {
      return res.status(400).json({ error: 'Your account must have an email address to apply.' });
    }
    const slug = normalizeAffiliateSlug(req.body?.slug);
    const displayName = String(req.body?.displayName || req.body?.display_name || '').trim().slice(0, 120);
    const payoutEmail = normalizeEmail(req.body?.payoutEmail || req.body?.payout_email) || email;
    if (!slug) {
      return res.status(400).json({
        error: 'Invalid referral slug. Use 3–32 characters: lowercase letters, numbers, and hyphens (e.g. jane-pins).',
      });
    }

    const existing = await findAffiliateRowForUser(req.user.id, email);
    if (existing?.status === 'active') {
      return res.status(400).json({ error: 'You already have an active partner account.' });
    }
    if (existing?.status === 'pending') {
      return res.json({ ok: true, status: 'pending', message: 'Your application is already pending review.' });
    }

    const { data: slugTaken } = await supabaseAdmin
      .from('affiliates')
      .select('id, status')
      .eq('slug', slug)
      .maybeSingle();
    if (slugTaken) {
      return res.status(409).json({ error: 'This referral slug is already taken. Choose another.' });
    }

    const now = new Date().toISOString();
    if (existing?.id && existing.status === 'disabled') {
      const { error: updErr } = await supabaseAdmin
        .from('affiliates')
        .update({
          slug,
          email,
          display_name: displayName || slug,
          payout_email: payoutEmail,
          status: 'pending',
          user_id: req.user.id,
          updated_at: now,
        })
        .eq('id', existing.id);
      if (updErr) {
        return res.status(500).json({ error: 'Failed to submit application' });
      }
      return res.json({ ok: true, status: 'pending', message: 'Application resubmitted. We will review it shortly.' });
    }

    const { error: insErr } = await supabaseAdmin.from('affiliates').insert({
      slug,
      email,
      display_name: displayName || slug,
      payout_email: payoutEmail,
      user_id: req.user.id,
      status: 'pending',
      commission_rate: DEFAULT_AFFILIATE_COMMISSION_RATE,
      recurring_months: null,
      created_at: now,
      updated_at: now,
    });
    if (insErr) {
      if (insErr.code === '23505') {
        return res.status(409).json({ error: 'This referral slug or email is already registered.' });
      }
      return res.status(500).json({ error: 'Failed to submit application' });
    }
    return res.json({
      ok: true,
      status: 'pending',
      message: 'Application received. We will email you when your partner account is approved.',
    });
  } catch (err) {
    console.error('affiliate/apply error:', err);
    return res.status(500).json({ error: 'Failed to submit application' });
  }
});

app.get('/api/affiliate/me', requireUser, async (req, res) => {
  try {
    const email = normalizeEmail(req.user?.email);
    const affiliate = await findAffiliateRowForUser(req.user.id, email);
    const recurringMonthsDefault = DEFAULT_AFFILIATE_RECURRING_MONTHS;

    if (!affiliate) {
      return res.json({
        ok: true,
        isAffiliate: false,
        isAdmin: isAffiliateAdminUser(req.user),
        recurringMonthsDefault,
      });
    }

    const status = String(affiliate.status || 'pending');
    if (status === 'pending') {
      return res.json({
        ok: true,
        isAffiliate: false,
        applicationStatus: 'pending',
        isAdmin: isAffiliateAdminUser(req.user),
        recurringMonthsDefault,
        message: 'Your partner application is pending approval.',
      });
    }
    if (status === 'disabled') {
      return res.json({
        ok: true,
        isAffiliate: false,
        applicationStatus: 'disabled',
        isAdmin: isAffiliateAdminUser(req.user),
        recurringMonthsDefault,
        message: 'Your partner application was not approved or was deactivated.',
      });
    }

    const slug = affiliate.slug;
    const recurringMonths = getRecurringMonthsForAffiliate(affiliate);
    const { count: signups } = await supabaseAdmin
      .from('profiles')
      .select('id', { count: 'exact', head: true })
      .eq('referred_by_affiliate_slug', slug);

    const { data: commissions } = await supabaseAdmin
      .from('affiliate_commissions')
      .select('id, plan_type, amount_cents, commission_cents, status, created_at, commission_kind')
      .eq('affiliate_id', affiliate.id)
      .order('created_at', { ascending: false })
      .limit(50);

    const rows = commissions || [];
    const pendingCents = rows
      .filter((r) => r.status === 'pending' || r.status === 'approved')
      .reduce((sum, r) => sum + (Number(r.commission_cents) || 0), 0);
    const paidCents = rows
      .filter((r) => r.status === 'paid')
      .reduce((sum, r) => sum + (Number(r.commission_cents) || 0), 0);

    let frontendBase = process.env.FRONTEND_BASE_URL || 'https://url2pin.com';
    try {
      const u = new URL(frontendBase);
      frontendBase = `${u.protocol}//${u.host}`;
    } catch {
      frontendBase = frontendBase.replace(/\/app\/?$/, '');
    }
    const referralLink = `${frontendBase.replace(/\/$/, '')}/?ref=${encodeURIComponent(slug)}`;

    return res.json({
      ok: true,
      isAffiliate: true,
      applicationStatus: 'active',
      isAdmin: isAffiliateAdminUser(req.user),
      affiliate: {
        slug,
        displayName: affiliate.display_name || slug,
        commissionRate: Number(affiliate.commission_rate) || DEFAULT_AFFILIATE_COMMISSION_RATE,
        payoutEmail: affiliate.payout_email || affiliate.email,
        recurringMonths,
      },
      referralLink,
      stats: {
        signups: signups || 0,
        conversions: rows.length,
        pendingCommissionCents: pendingCents,
        paidCommissionCents: paidCents,
      },
      commissions: rows,
      recurringMonthsDefault,
    });
  } catch (err) {
    console.error('affiliate/me error:', err);
    return res.status(500).json({ error: 'Failed to load affiliate dashboard' });
  }
});

app.get('/api/admin/affiliates/pending', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('affiliates')
      .select('id, slug, email, display_name, payout_email, commission_rate, recurring_months, created_at, user_id')
      .eq('status', 'pending')
      .order('created_at', { ascending: true });
    if (error) return res.status(500).json({ error: 'Failed to load applications' });
    return res.json({ ok: true, applications: data || [] });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to load applications' });
  }
});

app.post('/api/admin/affiliates/:id/approve', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    if (!id) return res.status(400).json({ error: 'Missing id' });
    const now = new Date().toISOString();
    const { data, error } = await supabaseAdmin
      .from('affiliates')
      .update({ status: 'active', updated_at: now })
      .eq('id', id)
      .eq('status', 'pending')
      .select('id, slug, email')
      .maybeSingle();
    if (error) return res.status(500).json({ error: 'Failed to approve' });
    if (!data) return res.status(404).json({ error: 'Application not found or already processed' });
    return res.json({ ok: true, affiliate: data });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to approve' });
  }
});

app.post('/api/admin/affiliates/:id/reject', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    if (!id) return res.status(400).json({ error: 'Missing id' });
    const now = new Date().toISOString();
    const { data, error } = await supabaseAdmin
      .from('affiliates')
      .update({ status: 'disabled', updated_at: now })
      .eq('id', id)
      .eq('status', 'pending')
      .select('id, slug, email')
      .maybeSingle();
    if (error) return res.status(500).json({ error: 'Failed to reject' });
    if (!data) return res.status(404).json({ error: 'Application not found or already processed' });
    return res.json({ ok: true, affiliate: data });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to reject' });
  }
});

// ===========================================================================
// Founder analytics dashboard (admin only)
// ---------------------------------------------------------------------------
// Revenue/fees/refunds are pulled live from the Dodo Payments API; subscription
// STATE comes from billing_subscriptions; expenses/acquisition/targets come from
// the manual admin_* tables. Results are cached in-memory for a few minutes
// because the Dodo list endpoints are paginated.
// ===========================================================================

// Canonical USD pricing + per-sale monthly profit (provided by the founder).
const FOUNDER_PLAN_PRICE_USD = { starter: 9, creator: 19, pro: 39, agency: 79 };
const FOUNDER_PLAN_ANNUAL_PRICE_USD = { starter: 84, creator: 180, pro: 384, agency: 780 };
const FOUNDER_PLAN_MONTHLY_PROFIT_USD = { starter: 5.67, creator: 11.74, pro: 17.91, agency: 33.34 };
const FOUNDER_PLAN_ORDER = ['starter', 'creator', 'pro', 'agency'];
const FOUNDER_PLAN_DISPLAY = { starter: 'Starter', creator: 'Creator', pro: 'Pro', agency: 'Agency' };
const FOUNDER_EXPENSE_CATEGORIES = ['openai', 'hosting', 'domains', 'email', 'ads', 'contractors', 'other'];
const FOUNDER_ACQUISITION_SOURCES = ['tiktok', 'pinterest', 'seo', 'affiliate', 'direct', 'other'];
// Sentinel "still active / auto-renewing" interval end (far future).
const FOUNDER_ONGOING_MS = Date.UTC(2999, 0, 1);

// Default config; overridden by admin_settings row 'founder_dashboard'.
const FOUNDER_DEFAULT_SETTINGS = {
  // USD value of 1 unit of each currency (Dodo amounts are in minor units / cents).
  exchangeRates: { USD: 1, EUR: 1.08, CAD: 0.73, AUD: 0.66, BRL: 0.18 },
  startingCashUsd: 0,
  targets: { mrr: 10000, customers: 500, profit: 5000 },
  forecast: { monthlyGrowthPct: 10, monthlyChurnPct: 5 },
  testEmails: [],
  paymentFeePctFallback: 5, // used only when Dodo doesn't return settlement_amount
  immediateRefundHours: 24, // refunds within this window (or reason=test) void the sale; set 0 to disable
};

function founderMonthKey(d) {
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return null;
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, '0')}`;
}

function founderMonthStart(monthKey) {
  const [y, m] = String(monthKey).split('-').map(Number);
  return Date.UTC(y, m - 1, 1);
}

function founderAddMonths(monthKey, n) {
  const [y, m] = String(monthKey).split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1 + n, 1));
  return founderMonthKey(dt);
}

function founderLastNMonths(n, endMonthKey) {
  const end = endMonthKey || founderMonthKey(new Date());
  const out = [];
  for (let i = n - 1; i >= 0; i--) out.push(founderAddMonths(end, -i));
  return out;
}

function founderMonthlyPriceUsd(planType, billingInterval) {
  const p = String(planType || '').toLowerCase();
  if (!FOUNDER_PLAN_PRICE_USD[p]) return 0;
  if (normalizeBillingInterval(billingInterval) === 'year') {
    return (FOUNDER_PLAN_ANNUAL_PRICE_USD[p] || 0) / 12;
  }
  return FOUNDER_PLAN_PRICE_USD[p] || 0;
}

function founderConvertToUsd(minorAmount, currency, rates) {
  const n = Number(minorAmount);
  if (!Number.isFinite(n) || n === 0) return 0;
  const code = String(currency || 'USD').toUpperCase();
  const rate = rates && Number.isFinite(Number(rates[code])) ? Number(rates[code]) : 1;
  return (n / 100) * rate; // minor units -> major, then -> USD
}

// --- Dodo API list helper (paginated) ---
async function founderDodoList(path, extraParams = {}) {
  if (!DODO_API_KEY) return [];
  const out = [];
  const pageSize = 100;
  for (let page = 0; page < 200; page++) {
    const qs = new URLSearchParams({ page_size: String(pageSize), page_number: String(page), ...extraParams });
    let json;
    try {
      const resp = await fetch(`${DODO_BASE_URL}${path}?${qs.toString()}`, {
        headers: { Authorization: `Bearer ${DODO_API_KEY}` },
      });
      if (!resp.ok) {
        console.warn(`founderDodoList ${path} HTTP ${resp.status}`);
        break;
      }
      json = await resp.json();
    } catch (e) {
      console.warn(`founderDodoList ${path} error:`, e?.message || e);
      break;
    }
    const items = Array.isArray(json) ? json : Array.isArray(json?.items) ? json.items : Array.isArray(json?.data) ? json.data : [];
    out.push(...items);
    if (items.length < pageSize) break;
  }
  return out;
}

// --- Supabase paginated fetch helper ---
async function founderFetchAll(table, columns) {
  const out = [];
  const pageSize = 1000;
  for (let from = 0; from < 500000; from += pageSize) {
    const { data, error } = await supabaseAdmin.from(table).select(columns).range(from, from + pageSize - 1);
    if (error) {
      console.warn(`founderFetchAll ${table} error:`, error.message || error);
      break;
    }
    const rows = data || [];
    out.push(...rows);
    if (rows.length < pageSize) break;
  }
  return out;
}

async function founderFetchAuthUsers() {
  const all = [];
  for (let page = 1; page <= 500; page++) {
    let res;
    try {
      res = await supabaseAdmin.auth.admin.listUsers({ page, perPage: 1000 });
    } catch (e) {
      console.warn('founderFetchAuthUsers error:', e?.message || e);
      break;
    }
    const batch = res?.data?.users || [];
    all.push(...batch);
    if (batch.length < 1000) break;
  }
  return all;
}

// --- Manual admin_* tables (graceful if missing) ---
async function founderLoadSettings() {
  try {
    const { data, error } = await supabaseAdmin
      .from('admin_settings')
      .select('value')
      .eq('key', 'founder_dashboard')
      .maybeSingle();
    if (error) throw error;
    const stored = (data && data.value) || {};
    return {
      ...FOUNDER_DEFAULT_SETTINGS,
      ...stored,
      exchangeRates: { ...FOUNDER_DEFAULT_SETTINGS.exchangeRates, ...(stored.exchangeRates || {}) },
      targets: { ...FOUNDER_DEFAULT_SETTINGS.targets, ...(stored.targets || {}) },
      forecast: { ...FOUNDER_DEFAULT_SETTINGS.forecast, ...(stored.forecast || {}) },
    };
  } catch (e) {
    return { ...FOUNDER_DEFAULT_SETTINGS };
  }
}

async function founderSaveSettings(partial) {
  const current = await founderLoadSettings();
  const merged = {
    ...current,
    ...partial,
    exchangeRates: { ...current.exchangeRates, ...(partial.exchangeRates || {}) },
    targets: { ...current.targets, ...(partial.targets || {}) },
    forecast: { ...current.forecast, ...(partial.forecast || {}) },
  };
  const { error } = await supabaseAdmin
    .from('admin_settings')
    .upsert({ key: 'founder_dashboard', value: merged, updated_at: new Date().toISOString() }, { onConflict: 'key' });
  if (error) throw error;
  return merged;
}

async function founderLoadExpenses() {
  try {
    const { data, error } = await supabaseAdmin
      .from('admin_expenses')
      .select('id, month, category, amount_usd, cost_type, note, created_at')
      .order('month', { ascending: true });
    if (error) throw error;
    return data || [];
  } catch (e) {
    return [];
  }
}

async function founderLoadAcquisition() {
  try {
    const { data, error } = await supabaseAdmin
      .from('customer_acquisition')
      .select('user_id, source, note, updated_at');
    if (error) throw error;
    const map = new Map();
    (data || []).forEach((r) => map.set(r.user_id, String(r.source || 'other').toLowerCase()));
    return map;
  } catch (e) {
    return new Map();
  }
}

// --- Heavy aggregator (cached) ---
let founderMetricsCache = { at: 0, payload: null };
const FOUNDER_CACHE_TTL_MS = 5 * 60 * 1000;

async function founderComputeMetrics() {
  const settings = await founderLoadSettings();
  const rates = settings.exchangeRates || FOUNDER_DEFAULT_SETTINGS.exchangeRates;
  const now = new Date();
  const nowMs = now.getTime();
  const thisMonth = founderMonthKey(now);
  const lastMonth = founderAddMonths(thisMonth, -1);
  const months12 = founderLastNMonths(12);
  const months24 = founderLastNMonths(24);

  // ---- Load everything in parallel ----
  const [subs, profiles, authUsers, expenses, acquisitionMap, payments, refunds] = await Promise.all([
    founderFetchAll(
      'billing_subscriptions',
      'id, user_id, plan_type, status, billing_interval, current_period_start, current_period_end, cancel_at_period_end, cancelled_at, dodo_subscription_id, created_at'
    ),
    founderFetchAll('profiles', 'id, plan_type, email, referred_by_affiliate_slug, created_at'),
    founderFetchAuthUsers(),
    founderLoadExpenses(),
    founderLoadAcquisition(),
    founderDodoList('/payments'),
    founderDodoList('/refunds'),
  ]);

  // ---- Identity maps ----
  const emailByUser = new Map();
  const signupByUser = new Map();
  const nameByUser = new Map();
  authUsers.forEach((u) => {
    if (u?.id) {
      emailByUser.set(u.id, normalizeEmail(u.email) || '');
      if (u.created_at) signupByUser.set(u.id, u.created_at);
      const nm = u.user_metadata?.full_name || u.user_metadata?.name || '';
      if (nm) nameByUser.set(u.id, nm);
    }
  });
  profiles.forEach((p) => {
    if (p?.id) {
      if (!emailByUser.get(p.id) && p.email) emailByUser.set(p.id, normalizeEmail(p.email));
      if (!signupByUser.get(p.id) && p.created_at) signupByUser.set(p.id, p.created_at);
    }
  });

  // ---- Excluded (test) accounts ----
  // ONLY emails the founder explicitly lists as test accounts (Settings → "Test emails").
  // Admin emails are for dashboard ACCESS control and must NOT be excluded from revenue —
  // the founder's own login is often also a real paying customer.
  const excludedEmails = new Set((settings.testEmails || []).map(normalizeEmail).filter(Boolean));
  const isExcludedUser = (userId) => excludedEmails.has(emailByUser.get(userId));

  // ---- Subscription intervals (for MRR history / churn / cohorts) ----
  // One interval per paid subscription row: [start, end).
  const subIntervals = [];
  const subsByUser = new Map();
  subs.forEach((s) => {
    const plan = String(s.plan_type || '').toLowerCase();
    if (!FOUNDER_PLAN_PRICE_USD[plan]) return; // skip free/unknown
    if (isExcludedUser(s.user_id)) return;
    const startMs = s.created_at ? new Date(s.created_at).getTime() : (s.current_period_start ? new Date(s.current_period_start).getTime() : null);
    if (startMs == null || Number.isNaN(startMs)) return;
    const cpe = s.current_period_end ? new Date(s.current_period_end).getTime() : null;
    // Interval end semantics:
    //  - cancelled            -> ended at cancelled_at (or period end)
    //  - active but lapsed     -> stale row, ended at period end
    //  - active + will cancel  -> ends at current_period_end (scheduled churn)
    //  - active + renewing     -> ONGOING (it auto-renews; do NOT cap at period end,
    //                             otherwise every monthly sub looks "churned" each month)
    let endMs;
    if (s.status === 'cancelled') {
      endMs = s.cancelled_at ? new Date(s.cancelled_at).getTime() : (cpe || startMs);
    } else if (cpe && cpe <= nowMs) {
      endMs = cpe; // active row whose period already lapsed (not yet cleaned up)
    } else if (s.cancel_at_period_end) {
      endMs = cpe || FOUNDER_ONGOING_MS;
    } else {
      endMs = FOUNDER_ONGOING_MS;
    }
    const interval = {
      userId: s.user_id,
      plan,
      interval: s.billing_interval,
      mrr: founderMonthlyPriceUsd(plan, s.billing_interval),
      startMs,
      endMs: Math.max(endMs, startMs),
      status: s.status,
      cancelAtPeriodEnd: !!s.cancel_at_period_end,
      currentPeriodEnd: s.current_period_end,
      createdAt: s.created_at,
    };
    subIntervals.push(interval);
    if (!subsByUser.has(s.user_id)) subsByUser.set(s.user_id, []);
    subsByUser.get(s.user_id).push(interval);
  });
  // Collapse upgrade/downgrade/re-subscribe CHAINS into a single continuous timeline per
  // customer: an earlier subscription can't outlive the start of the next one. This removes
  // phantom overlaps from old rows whose cancelled_at is null but current_period_end is still
  // in the future (otherwise the same customer counts as several active subs at once).
  subsByUser.forEach((arr) => {
    arr.sort((a, b) => a.startMs - b.startMs);
    for (let i = 0; i < arr.length - 1; i++) {
      if (arr[i].endMs > arr[i + 1].startMs) arr[i].endMs = arr[i + 1].startMs;
    }
  });

  const activeAtMs = (interval, ms) => interval.startMs <= ms && ms < interval.endMs;

  // MRR for a given timestamp: one (highest) active sub per user.
  function mrrAt(ms) {
    const perUser = new Map();
    for (const iv of subIntervals) {
      if (!activeAtMs(iv, ms)) continue;
      const prev = perUser.get(iv.userId);
      if (!prev || iv.mrr > prev.mrr) perUser.set(iv.userId, iv);
    }
    let mrr = 0;
    perUser.forEach((iv) => { mrr += iv.mrr; });
    return { mrr, customers: perUser.size, perUser };
  }

  // ---- Current snapshot ----
  const current = mrrAt(nowMs);
  const currentMrr = current.mrr;
  const activeCustomers = current.customers;
  const activeSubscriptions = current.perUser.size; // one paid sub per customer (deduped)
  const arpu = activeCustomers ? currentMrr / activeCustomers : 0;

  // ---- MRR history (start of each month) ----
  const mrrSeries = months24.map((mk) => {
    const ms = founderMonthStart(mk);
    const snap = mrrAt(ms);
    return { month: mk, mrr: Math.round(snap.mrr * 100) / 100, customers: snap.customers };
  });
  // Growth rate: trailing 3-month average of month-over-month MRR growth (smooths the
  // explosive ratios you get from a single MoM step on a small base).
  const growthSeries = mrrSeries.map((s) => s.mrr);
  const growthRatesArr = [];
  for (let i = Math.max(1, growthSeries.length - 3); i < growthSeries.length; i++) {
    const prev = growthSeries[i - 1];
    if (prev > 0) growthRatesArr.push((growthSeries[i] - prev) / prev);
  }
  const growthRate = growthRatesArr.length ? growthRatesArr.reduce((a, b) => a + b, 0) / growthRatesArr.length : 0;

  // ---- Churn ----
  // Realized churn over a single completed-month transition: who was active at the start of
  // `startMk` but is no longer active at the start of `endMk`.
  function churnForTransition(startMk, endMk) {
    const snap = mrrAt(founderMonthStart(startMk));
    const endMsLocal = founderMonthStart(endMk);
    const churned = [];
    let lost = 0;
    snap.perUser.forEach((iv, userId) => {
      const stillActive = subIntervals.some((x) => x.userId === userId && activeAtMs(x, endMsLocal));
      if (!stillActive) { churned.push(userId); lost += iv.mrr; }
    });
    return { startCustomers: snap.customers, startMrr: snap.mrr, snap, churned, lostMrr: lost };
  }

  // Last completed month — used for per-plan and per-source realized churn.
  const lastMonthChurn = churnForTransition(lastMonth, thisMonth);
  const snapStart = lastMonthChurn.snap;
  const churnedCustomers = lastMonthChurn.churned;

  // Executive churn = trailing 3-month average (smooths small-base volatility).
  const churnTransitions = [];
  for (let i = 3; i >= 1; i--) {
    churnTransitions.push(churnForTransition(founderAddMonths(thisMonth, -i), founderAddMonths(thisMonth, -(i - 1))));
  }
  const avgArr = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);
  const customerChurnPct = avgArr(churnTransitions.filter((t) => t.startCustomers > 0).map((t) => t.churned.length / t.startCustomers)) * 100;
  const revenueChurnPct = avgArr(churnTransitions.filter((t) => t.startMrr > 0).map((t) => t.lostMrr / t.startMrr)) * 100;
  const monthlyChurnRate = customerChurnPct / 100;

  // ---- MRR at risk (active subs scheduled to cancel) ----
  let mrrAtRisk = 0;
  const scheduledCancellations = [];
  current.perUser.forEach((iv, userId) => {
    if (iv.cancelAtPeriodEnd) {
      mrrAtRisk += iv.mrr;
      const daysUntil = iv.currentPeriodEnd ? Math.max(0, Math.ceil((new Date(iv.currentPeriodEnd).getTime() - nowMs) / 86400000)) : null;
      scheduledCancellations.push({
        userId,
        email: emailByUser.get(userId) || '',
        name: nameByUser.get(userId) || '',
        plan: FOUNDER_PLAN_DISPLAY[iv.plan] || iv.plan,
        mrr: Math.round(iv.mrr * 100) / 100,
        nextBillingDate: iv.currentPeriodEnd || null,
        daysUntilChurn: daysUntil,
      });
    }
  });
  scheduledCancellations.sort((a, b) => (a.daysUntilChurn ?? 1e9) - (b.daysUntilChurn ?? 1e9));

  // Projected next-month MRR: continue the recent absolute trend (avg of the last up-to-3
  // month-over-month changes), then subtract MRR already scheduled to cancel. Additive trend
  // avoids the explosive ratios you get from MoM % on a tiny base.
  const recentMrr = mrrSeries.map((s) => s.mrr);
  const recentDeltas = [];
  for (let i = Math.max(1, recentMrr.length - 3); i < recentMrr.length; i++) {
    recentDeltas.push(recentMrr[i] - recentMrr[i - 1]);
  }
  const avgRecentDelta = recentDeltas.length ? recentDeltas.reduce((a, b) => a + b, 0) / recentDeltas.length : 0;
  const projectedNextMonthMrr = Math.max(0, currentMrr + avgRecentDelta - mrrAtRisk);

  // Forward-looking churn: active subs scheduled to cancel at period end, as a % of the
  // current base (distinct from realized churn, which only counts subs that already lapsed).
  const customersAtRisk = scheduledCancellations.length;
  const scheduledCustomerChurnPct = activeCustomers ? (customersAtRisk / activeCustomers) * 100 : 0;
  const scheduledRevenueChurnPct = currentMrr ? (mrrAtRisk / currentMrr) * 100 : 0;

  // ---- Dodo payments (revenue / fees / refunds) ----
  const dodoSubToPlan = new Map();
  subs.forEach((s) => { if (s.dodo_subscription_id) dodoSubToPlan.set(s.dodo_subscription_id, String(s.plan_type || '').toLowerCase()); });
  const userByDodoSub = new Map();
  subs.forEach((s) => { if (s.dodo_subscription_id) userByDodoSub.set(s.dodo_subscription_id, s.user_id); });

  const paymentPlan = (p) => {
    const sid = p.subscription_id || p.subscription?.subscription_id;
    if (sid && dodoSubToPlan.get(sid)) return dodoSubToPlan.get(sid);
    const metaPlan = String(p.metadata?.app_plan_type || '').toLowerCase();
    if (FOUNDER_PLAN_PRICE_USD[metaPlan]) return metaPlan;
    return 'unknown';
  };
  const paymentUser = (p) => {
    const sid = p.subscription_id || p.subscription?.subscription_id;
    if (sid && userByDodoSub.get(sid)) return userByDodoSub.get(sid);
    return p.customer?.customer_id || null;
  };
  const paymentStatusOk = (p) => {
    const st = String(p.status || '').toLowerCase();
    return st === 'succeeded' || st === 'completed' || st === 'paid' || st === '';
  };

  const paymentId = (p) => p.payment_id || p.id || null;
  const paymentById = new Map();
  payments.forEach((p) => { const id = paymentId(p); if (id) paymentById.set(id, p); });

  // ---- DEDUP / IGNORE: immediate ("test") refunds void the underlying sale ----
  // A refund issued within 24h of its payment (or whose reason mentions "test") is
  // treated as a cancelled/test sale: the payment is voided from revenue and the
  // refund itself is NOT counted as a loss. Later refunds are real and subtract.
  const refundWindowHours = Number(settings.immediateRefundHours);
  const REFUND_TEST_WINDOW_MS = (Number.isFinite(refundWindowHours) ? refundWindowHours : 24) * 60 * 60 * 1000;
  const voidedPaymentIds = new Set();
  const countedRefunds = [];
  let immediateRefundCount = 0;
  refunds.forEach((r) => {
    const st = String(r.status || '').toLowerCase();
    if (st && st !== 'succeeded' && st !== 'completed' && st !== 'processed') return;
    const pid = r.payment_id || r.payment?.payment_id || null;
    const pay = pid ? paymentById.get(pid) : null;
    const reasonTest = /test/i.test(String(r.reason || ''));
    let immediate = false;
    if (REFUND_TEST_WINDOW_MS > 0 && pay && pay.created_at && r.created_at) {
      const delta = new Date(r.created_at).getTime() - new Date(pay.created_at).getTime();
      if (delta >= 0 && delta <= REFUND_TEST_WINDOW_MS) immediate = true;
    }
    if (immediate || reasonTest) {
      if (pid) voidedPaymentIds.add(pid);
      immediateRefundCount += 1;
    } else {
      countedRefunds.push(r);
    }
  });

  let grossRevenue = 0;
  let netRevenue = 0;
  let paymentFees = 0;
  const revenueByMonth = new Map(); // mk -> { gross, net, fees }
  const revenueByPlan = new Map();
  const grossByUser = new Map();
  let voidedSaleCount = 0;
  months24.forEach((mk) => revenueByMonth.set(mk, { gross: 0, net: 0, fees: 0 }));

  payments.forEach((p) => {
    if (!paymentStatusOk(p)) return;
    if (voidedPaymentIds.has(paymentId(p))) { voidedSaleCount += 1; return; } // immediate/test refund -> void sale
    const uid = paymentUser(p);
    if (uid && isExcludedUser(uid)) return;
    const gross = founderConvertToUsd(p.total_amount ?? p.amount, p.currency, rates);
    let net = founderConvertToUsd(p.settlement_amount, p.settlement_currency || p.currency, rates);
    if (!net) net = gross * (1 - (Number(settings.paymentFeePctFallback) || 0) / 100);
    net = Math.min(net, gross); // settlement should never exceed gross; guards FX/shape oddities
    const fee = Math.max(0, gross - net);
    grossRevenue += gross;
    netRevenue += net;
    paymentFees += fee;
    const mk = founderMonthKey(p.created_at);
    if (mk && revenueByMonth.has(mk)) {
      const r = revenueByMonth.get(mk);
      r.gross += gross; r.net += net; r.fees += fee;
    }
    const plan = paymentPlan(p);
    revenueByPlan.set(plan, (revenueByPlan.get(plan) || 0) + gross);
    if (uid) grossByUser.set(uid, (grossByUser.get(uid) || 0) + gross);
  });

  // ---- Refunds (real refunds only; immediate/test ones already voided above) ----
  let refundsTotal = 0;
  const refundsByMonth = new Map();
  countedRefunds.forEach((r) => {
    const uid = r.subscription_id && userByDodoSub.get(r.subscription_id);
    if (uid && isExcludedUser(uid)) return;
    const amt = founderConvertToUsd(r.amount, r.currency, rates);
    refundsTotal += amt;
    const mk = founderMonthKey(r.created_at);
    if (mk) refundsByMonth.set(mk, (refundsByMonth.get(mk) || 0) + amt);
  });
  const netRevenueAfterRefunds = netRevenue - refundsTotal;

  // ---- Failed payments (dedup duplicate retries of the same charge) ----
  // Multiple dunning retries for the same subscription within a month are one event.
  const failedRaw = payments.filter((p) => {
    const st = String(p.status || '').toLowerCase();
    if (st !== 'failed' && st !== 'declined') return false;
    const uid = paymentUser(p);
    return !(uid && isExcludedUser(uid));
  });
  const failedSeen = new Set();
  const failedPayments = [];
  failedRaw
    .slice()
    .sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime())
    .forEach((p) => {
      const key = `${p.subscription_id || paymentId(p)}|${founderMonthKey(p.created_at)}`;
      if (failedSeen.has(key)) return; // duplicate retry
      failedSeen.add(key);
      failedPayments.push(p);
    });
  const failedRetriesIgnored = failedRaw.length - failedPayments.length;
  let recoveredCount = 0;
  let revenueSaved = 0;
  let revenueLost = 0;
  failedPayments.forEach((fp) => {
    const sid = fp.subscription_id;
    const failMs = new Date(fp.created_at).getTime();
    const recovered = sid && payments.some((p) => p.subscription_id === sid && paymentStatusOk(p) && new Date(p.created_at).getTime() > failMs);
    const amt = founderConvertToUsd(fp.total_amount ?? fp.amount, fp.currency, rates);
    if (recovered) { recoveredCount += 1; revenueSaved += amt; }
    else { revenueLost += amt; }
  });
  const recoveryRate = failedPayments.length ? (recoveredCount / failedPayments.length) * 100 : 0;

  // ---- Subscription analytics (this month) ----
  let newCustomers = 0;
  let reactivatedCustomers = 0;
  let upgradeEvents = 0;
  let downgradeEvents = 0;
  let expansionRevenue = 0;
  let contractionRevenue = 0;
  subsByUser.forEach((arr) => {
    const first = arr[0];
    if (first && founderMonthKey(first.startMs) === thisMonth) newCustomers += 1;
    for (let i = 1; i < arr.length; i++) {
      const prev = arr[i - 1];
      const curr = arr[i];
      if (founderMonthKey(curr.startMs) !== thisMonth) continue;
      // reactivation: gap between previous end and this start
      if (curr.startMs > prev.endMs + 86400000) reactivatedCustomers += 1;
      const dRank = planRank(curr.plan) - planRank(prev.plan);
      if (dRank > 0) { upgradeEvents += 1; expansionRevenue += Math.max(0, curr.mrr - prev.mrr); }
      else if (dRank < 0) { downgradeEvents += 1; contractionRevenue += Math.max(0, prev.mrr - curr.mrr); }
    }
  });
  const churnedCustomersCount = churnedCustomers.length;

  // ---- Plan analytics ----
  const planAnalytics = FOUNDER_PLAN_ORDER.map((plan) => {
    let custs = 0;
    let mrr = 0;
    current.perUser.forEach((iv) => { if (iv.plan === plan) { custs += 1; mrr += iv.mrr; } });
    // churned this month on this plan
    let planChurned = 0;
    snapStart.perUser.forEach((iv, userId) => {
      if (iv.plan !== plan) return;
      if (churnedCustomers.includes(userId)) planChurned += 1;
    });
    const profit = (FOUNDER_PLAN_MONTHLY_PROFIT_USD[plan] || 0) * custs;
    return {
      plan: FOUNDER_PLAN_DISPLAY[plan],
      slug: plan,
      price: FOUNDER_PLAN_PRICE_USD[plan],
      activeCustomers: custs,
      mrr: Math.round(mrr * 100) / 100,
      revenue: Math.round((revenueByPlan.get(plan) || 0) * 100) / 100,
      churnedThisMonth: planChurned,
      monthlyProfit: Math.round(profit * 100) / 100,
    };
  });
  const totalMonthlyProfit = planAnalytics.reduce((a, b) => a + b.monthlyProfit, 0);
  const grossMargin = currentMrr > 0 ? (totalMonthlyProfit / currentMrr) * 100 : 0;

  // ---- Expenses (this month) ----
  const expenseByMonthCat = new Map();
  expenses.forEach((e) => {
    const key = `${e.month}|${e.category}`;
    expenseByMonthCat.set(key, (expenseByMonthCat.get(key) || 0) + Number(e.amount_usd || 0));
  });
  const expenseTotalsByMonth = new Map();
  const fixedByMonth = new Map();
  const variableByMonth = new Map();
  expenses.forEach((e) => {
    const amt = Number(e.amount_usd || 0);
    expenseTotalsByMonth.set(e.month, (expenseTotalsByMonth.get(e.month) || 0) + amt);
    if (String(e.cost_type) === 'variable') variableByMonth.set(e.month, (variableByMonth.get(e.month) || 0) + amt);
    else fixedByMonth.set(e.month, (fixedByMonth.get(e.month) || 0) + amt);
  });
  const fixedThis = fixedByMonth.get(thisMonth) || 0;
  const variableThis = variableByMonth.get(thisMonth) || 0;
  const grossThisMonth = (revenueByMonth.get(thisMonth) || {}).gross || 0;
  const feesThisMonth = (revenueByMonth.get(thisMonth) || {}).fees || 0;
  const refundsThisMonth = refundsByMonth.get(thisMonth) || 0;
  const netProfitThisMonth = grossThisMonth - feesThisMonth - refundsThisMonth - variableThis - fixedThis;
  const netMargin = grossThisMonth > 0 ? (netProfitThisMonth / grossThisMonth) * 100 : 0;
  const expenseCategoryThisMonth = FOUNDER_EXPENSE_CATEGORIES.map((cat) => ({
    category: cat,
    amount: Math.round((expenseByMonthCat.get(`${thisMonth}|${cat}`) || 0) * 100) / 100,
  }));

  // ---- LTV / CAC / runway ----
  const grossMarginFrac = grossMargin / 100;
  const ltv = monthlyChurnRate > 0 ? (arpu * grossMarginFrac) / monthlyChurnRate : arpu * grossMarginFrac * 36;
  const adsThisMonth = expenseByMonthCat.get(`${thisMonth}|ads`) || 0;
  const cac = newCustomers > 0 ? adsThisMonth / newCustomers : 0;
  const ltvCacRatio = cac > 0 ? ltv / cac : null;
  const monthlyBurn = Math.max(0, fixedThis + variableThis + feesThisMonth + refundsThisMonth - ((revenueByMonth.get(thisMonth) || {}).net || 0));
  const startingCash = Number(settings.startingCashUsd) || 0;
  const cumulativeProfitAllTime = netRevenueAfterRefunds - expenses.reduce((a, e) => a + Number(e.amount_usd || 0), 0);
  const cashOnHand = startingCash + cumulativeProfitAllTime;
  const runwayMonths = monthlyBurn > 0 ? cashOnHand / monthlyBurn : null; // null = profitable / infinite

  // ---- Cohort retention ----
  // Cohort = month of a user's first paid subscription. Retention[k] = fraction active at cohort+k.
  const cohorts = new Map();
  subsByUser.forEach((arr, userId) => {
    const first = arr[0];
    const cohortMonth = founderMonthKey(first.startMs);
    if (!cohorts.has(cohortMonth)) cohorts.set(cohortMonth, []);
    cohorts.get(cohortMonth).push(userId);
  });
  const cohortOffsets = [0, 1, 2, 3, 6, 12];
  const cohortRows = founderLastNMonths(12)
    .filter((mk) => cohorts.has(mk))
    .map((mk) => {
      const users = cohorts.get(mk) || [];
      const size = users.length;
      const retention = {};
      cohortOffsets.forEach((k) => {
        const offMonth = founderAddMonths(mk, k);
        const mStart = founderMonthStart(offMonth);
        const mEnd = founderMonthStart(founderAddMonths(offMonth, 1));
        if (mStart > nowMs) { retention[k] = null; return; } // future month
        // Retained = had a subscription active at ANY point during the offset month
        // (overlap test). This makes M0 = 100% since the signup happens that month,
        // instead of checking only the 1st (before a mid-month signup is active).
        const retained = users.filter((u) => subIntervals.some((x) => x.userId === u && x.startMs < mEnd && x.endMs > mStart)).length;
        retention[k] = size ? Math.round((retained / size) * 1000) / 10 : 0;
      });
      return { cohort: mk, size, retention };
    });

  // ---- Acquisition ----
  const acqAgg = new Map();
  FOUNDER_ACQUISITION_SOURCES.forEach((s) => acqAgg.set(s, { source: s, customers: 0, mrr: 0, revenue: 0, churned: 0, ltvSum: 0 }));
  const sourceForUser = (userId) => {
    const manual = acquisitionMap.get(userId);
    if (manual && acqAgg.has(manual)) return manual;
    const prof = profiles.find((p) => p.id === userId);
    if (prof && prof.referred_by_affiliate_slug) return 'affiliate';
    return 'other';
  };
  current.perUser.forEach((iv, userId) => {
    const src = sourceForUser(userId);
    const a = acqAgg.get(src);
    a.customers += 1;
    a.mrr += iv.mrr;
    a.revenue += grossByUser.get(userId) || 0;
  });
  // Per-source realized churn uses the SAME definition as the executive churn:
  // churned last month / active at the start of last month (per source).
  const startBySource = new Map();
  snapStart.perUser.forEach((iv, userId) => {
    const src = sourceForUser(userId);
    startBySource.set(src, (startBySource.get(src) || 0) + 1);
  });
  churnedCustomers.forEach((userId) => {
    const src = sourceForUser(userId);
    if (acqAgg.has(src)) acqAgg.get(src).churned += 1;
  });
  const acquisition = Array.from(acqAgg.values()).map((a) => {
    const startCount = startBySource.get(a.source) || 0;
    const churnPct = startCount > 0 ? (a.churned / startCount) * 100 : 0;
    const arpuSrc = a.customers ? a.mrr / a.customers : 0;
    const ltvSrc = monthlyChurnRate > 0 ? (arpuSrc * grossMarginFrac) / monthlyChurnRate : arpuSrc * grossMarginFrac * 36;
    return {
      source: a.source,
      customers: a.customers,
      mrr: Math.round(a.mrr * 100) / 100,
      revenue: Math.round(a.revenue * 100) / 100,
      churnPct: Math.round(churnPct * 10) / 10,
      ltv: Math.round(ltvSrc * 100) / 100,
    };
  });

  // ---- Cash flow (last 12 months) ----
  const cashFlow = (() => {
    let cumulative = startingCash;
    return months12.map((mk) => {
      const r = revenueByMonth.get(mk) || { gross: 0, net: 0, fees: 0 };
      const cashIn = r.net - (refundsByMonth.get(mk) || 0);
      const cashOut = expenseTotalsByMonth.get(mk) || 0;
      const profit = cashIn - cashOut;
      cumulative += profit;
      return {
        month: mk,
        cashIn: Math.round(cashIn * 100) / 100,
        cashOut: Math.round(cashOut * 100) / 100,
        monthlyProfit: Math.round(profit * 100) / 100,
        cumulativeProfit: Math.round(cumulative * 100) / 100,
      };
    });
  })();

  // ---- Forecast (12 + 24 months) ----
  const fGrowth = (Number(settings.forecast?.monthlyGrowthPct) || 0) / 100;
  const fChurn = (Number(settings.forecast?.monthlyChurnPct) || 0) / 100;
  const netRate = fGrowth - fChurn;
  const avgMarginFrac = grossMarginFrac > 0 ? grossMarginFrac : 0.5;
  function buildForecast(nMonths) {
    const out = [];
    let mrr = currentMrr;
    for (let i = 1; i <= nMonths; i++) {
      mrr = Math.max(0, mrr * (1 + netRate));
      out.push({
        month: founderAddMonths(thisMonth, i),
        mrr: Math.round(mrr * 100) / 100,
        arr: Math.round(mrr * 12 * 100) / 100,
        profit: Math.round(mrr * avgMarginFrac * 100) / 100,
      });
    }
    return out;
  }
  const forecast12 = buildForecast(12);
  const forecast24 = buildForecast(24);

  // ---- Valuation ----
  const arr = currentMrr * 12;
  const valuation = [3, 5, 8, 10].map((mult) => ({ multiple: mult, value: Math.round(arr * mult) }));

  // ---- Founder scorecard ----
  const targets = settings.targets || FOUNDER_DEFAULT_SETTINGS.targets;
  const milestones = [1000, 3000, 5000, 10000, 25000, 50000].map((m) => ({ mrr: m, reached: currentMrr >= m }));
  const scorecard = {
    mrr: { current: Math.round(currentMrr), target: targets.mrr, gap: Math.round((targets.mrr || 0) - currentMrr) },
    customers: { current: activeCustomers, target: targets.customers, gap: (targets.customers || 0) - activeCustomers },
    profit: { current: Math.round(totalMonthlyProfit), target: targets.profit, gap: Math.round((targets.profit || 0) - totalMonthlyProfit) },
    milestones,
  };

  const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

  // ---- Data hygiene summary (dedup / ignore rules applied) ----
  const rawActiveSubRows = subIntervals.filter((iv) => activeAtMs(iv, nowMs)).length;
  const duplicateActiveSubsDeduped = Math.max(0, rawActiveSubRows - activeSubscriptions);

  return {
    generatedAt: new Date().toISOString(),
    dodoMode: DODO_BASE_URL.includes('test') ? 'test' : 'live',
    dodoConfigured: !!DODO_API_KEY,
    paymentsCount: payments.length,
    dataHygiene: {
      excludedTestAccounts: excludedEmails.size,
      immediateOrTestRefundsVoided: voidedSaleCount,
      immediateOrTestRefundsDetected: immediateRefundCount,
      realRefundsCounted: countedRefunds.length,
      failedRetriesIgnored,
      duplicateActiveSubsDeduped,
      note: 'Test/admin accounts excluded by email. Refunds within 24h (or reason=test) void the underlying sale and are not counted as losses. Duplicate failed-payment retries are collapsed to one per subscription per month. Upgrade/downgrade chains and duplicate subscriptions are deduped to one (highest) active sub per customer.',
    },
    executive: {
      mrr: round2(currentMrr),
      arr: round2(arr),
      activeCustomers,
      activeSubscriptions,
      activeScheduledToCancel: customersAtRisk,
      netActiveCustomers: Math.max(0, activeCustomers - customersAtRisk),
      netRevenue: round2(netRevenueAfterRefunds),
      grossRevenue: round2(grossRevenue),
      paymentFees: round2(paymentFees),
      netProfit: round2(netProfitThisMonth),
      mrrAtRisk: round2(mrrAtRisk),
      projectedNextMonthMrr: round2(projectedNextMonthMrr),
      customerChurnPct: round2(customerChurnPct),
      revenueChurnPct: round2(revenueChurnPct),
      scheduledCustomerChurnPct: round2(scheduledCustomerChurnPct),
      scheduledRevenueChurnPct: round2(scheduledRevenueChurnPct),
      growthRatePct: round2(growthRate * 100),
      arpu: round2(arpu),
      ltv: round2(ltv),
      ltvCacRatio: ltvCacRatio == null ? null : round2(ltvCacRatio),
      cac: round2(cac),
      runwayMonths: runwayMonths == null ? null : round2(runwayMonths),
      cashOnHand: round2(cashOnHand),
    },
    revenueAnalytics: {
      mrrSeries,
      revenueByMonth: months24.map((mk) => {
        const r = revenueByMonth.get(mk) || { gross: 0, net: 0, fees: 0 };
        return { month: mk, gross: round2(r.gross), net: round2(r.net), fees: round2(r.fees) };
      }),
      revenueByPlan: planAnalytics.map((p) => ({ plan: p.plan, revenue: p.revenue })),
    },
    subscriptionAnalytics: {
      newCustomers,
      churnedCustomers: churnedCustomersCount,
      reactivatedCustomers,
      expansionRevenue: round2(expansionRevenue),
      contractionRevenue: round2(contractionRevenue),
      upgradeEvents,
      downgradeEvents,
    },
    planAnalytics: {
      plans: planAnalytics,
      grossMarginPct: round2(grossMargin),
      netMarginPct: round2(netMargin),
      totalMonthlyProfit: round2(totalMonthlyProfit),
    },
    scheduledCancellations: {
      customersAtRisk: scheduledCancellations.length,
      mrrAtRisk: round2(mrrAtRisk),
      revenueAtRisk: round2(mrrAtRisk * 12),
      rows: scheduledCancellations,
    },
    cohorts: { offsets: cohortOffsets, rows: cohortRows },
    failedPayments: {
      failed: failedPayments.length,
      recovered: recoveredCount,
      recoveryRatePct: round2(recoveryRate),
      revenueSaved: round2(revenueSaved),
      revenueLost: round2(revenueLost),
    },
    acquisition,
    expenses: {
      thisMonth,
      categories: expenseCategoryThisMonth,
      fixed: round2(fixedThis),
      variable: round2(variableThis),
      fees: round2(feesThisMonth),
      refunds: round2(refundsThisMonth),
      grossRevenueThisMonth: round2(grossThisMonth),
      netProfitThisMonth: round2(netProfitThisMonth),
      rows: expenses,
    },
    cashFlow,
    forecast: { months12: forecast12, months24: forecast24, assumptions: { monthlyGrowthPct: settings.forecast?.monthlyGrowthPct, monthlyChurnPct: settings.forecast?.monthlyChurnPct } },
    valuation: { arr: round2(arr), scenarios: valuation },
    scorecard,
    settings,
  };
}

app.get('/api/admin/metrics', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const force = String(req.query.refresh || '') === '1';
    if (!force && founderMetricsCache.payload && Date.now() - founderMetricsCache.at < FOUNDER_CACHE_TTL_MS) {
      return res.json({ ok: true, cached: true, ...founderMetricsCache.payload });
    }
    const payload = await founderComputeMetrics();
    founderMetricsCache = { at: Date.now(), payload };
    return res.json({ ok: true, cached: false, ...payload });
  } catch (err) {
    console.error('admin/metrics error:', err);
    return res.status(500).json({ error: 'Failed to compute metrics' });
  }
});

app.get('/api/admin/metrics/settings', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const settings = await founderLoadSettings();
    return res.json({ ok: true, settings });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to load settings' });
  }
});

app.put('/api/admin/metrics/settings', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const merged = await founderSaveSettings(req.body || {});
    founderMetricsCache = { at: 0, payload: null };
    return res.json({ ok: true, settings: merged });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to save settings (is the admin_settings table created?)' });
  }
});

app.get('/api/admin/metrics/expenses', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const rows = await founderLoadExpenses();
    return res.json({ ok: true, expenses: rows, categories: FOUNDER_EXPENSE_CATEGORIES });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to load expenses' });
  }
});

app.post('/api/admin/metrics/expenses', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const { month, category, amount_usd, cost_type, note } = req.body || {};
    if (!/^\d{4}-\d{2}$/.test(String(month || ''))) return res.status(400).json({ error: 'month must be YYYY-MM' });
    if (!FOUNDER_EXPENSE_CATEGORIES.includes(String(category))) return res.status(400).json({ error: 'invalid category' });
    const { data, error } = await supabaseAdmin
      .from('admin_expenses')
      .insert({
        month: String(month),
        category: String(category),
        amount_usd: Number(amount_usd) || 0,
        cost_type: String(cost_type) === 'variable' ? 'variable' : 'fixed',
        note: note ? String(note) : null,
      })
      .select('id')
      .maybeSingle();
    if (error) throw error;
    founderMetricsCache = { at: 0, payload: null };
    return res.json({ ok: true, id: data?.id });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to add expense (is the admin_expenses table created?)' });
  }
});

app.delete('/api/admin/metrics/expenses/:id', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const { error } = await supabaseAdmin.from('admin_expenses').delete().eq('id', String(req.params.id));
    if (error) throw error;
    founderMetricsCache = { at: 0, payload: null };
    return res.json({ ok: true });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to delete expense' });
  }
});

app.get('/api/admin/metrics/acquisition', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const [profiles, authUsers, acqMap] = await Promise.all([
      founderFetchAll('profiles', 'id, plan_type, email, referred_by_affiliate_slug'),
      founderFetchAuthUsers(),
      founderLoadAcquisition(),
    ]);
    const emailByUser = new Map();
    authUsers.forEach((u) => u?.id && emailByUser.set(u.id, normalizeEmail(u.email) || ''));
    const paid = profiles
      .filter((p) => p.plan_type && p.plan_type !== 'free')
      .map((p) => ({
        userId: p.id,
        email: emailByUser.get(p.id) || normalizeEmail(p.email) || '',
        plan: p.plan_type,
        source: acqMap.get(p.id) || (p.referred_by_affiliate_slug ? 'affiliate' : 'other'),
      }));
    return res.json({ ok: true, customers: paid, sources: FOUNDER_ACQUISITION_SOURCES });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to load acquisition data' });
  }
});

app.post('/api/admin/metrics/acquisition', requireUser, requireAffiliateAdmin, async (req, res) => {
  try {
    const { user_id, source, note } = req.body || {};
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    if (!FOUNDER_ACQUISITION_SOURCES.includes(String(source))) return res.status(400).json({ error: 'invalid source' });
    const { error } = await supabaseAdmin
      .from('customer_acquisition')
      .upsert({ user_id: String(user_id), source: String(source), note: note ? String(note) : null, updated_at: new Date().toISOString() }, { onConflict: 'user_id' });
    if (error) throw error;
    founderMetricsCache = { at: 0, payload: null };
    return res.json({ ok: true });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to save acquisition source (is the customer_acquisition table created?)' });
  }
});

// Hosted billing recovery flow (update payment method) for failed renewals.
// Uses Dodo "update payment method" which returns a payment_link to redirect the customer.
app.post('/api/account/update-payment-method', requireUser, async (req, res) => {
  try {
    if (!DODO_API_KEY) {
      return res.status(400).json({ error: 'Billing provider is not configured.' });
    }

    const { data: sub, error } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, status, dodo_subscription_id')
      .eq('user_id', req.user.id)
      .in('status', ['active', 'past_due'])
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) {
      return res.status(500).json({ error: 'Failed to load subscription.' });
    }

    const dodoSubId = String(sub?.dodo_subscription_id || '').trim();
    if (!dodoSubId) {
      return res.status(400).json({ error: 'No billing subscription found for this account.' });
    }

    // Compute frontend base (strip any /app or deeper path)
    let frontendBase = process.env.FRONTEND_BASE_URL || 'http://localhost:3000';
    try {
      const u = new URL(frontendBase);
      frontendBase = `${u.protocol}//${u.host}`;
    } catch {
      frontendBase = frontendBase.replace(/\/app\/?$/, '');
    }

    const returnUrl = `${frontendBase.replace(/\/$/, '')}/my-account`;

    const resp = await fetch(`${DODO_BASE_URL}/subscriptions/${encodeURIComponent(dodoSubId)}/update-payment-method`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DODO_API_KEY}`,
      },
      body: JSON.stringify({ type: 'new', return_url: returnUrl }),
    });

    const json = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      console.error('Dodo update payment method error:', resp.status, json);
      return res.status(502).json({ error: 'Failed to start payment method update.', details: json });
    }

    const url = json.payment_link || json.link || json.url || null;
    if (!url) {
      return res.status(502).json({ error: 'Billing provider did not return a redirect link.', details: json });
    }

    return res.json({ ok: true, url });
  } catch (err) {
    console.error('account/update-payment-method error:', err);
    return res.status(500).json({ error: 'Failed to start payment method update.', details: err.message || String(err) });
  }
});

// Recovery for stuck billing: close any past_due (or provider-inactive) subscription rows
// so the user can start a clean checkout instead of being trapped in the update-payment loop.
app.post('/api/account/reset-failed-billing', requireUser, async (req, res) => {
  try {
    const result = await clearFailedBillingForUser(req.user.id);
    if (!result.ok) {
      return res.status(500).json({ error: 'Failed to reset billing.', details: result.error || null });
    }
    return res.json({
      ok: true,
      closed: result.closed,
      message:
        'Your failed subscription has been cleared. You can now choose a plan and start a fresh checkout from the Pricing page.',
    });
  } catch (err) {
    console.error('account/reset-failed-billing error:', err);
    return res.status(500).json({ error: 'Failed to reset billing.', details: err.message || String(err) });
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
    const { planType, billingInterval: rawBillingInterval, checkoutSessionId } = req.body || {};
    if (!planType || !PLAN_PIN_LIMITS[planType]) {
      return res.status(400).json({ error: 'Invalid planType' });
    }
    const billingInterval = normalizeBillingInterval(rawBillingInterval);

    const pending = consumePendingDodoActivation(req.user.id, planType, billingInterval);
    const checkoutId = String(checkoutSessionId || pending.pending?.sessionId || '').trim();

    let dodoSubId = null;
    if (checkoutId && DODO_API_KEY) {
      dodoSubId = await fetchDodoSubscriptionIdFromCheckoutSessionWithRetry(checkoutId, 20000);
    }
    const canVerifyViaCheckout = Boolean(checkoutId && dodoSubId);

    if (!pending.ok && !canVerifyViaCheckout) {
      console.warn('activate-plan blocked (no verified pending checkout and no verifiable checkout session)', {
        userId: req.user.id,
        planType,
        billingInterval,
        reason: pending.reason,
        pendingPlanType: pending.pendingPlanType || null,
        pendingInterval: pending.pendingInterval || null,
        checkoutId: checkoutId || null,
      });
      return res.status(409).json({
        error: 'Payment is complete, but activation could not be verified yet. Please retry in a minute.',
        code: 'activation_not_yet_verifiable',
        reason: pending.reason,
      });
    }

    if (!dodoSubId) {
      console.warn('activate-plan: could not resolve dodo_subscription_id from checkout', {
        userId: req.user.id,
        planType,
        checkoutId: checkoutId || null,
      });
    }

    // If we resolved a provider subscription id, verify it belongs to this user/plan via metadata.
    if (dodoSubId && DODO_API_KEY) {
      try {
        const getResp = await fetch(`${DODO_BASE_URL}/subscriptions/${encodeURIComponent(dodoSubId)}`, {
          method: 'GET',
          headers: { Authorization: `Bearer ${DODO_API_KEY}` },
        });
        const getJson = await getResp.json().catch(() => ({}));
        if (!getResp.ok) {
          console.warn('activate-plan: Dodo GET subscription failed (continuing)', {
            status: getResp.status,
            subId: dodoSubId,
            details: getJson,
          });
        } else {
          const md = getJson?.metadata || {};
          const mdUserId = String(md?.supabase_user_id || '').trim();
          const mdPlan = String(md?.app_plan_type || md?.plan_type || '').trim();
          const mdInterval = normalizeBillingInterval(md?.app_billing_interval || md?.billing_interval || billingInterval);
          if (mdUserId && mdUserId !== req.user.id) {
            return res.status(409).json({
              error: 'checkout_belongs_to_different_account',
              code: 'checkout_belongs_to_different_account',
              message: 'This payment appears to belong to a different account. Please contact support.',
            });
          }
          if (mdPlan && mdPlan !== planType) {
            return res.status(409).json({
              error: 'checkout_plan_mismatch',
              code: 'checkout_plan_mismatch',
              message: 'This payment does not match the selected plan. Please contact support.',
            });
          }
          if (mdInterval && mdInterval !== billingInterval) {
            return res.status(409).json({
              error: 'checkout_interval_mismatch',
              code: 'checkout_interval_mismatch',
              message: 'This payment does not match the selected billing interval. Please contact support.',
            });
          }
        }
      } catch (e) {
        console.warn('activate-plan: Dodo GET subscription metadata check error (continuing)', e?.message || e);
      }
    }

    // Webhook may have activated first: avoid replacing a good row with a duplicate; backfill Dodo id if missing.
    const { data: activeRow } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, plan_type, dodo_subscription_id')
      .eq('user_id', req.user.id)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (activeRow && activeRow.plan_type === planType) {
      if (activeRow.dodo_subscription_id) {
        return returnActivatePlanSuccess(res, checkoutId, {
          ok: true,
          planType,
          pinsLimit: PLAN_PIN_LIMITS[planType],
          note: 'already_active',
        });
      }
      if (dodoSubId) {
        await supabaseAdmin
          .from('billing_subscriptions')
          .update({
            dodo_subscription_id: dodoSubId,
            billing_interval: billingInterval,
            updated_at: new Date().toISOString(),
          })
          .eq('id', activeRow.id)
          .eq('user_id', req.user.id);
        console.log('✅ activate-plan backfilled dodo_subscription_id', {
          userId: req.user.id,
          planType,
          subscriptionId: dodoSubId,
        });
        return returnActivatePlanSuccess(res, checkoutId, {
          ok: true,
          planType,
          pinsLimit: PLAN_PIN_LIMITS[planType],
          backfilled_dodo_id: true,
        });
      }
      console.warn('activate-plan: active row exists but Dodo subscription id unknown (webhook may backfill)', {
        userId: req.user.id,
        planType,
        checkoutId: checkoutId || null,
      });
      return returnActivatePlanSuccess(res, checkoutId, {
        ok: true,
        planType,
        pinsLimit: PLAN_PIN_LIMITS[planType],
        note: 'active_missing_dodo_provider_id',
      });
    }

    // Safety: prevent claiming a provider subscription id that is already linked to another user in our DB.
    if (dodoSubId) {
      const { data: existingByDodo } = await supabaseAdmin
        .from('billing_subscriptions')
        .select('id, user_id, status, plan_type')
        .eq('dodo_subscription_id', dodoSubId)
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (existingByDodo && String(existingByDodo.user_id) !== String(req.user.id)) {
        return res.status(409).json({
          error: 'subscription_already_linked_to_another_user',
          code: 'subscription_already_linked_to_another_user',
          message: 'This subscription is already linked to a different account. Please contact support.',
        });
      }
    }

    const result = await applyPlanActivationForUser(req.user.id, planType, 'payment_success_fallback', {
      dodoSubscriptionId: dodoSubId,
      billingInterval,
    });
    if (!result.ok) {
      return res.status(500).json({ error: 'Failed to activate plan', details: result.error });
    }

    console.log('✅ activate-plan fallback succeeded', {
      userId: req.user.id,
      planType,
      checkoutSessionId: checkoutId || null,
      dodo_subscription_id: dodoSubId || null,
      via: 'payment-success',
    });
    return returnActivatePlanSuccess(res, checkoutId, { ok: true, planType: result.planType, pinsLimit: result.pinsLimit });
  } catch (err) {
    console.error('activate-plan error:', err);
    return res.status(500).json({
      error: 'Failed to activate plan',
      details: err.message || String(err),
    });
  }
});

app.post('/api/account/cancel', requireUser, async (req, res) => {
  try {
    const now = new Date().toISOString();
    const { data: activeSub, error } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, plan_type, status, current_period_end, dodo_subscription_id')
      .eq('user_id', req.user.id)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) {
      console.error('account/cancel fetch error:', error);
      return res.status(500).json({ error: 'Failed to load active subscription' });
    }

    if (!activeSub) {
      return res.status(400).json({ error: 'No active subscription to cancel.' });
    }

    // Prefer cancelling in Dodo (cancel at next billing date) so the user keeps benefits until period end.
    if (DODO_API_KEY && activeSub.dodo_subscription_id) {
      try {
        const dodoResp = await fetch(
          `${DODO_BASE_URL}/subscriptions/${encodeURIComponent(activeSub.dodo_subscription_id)}`,
          {
            method: 'PATCH',
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${DODO_API_KEY}`,
            },
            body: JSON.stringify({ cancel_at_next_billing_date: true }),
          }
        );
        const dodoJson = await dodoResp.json().catch(() => ({}));
        if (!dodoResp.ok) {
          console.warn('account/cancel: Dodo cancel failed', {
            status: dodoResp.status,
            details: dodoJson,
            subId: activeSub.dodo_subscription_id,
          });
        }
      } catch (e) {
        console.warn('account/cancel: Dodo cancel error', e.message || e);
      }
    }

    await supabaseAdmin
      .from('billing_subscriptions')
      .update({ cancel_at_period_end: true, cancelled_at: now, updated_at: now })
      .eq('id', activeSub.id)
      .eq('user_id', req.user.id);

    console.log('ℹ️ account/cancel: marked subscription cancelled in app only', {
      userId: req.user.id,
      planType: activeSub.plan_type,
      note: 'Cancellation is scheduled at period end (benefits stay active until expiry).',
    });

    return res.json({
      ok: true,
      message:
        'Your subscription will cancel at the end of the current billing period. You keep your current plan benefits until it expires.',
      currentPeriodEnd: activeSub.current_period_end || null,
    });
  } catch (err) {
    console.error('account/cancel error:', err);
    return res.status(500).json({
      error: 'Failed to cancel subscription',
      details: err.message || String(err),
    });
  }
});

app.post('/api/account/billing-action', requireUser, async (req, res) => {
  try {
    const { planType: rawPlanType, billingInterval: rawBillingInterval } = req.body || {};
    const targetPlanType = String(rawPlanType || '').trim();
    if (!targetPlanType || !PLAN_PIN_LIMITS[targetPlanType]) {
      return res.status(400).json({ error: 'Invalid planType' });
    }
    const targetInterval = normalizeBillingInterval(rawBillingInterval);
    const authedEmail = normalizeEmail(req.user?.email);
    let consolidated = null;
    if (authedEmail) {
      consolidated = await tryConsolidateDuplicateEmailSubscription(req.user.id, authedEmail);
      if (consolidated.ok) {
        console.log('billing-action: consolidated duplicate-email subscription', {
          userId: req.user.id,
          fromUserId: consolidated.otherUserId,
        });
      }
    }
    const decision = await resolveBillingUpgradeAction(req.user.id, targetPlanType, targetInterval);
    if (decision.action === 'invalid_plan') {
      return res.status(400).json({ error: decision.error || 'Invalid planType' });
    }
    return res.json({
      ok: true,
      ...(consolidated?.ok ? { consolidated: true, consolidatedPlanType: consolidated.subscription?.plan_type || null } : {}),
      ...decision,
    });
  } catch (err) {
    console.error('account/billing-action error:', err);
    return res.status(500).json({ error: 'Failed to resolve billing action', details: err.message || String(err) });
  }
});

app.post('/api/account/resume-subscription', requireUser, async (req, res) => {
  try {
    const { data: activeSub, error } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, plan_type, status, dodo_subscription_id, cancel_at_period_end')
      .eq('user_id', req.user.id)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: 'Failed to load active subscription' });
    }
    if (!activeSub) {
      return res.status(400).json({ error: 'No active subscription to resume.' });
    }
    const dodoSubId = String(activeSub.dodo_subscription_id || '').trim();
    if (!dodoSubId || !DODO_API_KEY) {
      return res.status(400).json({ error: 'No linked billing subscription found for this account.' });
    }

    await cancelScheduledDodoPlanChange(dodoSubId);
    const resume = await resumeDodoSubscriptionIfScheduledCancel(dodoSubId);
    if (!resume.ok) {
      return res.status(502).json({
        error: 'Could not resume subscription with the billing provider. Please contact support.',
        details: resume.json || null,
      });
    }
    await clearLocalCancelAtPeriodEnd(activeSub.id, req.user.id);
    return res.json({
      ok: true,
      message: 'Your subscription cancellation has been removed. You can change plans again from Pricing.',
    });
  } catch (err) {
    console.error('account/resume-subscription error:', err);
    return res.status(500).json({ error: 'Failed to resume subscription', details: err.message || String(err) });
  }
});

// Change subscription plan in-place (Dodo Change Plan API). Used for upgrades/downgrades on existing subscriptions.
app.post('/api/account/change-plan', requireUser, async (req, res) => {
  try {
    const { planType: rawPlanType, billingInterval: rawBillingInterval } = req.body || {};
    const targetPlanType = String(rawPlanType || '').trim();
    if (!targetPlanType || !PLAN_PIN_LIMITS[targetPlanType]) {
      return res.status(400).json({ error: 'Invalid planType' });
    }
    const targetInterval = normalizeBillingInterval(rawBillingInterval);
    const nowIso = new Date().toISOString();

    const { data: activeSub, error: fetchErr } = await supabaseAdmin
      .from('billing_subscriptions')
      .select('id, plan_type, status, current_period_end, cancel_at_period_end, dodo_subscription_id')
      .eq('user_id', req.user.id)
      .eq('status', 'active')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (fetchErr) {
      console.error('account/change-plan fetch active subscription error:', fetchErr);
      return res.status(500).json({ error: 'Failed to load active subscription' });
    }
    if (!activeSub?.dodo_subscription_id) {
      return res.status(400).json({ error: 'No active Dodo subscription found for this account.' });
    }
    if (!DODO_API_KEY) {
      return res.status(500).json({ error: 'Dodo Payments API key not configured' });
    }

    // Retrieve current subscription details from Dodo so we can infer current interval from product_id.
    const dodoSubId = String(activeSub.dodo_subscription_id).trim();
    const ready = await ensureDodoSubscriptionReadyForPlanChange(dodoSubId, req.user.id, activeSub);
    if (!ready.ok) {
      const clientStatus = ready.code === 'inactive_subscription' ? 409 : ready.code === 'missing_dodo_subscription' ? 400 : 502;
      return res.status(clientStatus).json({
        error: ready.message,
        code: ready.code || null,
        provider_status: ready.provider_status || null,
      });
    }
    const getJson = ready.dodoJson || {};
    const currentProductId = String(getJson?.product_id || getJson?.productId || '').trim();
    const currentInterval = inferBillingIntervalFromDodoProductId(currentProductId);
    const currentPlanType = String(activeSub.plan_type || '').trim();

    // Determine upgrade/downgrade and apply your policy.
    const curRank = planRank(currentPlanType);
    const tgtRank = planRank(targetPlanType);
    const isDowngrade =
      (tgtRank > 0 && curRank > 0 && tgtRank < curRank) ||
      (currentInterval === 'year' && targetInterval === 'month' && tgtRank === curRank);

    // Select proration + timing policy:
    // - monthly -> higher monthly: full_immediately (charge full new plan and reset month)
    // - monthly -> annual: full_immediately
    // - annual -> higher annual: prorated_immediately
    // - downgrades: next_billing_date + do_not_bill (keep benefits until renewal)
    let effective_at = 'immediately';
    let proration_billing_mode = 'prorated_immediately';

    if (isDowngrade) {
      effective_at = 'next_billing_date';
      proration_billing_mode = 'do_not_bill';
    } else if (currentInterval === 'month' && targetInterval === 'month' && tgtRank > curRank) {
      proration_billing_mode = 'full_immediately';
    } else if (currentInterval === 'month' && targetInterval === 'year') {
      proration_billing_mode = 'full_immediately';
    } else if (currentInterval === 'year' && targetInterval === 'year' && tgtRank > curRank) {
      proration_billing_mode = 'prorated_immediately';
    } else if (currentInterval === targetInterval && tgtRank === curRank) {
      // no-op plan change
      return res.json({ ok: true, noop: true, message: 'Already on this plan.' });
    } else {
      // Default for other upgrade-like moves: immediate proration
      proration_billing_mode = 'prorated_immediately';
    }

    const targetProductId = resolveDodoProductIdForPlan(targetPlanType, targetInterval);
    if (!targetProductId) {
      return res.status(400).json({
        error: `No Dodo product configured for planType "${targetPlanType}" (${targetInterval})`,
      });
    }

    const changeBody = {
      product_id: targetProductId,
      quantity: 1,
      proration_billing_mode,
      effective_at,
      on_payment_failure: 'prevent_change',
      metadata: {
        supabase_user_id: req.user.id,
        app_plan_type: targetPlanType,
        app_billing_interval: targetInterval,
        app_change_requested_at: nowIso,
      },
    };

    const changeResp = await fetch(
      `${DODO_BASE_URL}/subscriptions/${encodeURIComponent(dodoSubId)}/change-plan`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${DODO_API_KEY}`,
        },
        body: JSON.stringify(changeBody),
      }
    );
    const changeJson = await changeResp.json().catch(() => ({}));
    if (!changeResp.ok) {
      const status = changeResp.status;
      console.warn('account/change-plan: Dodo change-plan failed', {
        status,
        subId: dodoSubId,
        body: changeBody,
        details: changeJson,
      });
      const userError = formatDodoChangePlanUserError(status, changeJson);
      const clientStatus = status === 409 || changeJson?.code === 'INACTIVE_SUBSCRIPTION_PLAN_CHANGE_NOT_SUPPORTED' ? 409 : 502;
      return res.status(clientStatus).json({
        error: userError,
        code: changeJson?.code || null,
        details: changeJson,
      });
    }

    return res.json({
      ok: true,
      subscription_id: dodoSubId,
      current: { planType: currentPlanType, interval: currentInterval, product_id: currentProductId || null },
      target: { planType: targetPlanType, interval: targetInterval, product_id: targetProductId },
      effective_at,
      proration_billing_mode,
      provider: changeJson,
      note: 'Plan change requested. Webhooks will finalize state.',
    });
  } catch (err) {
    console.error('account/change-plan error:', err);
    return res.status(500).json({ error: 'Failed to change plan', details: err.message || String(err) });
  }
});

// Dodo webhook: primary source of subscription truth.
app.post('/api/dodo/webhook', async (req, res) => {
  try {
    const event = req.body || {};
    const eventType = String(event.type || event.event_type || event.name || '').toLowerCase();
    const dataObj = event?.data?.object || event?.data || event?.payload || {};
    const metadata = dataObj?.metadata || event?.metadata || {};
    const userIdFromMeta = String(metadata?.supabase_user_id || '').trim();
    const planTypeFromMeta = String(metadata?.app_plan_type || metadata?.plan_type || '').trim();
    const billingIntervalFromMeta = normalizeBillingInterval(metadata?.app_billing_interval || metadata?.billing_interval || 'month');
    const dodoSubId = extractDodoSubscriptionIdForWebhook(eventType, dataObj);
    const periodStartRaw =
      dataObj?.current_period_start ||
      dataObj?.current_period_started_at ||
      dataObj?.period_start ||
      dataObj?.starts_at ||
      null;
    const periodEndRaw =
      dataObj?.current_period_end ||
      dataObj?.current_period_ends_at ||
      dataObj?.period_end ||
      dataObj?.ends_at ||
      dataObj?.next_billing_date ||
      null;

    if (!eventType) return res.status(400).json({ error: 'Missing event type' });
    // See Dodo subscription guide: rely on payment.succeeded + subscription.active / renewed — not checkout.* or subscription.created.
    const activateSignals = [
      'payment.succeeded',
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
    const cancelAtPeriodEndSignals = [
      'subscription.updated',
      'subscription.update',
    ];

    if (activateSignals.some((sig) => eventType.includes(sig))) {
      if (!dodoWebhookEventConfirmsPaidSubscription(eventType, dataObj)) {
        console.warn('dodo webhook activation skipped: event does not confirm paid/active subscription', {
          eventType,
          status: dataObj?.status || null,
        });
        return res.json({ ok: true, ignored: true, reason: 'not_paid_or_not_active', eventType });
      }
      let userId = userIdFromMeta;
      if (!userId && dodoSubId) {
        userId = await lookupUserIdByDodoSubscriptionId(dodoSubId);
      }
      if (!userId) {
        console.warn('dodo webhook activation ignored: missing user id (metadata + lookup failed)', { eventType, dodoSubId });
        return res.json({ ok: true, ignored: true, reason: 'missing_user_metadata' });
      }

      let planType = planTypeFromMeta;
      let billingInterval = billingIntervalFromMeta;
      if (!planType || !PLAN_PIN_LIMITS[planType]) {
        const local = dodoSubId ? await lookupLocalSubscriptionByDodoSubscriptionId(dodoSubId) : null;
        const localPlan = String(local?.plan_type || '').trim();
        if (localPlan && PLAN_PIN_LIMITS[localPlan]) {
          planType = localPlan;
        }
        const localInterval = normalizeBillingInterval(local?.billing_interval || '');
        if (localInterval) billingInterval = localInterval;
      }
      if (!planType || !PLAN_PIN_LIMITS[planType]) {
        console.warn('dodo webhook activation ignored: missing/invalid plan (metadata + local lookup failed)', {
          eventType,
          userId,
          dodoSubId,
          planTypeFromMeta,
        });
        return res.json({ ok: true, ignored: true, reason: 'missing_plan_metadata' });
      }

      const referralSlugFromMeta = normalizeAffiliateSlug(metadata?.referral_key);
      if (referralSlugFromMeta) {
        await attachAffiliateReferralToUser(userId, referralSlugFromMeta);
      }

      const affiliateCommissionOpts = {
        referralSlugFromMetadata: referralSlugFromMeta,
        billingInterval,
        amountCents: extractDodoPaymentAmountCents(dataObj),
        paymentId: extractDodoPaymentId(dataObj),
        dodoSubscriptionId: dodoSubId,
        currency: dataObj?.currency || metadata?.currency || 'usd',
      };

      // Idempotency: if this exact Dodo subscription is already active for this user+plan, just backfill fields.
      if (dodoSubId) {
        const { data: existing } = await supabaseAdmin
          .from('billing_subscriptions')
          .select('id, user_id, status, plan_type, dodo_subscription_id')
          .eq('dodo_subscription_id', dodoSubId)
          .order('created_at', { ascending: false })
          .limit(1)
          .maybeSingle();
        if (existing && String(existing.user_id) !== String(userId)) {
          console.warn('dodo webhook activation blocked: subscription already linked to another user', {
            eventType,
            userId,
            existingUserId: existing.user_id,
            dodoSubId,
          });
          return res.json({ ok: true, ignored: true, reason: 'subscription_already_linked_to_another_user' });
        }
        if (existing && existing.user_id === userId && existing.status === 'active' && existing.plan_type === planType) {
          await supabaseAdmin
            .from('billing_subscriptions')
            .update({
              ...(periodStartRaw ? { current_period_start: periodStartRaw } : {}),
              ...(periodEndRaw ? { current_period_end: periodEndRaw } : {}),
              billing_interval: billingInterval,
              updated_at: new Date().toISOString(),
            })
            .eq('id', existing.id)
            .eq('user_id', userId);
          if (
            eventType.includes('payment.succeeded') ||
            eventType.includes('subscription.renewed')
          ) {
            await recordAffiliateCommissionOnPaidSubscription(userId, planType, {
              ...affiliateCommissionOpts,
              commissionKind: eventType.includes('subscription.renewed') ? 'renewal' : 'subscription',
            });
          }
          return res.json({ ok: true, action: 'noop_already_active', userId, planType });
        }
        // Failed renewal (past_due) then successful charge — reactivate same row, do not insert duplicate.
        if (existing && existing.user_id === userId && existing.status === 'past_due' && dodoSubId) {
          const nowIso = new Date().toISOString();
          await supabaseAdmin
            .from('billing_subscriptions')
            .update({
              status: 'active',
              plan_type: planType,
              pins_limit_per_month: PLAN_PIN_LIMITS[planType],
              billing_interval: billingInterval,
              ...(periodStartRaw ? { current_period_start: periodStartRaw } : {}),
              ...(periodEndRaw ? { current_period_end: periodEndRaw } : {}),
              updated_at: nowIso,
            })
            .eq('id', existing.id)
            .eq('user_id', userId);
          await supabaseAdmin.from('profiles').upsert(
            {
              id: userId,
              plan_type: planType,
              is_pro: planType !== 'free',
              updated_at: nowIso,
            },
            { onConflict: 'id' }
          );
          return res.json({ ok: true, action: 'reactivated_from_past_due', userId, planType });
        }
      }

      const activated = await applyPlanActivationForUser(userId, planType, `dodo_webhook:${eventType}`, {
          periodStart: periodStartRaw || null,
          periodEnd: periodEndRaw || null,
          dodoSubscriptionId: dodoSubId,
          billingInterval,
        });
      if (!activated.ok) {
        console.error('dodo webhook activation failed', { eventType, userId, planType, error: activated.error });
        return res.status(500).json({ error: 'Failed to activate plan from webhook' });
      }
      if (
        eventType.includes('payment.succeeded') ||
        eventType.includes('subscription.activated') ||
        eventType.includes('subscription.active') ||
        eventType.includes('subscription.renewed')
      ) {
        await recordAffiliateCommissionOnPaidSubscription(userId, planType, {
          ...affiliateCommissionOpts,
          commissionKind: eventType.includes('subscription.renewed') ? 'renewal' : 'subscription',
        });
      }
      return res.json({ ok: true, action: 'activated', userId, planType });
    }

    // Declined card / insufficient funds on a subscription charge — Dodo: payment.failed, subscription.on_hold, etc.
    if (eventType.includes('payment.failed')) {
      const subId = dodoSubId;
      if (!subId) {
        return res.json({ ok: true, ignored: true, reason: 'payment_failed_no_subscription' });
      }
      let uid = userIdFromMeta;
      if (!uid) {
        uid = await lookupUserIdByDodoSubscriptionId(subId);
      }
      if (!uid) {
        return res.json({ ok: true, ignored: true, reason: 'payment_failed_unknown_subscription' });
      }
      await markBillingSubscriptionPastDueAndDowngradeProfile(uid, subId, `dodo_webhook:${eventType}`);
      await triggerDunningEmail({
        userId: uid,
        dodoSubId: subId,
        planType: planTypeFromMeta,
        emailHint: dataObj?.customer?.email || dataObj?.customer_email || metadata?.email,
        logReason: `dodo_webhook:${eventType}`,
      });
      return res.json({ ok: true, action: 'marked_past_due_payment_failed', userId: uid });
    }

    if (eventType.includes('subscription.on_hold') || eventType.includes('subscription.failed')) {
      let uid = userIdFromMeta;
      if (!uid && dodoSubId) {
        uid = await lookupUserIdByDodoSubscriptionId(dodoSubId);
      }
      if (!uid || !dodoSubId) {
        return res.json({ ok: true, ignored: true, reason: 'subscription_hold_unknown' });
      }
      await markBillingSubscriptionPastDueAndDowngradeProfile(uid, dodoSubId, `dodo_webhook:${eventType}`);
      await triggerDunningEmail({
        userId: uid,
        dodoSubId,
        planType: planTypeFromMeta,
        emailHint: dataObj?.customer?.email || dataObj?.customer_email || metadata?.email,
        logReason: `dodo_webhook:${eventType}`,
      });
      return res.json({ ok: true, action: 'marked_past_due_subscription', userId: uid });
    }

    // Some providers send "subscription.updated" when a customer schedules cancellation at period end,
    // or when status becomes on_hold / failed after a failed renewal.
    if (cancelAtPeriodEndSignals.some((sig) => eventType.includes(sig))) {
      const st = String(dataObj?.status || '').toLowerCase();
      if (st === 'on_hold' || st === 'failed' || st === 'past_due') {
        let uid = userIdFromMeta;
        if (!uid && dodoSubId) {
          uid = await lookupUserIdByDodoSubscriptionId(dodoSubId);
        }
        if (uid && dodoSubId) {
          await markBillingSubscriptionPastDueAndDowngradeProfile(uid, dodoSubId, `subscription.updated:status=${st}`);
          await triggerDunningEmail({
            userId: uid,
            dodoSubId,
            planType: planTypeFromMeta,
            emailHint: dataObj?.customer?.email || dataObj?.customer_email || metadata?.email,
            logReason: `subscription.updated:status=${st}`,
          });
          return res.json({ ok: true, action: 'marked_past_due_status', userId: uid });
        }
      }
      if (dataObj?.cancel_at_next_billing_date === false || dataObj?.cancel_at_period_end === false) {
        let uid = userIdFromMeta;
        if (!uid && dodoSubId) {
          uid = await lookupUserIdByDodoSubscriptionId(dodoSubId);
        }
        if (uid) {
          const { error: clearErr } = await updateBillingSubscriptionsByTarget(
            { cancel_at_period_end: false, cancelled_at: null },
            { dodoSubId, userId: uid, activeOnly: true }
          );
          if (clearErr) {
            console.warn('dodo webhook cancel_cleared update error', {
              eventType,
              userId: uid,
              dodoSubId,
              error: clearErr.message || clearErr,
            });
          }
          return res.json({ ok: true, action: 'cancel_cleared', userId: uid });
        }
      }
      if (dataObj?.cancel_at_next_billing_date === true || dataObj?.cancel_at_period_end === true) {
        let uid = userIdFromMeta;
        if (!uid && dodoSubId) uid = await lookupUserIdByDodoSubscriptionId(dodoSubId);
        if (!uid) {
          console.warn('dodo webhook cancel_scheduled ignored: missing user id', { eventType });
          return res.json({ ok: true, ignored: true, reason: 'missing_user_metadata' });
        }
        const { error: cancelScheduledErr } = await updateBillingSubscriptionsByTarget(
          {
            cancel_at_period_end: true,
            cancelled_at: new Date().toISOString(),
            ...(dodoSubId ? { dodo_subscription_id: dodoSubId } : {}),
            ...(periodEndRaw ? { current_period_end: periodEndRaw } : {}),
          },
          { dodoSubId, userId: uid, activeOnly: true }
        );
        if (cancelScheduledErr) {
          console.warn('dodo webhook cancel_scheduled update error', {
            eventType,
            userId: uid,
            dodoSubId,
            error: cancelScheduledErr.message || cancelScheduledErr,
          });
          return res.status(500).json({ error: 'Failed to record scheduled cancellation' });
        }
        return res.json({ ok: true, action: 'cancel_scheduled', userId: uid });
      }
    }

    if (cancelSignals.some((sig) => eventType.includes(sig))) {
      let uidForCancel = userIdFromMeta;
      if (!uidForCancel && dodoSubId) uidForCancel = await lookupUserIdByDodoSubscriptionId(dodoSubId);
      const { error: cancelErr } = await updateBillingSubscriptionsByTarget(
        { status: 'cancelled' },
        { dodoSubId, userId: uidForCancel, activeOnly: true }
      );
      if (cancelErr) {
        console.warn('dodo webhook cancel update error', {
          eventType,
          userId: uidForCancel,
          dodoSubId,
          error: cancelErr.message || cancelErr,
        });
        return res.status(500).json({ error: 'Failed to record subscription cancellation' });
      }
      let uidForProfile = uidForCancel;
      if (!uidForProfile && dodoSubId) {
        uidForProfile = await lookupUserIdByDodoSubscriptionId(dodoSubId);
      }
      if (!uidForProfile) {
        return res.json({ ok: true, ignored: true, reason: 'cancel_missing_user' });
      }
      // Only downgrade profile if the user truly has no other active subscriptions left.
      const { data: stillActive } = await supabaseAdmin
        .from('billing_subscriptions')
        .select('id')
        .eq('user_id', uidForProfile)
        .eq('status', 'active')
        .limit(1);
      if (!stillActive || stillActive.length === 0) {
        await supabaseAdmin
          .from('profiles')
          .update({ plan_type: 'free', is_pro: false, updated_at: new Date().toISOString() })
          .eq('id', uidForProfile);
      }
      return res.json({ ok: true, action: 'cancelled', userId: uidForProfile });
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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
      // Prefer active subscription (billing_subscriptions is source-of-truth),
      // fallback to profile for legacy.
      const sub = await getActiveSubscriptionForUser(user.id);
      const { data: profile } = sub?.plan_type
        ? { data: null }
        : await supabaseAdmin
            .from('profiles')
            .select('plan_type')
            .eq('id', user.id)
            .single();
      const planType = sub?.plan_type || profile?.plan_type || 'free';
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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;
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

  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  if (accountId) {
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

  const { data: accounts } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, access_token, refresh_token, token_expires_at')
    .eq('user_id', userId)
    .not('access_token', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(1);
  if (accounts?.[0]) {
    const { accessToken } = await ensureValidPinterestAccessToken(accounts[0]);
    if (accessToken) return accessToken;
  }

  // Legacy single-token on profiles
  const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('pinterest_access_token')
    .eq('id', userId)
    .single();
  return profile?.pinterest_access_token || null;
}

/** Pinterest GET /v5/boards is paginated; first page alone misses boards when user has > page_size. */
async function fetchAllPinterestBoardPages(accessToken) {
  const collected = [];
  let bookmark = null;
  const maxPages = 40;
  for (let page = 0; page < maxPages; page++) {
    const url = new URL('https://api.pinterest.com/v5/boards');
    url.searchParams.set('page_size', '250');
    if (bookmark) url.searchParams.set('bookmark', String(bookmark));
    const pinterestRes = await fetch(url.toString(), {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    });
    const json = await pinterestRes.json().catch(() => ({}));
    if (!pinterestRes.ok) {
      console.warn('Pinterest GET /boards page failed', { status: pinterestRes.status, page, details: json });
      break;
    }
    const items = json.items || json.data || [];
    collected.push(...items);
    bookmark = json.bookmark ?? json.next_bookmark ?? null;
    if (!bookmark || items.length === 0) break;
  }
  return collected;
}

app.get('/api/pinterest/boards', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  const accountId = extractAccountId(req);
  const accessToken = await getPinterestAccessTokenForUser(user.id, accountId);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user.' });
  }

  const boards = await fetchAllPinterestBoardPages(accessToken);
  res.json({ boards });
});

// Suggest board name + description from pins about to be scheduled (scheduler Create Board modal)
app.post('/api/pinterest/suggest-board-copy', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  const { pins: rawPins, variation_seed: variationSeed } = req.body || {};
  const samples = normalizeSchedulerBoardPinSamples(rawPins);
  if (samples.length === 0) {
    return res.status(400).json({ error: 'Provide at least one pin with a title or description.' });
  }

  try {
    const heuristic = buildSchedulerBoardSuggestionHeuristic(samples, variationSeed);
    const ai = await maybeAiSchedulerBoardCopy(samples, variationSeed, openai);
    const name = (ai?.name || heuristic.name).trim();
    const description = (ai?.description || heuristic.description).trim();
    return res.json({
      name,
      description,
      topic: heuristic.topic || null,
      source: ai ? 'ai' : 'heuristic',
    });
  } catch (e) {
    console.error('suggest-board-copy error:', e);
    return res.status(500).json({ error: 'Failed to suggest board copy' });
  }
});

// Create a Pinterest board (name required; optional description, privacy PUBLIC/SECRET)
app.post('/api/pinterest/create-board', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  if (!(await enforcePaidSchedulingOrThrow(res, user.id, req))) return;

  const { image_url, title, description, board_id, link, account_id, alt_text: rawAltText } = req.body;
  if (!image_url || !title || !description || !board_id) {
    return res.status(400).json({ error: 'Missing required fields.' });
  }

  const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user/account.' });
  }

  try {
    const alt_text =
      String(rawAltText || '').trim() ||
      (await generatePinterestAltText(
        { title, description, overlayText: null, styleLabel: 'pinterest_create', linkDisplay: '' },
        openai
      ));
    const requestBody = {
      board_id,
      title,
      description,
      ...(alt_text ? { alt_text } : {}),
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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  if (!(await enforcePaidSchedulingOrThrow(res, user.id, req))) return;

  const { 
    image_url, title, description, board_id, link, account_id,
    scheduled_for, timezone = 'UTC', is_recurring = false, recurrence_pattern,
    force_duplicate = false,
    bake,
    alt_text: rawAltText,
    post_immediately = false
  } = req.body;

  const postImmediately = Boolean(post_immediately);

  // Validate required fields
  if ((!image_url && !bake) || !title || !description || !board_id) {
    return res.status(400).json({ error: 'Missing required fields: image_url (or bake), title, description, board_id' });
  }
  if (!postImmediately && !scheduled_for) {
    return res.status(400).json({ error: 'Missing required field: scheduled_for' });
  }

  // Validate scheduling time
  const now = new Date();
  const scheduleDate = postImmediately ? now : new Date(scheduled_for);
  if (!postImmediately && scheduleDate <= now) {
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

    // Generate alt text (or accept provided) for accessibility.
    const alt_text =
      String(rawAltText || '').trim() ||
      (await generatePinterestAltText(
        { title, description, overlayText: null, styleLabel: 'pinterest_schedule', linkDisplay: '' },
        openai
      ));

    // Store original pin data for reference
    const originalPinData = {
      image_url: finalImageUrl, title, description, board_id, link, account_id,
      ...(alt_text ? { alt_text } : {}),
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

    if (postImmediately) {
      try {
        await processScheduledPin(scheduledPin);
      } catch (postErr) {
        console.error('post_immediately process error:', postErr);
      }
      const { data: finalPin, error: finalErr } = await supabaseAdmin
        .from('scheduled_pins')
        .select('*')
        .eq('id', scheduledPin.id)
        .eq('user_id', user.id)
        .is('deleted_at', null)
        .single();
      const row = finalPin || scheduledPin;
      const posted = row.status === 'posted';
      const failed = row.status === 'failed' || row.status === 'error';
      if (finalErr && !posted) {
        return res.status(500).json({ error: 'Pin was queued but posting failed', scheduled_pin: row });
      }
      if (failed) {
        return res.status(400).json({
          error: row.last_error || 'Failed to post pin to Pinterest',
          scheduled_pin: row,
          posted: false,
        });
      }
      return res.json({
        success: true,
        posted,
        scheduled_pin: row,
        message: posted ? 'Pin posted to Pinterest' : `Pin queued (status: ${row.status})`,
      });
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

// Aggregate counts for scheduled-pins dashboard summary bar
app.get('/api/pinterest/scheduled-pins/summary', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  const { account_id } = req.query;

  const countForStatus = async (status, extraFilter) => {
    let q = supabaseAdmin
      .from('scheduled_pins')
      .select('id', { count: 'exact', head: true })
      .eq('user_id', user.id)
      .is('deleted_at', null)
      .eq('status', status);
    if (account_id && status !== 'generated') {
      q = q.eq('pinterest_account_id', account_id);
    }
    if (typeof extraFilter === 'function') {
      q = extraFilter(q);
    }
    const { count, error } = await q;
    if (error) throw error;
    return count ?? 0;
  };

  try {
    const startOfToday = new Date();
    startOfToday.setHours(0, 0, 0, 0);

    const [scheduled, posting, failed, generated, postedToday] = await Promise.all([
      countForStatus('scheduled'),
      countForStatus('posting'),
      countForStatus('failed'),
      countForStatus('generated'),
      countForStatus('posted', (q) => q.gte('posted_at', startOfToday.toISOString())),
    ]);

    return res.json({
      scheduled,
      posting,
      failed,
      posted_today: postedToday,
      generated,
    });
  } catch (error) {
    console.error('Error fetching scheduled pins summary:', error);
    return res.status(500).json({ error: 'Failed to fetch summary' });
  }
});

// Get user's scheduled pins
app.get('/api/pinterest/scheduled-pins', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  const { id } = req.params;
  const { 
    title, description, scheduled_for, timezone, status,
    is_recurring, recurrence_pattern 
  } = req.body;

  try {
    // If a free user attempts to schedule/unschedule via update, block scheduling-related updates.
    if (scheduled_for || status === 'scheduled') {
      if (!(await enforcePaidSchedulingOrThrow(res, user.id, req))) return;
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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

  if (!(await enforcePaidSchedulingOrThrow(res, user.id, req))) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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

app.get('/api/trends', async (req, res) => {
  try {
    res.setHeader('Cache-Control', 'private, no-store, max-age=0, must-revalidate');
    const syncDisk = String(req.query.sync || '').trim() === '1';
    const catalog = await getTrendsCatalog({ rereadDisk: syncDisk });
    const category = String(req.query.category || '').trim();
    const trends = Array.isArray(catalog?.trends) ? catalog.trends : [];
    const filtered = category ? trends.filter((t) => t.category === category) : trends;
    if (process.env.TRENDS_HTTP_LOG === '1' || (process.env.TRENDS_HTTP_LOG !== '0' && process.env.NODE_ENV !== 'production')) {
      console.log(
        '[trends-catalog]',
        catalog?.generatedAt || '(no ts)',
        `n=${filtered.length}`,
        syncDisk ? 'sync' : 'mem/disk'
      );
    }
    res.json({
      generatedAt: catalog?.generatedAt || null,
      season: catalog?.season || null,
      source: catalog?.source || 'automated',
      dataProviders: catalog?.dataProviders || [],
      stale: Boolean(catalog?.stale),
      trends: filtered,
    });
  } catch (e) {
    console.error('GET /api/trends failed:', e?.message || e);
    res.status(503).json({ error: 'trends_unavailable' });
  }
});

app.get('/api/trends/:slug', async (req, res) => {
  try {
    res.setHeader('Cache-Control', 'private, no-store, max-age=0, must-revalidate');
    const trend = await getTrendBySlug(req.params.slug);
    if (!trend) return res.status(404).json({ error: 'trend_not_found' });
    res.json({ trend });
  } catch (e) {
    console.error('GET /api/trends/:slug failed:', e?.message || e);
    res.status(503).json({ error: 'trends_unavailable' });
  }
});

app.post('/api/trends/refresh', async (req, res) => {
  const adminKey = String(process.env.TRENDS_ADMIN_KEY || '').trim();
  if (!adminKey) return res.status(404).json({ error: 'not_found' });
  const provided = String(req.headers['x-trends-admin-key'] || '').trim();
  if (!provided || provided !== adminKey) return res.status(401).json({ error: 'unauthorized' });
  try {
    const catalog = await getTrendsCatalog({ force: true });
    res.json({
      ok: true,
      generatedAt: catalog?.generatedAt || null,
      count: Array.isArray(catalog?.trends) ? catalog.trends.length : 0,
      source: catalog?.source || null,
      dataProviders: Array.isArray(catalog?.dataProviders) ? catalog.dataProviders : [],
      stale: Boolean(catalog?.stale),
    });
  } catch (e) {
    console.error('POST /api/trends/refresh failed:', e?.message || e);
    res.status(503).json({ error: 'trends_refresh_failed' });
  }
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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  const { data: { user: authUser }, error: userError } = await supabaseAuthGetUser(token);
  const user = respondSupabaseAuth(res, authUser, userError);
  if (!user) return;

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
  startRefImageCleanup();
  startTrendsScheduler();
  startOnboardingEmails();
}); 