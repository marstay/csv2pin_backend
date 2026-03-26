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
dotenv.config();

console.log('SUPABASE_URL:', process.env.SUPABASE_URL);
console.log('SUPABASE_SERVICE_ROLE_KEY:', process.env.SUPABASE_SERVICE_ROLE_KEY);

const supabaseAdmin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Dodo Payments config
const DODO_BASE_URL = (process.env.DODO_BASE_URL || 'https://test.dodopayments.com').replace(/\/$/, '');
const DODO_API_KEY = process.env.DODO_API_KEY || process.env.DODO_PAYMENTS_API_KEY || '';

// --- Plan & usage helpers (pin_usage / metadata_usage) ---

const PLAN_PIN_LIMITS = {
  free: 10,
  creator: 100,
  pro: 400,
  agency: 1000,
};

/** Monthly caps for “your photo + text overlay” pins (no image model). Separate from AI pin quota. */
const PLAN_USER_PHOTO_PIN_LIMITS = {
  free: 40,
  creator: 400,
  pro: 1600,
  agency: 4000,
};

const PLAN_METADATA_LIMITS = {
  free: 500,
  creator: 5000,
  pro: 20000,
  agency: 100000,
};

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

// Serialize pin consumption per user so concurrent requests (e.g. 2+ styles in URL→Pin)
// don't all read the same counters and only write one increment.
const pinUsageLocks = new Map();

function planAiPinsLimit(sub) {
  const planType = sub?.plan_type || 'free';
  if (typeof sub?.pins_limit_per_month === 'number' && sub.pins_limit_per_month > 0) {
    return sub.pins_limit_per_month;
  }
  return PLAN_PIN_LIMITS[planType] || PLAN_PIN_LIMITS.free;
}

function resolveUserPhotoPinLimitForPlan(sub) {
  const planType = sub?.plan_type || 'free';
  return PLAN_USER_PHOTO_PIN_LIMITS[planType] || PLAN_USER_PHOTO_PIN_LIMITS.free;
}

/**
 * @param {string} userId
 * @param {{ aiDelta?: number, userPhotoDelta?: number }} deltas — positive consume, negative refund
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

function extractMetaFromHtml(html, url) {
  let title = '';
  let description = '';
  try {
    const titleMatch = html.match(/<title[^>]*>([^<]*)<\/title>/i);
    if (titleMatch && titleMatch[1]) {
      title = titleMatch[1].trim();
    }
    const ogTitleMatch = html.match(/<meta[^>]+property=["']og:title["'][^>]*content=["']([^"']*)["'][^>]*>/i);
    if (ogTitleMatch && ogTitleMatch[1]) {
      title = ogTitleMatch[1].trim();
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
  try {
    const u = new URL(url);
    domain = u.hostname;
    const parts = (u.pathname || '').split('/').filter(Boolean);
    const last = parts[parts.length - 1] || '';
    keyword = last.replace(/[-_]/g, ' ').replace(/\.[a-zA-Z0-9]+$/, '').trim();
  } catch (_) {
    domain = '';
    keyword = '';
  }

  return { title, description, domain, keyword };
}

/**
 * Fetch full article HTML and build richer base metadata + summary.
 * Falls back gracefully to meta tags only if fetch or parsing fails.
 */
async function fetchArticleBaseAndSummary(url, clientArticleData) {
  let html = '';
  try {
    const resp = await fetch(url);
    if (resp.ok) {
      html = await resp.text();
    }
  } catch (e) {
    console.warn('fetchArticleBaseAndSummary fetch error:', e.message || e);
  }

  const metaFromHtml = extractMetaFromHtml(html || '', url);
  const base = {
    ...metaFromHtml,
    ...(clientArticleData || {}),
  };

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
    // Get pins that are due to be posted
    const { data: pinsToPost, error: fetchError } = await supabaseAdmin
      .from('scheduled_pins')
      .select(`
        *,
        pinterest_accounts(access_token)
      `)
      .in('status', ['scheduled', 'failed'])
      .lte('scheduled_for', new Date().toISOString())
      .or('next_retry_at.is.null,next_retry_at.lte.' + new Date().toISOString())
      .is('deleted_at', null)
      .order('scheduled_for', { ascending: true })
      .limit(10); // Process 10 pins at a time

    if (fetchError) {
      console.error('❌ Error fetching scheduled pins:', fetchError);
      return;
    }

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

    // Get access token
    const accessToken = pin.pinterest_accounts?.access_token;
    if (!accessToken) {
      await handlePinError(pin.id, 'No Pinterest access token found');
      return;
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
      const errorMessage = pinData.message || pinData.error || 'Pinterest API error';
      console.error(`❌ Pinterest API error for pin ${pin.id}:`, errorMessage);
      
      await handlePinError(pin.id, errorMessage, pin.retry_count || 0);
    }

  } catch (error) {
    console.error(`❌ Error processing pin ${pin.id}:`, error);
    await handlePinError(pin.id, error.message, pin.retry_count || 0);
  }
}

async function handlePinError(pinId, errorMessage, currentRetryCount = 0) {
  const maxRetries = 3;
  const nextRetryCount = currentRetryCount + 1;
  
  // Check if this is a spam-related error
  const isSpamError = errorMessage.toLowerCase().includes('spam') || 
                      errorMessage.toLowerCase().includes('blocked') ||
                      errorMessage.toLowerCase().includes('redirect');
  
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

async function generateImageWithNanoBanana(prompt, logLabel = '', options = {}) {
  if (!NANO_BANANA_API_URL || !NANO_BANANA_API_KEY) {
    console.warn('Nano Banana 2 API not configured (NANO_BANANA_API_URL / NANO_BANANA_API_KEY missing)');
    return null;
  }

  const imageInput = Array.isArray(options.imageInput)
    ? options.imageInput.filter((u) => u && String(u).trim()).map((u) => String(u).trim())
    : [];

  const baseUrl = NANO_BANANA_API_URL.replace(/\/$/, ''); // e.g. https://api.kie.ai/api/v1/jobs
  const maxRetries = 3; // Retry entire flow up to 3 times on transient failures
  const createTaskTimeoutMs = 25000;
  const recordInfoTimeoutMs = 15000;

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

      // 2) Poll recordInfo until success / fail / timeout
      const maxAttempts = 45; // ~67.5s (45 * 1.5s) for slow generations
      const delayMs = 1500;

      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        await new Promise((r) => setTimeout(r, delayMs));

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

        const state = infoJson.data.state;
        if (state === 'waiting' || state === 'queuing' || state === 'generating') {
          continue;
        }

        if (state === 'fail') {
          console.error('Nano Banana 2 generation failed:', infoJson.data.failCode, infoJson.data.failMsg);
          return null; // Don't retry on explicit API failure
        }

        if (state === 'success') {
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
const REALISTIC_PREFIX = 'Photorealistic, high-quality photograph. Natural lighting, lifelike imagery, professional photography style. ';

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
  const brandTail = brand?.brandName ? ` Use the brand name ${brand.brandName} subtly in the design.` : '';
  const nicheTail = niche && NICHE_VISUAL_HINTS[niche] ? ` ${NICHE_VISUAL_HINTS[niche]}` : '';
  const tail = nicheTail + brandTail;
  const useRealistic = !ILLUSTRATED_STYLES.has(styleId);
  const baseIntro = 'Vertical Pinterest pin 1000x1500 px. ' + (useRealistic ? REALISTIC_PREFIX : '');
  const numSteps = typeof stepCount === 'number' ? stepCount : (styleId === 'step_cards_3' || styleId === 'grid_3_images' ? 3 : styleId === 'grid_4_images' ? 4 : styleId === 'timeline_infographic' ? 5 : null);

  switch (styleId) {
    case 'before_after':
      return (
        baseIntro +
        `Split layout with a clear “Before” left half and “After” right half about "${keyword || topic}". ` +
        `Show the on-image main text "${headline}" across the top center. ` +
        `Label the left side "Before" and the right side "After" with short, readable labels. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the main text. ` : '') +
        `At the bottom, add small, readable source text "${source}".` +
        tail
      );
    case 'timeline_infographic': {
      const steps = numSteps || 5;
      const stepLabels = Array.from({ length: steps }, (_, i) => `Step ${i + 1}`).join(', ');
      return (
        baseIntro +
        `Clean vertical infographic timeline with exactly ${steps} steps explaining "${keyword || topic}". ` +
        `Label steps as: ${stepLabels}. ` +
        `At the top of the pin, place the main title text "${headline}". ` +
        (subheadline ? `Optionally place a short subheadline "${subheadline}" just below the title. ` : '') +
        `Each step box has a very short label, and the background stays simple and low-contrast so the text is readable. ` +
        `At the bottom, include small source text "${source}".` +
        tail
      );
    }
    case 'grid_4_images':
      return (
        baseIntro +
        `2×2 grid of four related photographs about "${keyword || topic}" with thin white gutters. ` +
        `Place the main headline "${headline}" in a banner at the top of the pin. ` +
        (subheadline ? `Optionally add a short subheadline "${subheadline}" under the headline. ` : '') +
        `At the very bottom, add small source text "${source}".` +
        tail
      );
    case 'offset_collage_3':
      return (
        baseIntro +
        `Asymmetrical collage: one large hero photograph on the left and two smaller stacked photographs on the right, all about "${keyword || topic}". ` +
        `Place the main text "${headline}" over a solid or semi-transparent area in the top-right region so it is very readable. ` +
        (subheadline ? `Add a short supporting line "${subheadline}" below the headline. ` : '') +
        `Include small source text "${source}" near the bottom edge.` +
        tail
      );
    case 'clean_appetizing':
      return (
        baseIntro +
        `Soft neutral background with subtle texture. Simple, clear focal object or photograph that represents "${keyword || topic}". Clean, modern blog-style design with plenty of white space. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'curiosity_shock':
      return (
        baseIntro +
        `Bold, dramatic, high-contrast image. Strong central subject that represents or relates to "${keyword || topic}" and creates shock and curiosity. The visual must be semantically relevant to the article topic—e.g. for tech/WordPress show a laptop, screen, or workspace; for food show ingredients or cooking. Dramatic lighting. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'money_saving':
      return (
        baseIntro +
        `Visual value motif: imagery suggesting saving time, money, or results. Clean layout with practical elements related to "${keyword || topic}". Bright but simple. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'minimal_typography':
      return (
        baseIntro +
        `Minimalist layout: pure white or light background, lots of whitespace. Single strong visual object representing "${keyword || topic}". High-contrast, typography-forward. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'question_style':
      return (
        baseIntro +
        `Image that visually represents a question or dilemma about "${keyword || topic}", with subtle question-mark elements or split choices. The scene or subject must relate to the article topic. Clean background. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'cozy_baking':
      return (
        baseIntro +
        `Lifestyle context scene: someone interacting with "${keyword || topic}" in an appropriate everyday setting. The setting must match the topic—e.g. for tech/WordPress/digital topics show a laptop, screen, or workspace; for food/recipes show a kitchen; for wellness show a calm home setting. Warm natural light. Warm, inviting, lifestyle photography. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'viral_curiosity':
      return (
        baseIntro +
        `Story-like composition: focused close-up of a hand holding something that represents "${keyword || topic}" (e.g. document, device, product). Slightly dramatic lighting. Mysterious, story-driven, personal experiment feel. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'clumpy_fix':
      return (
        baseIntro +
        `Practical, simple layout: light surface with a clear object representing "${keyword || topic}" and a small related element (tool, document). Minimal props, lots of breathing room. Non-dramatic. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'minimal_elegant':
      return (
        baseIntro +
        `Soft beige or light gray background. Elegant overhead shot of a single, simple object that is semantically relevant to "${keyword || topic}"—e.g. for tech/WordPress show a laptop, tablet, or document; for food show ingredients or a dish; for wellness show a journal or plant. Delicate shadows. Minimal, premium feel. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
        tail
      );
    case 'step_cards_3':
      return (
        baseIntro +
        `Three tall step cards stacked vertically, each with an icon or small image and short label (Step 1, Step 2, Step 3) for "${keyword || topic}". Simple flat or lightly textured background. ` +
        `At the top, place the main title text "${headline}". ` +
        (subheadline ? `Optionally place a short subheadline "${subheadline}" just below the title. ` : '') +
        `At the bottom, include small source text "${source}".` +
        tail
      );
    case 'grid_3_images':
      return (
        baseIntro +
        `Clean layout with a grid of three related photographs representing "${keyword || topic}" (e.g. three variations, steps, or examples). Thin white borders between panels. ` +
        `Place the main headline "${headline}" in a banner at the top. ` +
        (subheadline ? `Optionally add a short subheadline "${subheadline}" under the headline. ` : '') +
        `At the very bottom, add small source text "${source}".` +
        tail
      );
    case 'stacked_strips':
      return (
        baseIntro +
        `Three horizontal photograph strips stacked vertically on one side. Each strip shows a different scene or detail related to "${keyword || topic}". Clean editorial feel. ` +
        `Place the main text "${headline}" over a solid or semi-transparent area. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" below the headline. ` : '') +
        `Include small source text "${source}" at the bottom.` +
        tail
      );
    case 'circle_cluster_4':
      return (
        baseIntro +
        `Four circular photographs arranged around a central text area, each showing a different aspect of "${keyword || topic}". Clean light background. ` +
        `Place the main headline "${headline}" in the center. ` +
        (subheadline ? `Add a short subheadline "${subheadline}" below. ` : '') +
        `Add small source text "${source}" at the bottom of the pin.` +
        tail
      );
    default:
      return (
        baseIntro +
        `Eye-catching but not cluttered design about "${keyword || topic}". Use real-life photography or photorealistic imagery. ` +
        `Use the main on-image text "${headline}" as a bold, highly readable headline. ` +
        (subheadline ? `Optionally add a short subheadline "${subheadline}" under the headline. ` : '') +
        `Add small, readable source text "${source}" at the bottom of the pin.` +
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

// --- URL → Pin helper endpoints ---

app.post('/api/urltopin/scrape', async (req, res) => {
  try {
    const { url, enrich } = req.body || {};
    if (!url) return res.status(400).json({ error: 'Missing url' });

    const response = await fetch(url, { redirect: 'follow' });
    if (!response.ok) {
      return res.status(500).json({ error: `Failed to fetch URL: ${response.status}` });
    }
    const html = await response.text();
    const meta = extractMetaFromHtml(html, url);
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
    } = req.body || {};
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
    const useUserComposite =
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

    // Own-photo composites use a separate monthly cap (no image model). AI pins use pins_used.
    const pinsToGenerate = effectiveStyles.length;
    const aiPins = useUserComposite ? 0 : pinsToGenerate;
    const userPhotoPins = useUserComposite ? pinsToGenerate : 0;
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

    const { base, articleSummary } = req._fetchedArticle || await fetchArticleBaseAndSummary(url, articleData);
    const year = new Date().getFullYear();
    const domain = (base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

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

    const styleSpecificGuidance = {
      before_after:
        'Make the “Before” and “After” areas visually balanced and clearly separated. Avoid generic full-bleed photos; instead, clearly label each side and ensure the change from problem to solution is obvious even at a glance.',
      timeline_infographic:
        'The design must look like a clear vertical infographic timeline, not like a regular photo pin. Use simple shapes, lines and icons for each step, with short labels, and avoid busy photographic backgrounds that would make the steps hard to read.',
    };

    const styleSpecificSystemGuidance = {
      before_after:
        'When the requested style is a BEFORE/AFTER layout, you MUST describe two clearly separated halves labelled “Before” and “After”, showing the problem state vs the improved state for this topic. Do not describe a single generic background photo; focus instead on the contrast between the two labelled halves, while still keeping the on-image text large and readable.',
      timeline_infographic:
        'When the requested style is a TIMELINE INFOGRAPHIC, you MUST describe a vertical infographic timeline made of multiple clearly numbered or labelled steps (for example: Step 1, Step 2, Step 3...). Do not describe a photographic scene as the main background. Instead, use a simple flat or lightly textured background, vertical connectors or arrows, and boxes or circles for each step, with short labels that summarize each stage of the article. The steps themselves are the main visual focus.',
    };

    const useDummyImages = process.env.USE_DUMMY_IMAGES === 'true';

    for (let i = 0; i < effectiveStyles.length; i++) {
      const id = effectiveStyles[i];
      const strategicMeta = strategicMetadataByIndex[i];
      const baseStyleDescription = styleMeta[id] || 'High quality, scroll-stopping Pinterest pin background.';
      const nicheHint = niche && nicheVisualHints[niche] ? ` Niche-specific visual guidance: ${nicheVisualHints[niche]}` : '';
      const strategyHint = strategicMeta?.image_prompt_hint ? ` Strategy visual (must respect this): ${strategicMeta.image_prompt_hint}.` : '';
      const styleDescription = baseStyleDescription + nicheHint + strategyHint;
      if (useUserComposite) {
        stylePrompts.push({
          id,
          label: id,
          prompt: `[user_photo_composite] ${styleDescription}`,
          index: i,
        });
        continue;
      }
      try {
        const isTimeline = id === 'timeline_infographic';

        const systemPrompt =
          'You create detailed text-to-image prompts that generate full Pinterest pin images, including BOTH background visuals and on-image text. ' +
          'Each prompt must clearly state at the beginning: "Vertical Pinterest pin 1000x1500 px". ' +
          (isTimeline
            ? 'Describe a vertical infographic-style Pinterest pin where the main visual is a stack of clearly separated steps in a vertical timeline. The background must stay simple and low-contrast so the step boxes, connectors and labels are the main focus. Use very short on-image text: a short title and short labels for each step only. '
            : 'Describe a 9:16 Pinterest pin: eye-catching but not cluttered background, clear focal point, and bold, highly readable typography. Explicitly describe the main headline text, any subheadline, and small branding or source text that includes the website URL at the bottom of the pin (e.g. bottom center). ') +
          (brandPrimary || brandSecondary || brandAccent || brandName || brandLogoUrl
            ? 'Make sure the visual style and on-image text respect the provided brand colors, brand name and logo placement so that the pin looks fully on-brand. '
            : '') +
          (styleSpecificSystemGuidance[id] ? styleSpecificSystemGuidance[id] + ' ' : '') +
          'The entire design must remain readable on mobile screens and feel like a high-performing Pinterest pin. ' +
          'The prompt must be between 1 and 3 sentences, plain text only. Never write more than 3 sentences.';

        const userPromptBase =
          `Article title: ${topic}\n` +
          (base.description ? `Article description: ${base.description}\n` : '') +
          (articleSummary ? `Article key ideas (short summary): ${articleSummary.slice(0, 400)}\n` : '') +
          (keyIdeas && keyIdeas.length
            ? `Key ideas to highlight visually:\n- ${keyIdeas.join('\n- ')}\n`
            : '') +
          (keyword ? `Main keyword or ingredient: ${keyword}\n` : '') +
          `Domain / branding: ${domain}\n` +
          `Year/context: ${year}\n` +
          (brandName ? `Brand name: ${brandName}\n` : '') +
          (brandPrimary ? `Brand primary color: ${brandPrimary}\n` : '') +
          (brandSecondary ? `Brand secondary color: ${brandSecondary}\n` : '') +
          (brandAccent ? `Brand accent color: ${brandAccent}\n` : '') +
          (brandLogoUrl ? `Brand logo reference URL: ${brandLogoUrl}\n` : '') +
          `Desired visual style for the Pinterest pin: ${styleDescription}\n` +
          (styleSpecificGuidance[id] ? `Additional layout requirements: ${styleSpecificGuidance[id]}\n` : '') +
          '\n';

        const userPromptTimeline =
          'Write a single, self-contained text-to-image prompt for a complete Pinterest pin that looks like a vertical infographic timeline. ' +
          'Start the prompt with the exact phrase: "Vertical Pinterest pin 1000x1500 px". ' +
          'Describe a simple, low-contrast background plus the vertical stack of step boxes or circles, their connectors or arrows, and the very short labels for each step. ' +
          'Optionally mention a short title at the top and small source text with the website URL at the bottom, but avoid long headline paragraphs. ';

        const userPromptDefault =
          'Write a single, self-contained text-to-image prompt for a complete Pinterest pin (background plus on-image text). ' +
          'Start the prompt with the exact phrase: "Vertical Pinterest pin 1000x1500 px". ' +
          'Describe the background scene, the exact headline and subheadline text to show, their position on the pin, and how the typography should look. ' +
          'Also describe small source text at the bottom that displays the website URL (use the domain provided above). ';

        const userPromptBrandTail =
          brandPrimary || brandSecondary || brandAccent || brandName || brandLogoUrl
            ? 'In the prompt, explicitly mention using the brand colors, logo and name so the final image clearly matches this brand.'
            : '';

        const userPrompt =
          userPromptBase +
          (isTimeline ? userPromptTimeline : userPromptDefault) +
          userPromptBrandTail;

        const completion = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt },
          ],
          temperature: 0.8,
          max_tokens: 260,
        });

        let imagePrompt =
          completion.choices?.[0]?.message?.content?.trim() ||
          `Vertical Pinterest pin 1000x1500 px, ${styleDescription} about "${topic}". Main concept: ${keyword ||
            'blog article'}. No text in the image.`;

        // For certain layout-heavy styles, enforce a deterministic prompt shape with consistent step count
        const stepCount = strategicMeta?.step_count;
        const numSteps = typeof stepCount === 'number' ? stepCount : (id === 'step_cards_3' || id === 'grid_3_images' || id === 'stacked_strips' ? 3 : id === 'grid_4_images' || id === 'circle_cluster_4' ? 4 : id === 'timeline_infographic' ? 5 : 5);
        if (id === 'timeline_infographic') {
          const stepLabels = Array.from({ length: numSteps }, (_, i) => `Step ${i + 1}`).join(', ');
          imagePrompt =
            `Vertical Pinterest pin 1000x1500 px. Simple, light background with very low contrast so text and shapes are easy to read. ` +
            `A vertical infographic timeline runs from top to bottom with exactly ${numSteps} clearly separated steps in boxes or circles, connected by a thin line or arrows. ` +
            `Each step box has a very short label (a few words) that summarizes a stage of "${topic}", using icons or small illustrations instead of detailed photos. ` +
            `Label steps as: ${stepLabels}. ` +
            `At the very top, include a short title about "${topic}", and at the very bottom, small, readable source text that shows "${domain}".`;
        } else if (id === 'step_cards_3') {
          imagePrompt =
            `Vertical Pinterest pin 1000x1500 px. Three tall step cards stacked vertically, each labelled "Step 1", "Step 2", "Step 3" for "${topic}". ` +
            `Soft neutral background. Concise headline at top, small "${domain}" at bottom.`;
        } else if (id === 'grid_3_images') {
          imagePrompt =
            `Vertical Pinterest pin 1000x1500 px. Grid of exactly 3 related images for "${topic}". ` +
            `Short headline at top, small "${domain}" at bottom.`;
        } else if (id === 'grid_4_images') {
          imagePrompt =
            `Vertical Pinterest pin 1000x1500 px. 2×2 grid of exactly 4 related images for "${topic}". ` +
            `Short headline at top, small "${domain}" at bottom.`;
        }


        stylePrompts.push({
          id,
          label: id,
          prompt: imagePrompt,
          index: i,
        });
      } catch (promptErr) {
        console.warn('urltopin prompt generation error:', promptErr.message || promptErr);
        stylePrompts.push({
          id,
          label: id,
          prompt: `Vertical Pinterest pin 1000x1500 px. High quality, modern design about "${topic}". Domain text: ${domain}. Year: ${year}. Main concept keyword: ${keyword ||
            'blog article'}. No text baked into the image, only background photography and design.`,
          index: i,
        });
      }
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
        source: domain,
      };

      const imagePrompt = buildOverlayImagePrompt({
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

      const overlayText = {
        headline: onImageHeadline,
        subheadline: onImageSubheadline,
        source: domain,
      };

      let imageUrl = '';
      let userCompositeSourceUrl = null;
      const pinUserImageRaw =
        useUserComposite && userImageUrls.length
          ? userImageUrls[sp.index] ?? userImageUrls[sp.index % userImageUrls.length]
          : null;
      const pinUserImageUrl = pinUserImageRaw ? String(pinUserImageRaw).trim() : '';

      if (useDummyImages) {
        imageUrl = `https://via.placeholder.com/1000x1500.png?text=${encodeURIComponent('Dev Pin')}`;
      } else if (
        useUserComposite &&
        pinUserImageUrl &&
        isAllowedUserImageUrl(pinUserImageUrl, process.env.SUPABASE_URL)
      ) {
        userCompositeSourceUrl = pinUserImageUrl;
        try {
          const png = await buildUserPhotoPinBuffer(pinUserImageUrl, overlayText, brandForPrompt, req.body?.renderOptions || null);
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
          const nanoUrl = await generateImageWithNanoBanana(imagePrompt, sp.label);
          imageUrl = nanoUrl || '';
          if (!imageUrl) {
            console.warn('urltopin nano-banana first attempt returned no image (style:', sp.label, '), retrying once');
            const retryUrl = await generateImageWithNanoBanana(imagePrompt, sp.label);
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
        }),
        ...(!userCompositeSourceUrl &&
          !useUserComposite && { imageGenerationMode: 'ai' }),
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

// POST /api/urltopin/regenerate-metadata — regenerate only title or description (fast, no image)
app.post('/api/urltopin/regenerate-metadata', requireUser, async (req, res) => {
  try {
    const { url, articleData, styleId, type, currentTitle, currentDescription } = req.body || {};
    if (!url || !styleId || !type || (type !== 'title' && type !== 'description')) {
      return res.status(400).json({ error: 'Missing or invalid url, styleId, or type (must be "title" or "description")' });
    }
    const base = articleData || extractMetaFromHtml('', url);
    const domain = (base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
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
    const { url, styleId, overlayText, articleData, brand, userImageUrl, renderOptions } = req.body || {};
    if (!url || !styleId || !overlayText) {
      return res.status(400).json({ error: 'Missing url, styleId, or overlayText' });
    }

    const base = articleData || extractMetaFromHtml('', url);
    const year = new Date().getFullYear();
    const domain = (base.domain || '').replace(/^https?:\/\//, '') || 'example.com';
    const keyword = base.keyword || '';
    const topic = base.title || 'Does Brown Sugar Expire?';

    const imagePrompt = buildOverlayImagePrompt({
      styleId,
      topic,
      domain,
      keyword,
      year,
      overlayText,
      brand,
    });

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
        const png = await buildUserPhotoPinBuffer(trimmedUserImg, overlayText, brand, renderOptions || null);
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

    let imageUrl = '';
    try {
      imageUrl = await generateImageWithNanoBanana(imagePrompt, styleId);
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

    return res.json({ imageUrl, imagePrompt });
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

    const { planType } = req.body || {};
    if (!planType) {
      return res.status(400).json({ error: 'Missing planType' });
    }

    // Map internal plan types to Dodo product IDs via environment variables
    const productMap = {
      free: process.env.DODO_PRODUCT_FREE_ID,
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
      },
    // Redirect back to app after success/failure
    return_url: `${frontendBase.replace(/\/$/, '')}/payment-success?plan=${encodeURIComponent(planType)}`,
    };

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

// Simple endpoint to mark a plan as active for the current user
app.post('/api/account/activate-plan', requireUser, async (req, res) => {
  try {
    const { planType } = req.body || {};
    if (!planType || !PLAN_PIN_LIMITS[planType]) {
      return res.status(400).json({ error: 'Invalid planType' });
    }

    const now = new Date();
    const periodStart = now.toISOString();
    const nextMonth = new Date(now);
    nextMonth.setMonth(nextMonth.getMonth() + 1);
    const periodEnd = nextMonth.toISOString();

    const pinsLimit = PLAN_PIN_LIMITS[planType];

    // Mark any existing active subscriptions for this user as cancelled
    await supabaseAdmin
      .from('billing_subscriptions')
      .update({ status: 'cancelled', updated_at: now.toISOString() })
      .eq('user_id', req.user.id)
      .eq('status', 'active');

    // Insert new active subscription row
    const { error: insertError } = await supabaseAdmin
      .from('billing_subscriptions')
      .insert({
        user_id: req.user.id,
        plan_type: planType,
        pins_limit_per_month: pinsLimit,
        status: 'active',
        current_period_start: periodStart,
        current_period_end: periodEnd,
      });

    if (insertError) {
      console.error('activate-plan insert error:', insertError);
      return res.status(500).json({
        error: 'Failed to activate plan',
        details: insertError.message || String(insertError),
      });
    }

    return res.json({ ok: true, planType, pinsLimit });
  } catch (err) {
    console.error('activate-plan error:', err);
    return res.status(500).json({
      error: 'Failed to activate plan',
      details: err.message || String(err),
    });
  }
});

// CSV save endpoint for all registered users
app.post('/api/csv', async (req, res) => {
  // Save CSV logic here (not implemented)
  res.json({ message: 'CSV saved (available to all registered users)' });
});

// --- Pinterest OAuth2 Integration ---
// Redirect user to Pinterest OAuth2
app.get('/api/pinterest/login', (req, res) => {
  console.log('--- Pinterest OAuth Login Initiated ---');
  console.log('client_id:', process.env.PINTEREST_CLIENT_ID);
  console.log('redirect_uri:', process.env.PINTEREST_REDIRECT_URI);
  console.log('scope:', 'pins:write boards:read boards:write pins:read user_accounts:read');
  const params = new URLSearchParams({
    client_id: process.env.PINTEREST_CLIENT_ID,
    redirect_uri: process.env.PINTEREST_REDIRECT_URI,
    response_type: 'code',
    scope: 'pins:write boards:read boards:write pins:read user_accounts:read', // added user_accounts:read
    state: 'secureRandomState123', // TODO: Use a real random state for security
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


// Handle Pinterest OAuth2 callback
app.get('/api/pinterest/callback', async (req, res) => {
  const { code, state } = req.query;
  console.log('--- Pinterest OAuth Callback ---');
  console.log('Received code:', code);
  console.log('Received state:', state);
  if (!code) return res.status(400).send('Missing code');
  // Redirect to frontend with code for user association (use env FRONTEND_URL)
  const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:3000';
  res.redirect(`${FRONTEND_URL.replace(/\/$/, '')}/pinterest/finish?code=${code}`);
});

app.post('/api/pinterest/oauth', async (req, res) => {
  const { code, redirectUri } = req.body;
  const authHeader = req.headers.authorization;
  if (!code || !redirectUri) return res.status(400).json({ error: 'Missing code or redirectUri' });
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });
  // Log the values sent to Pinterest for debugging
  console.log({
    client_id: process.env.PINTEREST_CLIENT_ID,
    client_secret: process.env.PINTEREST_CLIENT_SECRET ? process.env.PINTEREST_CLIENT_SECRET.slice(0,3) + '...' + process.env.PINTEREST_CLIENT_SECRET.slice(-3) : undefined,
    redirect_uri: redirectUri,
    code
  });
  try {
    const tokenData = await exchangePinterestCodeForToken(code, redirectUri);
    if (tokenData.access_token) {
      // Enforce plan limits
      const { data: profile } = await supabaseAdmin
        .from('profiles')
        .select('plan_type')
        .eq('id', user.id)
        .single();
      const planType = (profile?.plan_type || 'free');
      const { data: existing } = await supabaseAdmin
        .from('pinterest_accounts')
        .select('id')
        .eq('user_id', user.id);
      const count = Array.isArray(existing) ? existing.length : 0;
      const planLimits = { free: 1, creator: 3, pro: Infinity, agency: Infinity };
      const limit = planLimits[planType] ?? 1;
      if (count >= limit) {
        return res.status(403).json({ error: `Plan limit reached. Your plan (${planType}) allows ${limit === Infinity ? 'unlimited' : limit} account(s).` });
      }

      // Fetch account info for labeling
      let accountName = '';
      try {
        const accRes = await fetch('https://api.pinterest.com/v5/user_account', {
          headers: { 'Authorization': `Bearer ${tokenData.access_token}`, 'Accept': 'application/json' }
        });
        const acc = await accRes.json();
        accountName = acc?.username || acc?.profile?.username || 'Pinterest Account';
      } catch (e) {
        console.warn('Failed to fetch Pinterest account info:', e?.message || e);
      }
      // Save token to pinterest_accounts
      const { error: insertError } = await supabaseAdmin
        .from('pinterest_accounts')
        .insert({ user_id: user.id, access_token: tokenData.access_token, account_name: accountName });
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
    .select('access_token')
    .eq('id', accountId)
    .eq('user_id', userId)
    .single();
  return account?.access_token || null;
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
      .order('scheduled_for', { ascending: true, nullsFirst: true })
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

  // Load accounts
  const { data: accounts, error: accError } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, account_name, access_token');
  const hasToken = Array.isArray(accounts) && accounts.some(a => !!a.access_token);
  let tokenValid = false;
  let pinterestError = null;

  if (hasToken) {
    try {
      const anyToken = accounts.find(a => a.access_token)?.access_token;
      const testRes = await fetch('https://api.pinterest.com/v5/user_account', {
        headers: {
          'Authorization': `Bearer ${anyToken}`,
          'Content-Type': 'application/json',
        },
      });
      if (testRes.ok) tokenValid = true; else pinterestError = await testRes.json();
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

    // Only allow permanent deletion of cancelled or posted pins
    if (!['cancelled', 'posted'].includes(existingPin.status)) {
      return res.status(400).json({ error: 'Can only permanently delete cancelled or posted pins' });
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