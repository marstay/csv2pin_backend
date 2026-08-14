/**
 * Pure decision rules for posting a pin to Pinterest.
 *
 * Extracted from index.js so they can be tested without booting the server or
 * touching Supabase — these two rules decide whether a user's pin is accepted and
 * whether a failure is worth retrying, so getting them wrong loses real pins.
 */

/**
 * Coerce a user-supplied pin destination into a single usable URL.
 *
 * Accepts bare hostnames ("www.example.com") — that is a URL the user simply typed
 * without a scheme, and Pinterest accepts those. Accepts a whitespace-separated list of
 * URLs (some users paste several at once) by taking the first, but only when *every*
 * token is itself a URL, so a pasted product title is still rejected. localhost is
 * allowed so local development can post to a dev frontend.
 *
 * Deliberately does NOT reject shorteners/redirects the way
 * normalizeProductPageUrlOverride does: geni.us and amzn.to are the normal case here.
 *
 * @param {unknown} raw
 * @returns {string|null} normalized href, or null when it cannot be a URL
 */
export function coercePinDestinationUrl(raw) {
  const s = String(raw ?? '').trim();
  if (!s) return null;

  const one = (token) => {
    const candidate = /^https?:\/\//i.test(token) ? token : `https://${token}`;
    try {
      const u = new URL(candidate);
      if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
      if (!u.hostname.includes('.') && u.hostname !== 'localhost') return null;
      return u.href;
    } catch {
      return null;
    }
  };

  const tokens = s.split(/\s+/).filter(Boolean);
  const coerced = tokens.map(one);
  // Every token must be a URL — one stray word means this is prose, not a destination.
  if (coerced.some((c) => c === null)) return null;
  return coerced[0];
}

/**
 * @param {number} status
 * @param {{ message?: string, error?: string }|null|undefined} pinData
 */
export function pinterestResponseIsPermissionFailure(status, pinData) {
  if (status !== 403) return false;
  const msg = String(pinData?.message || pinData?.error || '').toLowerCase();
  return (
    msg.includes('not permitted') ||
    msg.includes('forbidden') ||
    msg.includes('permission') ||
    msg.includes('access that resource')
  );
}

/**
 * Decide whether a failed POST /v5/pins is worth retrying.
 *
 * Pinterest returns its generic "Sorry! Something went wrong on our end." text with a
 * 400, but it is a transient server-side failure, not a bad request. Treating it as a
 * validation error killed ~60% of all pin failures with zero retries. Confirmed against
 * production: when Pinterest returns this, no pin is created, so retrying cannot
 * duplicate anything.
 *
 * @param {number} status HTTP status from Pinterest
 * @param {{ message?: string, error?: string }|null|undefined} pinData parsed response body
 * @returns {{ retryable: boolean, isPermissionError: boolean, isValidationError: boolean }}
 */
export function classifyPinterestPostError(status, pinData) {
  const errorMessage = String(pinData?.message || pinData?.error || '');
  const isPermissionError = pinterestResponseIsPermissionFailure(status, pinData);
  const isGenericPinterestError = /something went wrong on our end/i.test(errorMessage);
  const isValidationError = (status === 400 || status === 404) && !isGenericPinterestError;

  return {
    isPermissionError,
    isValidationError,
    // Permission and validation errors are not fixed by retries or token refresh.
    retryable: !isPermissionError && !isValidationError,
  };
}

/**
 * Minutes to wait before retry #`attempt` of a failed pin post, per failure tier.
 *
 * Three ladders, because these failures recover on very different timescales:
 *
 * - `transient` Pinterest's generic "something went wrong on our end".
 *   classifyPinterestPostError already earns this a retry, but the old 5/15/45 ladder spent
 *   the whole budget inside ~65 minutes, so any wobble lasting longer than that killed the
 *   pin for good. Seen in production: a Creator user lost 3 of 5 pins to this in one morning
 *   while the same link and the same board posted fine either side of it. ~5h of cover
 *   instead, which is long enough to outlast a bad patch but still posts the same day.
 * - `spam` Pinterest is soft-blocking the link or throttling the account. Retrying quickly
 *   makes it worse, so these wait hours (unchanged).
 * - `standard` 429s, 5xx, network blips. These clear in minutes (unchanged).
 *
 * @type {{ transient: number[], spam: number[], standard: number[] }}
 */
export const PIN_RETRY_BACKOFF_MINUTES = {
  transient: [15, 60, 240],
  spam: [30, 90, 270],
  standard: [5, 15, 45],
};

/**
 * Pick the retry delay for a failed pin post.
 *
 * Tier is chosen from Pinterest's own wording. The generic-error test runs FIRST: that
 * message is the one case where a 400 still deserves a retry, and it must not fall through
 * to another ladder.
 *
 * @param {number} attempt 1-based retry number (1 = first retry). Clamped to the ladder.
 * @param {string|null|undefined} errorMessage Pinterest's message for this failure
 * @returns {{ tier: 'transient'|'spam'|'standard', minutes: number }}
 */
export function pinRetryBackoffMinutes(attempt, errorMessage) {
  const msg = String(errorMessage ?? '');
  const tier = /something went wrong on our end/i.test(msg)
    ? 'transient'
    : /spam|blocked|redirect/i.test(msg)
      ? 'spam'
      : 'standard';

  const ladder = PIN_RETRY_BACKOFF_MINUTES[tier];
  const n = Number.isFinite(attempt) ? Math.trunc(attempt) : 1;
  const index = Math.min(Math.max(n, 1), ladder.length) - 1;
  return { tier, minutes: ladder[index] };
}
