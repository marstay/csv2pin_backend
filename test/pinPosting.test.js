import test from 'node:test';
import assert from 'node:assert/strict';

import {
  coercePinDestinationUrl,
  classifyPinterestPostError,
  pinterestResponseIsPermissionFailure,
  pinRetryBackoffMinutes,
  PIN_RETRY_BACKOFF_MINUTES,
} from '../src/pinPosting.js';

/**
 * The strings below are real values taken from the scheduled_pins table, not invented
 * examples. Each rejection case cost a user a pin; each acceptance case is a link that
 * has actually posted to Pinterest, so tightening the rule must not break it.
 */

test('coercePinDestinationUrl — rejects a pasted product title', () => {
  const title =
    'Modern Loveseat Sofa with USB Type-C Ports, Linen Fabric 2-Seater Deep Seat Couch, ' +
    'Cozy Small Couch Sofa for Living Room Bedroom Office Apartment';
  assert.equal(coercePinDestinationUrl(title), null);
});

test('coercePinDestinationUrl — rejects prose whose first word looks like a host', () => {
  // "Amazon.com" alone would coerce; the trailing words prove this is not a URL.
  assert.equal(coercePinDestinationUrl('Amazon.com Echo Dot'), null);
});

test('coercePinDestinationUrl — adds a scheme to a bare hostname', () => {
  assert.equal(
    coercePinDestinationUrl('www.nataliaprotecaoveicular.com.br'),
    'https://www.nataliaprotecaoveicular.com.br/'
  );
});

test('coercePinDestinationUrl — leaves affiliate shorteners intact', () => {
  assert.equal(coercePinDestinationUrl('https://geni.us/dpqYhD'), 'https://geni.us/dpqYhD');
  assert.equal(coercePinDestinationUrl('https://amzn.to/45dutl6'), 'https://amzn.to/45dutl6');
});

test('coercePinDestinationUrl — allows localhost so local dev can generate pins', () => {
  assert.equal(
    coercePinDestinationUrl('http://localhost:3000/blog/why-your-pinterest-account-isnt-growing'),
    'http://localhost:3000/blog/why-your-pinterest-account-isnt-growing'
  );
});

test('coercePinDestinationUrl — takes the first of a whitespace-separated URL list', () => {
  // 11 already-posted pins store four Etsy URLs in one link field; rejecting them
  // outright would have broken a working flow.
  const many =
    'https://clickandprint3d.etsy.com/listing/4533139468    ' +
    'https://clickandprint3d.etsy.com/listing/4530711410    ' +
    'https://clickandprint3d.etsy.com/listing/4526496437';
  assert.equal(coercePinDestinationUrl(many), 'https://clickandprint3d.etsy.com/listing/4533139468');
});

test('coercePinDestinationUrl — trims surrounding whitespace', () => {
  assert.equal(
    coercePinDestinationUrl('  https://example.com/product  '),
    'https://example.com/product'
  );
});

test('coercePinDestinationUrl — normalizes host case but preserves path, query and fragment', () => {
  assert.equal(
    coercePinDestinationUrl('Https://FindsThatMatter.com/#bedroom'),
    'https://findsthatmatter.com/#bedroom'
  );
  assert.equal(
    coercePinDestinationUrl('https://example.com/Path?a=1&b=Two#Frag'),
    'https://example.com/Path?a=1&b=Two#Frag'
  );
});

test('coercePinDestinationUrl — percent-encodes non-ASCII paths', () => {
  assert.equal(
    coercePinDestinationUrl('https://www.amazon.com.au/RüK-Accessories'),
    'https://www.amazon.com.au/R%C3%BCK-Accessories'
  );
});

test('coercePinDestinationUrl — rejects non-http schemes', () => {
  assert.equal(coercePinDestinationUrl('javascript:alert(1)'), null);
  assert.equal(coercePinDestinationUrl('ftp://files.example.com/x'), null);
  assert.equal(coercePinDestinationUrl('data:text/html,<h1>x</h1>'), null);
});

test('coercePinDestinationUrl — rejects hosts without a dot', () => {
  assert.equal(coercePinDestinationUrl('just-a-word'), null);
  assert.equal(coercePinDestinationUrl('https://intranet/page'), null);
});

test('coercePinDestinationUrl — treats empty and non-string input as absent', () => {
  for (const empty of ['', '   ', null, undefined]) {
    assert.equal(coercePinDestinationUrl(empty), null);
  }
  assert.doesNotThrow(() => coercePinDestinationUrl({}));
  assert.doesNotThrow(() => coercePinDestinationUrl(42));
});

/**
 * Retry classification. Every message below is a verbatim Pinterest response body
 * observed in production.
 */

const GENERIC = 'Sorry! Something went wrong on our end.';

test('classifyPinterestPostError — generic Pinterest error is retryable despite a 400', () => {
  // The regression this rule exists for: 60% of all pin failures used to die here with
  // zero retries. Verified against Pinterest that no pin is created in this case.
  const result = classifyPinterestPostError(400, { message: GENERIC });
  assert.equal(result.retryable, true);
  assert.equal(result.isValidationError, false);
});

test('classifyPinterestPostError — generic error is retryable at any status', () => {
  for (const status of [400, 404, 500, 503]) {
    assert.equal(classifyPinterestPostError(status, { message: GENERIC }).retryable, true, `status ${status}`);
  }
});

test('classifyPinterestPostError — a malformed link is permanent', () => {
  const result = classifyPinterestPostError(400, {
    message: "Parameter 'link' with the value 'Modern Loveseat Sofa' is not a valid URL format.",
  });
  assert.equal(result.retryable, false);
  assert.equal(result.isValidationError, true);
});

test('classifyPinterestPostError — an unfetchable image is permanent', () => {
  assert.equal(
    classifyPinterestPostError(400, { message: 'Sorry we could not fetch the image.' }).retryable,
    false
  );
});

test('classifyPinterestPostError — a spam-blocked link is permanent', () => {
  assert.equal(
    classifyPinterestPostError(400, {
      message: 'Sorry! We blocked this link because it may lead to spam.',
    }).retryable,
    false
  );
});

test('classifyPinterestPostError — a missing board is permanent', () => {
  assert.equal(classifyPinterestPostError(404, { message: 'Board not found.' }).retryable, false);
});

test('classifyPinterestPostError — a permission failure is permanent and flagged', () => {
  const result = classifyPinterestPostError(403, {
    message: 'You are not permitted to access that resource.',
  });
  assert.equal(result.retryable, false);
  assert.equal(result.isPermissionError, true);
});

test('classifyPinterestPostError — rate limits and server errors are retryable', () => {
  assert.equal(classifyPinterestPostError(429, { message: 'Too many requests' }).retryable, true);
  assert.equal(classifyPinterestPostError(500, { message: 'Internal error' }).retryable, true);
  assert.equal(classifyPinterestPostError(503, { message: 'Service unavailable' }).retryable, true);
});

test('classifyPinterestPostError — reads the error field when message is absent', () => {
  assert.equal(classifyPinterestPostError(400, { error: GENERIC }).retryable, true);
});

test('classifyPinterestPostError — an empty or missing body does not throw', () => {
  for (const body of [null, undefined, {}]) {
    assert.doesNotThrow(() => classifyPinterestPostError(400, body));
  }
  // An unexplained 400 stays permanent — only the known-generic text earns a retry.
  assert.equal(classifyPinterestPostError(400, null).retryable, false);
});

test('pinterestResponseIsPermissionFailure — only fires on 403 with permission wording', () => {
  assert.equal(pinterestResponseIsPermissionFailure(403, { message: 'Forbidden' }), true);
  assert.equal(pinterestResponseIsPermissionFailure(403, { message: GENERIC }), false);
  assert.equal(pinterestResponseIsPermissionFailure(400, { message: 'not permitted' }), false);
});

/**
 * Retry backoff. The regression these guard: the generic Pinterest error used to share the
 * 5/15/45 ladder, spending every retry inside ~65 minutes. A Creator user lost 3 of 5 pins
 * in one morning to a wobble that outlasted that window.
 */

const totalMinutes = (tier) => PIN_RETRY_BACKOFF_MINUTES[tier].reduce((a, b) => a + b, 0);

test('pinRetryBackoffMinutes — generic Pinterest error retries over hours, not one hour', () => {
  const delays = [1, 2, 3].map((n) => pinRetryBackoffMinutes(n, GENERIC));
  assert.deepEqual(delays.map((d) => d.tier), ['transient', 'transient', 'transient']);
  assert.deepEqual(delays.map((d) => d.minutes), [15, 60, 240]);

  // The point of the change: the budget must outlast the ~65min that lost Paul's pins.
  assert.ok(totalMinutes('transient') > 65 * 3, 'transient budget should span several hours');
});

test('pinRetryBackoffMinutes — the generic error outranks the spam wording test', () => {
  // "blocked"/"redirect" must not capture a message that is really the generic error.
  assert.equal(pinRetryBackoffMinutes(1, 'Sorry! Something went wrong on our end.').tier, 'transient');
});

test('pinRetryBackoffMinutes — spam wording keeps its long ladder', () => {
  const spam = 'Sorry! We blocked this link because it may lead to spam.';
  assert.deepEqual([1, 2, 3].map((n) => pinRetryBackoffMinutes(n, spam).minutes), [30, 90, 270]);
  assert.equal(pinRetryBackoffMinutes(1, 'too many redirects').tier, 'spam');
});

test('pinRetryBackoffMinutes — ordinary transient failures still retry fast', () => {
  for (const msg of ['Too many requests', 'Internal error', 'fetch failed']) {
    assert.equal(pinRetryBackoffMinutes(1, msg).tier, 'standard', msg);
  }
  assert.deepEqual([1, 2, 3].map((n) => pinRetryBackoffMinutes(n, 'Internal error').minutes), [5, 15, 45]);
});

test('pinRetryBackoffMinutes — every ladder grows monotonically', () => {
  for (const [tier, ladder] of Object.entries(PIN_RETRY_BACKOFF_MINUTES)) {
    for (let i = 1; i < ladder.length; i += 1) {
      assert.ok(ladder[i] > ladder[i - 1], `${tier} ladder must increase at step ${i}`);
    }
  }
});

test('pinRetryBackoffMinutes — out-of-range attempts clamp to the ladder ends', () => {
  assert.equal(pinRetryBackoffMinutes(0, GENERIC).minutes, 15);
  assert.equal(pinRetryBackoffMinutes(-3, GENERIC).minutes, 15);
  // maxRetries is 3, but a 4th attempt must not return undefined and produce an Invalid Date.
  assert.equal(pinRetryBackoffMinutes(99, GENERIC).minutes, 240);
});

test('pinRetryBackoffMinutes — a missing message does not throw', () => {
  // handlePinError's catch path passes error.message, which can be undefined.
  for (const msg of [null, undefined, '']) {
    assert.doesNotThrow(() => pinRetryBackoffMinutes(1, msg));
    assert.equal(pinRetryBackoffMinutes(1, msg).tier, 'standard');
  }
  assert.ok(Number.isFinite(pinRetryBackoffMinutes(1, undefined).minutes));
});
