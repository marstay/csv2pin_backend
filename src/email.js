/**
 * Transactional email via Resend (REST API — no SDK dependency).
 *
 * Fails gracefully: if RESEND_API_KEY is not configured, sends become no-ops
 * (logged) so the app keeps working in dev / before the key is added.
 *
 * Required env:
 *   RESEND_API_KEY        Resend API key (re_...). Without it, emails are skipped.
 * Optional env:
 *   EMAIL_FROM            From header. Prefer a personal, monitored sender (NOT "no-reply"),
 *                         e.g. "Aristomenis from URL2Pin <hello@url2pin.com>". Use a mailbox
 *                         that exists and is read — this is the address recipients reply to.
 *                         Defaults to Resend's shared test sender for early testing.
 *   FRONTEND_URL          Base app URL (used to build recovery links).
 *   BILLING_RECOVERY_URL  Where dunning links point. Defaults to `${FRONTEND_URL}/my-account`
 *                         (the page with the "Update payment method" recovery flow).
 *   SUPPORT_EMAIL         Support address (also the default reply-to).
 *   REPLY_TO_EMAIL        Reply-to for all emails. Defaults to SUPPORT_EMAIL. Set so replies
 *                         always reach a real inbox — never send from an unmonitored "no-reply".
 */
import fetch from 'node-fetch';

const RESEND_API_KEY = String(process.env.RESEND_API_KEY || '').trim();
const EMAIL_FROM = String(process.env.EMAIL_FROM || 'URL2Pin <onboarding@resend.dev>').trim();
const FRONTEND_URL = String(process.env.FRONTEND_URL || 'https://url2pin.com').trim().replace(/\/$/, '');
const BILLING_RECOVERY_URL = String(process.env.BILLING_RECOVERY_URL || `${FRONTEND_URL}/my-account`).trim();
const SUPPORT_EMAIL = String(process.env.SUPPORT_EMAIL || 'support@url2pin.com').trim();
// Default reply-to so every email is replyable even if a send doesn't pass one.
// Avoid "no-reply" senders: replies build trust + are a positive inbox signal,
// and at this stage they're a valuable feedback/support channel.
const REPLY_TO_EMAIL = String(process.env.REPLY_TO_EMAIL || SUPPORT_EMAIL).trim();

// Upgrades / plan changes go through the Pricing page, which routes active
// subscribers to change-plan (and free users to checkout) automatically.
const PRICING_URL = String(process.env.PRICING_URL || `${FRONTEND_URL}/pricing`).trim();
const UPGRADE_URL = String(process.env.UPGRADE_URL || PRICING_URL).trim();
const APP_URL = String(process.env.APP_URL || FRONTEND_URL).trim().replace(/\/$/, '');

const BRAND = 'URL2Pin';
const ACCENT = '#1A237E';
// Personal founder voice: emails are signed by the founder and invite replies.
const FOUNDER_NAME = String(process.env.FOUNDER_NAME || 'Aristomenis').trim();
const PLAN_LABELS = { free: 'Free', starter: 'Starter', creator: 'Creator', pro: 'Pro', agency: 'Agency' };
// 2026 pricing. These are the prices a reader would actually be charged if they act on the email:
// both new checkouts and plan changes resolve to the CURRENT products, so a grandfathered $9
// Starter customer who upgrades to Creator pays $25, not the old $19. Never quote legacy prices
// in an outbound email — quote what the checkout will charge.
const PLAN_PRICES_USD = { free: 0, starter: 12, creator: 25, pro: 55, agency: 129 };
const PLAN_ANNUAL_PRICE_USD = { starter: 108, creator: 225, pro: 495, agency: 1161 };
// MUST MATCH PLAN_PIN_LIMITS in index.js — that table is what the app actually enforces; this
// copy only decides what outbound email QUOTES. If they drift, customers are promised an allowance
// they do not get (or vice versa). Raised 2026-08-16 alongside index.js.
const PLAN_AI_PIN_LIMITS = { free: 10, starter: 90, creator: 250, pro: 600, agency: 1300 };
const NEXT_PLAN = { free: 'starter', starter: 'creator', creator: 'pro', pro: 'agency', agency: null };

/** The plan to recommend upgrading to, or null if already on the top tier. */
export function nextPlanFor(planType) {
  return NEXT_PLAN[String(planType || '').toLowerCase()] ?? null;
}

export function isEmailEnabled() {
  return Boolean(RESEND_API_KEY);
}

/** Low-level send. Returns { ok, id? , skipped?, error? } and never throws. */
export async function sendEmail({ to, subject, html, replyTo } = {}) {
  const recipient = String(to || '').trim();
  if (!recipient) return { ok: false, skipped: true, reason: 'no_recipient' };
  if (!RESEND_API_KEY) {
    console.warn('email: RESEND_API_KEY not set — skipping send', { to: recipient, subject });
    return { ok: false, skipped: true, reason: 'no_api_key' };
  }
  try {
    const resp = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        from: EMAIL_FROM,
        to: [recipient],
        subject,
        html,
        ...((replyTo || REPLY_TO_EMAIL) ? { reply_to: String(replyTo || REPLY_TO_EMAIL).trim() } : {}),
      }),
    });
    const json = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      console.warn('email: Resend send failed', { status: resp.status, details: json, to: recipient, subject });
      return { ok: false, error: json?.message || `HTTP ${resp.status}` };
    }
    console.log('email: sent', { id: json?.id, to: recipient, subject });
    return { ok: true, id: json?.id };
  } catch (e) {
    console.warn('email: Resend send error', { error: e?.message || e, to: recipient, subject });
    return { ok: false, error: e?.message || String(e) };
  }
}

/** Shared responsive shell. Keep inline styles — many clients strip <style>. */
function emailLayout({ heading, bodyHtml, ctaText, ctaUrl, footerNote, ps, signoff = true }) {
  const cta = ctaText && ctaUrl
    ? `<tr><td style="padding:8px 0 24px;">
         <a href="${ctaUrl}" style="display:inline-block;background:${ACCENT};color:#ffffff;text-decoration:none;font-weight:600;font-size:15px;padding:12px 22px;border-radius:8px;">${ctaText}</a>
       </td></tr>`
    : '';
  const signature = signoff
    ? `<tr><td style="font-size:15px;line-height:1.6;color:#3a3a3a;padding:4px 0 0;">
         — ${FOUNDER_NAME}<br/>
         <span style="color:#8a8f98;font-size:13px;">Founder, ${BRAND}</span>
       </td></tr>`
    : '';
  const psBlock = ps
    ? `<tr><td style="font-size:13px;line-height:1.6;color:#6a6f78;padding:18px 0 0;">P.S. ${ps}</td></tr>`
    : '';
  return `<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#f4f5f7;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f4f5f7;padding:24px 0;">
      <tr><td align="center">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;background:#ffffff;border-radius:12px;border:1px solid #e6e7eb;overflow:hidden;">
          <tr><td style="padding:20px 28px;border-bottom:1px solid #eef0f3;">
            <span style="font-size:18px;font-weight:700;color:${ACCENT};letter-spacing:-0.2px;">${BRAND}</span>
          </td></tr>
          <tr><td style="padding:28px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
              <tr><td style="font-size:20px;font-weight:700;color:#1a1a1a;padding-bottom:12px;">${heading}</td></tr>
              <tr><td style="font-size:15px;line-height:1.6;color:#3a3a3a;padding-bottom:20px;">${bodyHtml}</td></tr>
              ${cta}
              ${signature}
              ${psBlock}
            </table>
          </td></tr>
          <tr><td style="padding:18px 28px;border-top:1px solid #eef0f3;font-size:12px;line-height:1.5;color:#8a8f98;">
            ${footerNote || ''}
            <div style="margin-top:8px;">Need help? Just reply to this email.</div>
          </td></tr>
        </table>
      </td></tr>
    </table>
  </body>
</html>`;
}

/** Build the "your payment failed" dunning email. */
export function renderPaymentFailedEmail({ planType, recoveryUrl } = {}) {
  const plan = PLAN_LABELS[String(planType || '').toLowerCase()] || '';
  const url = String(recoveryUrl || BILLING_RECOVERY_URL);
  const planPhrase = plan ? `your <strong>${plan}</strong> plan` : 'your plan';
  const subject = `Your ${BRAND} payment didn't go through — quick fix`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">We tried to charge your card for ${planPhrase} on ${BRAND}, but the payment didn't go through — usually an expired card, a temporary bank hold, or insufficient funds.</p>
    <p style="margin:0 0 14px;">Your account features are paused until the payment is updated. It only takes a minute to restore everything:</p>`;
  const html = emailLayout({
    heading: 'Your payment needs attention',
    bodyHtml,
    ctaText: 'Update payment & restore my plan',
    ctaUrl: url,
    ps: `If you think this is a mistake or you'd like a hand, just reply to this email — it comes straight to me and I'll sort it out personally.`,
    footerNote: `You're receiving this because a recent charge for your ${BRAND} subscription failed.`,
  });
  return { subject, html };
}

/** Send the dunning email. Returns the same shape as sendEmail. */
export async function sendPaymentFailedEmail({ to, planType, recoveryUrl } = {}) {
  const { subject, html } = renderPaymentFailedEmail({ planType, recoveryUrl });
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

/**
 * A FIRST payment that was declined — someone trying to subscribe, not an existing customer.
 *
 * Deliberately separate from renderPaymentFailedEmail: that one says "we tried to charge your card"
 * and "your account features are paused", which is alarming and untrue here. Nobody has been
 * charged and nothing has been taken away — they simply stayed on the free plan. Getting this
 * wrong costs a sale, because the reader concludes they have been billed and locked out.
 */
export function renderSignupPaymentFailedEmail({ planType, retryUrl } = {}) {
  const plan = PLAN_LABELS[String(planType || '').toLowerCase()] || '';
  const url = String(retryUrl || PRICING_URL);
  const planPhrase = plan ? `the <strong>${plan}</strong> plan` : 'a paid plan';
  const subject = `Your ${BRAND} payment didn't go through — nothing was charged`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">Your payment for ${planPhrase} on ${BRAND} didn't go through — your bank declined it, so <strong>nothing was charged</strong>.</p>
    <p style="margin:0 0 14px;">Banks often decline a first international payment without giving a reason. It usually works on a second attempt, or with a different card:</p>`;
  const html = emailLayout({
    heading: "Your payment didn't go through",
    bodyHtml,
    ctaText: 'Try again',
    ctaUrl: url,
    ps: `Your account and everything you've already made are untouched — you're simply still on the free plan. If it keeps failing, just reply to this email and I'll sort it out with you personally.`,
    footerNote: `You're receiving this because a payment you started for ${BRAND} was declined.`,
  });
  return { subject, html };
}

export async function sendSignupPaymentFailedEmail({ to, planType, retryUrl } = {}) {
  const { subject, html } = renderSignupPaymentFailedEmail({ planType, retryUrl });
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

/**
 * Build the "your Pinterest connection expired" email.
 *
 * A dead Pinterest token fails silently today: analytics stop updating AND scheduled pins stop
 * posting, while the dashboard still shows the account as connected. The customer only finds out
 * when they notice their pins never went live — which is a churn event you never see coming.
 */
export function renderPinterestReconnectEmail({ accountName } = {}) {
  const url = `${FRONTEND_URL}/my-account`;
  const who = String(accountName || '').trim();
  const whoPhrase = who ? ` for <strong>${who}</strong>` : '';
  const subject = `Reconnect your Pinterest account — pins aren't posting`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">Pinterest has stopped accepting our connection${whoPhrase}. This normally happens when the account password changed, access was revoked, or the connection simply expired.</p>
    <p style="margin:0 0 14px;">While it's disconnected, <strong>scheduled pins won't post and analytics won't update</strong>. Nothing is lost — your scheduled pins are safe and will resume once you reconnect.</p>
    <p style="margin:0 0 14px;">Reconnecting takes about 30 seconds:</p>`;
  const html = emailLayout({
    heading: 'Your Pinterest connection expired',
    bodyHtml,
    ctaText: 'Reconnect Pinterest',
    ctaUrl: url,
    ps: `If you reconnect and still see problems, just reply to this email — it comes straight to me.`,
    footerNote: `You're receiving this because ${BRAND} could not reach Pinterest with your saved connection.`,
  });
  return { subject, html };
}

/** Send the Pinterest reconnect email. Returns the same shape as sendEmail. */
export async function sendPinterestReconnectEmail({ to, accountName } = {}) {
  const { subject, html } = renderPinterestReconnectEmail({ accountName });
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

/**
 * Build the "you're out of / running low on pins — upgrade" expansion email.
 * `reason` is 'limit_reached' (sent when the user hits their monthly AI pin cap).
 */
export function renderUpgradeNudgeEmail({ currentPlan, used, limit, reason } = {}) {
  const cur = String(currentPlan || '').toLowerCase();
  const next = nextPlanFor(cur);
  if (!next) return null; // top tier — nothing to upsell

  const curLabel = PLAN_LABELS[cur] || 'your';
  const nextLabel = PLAN_LABELS[next] || next;
  const nextLimit = PLAN_AI_PIN_LIMITS[next];
  const nextPrice = PLAN_PRICES_USD[next];
  const multiple = limit > 0 ? Math.round((nextLimit / limit) * 10) / 10 : null;
  // Free users start a fresh checkout; paying users change plan in My Account.
  const url = cur === 'free' ? PRICING_URL : UPGRADE_URL;

  const atLimit = reason === 'limit_reached';
  const isFreeLifetime = cur === 'free';
  const heading = atLimit
    ? isFreeLifetime
      ? `You've used all your free AI pins`
      : `You're out of pins for this month`
    : isFreeLifetime
      ? `You're running low on free AI pins`
      : `You're running low on pins`;
  const subject = atLimit
    ? isFreeLifetime
      ? `You've hit your ${curLabel} AI pin limit — upgrade for more`
      : `You've hit your ${curLabel} pin limit — upgrade for more`
    : isFreeLifetime
      ? `You're almost out of free AI pins`
      : `You're almost out of pins this month`;

  const opening = atLimit
    ? isFreeLifetime
      ? `You've used all <strong>${limit}</strong> AI pins on your ${curLabel} plan (lifetime allowance). Nice work — that means it's driving real output for you.`
      : `You've used all <strong>${limit}</strong> AI pins on your ${curLabel} plan this month. Nice work — that means it's driving real output for you.`
    : isFreeLifetime
      ? `You've used <strong>${used}</strong> of your <strong>${limit}</strong> AI pins on your ${curLabel} plan — you're getting close to your lifetime limit.`
      : `You've used <strong>${used}</strong> of your <strong>${limit}</strong> AI pins this month on your ${curLabel} plan — you're getting close to the limit.`;

  const pitch = `Upgrade to <strong>${nextLabel}</strong> for <strong>${nextLimit} pins/month</strong>${multiple && multiple >= 1.5 ? ` (${multiple}× more)` : ''} at $${nextPrice}/mo. Your current usage carries over — you'll be able to keep creating right away.`;

  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">${opening}</p>
    <p style="margin:0 0 14px;">${pitch}</p>`;

  const html = emailLayout({
    heading,
    bodyHtml,
    ctaText: `Upgrade to ${nextLabel}`,
    ctaUrl: url,
    ps: `Not sure which plan fits your volume? Reply and tell me what you're working on — I'll point you to the right one.`,
    footerNote: `You're receiving this because you're an active ${curLabel} user on ${BRAND}.`,
  });
  return { subject, html };
}

/** Send the upgrade nudge. Returns sendEmail shape, or { skipped, reason:'top_tier' } if no upsell. */
export async function sendUpgradeNudgeEmail({ to, currentPlan, used, limit, reason } = {}) {
  const rendered = renderUpgradeNudgeEmail({ currentPlan, used, limit, reason });
  if (!rendered) return { ok: false, skipped: true, reason: 'top_tier' };
  return sendEmail({ to, subject: rendered.subject, html: rendered.html, replyTo: SUPPORT_EMAIL });
}

// --- Onboarding / activation lifecycle emails ---

/** Day 0: welcome + orientation. */
export function renderWelcomeEmail() {
  const subject = `Welcome to ${BRAND} 🎉`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Welcome aboard!</p>
    <p style="margin:0 0 14px;">${BRAND} turns a product page, Etsy listing, or blog post into <strong>multiple Pinterest-ready pins</strong> — images, titles, and descriptions included. Built for Amazon affiliates and bloggers who want traffic without designing in Canva.</p>
    <p style="margin:0 0 8px;font-weight:600;">Your first pins in about a minute:</p>
    <ol style="margin:0 0 16px 18px;padding:0;color:#3a3a3a;">
      <li style="margin-bottom:6px;">Paste your URL.</li>
      <li style="margin-bottom:6px;">Hit Generate — we analyze the page and create several pin angles for you (no design work).</li>
      <li>Post to Pinterest, schedule, or download.</li>
    </ol>`;
  const html = emailLayout({
    heading: `Welcome to ${BRAND}`,
    bodyHtml,
    ctaText: 'Create my first pin',
    ctaUrl: APP_URL,
    // Asking how they found us here is the only acquisition attribution that exists: 99% of
    // signups arrive from an unidentified source, and this email already sends to everyone.
    ps: `Hit reply and tell me two things: what you're promoting, and how you found ${BRAND}. I read every email, and I'm happy to suggest the best first URL to try.`,
    footerNote: `You're receiving this because you just created a ${BRAND} account.`,
  });
  return { subject, html };
}

/** Day ~1, only if the user hasn't generated a pin yet. */
export function renderFirstPinEmail() {
  const subject = `Make your first pin in about a minute`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">You signed up for ${BRAND} but haven't made a pin yet — and the first one takes about a minute. The affiliates who win on Pinterest are the ones who just start publishing.</p>
    <p style="margin:0 0 14px;">Grab any Amazon product link (or a blog post) and paste it in. We'll handle the image, the title, and the SEO description for you.</p>`;
  const html = emailLayout({
    heading: 'Your first pin is about a minute away',
    bodyHtml,
    ctaText: 'Create my first pin',
    ctaUrl: APP_URL,
    ps: `Stuck on what to pin first? Reply with your niche and I'll suggest a good URL to start with.`,
    footerNote: `You're receiving this because you have a ${BRAND} account but no pins yet.`,
  });
  return { subject, html };
}

/** Day ~3: a value tip to build the habit. */
export function renderDay3TipEmail() {
  const subject = `A quick win for your Pinterest traffic`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">One tip that compounds on Pinterest: <strong>make several pins per product</strong>, not one. Different angles, titles and images mean more chances to get picked up in search and feeds.</p>
    <p style="margin:0 0 14px;">With ${BRAND} you can spin up multiple pin angles from the same URL in seconds — try generating 3–5 variations for your best product and scheduling them across the week.</p>
    <p style="margin:0 0 14px;">One thing worth knowing early: <strong>Pinterest is slow before it's fast.</strong> Pins typically take 60&ndash;90 days to reach full traffic, because the platform keeps surfacing good pins months after they're published &mdash; which is exactly why volume pays off here and doesn't on other platforms.</p>
    <p style="margin:0 0 14px;">So in the first few weeks, watch <strong>impressions and saves</strong> in your analytics rather than clicks. Those move first, and they tell you which pins Pinterest has decided to show.</p>`;
  const html = emailLayout({
    heading: 'Multiply your pins, multiply your reach',
    bodyHtml,
    ctaText: 'Create more pins',
    ctaUrl: APP_URL,
    ps: `Got a product you're not sure how to pin? Reply and I'll brainstorm a few angles with you.`,
    footerNote: `You're receiving this as part of getting started with ${BRAND}.`,
  });
  return { subject, html };
}

/**
 * Sent once, immediately after someone registers as a partner.
 *
 * Registration used to be followed by silence: no link, no terms, no idea what to promote. Two of
 * the first nine partners signed up within minutes of creating an account and never returned.
 * Commission is stated as NET here on purpose -- discovering that later feels like a bait-and-switch.
 */
export function renderAffiliateWelcomeEmail({ displayName, slug, ratePct = 30, months = 12 } = {}) {
  const name = String(displayName || '').trim();
  const link = `${FRONTEND_URL}/?ref=${encodeURIComponent(String(slug || '').trim())}`;
  const subject = `You're in — here's your ${BRAND} referral link`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi${name ? ` ${name}` : ''},</p>
    <p style="margin:0 0 14px;">Thanks for joining the ${BRAND} partner programme. Here is your referral link:</p>
    <p style="margin:0 0 16px;"><a href="${link}" style="font-weight:600;">${link}</a></p>
    <p style="margin:0 0 14px;">Anyone who signs up through it is credited to you for <strong>${months} months</strong>. You earn <strong>${ratePct}% of what they pay every month they stay</strong> &mdash; not just on their first payment.</p>
    <p style="margin:0 0 14px;">One thing worth knowing up front: commission is calculated on <strong>net</strong> revenue, after payment processing and tax, rather than the sticker price. On a $25/month plan that works out around $7 a month. Plans run $12 to $129, so one customer on the top tier is worth roughly $430 over a year.</p>
    <p style="margin:0 0 8px;font-weight:600;">What tends to convert</p>
    <p style="margin:0 0 14px;">Amazon affiliates, Etsy sellers and bloggers who already use Pinterest and are stuck producing enough pins. An honest post about that specific problem lands far better than a feature list &mdash; the tool sells itself once someone recognises the grind it removes.</p>
    <p style="margin:0 0 14px;">Your dashboard shows clicks, signups and earnings as they come in. I handle payouts personally, so I will be in touch once you have commissions to pay out.</p>`;
  const html = emailLayout({
    heading: "You're in — here's your link",
    bodyHtml,
    ctaText: 'Open my partner dashboard',
    ctaUrl: `${FRONTEND_URL}/affiliate/dashboard`,
    ps: `Reply and tell me where you plan to share it — I read every email, and I can tell you what tends to land with that audience.`,
    footerNote: `You're receiving this because you just registered as a ${BRAND} partner.`,
  });
  return { subject, html };
}

export async function sendAffiliateWelcomeEmail({ to, displayName, slug, ratePct, months } = {}) {
  const { subject, html } = renderAffiliateWelcomeEmail({ displayName, slug, ratePct, months });
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

export async function sendWelcomeEmail({ to } = {}) {
  const { subject, html } = renderWelcomeEmail();
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}
export async function sendFirstPinEmail({ to } = {}) {
  const { subject, html } = renderFirstPinEmail();
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}
export async function sendDay3TipEmail({ to } = {}) {
  const { subject, html } = renderDay3TipEmail();
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

// --- Price-change notice (to customers grandfathered on superseded pricing) ---

/**
 * "Prices went up, yours didn't" — sent once to existing subscribers when new pricing ships.
 *
 * `currentMonthlyUsd` / `newMonthlyUsd` are the customer's REAL rate and the new list rate for
 * the same plan, so the email states their actual numbers rather than a generic claim.
 *
 * The caveat paragraph is not optional. Two paths lose the old rate permanently, and both run
 * through resolveDodoProductIdForPlan / the checkout map, which ALWAYS return current pricing:
 *   - changing plan  -> the subscription is moved onto the current product
 *   - cancelling     -> re-subscribing later is a fresh checkout at current prices
 * Neither is reversible; there is no code path that puts anyone back on a legacy product.
 * Promising a locked price without saying so would be a promise the billing code does not keep.
 */
export function renderPriceLockEmail({ planType, currentUsd, newUsd, yearly = false } = {}) {
  const plan = String(planType || '').toLowerCase();
  const label = PLAN_LABELS[plan] || plan;
  const cur = Number(currentUsd);
  const nxt = Number(newUsd);
  if (!label || !Number.isFinite(cur) || !Number.isFinite(nxt) || cur <= 0) return null;
  if (nxt <= cur) return null; // nothing to reassure them about

  const per = yearly ? '/year' : '/month';
  const saving = Math.round((nxt - cur) * 100) / 100;
  const savingPerYear = yearly ? saving : saving * 12;

  const subject = `Your ${label} price isn't changing`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">${BRAND} pricing went up this week — ${label} is now <strong>$${nxt}${per}</strong> for new customers.</p>
    <p style="margin:0 0 14px;"><strong>Your price is not changing.</strong> You keep paying <strong>$${cur}${per}</strong>, with the same ${PLAN_AI_PIN_LIMITS[plan] ? `${PLAN_AI_PIN_LIMITS[plan]} AI pins a month and the ` : ''}same features. That's $${savingPerYear} a year less than the new rate.</p>
    <p style="margin:0 0 14px;">This isn't a temporary promotion. As long as your subscription stays active, it keeps renewing at $${cur}${per} — there's no end date and nothing you need to do to keep it.</p>
    <p style="margin:0 0 14px;">The one thing worth knowing: <strong>the old rate is tied to this subscription.</strong> If you switch plans, or cancel and re-subscribe later, you'd be on whatever pricing is current at that point — I can't put you back on $${cur}${per} afterwards. So if you're ever considering a change, reply first and I'll tell you exactly what it would cost you.</p>
    <p style="margin:0 0 14px;">Otherwise there's nothing to do and nothing to click. I'm only writing so you don't see the new prices on the site and assume your bill went up.</p>`;
  return {
    subject,
    html: emailLayout({
      heading: `Your price stays at $${cur}${per}`,
      bodyHtml,
      ps: `Genuinely — reply to this email if anything about your plan or billing is unclear. It comes straight to me.`,
      footerNote: `You're receiving this because you're a ${BRAND} ${label} subscriber.`,
    }),
  };
}

export async function sendPriceLockEmail({ to, planType, currentUsd, newUsd, yearly } = {}) {
  const rendered = renderPriceLockEmail({ planType, currentUsd, newUsd, yearly });
  if (!rendered) return { ok: false, skipped: true, reason: 'no_price_increase_for_plan' };
  return sendEmail({ to, subject: rendered.subject, html: rendered.html, replyTo: SUPPORT_EMAIL });
}

// --- Annual-plan push (to current monthly subscribers) ---

/**
 * Build the "switch to annual and save" email for a given monthly plan.
 *
 * `currentMonthlyUsd` is what THIS customer pays today, which is not always the list price:
 * grandfathered subscribers are still billed the pre-2026 rate. A legacy $9 Starter switching to
 * the $108 annual plan saves nothing, so quoting the list-price saving ($36) would be a false
 * claim. Callers that know the customer's real rate should pass it; when the saving works out to
 * zero or less, this returns null and the send is skipped.
 */
export function renderAnnualUpgradeEmail({ planType, currentMonthlyUsd } = {}) {
  const plan = String(planType || '').toLowerCase();
  const listMonthly = PLAN_PRICES_USD[plan];
  const annual = PLAN_ANNUAL_PRICE_USD[plan];
  if (!listMonthly || !annual) return null;
  const parsed = Number(currentMonthlyUsd);
  const monthly = Number.isFinite(parsed) && parsed > 0 ? parsed : listMonthly;
  const label = PLAN_LABELS[plan] || plan;
  const savings = Math.round((monthly * 12 - annual) * 100) / 100;
  // Require at least one month free. Legacy rates make this campaign a bad deal — a $39 Pro
  // customer moving to the $495 annual plan would pay $27 MORE per year — and a token few
  // dollars is not worth an email either.
  if (savings < monthly) return null;
  const monthsFree = Math.round((savings / monthly) * 10) / 10;
  // Whole dollars, rounded down — matches the pricing page headline ($18, not $18.75).
  const effMonthly = Math.floor(annual / 12);

  const subject = `Save $${savings}/year on your ${label} plan`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">You're on the monthly <strong>${label}</strong> plan at $${monthly}/mo. Switching to annual drops it to <strong>$${effMonthly}/mo</strong> (billed $${annual}/year) — that's <strong>$${savings} saved a year</strong>, roughly ${monthsFree} months free.</p>
    <p style="margin:0 0 14px;">Same plan, same features — just a lower rate locked in for 12 months. You can switch in a couple of clicks from your account.</p>`;
  const html = emailLayout({
    heading: `Lock in ${monthsFree} months free`,
    bodyHtml,
    ctaText: 'Switch to annual',
    ctaUrl: UPGRADE_URL,
    ps: `Want me to switch you over manually so you don't lose your current billing date? Just reply and I'll handle it.`,
    footerNote: `You're receiving this because you're on the monthly ${label} plan.`,
  });
  return { subject, html };
}

export async function sendAnnualUpgradeEmail({ to, planType, currentMonthlyUsd } = {}) {
  const rendered = renderAnnualUpgradeEmail({ planType, currentMonthlyUsd });
  if (!rendered) return { ok: false, skipped: true, reason: 'no_annual_saving_for_plan' };
  return sendEmail({ to, subject: rendered.subject, html: rendered.html, replyTo: SUPPORT_EMAIL });
}

// --- One-off outreach (2026-08). Driven by scripts/send-outreach.mjs, not by the app. ---

/**
 * A paying customer who has never scheduled a pin. They are being billed for the one feature
 * (scheduling/publishing) they have never touched — usually because Pinterest was never connected.
 * Deliberately short, no upsell, single question: this is a retention save, not a campaign.
 */
export function renderPaidNotStartedEmail({ planType, generated = 0 } = {}) {
  const label = PLAN_LABELS[String(planType || '').toLowerCase()] || 'paid';
  const madePins = Number(generated) > 0;
  const subject = `Is something blocking you in ${BRAND}?`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">I was going through ${BRAND} accounts and noticed yours: you're on the <strong>${label}</strong> plan, but you haven't scheduled or published a pin yet${
      madePins ? ` — even though you've generated ${generated} pins` : ''
    }.</p>
    <p style="margin:0 0 14px;">That usually means one thing: <strong>Pinterest isn't connected yet.</strong> Publishing and scheduling only work once your account is linked and a board is selected, so until that's done the part you're paying for sits idle.</p>
    <p style="margin:0 0 14px;">It takes about a minute, and I'm happy to walk you through it — or just do it with you on a quick call if that's easier.</p>`;
  const html = emailLayout({
    heading: 'Can I help you get started?',
    bodyHtml,
    ctaText: 'Connect Pinterest',
    ctaUrl: `${APP_URL}/scheduled-pins`,
    ps: `If ${BRAND} isn't the right fit, tell me and I'll refund you — I'd rather do that than bill someone who isn't getting value from it.`,
    footerNote: `You're receiving this because you have an active ${BRAND} subscription.`,
  });
  return { subject, html };
}

export async function sendPaidNotStartedEmail({ to, planType, generated } = {}) {
  const { subject, html } = renderPaidNotStartedEmail({ planType, generated });
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

/**
 * Free user who used their whole free allowance AND connected Pinterest — then hit the paywall,
 * because scheduling and post-now are both paid-only. They got all the way to the last step and
 * received nothing for it, so the email leads by acknowledging that rather than pretending.
 */
export function renderConnectedPaywallEmail({ generated = 0 } = {}) {
  const starter = PLAN_PRICES_USD.starter;
  const starterPins = PLAN_AI_PIN_LIMITS.starter;
  const subject = `You connected Pinterest — here's the last step`;
  const bodyHtml = `
    <p style="margin:0 0 14px;">Hi there,</p>
    <p style="margin:0 0 14px;">You generated ${generated > 0 ? `<strong>${generated} pins</strong>` : 'your pins'} in ${BRAND} and connected your Pinterest account — which means you got all the way to the final step and then hit a wall. Publishing and scheduling are on the paid plans, and I don't think that was obvious enough before you connected. Sorry about that.</p>
    <p style="margin:0 0 8px;">Here's what the <strong>Starter</strong> plan ($${starter}/mo) actually changes:</p>
    <ul style="margin:0 0 16px 18px;padding:0;color:#3a3a3a;">
      <li style="margin-bottom:6px;">Your pins post to Pinterest automatically, on a schedule you set.</li>
      <li style="margin-bottom:6px;">${starterPins} AI pins a month instead of ${PLAN_AI_PIN_LIMITS.free} total.</li>
      <li>Spread a batch across days or weeks in one go, instead of posting by hand.</li>
    </ul>
    <p style="margin:0 0 14px;">The pins you already made are still in your account, ready to schedule.</p>`;
  const html = emailLayout({
    heading: 'Your pins are ready to publish',
    bodyHtml,
    ctaText: 'See plans',
    ctaUrl: PRICING_URL,
    ps: `Not sure it's worth it yet? Reply and tell me what you're promoting — I'll tell you honestly whether ${BRAND} will move the needle for your niche.`,
    footerNote: `You're receiving this because you created a ${BRAND} account and connected Pinterest.`,
  });
  return { subject, html };
}

export async function sendConnectedPaywallEmail({ to, generated } = {}) {
  const { subject, html } = renderConnectedPaywallEmail({ generated });
  return sendEmail({ to, subject, html, replyTo: SUPPORT_EMAIL });
}

export const emailConfig = { EMAIL_FROM, FRONTEND_URL, APP_URL, BILLING_RECOVERY_URL, UPGRADE_URL, PRICING_URL, SUPPORT_EMAIL };
