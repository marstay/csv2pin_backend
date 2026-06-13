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
 *                         e.g. "Aristomenis from URL2Pin <aristomenis@url2pin.com>".
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
const PLAN_PRICES_USD = { free: 0, starter: 9, creator: 19, pro: 39, agency: 79 };
const PLAN_ANNUAL_PRICE_USD = { starter: 84, creator: 180, pro: 384, agency: 780 };
const PLAN_AI_PIN_LIMITS = { free: 10, starter: 60, creator: 150, pro: 450, agency: 1000 };
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
 * Build the "you're out of / running low on pins — upgrade" expansion email.
 * `reason` is 'limit_reached' or 'approaching_limit'.
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
  const heading = atLimit ? `You're out of pins for this month` : `You're running low on pins`;
  const subject = atLimit
    ? `You've hit your ${curLabel} pin limit — upgrade for more`
    : `You're almost out of pins this month`;

  const opening = atLimit
    ? `You've used all <strong>${limit}</strong> AI pins on your ${curLabel} plan this month. Nice work — that means it's driving real output for you.`
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
    ps: `Hit reply and tell me what you're promoting — I read every email and I'm happy to suggest the best first URL to try.`,
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
    <p style="margin:0 0 14px;">With ${BRAND} you can spin up multiple pin angles from the same URL in seconds — try generating 3–5 variations for your best product and scheduling them across the week.</p>`;
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

// --- Annual-plan push (to current monthly subscribers) ---

/** Build the "switch to annual and save" email for a given monthly plan. */
export function renderAnnualUpgradeEmail({ planType } = {}) {
  const plan = String(planType || '').toLowerCase();
  const monthly = PLAN_PRICES_USD[plan];
  const annual = PLAN_ANNUAL_PRICE_USD[plan];
  if (!monthly || !annual) return null;
  const label = PLAN_LABELS[plan] || plan;
  const savings = monthly * 12 - annual;
  const monthsFree = Math.round((savings / monthly) * 10) / 10;
  const effMonthly = Math.round((annual / 12) * 100) / 100;

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

export async function sendAnnualUpgradeEmail({ to, planType } = {}) {
  const rendered = renderAnnualUpgradeEmail({ planType });
  if (!rendered) return { ok: false, skipped: true, reason: 'no_annual_for_plan' };
  return sendEmail({ to, subject: rendered.subject, html: rendered.html, replyTo: SUPPORT_EMAIL });
}

export const emailConfig = { EMAIL_FROM, FRONTEND_URL, APP_URL, BILLING_RECOVERY_URL, UPGRADE_URL, PRICING_URL, SUPPORT_EMAIL };
