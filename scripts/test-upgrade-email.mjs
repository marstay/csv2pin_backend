/**
 * Send a real "upgrade / out of pins" expansion email to verify Resend + the template.
 *
 * Usage:  node backend/scripts/test-upgrade-email.mjs you@example.com [currentPlan] [reason]
 *   currentPlan: free|starter|creator|pro|agency   (default starter)
 *   reason:      limit_reached | approaching_limit  (default limit_reached)
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '../.env') });

const to = process.argv[2];
const currentPlan = process.argv[3] || 'starter';
const reason = process.argv[4] || 'limit_reached';
if (!to) {
  console.error('Usage: node backend/scripts/test-upgrade-email.mjs you@example.com [currentPlan] [reason]');
  process.exit(1);
}

const limits = { free: 10, starter: 60, creator: 150, pro: 450, agency: 1000 };
const limit = limits[currentPlan] ?? 60;
const used = reason === 'approaching_limit' ? Math.round(limit * 0.8) : limit;

const { sendUpgradeNudgeEmail, isEmailEnabled } = await import('../src/email.js');

console.log('Email enabled:', isEmailEnabled(), '| plan:', currentPlan, '| reason:', reason);
const result = await sendUpgradeNudgeEmail({ to, currentPlan, used, limit, reason });
console.log('Result:', JSON.stringify(result, null, 2));
if (result.ok) console.log(`\nSent. Check ${to} (and spam).`);
else if (result.skipped && result.reason === 'top_tier') console.log('\nSkipped — top tier has no upsell.');
else if (result.skipped) console.log('\nSkipped — add RESEND_API_KEY to backend/.env to send for real.');
else console.log('\nSend failed — see error above.');
