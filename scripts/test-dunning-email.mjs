/**
 * Send a real "payment failed" dunning email to verify Resend is wired up.
 *
 * Usage:  node backend/scripts/test-dunning-email.mjs you@example.com [planType]
 *   planType is optional (starter|creator|pro|agency), defaults to creator.
 *
 * Reads RESEND_API_KEY / EMAIL_FROM / etc. from backend/.env. If the key is
 * missing the send is skipped (the email module no-ops).
 */
import dotenv from 'dotenv';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
// Load env BEFORE importing the email module (which reads process.env at load).
dotenv.config({ path: resolve(__dirname, '../.env') });

const to = process.argv[2];
const planType = process.argv[3] || 'creator';
if (!to) {
  console.error('Usage: node backend/scripts/test-dunning-email.mjs you@example.com [planType]');
  process.exit(1);
}

const { sendPaymentFailedEmail, isEmailEnabled } = await import('../src/email.js');

console.log('Email enabled (RESEND_API_KEY present):', isEmailEnabled());
const result = await sendPaymentFailedEmail({ to, planType });
console.log('Result:', JSON.stringify(result, null, 2));
if (result.ok) console.log(`\nSent. Check ${to} (and spam) for the dunning email.`);
else if (result.skipped) console.log('\nSkipped — add RESEND_API_KEY to backend/.env to send for real.');
else console.log('\nSend failed — see the error above (often an unverified EMAIL_FROM domain).');
