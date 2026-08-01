-- Affiliate net accounting + payout tracking.
-- Run in the Supabase SQL editor. Safe to re-run.
--
-- WHY THESE COLUMNS
--   net_amount_cents / tax_cents / fee_cents
--     Commission is paid on what we actually KEEP, not what the customer was charged. Verified
--     against live Dodo data: a CNY 6338 payment settles as 900 USD, and a EUR 965 sale carried
--     174 cents of VAT that Dodo remits to the tax authority. Storing the breakdown means a
--     partner can be shown exactly how their commission was derived.
--   paid_at / payout_reference
--     Nothing previously recorded that a payout had happened, so settled money kept reading as
--     owed and there was no audit trail — the fastest route to a dispute with a revenue partner.

ALTER TABLE affiliate_commissions
  ADD COLUMN IF NOT EXISTS net_amount_cents integer,
  ADD COLUMN IF NOT EXISTS tax_cents        integer,
  ADD COLUMN IF NOT EXISTS fee_cents        integer,
  ADD COLUMN IF NOT EXISTS paid_at          timestamptz,
  ADD COLUMN IF NOT EXISTS payout_reference text;

-- Payout queries are always "what is owed to this partner in this currency".
CREATE INDEX IF NOT EXISTS affiliate_commissions_payout_idx
  ON affiliate_commissions (affiliate_id, currency, status);

-- Belt and braces: the app relies on this to make concurrent webhook retries safe.
-- Already present as of 2026-08 (verified by probe); the guard makes this file self-contained.
CREATE UNIQUE INDEX IF NOT EXISTS affiliate_commissions_payment_id_key
  ON affiliate_commissions (payment_id)
  WHERE payment_id IS NOT NULL;
