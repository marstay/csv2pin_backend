-- Add Stripe-related columns to profiles table
ALTER TABLE profiles 
ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT,
ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_profiles_stripe_customer_id ON profiles(stripe_customer_id);
CREATE INDEX IF NOT EXISTS idx_profiles_stripe_subscription_id ON profiles(stripe_subscription_id);

-- Update existing profiles to have default values
UPDATE profiles 
SET 
  plan_type = COALESCE(plan_type, 'free'),
  credits_remaining = COALESCE(credits_remaining, 50),
  is_pro = COALESCE(is_pro, false)
WHERE plan_type IS NULL OR credits_remaining IS NULL OR is_pro IS NULL; 