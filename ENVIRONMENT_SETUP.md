# Environment Setup Instructions

## Required Environment Variables

Create a `.env` file in the backend directory with the following variables:

```env
# Supabase Configuration
SUPABASE_URL=your_supabase_url_here
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key_here

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Replicate Configuration
REPLICATE_API_TOKEN=your_replicate_api_token_here

# Stripe Configuration
STRIPE_SECRET_KEY=your_stripe_secret_key_here

# Stripe Webhook Secret (set this up in Stripe dashboard)
STRIPE_WEBHOOK_SECRET=your_stripe_webhook_secret_here

# Frontend URL (for Stripe success/cancel URLs)
FRONTEND_URL=http://localhost:3000
```

## Steps to Set Up

1. Copy the above content into a new file called `.env` in the backend directory
2. Replace the placeholder values with your actual API keys
3. The Stripe secret key is already provided above
4. For the webhook secret, you'll need to set up a webhook in your Stripe dashboard

## Note
The `.env` file is already in `.gitignore` to prevent committing sensitive keys to version control. 