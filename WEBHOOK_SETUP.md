# Stripe Webhook Setup Guide

## For Production

### 1. Set up Webhook Endpoint in Stripe Dashboard

1. Go to your Stripe Dashboard
2. Navigate to **Developers** > **Webhooks**
3. Click **Add endpoint**
4. Set the endpoint URL to: `https://your-domain.com/api/stripe-webhook`
5. Select these events to listen for:
   - `checkout.session.completed`
   - `invoice.payment_succeeded`
   - `customer.subscription.deleted`
6. Click **Add endpoint**

### 2. Get the Webhook Secret

1. After creating the webhook, click on it
2. Find the **Signing secret** section
3. Click **Reveal** to see the secret
4. Copy the secret (starts with `whsec_`)

### 3. Add to Environment Variables

Add the webhook secret to your `.env` file:

```env
STRIPE_WEBHOOK_SECRET=whsec_your_webhook_secret_here
```

### 4. Test the Webhook

1. In the Stripe Dashboard, go to your webhook
2. Click **Send test webhook**
3. Select `checkout.session.completed`
4. Click **Send test webhook**
5. Check your server logs to see if the webhook was received

## For Local Development

### Option 1: Use Stripe CLI (Recommended)

1. Install Stripe CLI: https://stripe.com/docs/stripe-cli
2. Login: `stripe login`
3. Forward webhooks to your local server:
   ```bash
   stripe listen --forward-to localhost:4000/api/stripe-webhook
   ```
4. Copy the webhook signing secret that's displayed
5. Add it to your `.env` file:
   ```env
   STRIPE_WEBHOOK_SECRET=whsec_... (from stripe listen output)
   ```

### Option 2: Manual Testing (Current Implementation)

The current implementation includes:
- Immediate credit updates when checkout session is created (for testing)
- Manual credit update endpoint at `/api/update-credits`
- Test component at `/credit-test` route

## Testing Credit Updates

1. **Automatic**: Credits should update immediately when you create a checkout session
2. **Manual**: Go to `http://localhost:3000/credit-test` to manually update credits
3. **Check Database**: Verify credits are updated in your Supabase `profiles` table

## Production Checklist

- [ ] Set up webhook endpoint in Stripe Dashboard
- [ ] Add webhook secret to environment variables
- [ ] Test webhook with Stripe CLI or dashboard
- [ ] Remove manual credit update endpoint (`/api/update-credits`)
- [ ] Remove immediate credit update from checkout session creation
- [ ] Remove test component (`CreditTest.jsx`)
- [ ] Deploy with proper webhook URL

## Troubleshooting

### Webhook Not Receiving Events
- Check if the webhook URL is accessible from the internet
- Verify the webhook secret is correct
- Check server logs for webhook signature verification errors

### Credits Not Updating
- Check if the webhook handler is being called
- Verify the user ID in the session metadata
- Check Supabase connection and permissions
- Look for errors in the `handleCheckoutSessionCompleted` function 