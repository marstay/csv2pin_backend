import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
dotenv.config();

console.log('SUPABASE_URL:', process.env.SUPABASE_URL);
console.log('SUPABASE_SERVICE_ROLE_KEY:', process.env.SUPABASE_SERVICE_ROLE_KEY);

const supabaseAdmin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

async function requirePro(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !user) return res.status(401).json({ error: 'Unauthorized' });
  const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('is_pro')
    .eq('id', user.id)
    .single();
  if (!profile?.is_pro) {
    return res.status(403).json({ error: 'Pro membership required.' });
  }
  req.user = user;
  next();
}

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

const REPLICATE_API_TOKEN = process.env.REPLICATE_API_TOKEN;

// POST /api/generate-image
app.post('/api/generate-image', async (req, res) => {
  console.log('Received request:', req.body);
  const { title } = req.body;
  if (!title) {
    return res.status(400).json({ error: 'Title is required' });
  }

  if (!REPLICATE_API_TOKEN) {
    return res.status(500).json({ error: 'Replicate API token not set' });
  }

  try {
    // Construct a highly optimized Pinterest-style prompt
    const pinterestPrompt = `Create an eye-catching, scroll-stopping Pinterest pin background for a blog post titled "${title}". The image must be vertical (portrait layout), visually stunning, and use vibrant, modern colors with a clean, contemporary style. Soft lighting, shallow depth of field, high resolution, professional photographer style. Absolutely no text, words, or lettering—only visuals. The design should be suitable as a background for a Pinterest pin, with clear space for text overlay.`;
    // Call Replicate SDXL API
    const response = await fetch('https://api.replicate.com/v1/predictions', {
      method: 'POST',
      headers: {
        'Authorization': `Token ${REPLICATE_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        version: '7762fd07cf82c948538e41f63f77d685e02b063e37e496e96eefd46c929f9bdc',
        input: {
          prompt: pinterestPrompt,
          width: 1024,
          height: 1536,
        }
      }),
    });

    const data = await response.json();

    // Replicate returns a prediction object; poll until status is 'succeeded'
    let prediction = data;
    while (prediction.status !== 'succeeded' && prediction.status !== 'failed') {
      await new Promise(r => setTimeout(r, 1500));
      const pollRes = await fetch(`https://api.replicate.com/v1/predictions/${prediction.id}`, {
        headers: { 'Authorization': `Token ${REPLICATE_API_TOKEN}` }
      });
      prediction = await pollRes.json();
    }

    if (prediction.status === 'succeeded') {
      // SDXL returns an array of image URLs
      const replicateUrl = prediction.output[0];
      try {
        // Download the image as a buffer
        const imageRes = await fetch(replicateUrl);
        const arrayBuffer = await imageRes.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        // Determine file extension
        const fileExt = replicateUrl.split('.').pop().split('?')[0];
        const fileName = `ai-image-${Date.now()}-${Math.floor(Math.random()*10000)}.${fileExt}`;
        // Upload to Supabase Storage
        const { error: uploadError } = await supabaseAdmin.storage.from('ai-images').upload(fileName, buffer, {
          contentType: imageRes.headers.get('content-type') || 'image/png',
          upsert: true,
        });
        if (uploadError) {
          console.error('Error uploading to Supabase Storage:', uploadError);
          return res.json({ imageUrl: replicateUrl });
        }
        // Get public URL
        const { data: publicUrlData } = supabaseAdmin.storage.from('ai-images').getPublicUrl(fileName);
        const publicUrl = publicUrlData?.publicUrl || replicateUrl;
        return res.json({ imageUrl: publicUrl });
      } catch (err) {
        console.error('Error downloading/uploading image:', err);
        return res.json({ imageUrl: replicateUrl });
      }
    } else {
      return res.status(500).json({ error: 'Image generation failed' });
    }
  } catch (err) {
    return res.status(500).json({ error: 'Error calling Replicate API', details: err.message });
  }
});

// POST /api/export-pin (Puppeteer server-side rendering)
app.post('/api/export-pin', async (req, res) => {
  const { pinData, template } = req.body;
  console.log('[export-pin] Request received', { pinData: !!pinData, template });
  if (!pinData || !template) {
    console.error('[export-pin] Missing pinData or template');
    return res.status(400).json({ error: 'pinData and template are required' });
  }
  let browser;
  try {
    console.log('[export-pin] Launching Puppeteer...');
    browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.setViewport({ width: 1000, height: 1500 });
    // Use environment variable for frontend base URL, fallback to localhost for local dev
    const FRONTEND_BASE_URL = process.env.FRONTEND_BASE_URL || 'http://localhost:3000';
    const url = `${FRONTEND_BASE_URL}/export-pin?data=${encodeURIComponent(JSON.stringify(pinData))}&template=${encodeURIComponent(template)}`;
    console.log('[export-pin] Navigating to', url);
    await page.goto(url, { waitUntil: 'networkidle0' });
    console.log('[export-pin] Taking screenshot...');
    const buffer = await page.screenshot({ type: 'png', fullPage: true });
    await browser.close();
    console.log('[export-pin] Screenshot taken and browser closed. Sending response.');
    res.set('Content-Type', 'image/png');
    res.send(buffer);
  } catch (err) {
    console.error('[export-pin] Error:', err, err.stack);
    if (browser) await browser.close();
    res.status(500).json({ error: 'Failed to export pin', details: err.message, stack: err.stack });
  }
});

// POST /api/generate-field (Pro only)
app.post('/api/generate-field', requirePro, async (req, res) => {
  const { content, type } = req.body; // type: 'title' or 'description'
  if (!content || !type) return res.status(400).json({ error: 'Missing content or type' });
  const prompt = type === 'title'
    ? `Write a catchy Pinterest title (max 100 characters) for this content. Only return the title, nothing else. Do not include any quotes or special characters in your response:\n${content}`
    : `Write a Pinterest description (max 500 characters) for this content. Only return the description, nothing else:\n${content}`;
  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: type === 'title' ? 100 : 500,
      temperature: 0.7,
    });
    let result = completion.choices[0].message.content.trim();
    if (type === 'title') {
      // Remove quotes and special characters except basic punctuation
      result = result.replace(/["'`~!@#$%^&*()_+=\[\]{}|;:<>/?]+/g, '').slice(0, 100);
    }
    if (type === 'description') result = result.slice(0, 500);
    res.json({ result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Example Pro-only endpoint for saving CSV
app.post('/api/csv', requirePro, async (req, res) => {
  // Save CSV logic here (not implemented)
  res.json({ message: 'CSV saved (Pro only endpoint)' });
});

// --- Pinterest OAuth2 Integration ---
// Redirect user to Pinterest OAuth2
app.get('/api/pinterest/login', (req, res) => {
  console.log('--- Pinterest OAuth Login Initiated ---');
  console.log('client_id:', process.env.PINTEREST_CLIENT_ID);
  console.log('redirect_uri:', process.env.PINTEREST_REDIRECT_URI);
  console.log('scope:', 'pins:read boards:read');
  const params = new URLSearchParams({
    client_id: process.env.PINTEREST_CLIENT_ID,
    redirect_uri: process.env.PINTEREST_REDIRECT_URI,
    response_type: 'code',
    scope: 'pins:read boards:read',
    state: 'secureRandomState123', // TODO: Use a real random state for security
  });
  const redirectUrl = `https://www.pinterest.com/oauth/?${params.toString()}`;
  console.log('Redirecting to:', redirectUrl);
  res.redirect(redirectUrl);
});


async function exchangePinterestCodeForToken(code, redirectUri) {
  // Use the redirectUri as-is (should be plain, not encoded)
  console.log('redirectUri used in token exchange (should be plain):', redirectUri);
  const params = new URLSearchParams();
  params.append('grant_type', 'authorization_code');
  params.append('code', code);
  params.append('client_id', process.env.PINTEREST_CLIENT_ID);
  params.append('client_secret', process.env.PINTEREST_CLIENT_SECRET);
  params.append('redirect_uri', redirectUri);
  const bodyString = params.toString();
  console.log('--- Exchanging Pinterest Code for Token ---');
  console.log('Request body:', bodyString);
  const response = await fetch('https://api.pinterest.com/v5/oauth/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json',
    },
    body: bodyString,
  });
  const text = await response.text();
  let result;
  try { result = JSON.parse(text); } catch { result = text; }
  console.log('Pinterest token endpoint response:', response.status, result);
  return result;
}



// Handle Pinterest OAuth2 callback
app.get('/api/pinterest/callback', async (req, res) => {
  const { code, state } = req.query;
  console.log('--- Pinterest OAuth Callback ---');
  console.log('Received code:', code);
  console.log('Received state:', state);
  if (!code) return res.status(400).send('Missing code');
  try {
    console.log('Calling exchangePinterestCodeForToken with:');
    console.log('client_id:', process.env.PINTEREST_CLIENT_ID);
    console.log('redirect_uri:', process.env.PINTEREST_REDIRECT_URI);
    console.log('code:', code);
    console.log('client_secret present:', !!process.env.PINTEREST_CLIENT_SECRET);
    const tokenData = await exchangePinterestCodeForToken(code, process.env.PINTEREST_REDIRECT_URI);
    console.log('Token exchange result:', tokenData);
    if (tokenData.access_token) {
      res.send(`<pre>Access Token: ${tokenData.access_token}\n\n${JSON.stringify(tokenData, null, 2)}</pre>`);
    } else {
      res.status(400).send(`<pre>Failed to get access token:\n${JSON.stringify(tokenData, null, 2)}</pre>`);
    }
  } catch (err) {
    console.error('OAuth2 error:', err);
    res.status(500).send('OAuth2 error: ' + err.message);
  }
});

app.post('/api/pinterest/oauth', async (req, res) => {
  const { code, redirectUri } = req.body;
  const authHeader = req.headers.authorization;
  if (!code || !redirectUri) return res.status(400).json({ error: 'Missing code or redirectUri' });
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });
  // Log the values sent to Pinterest for debugging
  console.log({
    client_id: process.env.PINTEREST_CLIENT_ID,
    client_secret: process.env.PINTEREST_CLIENT_SECRET ? process.env.PINTEREST_CLIENT_SECRET.slice(0,3) + '...' + process.env.PINTEREST_CLIENT_SECRET.slice(-3) : undefined,
    redirect_uri: redirectUri,
    code
  });
  try {
    const tokenData = await exchangePinterestCodeForToken(code, redirectUri);
    if (tokenData.access_token) {
      await supabaseAdmin
        .from('profiles')
        .update({ pinterest_access_token: tokenData.access_token })
        .eq('id', user.id);
      return res.json({ access_token: tokenData.access_token });
    } else {
      return res.status(400).json({ error: tokenData });
    }
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// In-memory store for latest boards (for demo; use a DB for production)
let latestBoards = {};

// Endpoint for Make.com to POST boards
app.post('/api/pinterest-boards-result', express.json(), (req, res) => {
  latestBoards['default'] = req.body.boards || req.body; // Accept either { boards: [...] } or just an array
  res.json({ status: 'ok' });
});

// Endpoint for frontend to GET boards
app.get('/api/pinterest-boards', (req, res) => {
  res.json({ boards: latestBoards['default'] || [] });
});

app.listen(PORT, () => {
  console.log(`Backend listening on port ${PORT}`);
}); 