import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';
dotenv.config();

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
      return res.json({ imageUrl: prediction.output[0] });
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
  if (!pinData || !template) {
    return res.status(400).json({ error: 'pinData and template are required' });
  }
  let browser;
  try {
    browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.setViewport({ width: 1000, height: 1500 });
    // Point to the frontend export page, passing data as query param
    const url = `http://localhost:3000/export-pin?data=${encodeURIComponent(JSON.stringify(pinData))}&template=${encodeURIComponent(template)}`;
    await page.goto(url, { waitUntil: 'networkidle0' });
    // Screenshot the full page (should be just the pin)
    const buffer = await page.screenshot({ type: 'png', fullPage: true });
    await browser.close();
    res.set('Content-Type', 'image/png');
    res.send(buffer);
  } catch (err) {
    if (browser) await browser.close();
    res.status(500).json({ error: 'Failed to export pin', details: err.message });
  }
});

// --- Pinterest OAuth2 Integration ---
// Redirect user to Pinterest OAuth2
app.get('/api/pinterest/login', (req, res) => {
  const params = new URLSearchParams({
    client_id: process.env.PINTEREST_CLIENT_ID,
    redirect_uri: process.env.PINTEREST_REDIRECT_URI,
    response_type: 'code',
    scope: 'pins:read pins:write boards:read boards:write user_accounts:read',
    state: 'secureRandomState123', // TODO: Use a real random state for security
  });
  res.redirect(`https://www.pinterest.com/oauth/?${params.toString()}`);
});

// Handle Pinterest OAuth2 callback
app.get('/api/pinterest/callback', async (req, res) => {
  const { code, state } = req.query;
  if (!code) return res.status(400).send('Missing code');
  // Optionally, validate the state parameter here

  // Exchange code for access token
  try {
    const tokenRes = await fetch('https://api.pinterest.com/v5/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        grant_type: 'authorization_code',
        code,
        client_id: process.env.PINTEREST_CLIENT_ID,
        client_secret: process.env.PINTEREST_CLIENT_SECRET,
        redirect_uri: process.env.PINTEREST_REDIRECT_URI,
      }),
    });
    const tokenData = await tokenRes.json();
    if (tokenData.access_token) {
      // You should store the access_token securely (e.g., in a DB or session)
      // For demo, just show it
      res.send(`<pre>Access Token: ${tokenData.access_token}\n\n${JSON.stringify(tokenData, null, 2)}</pre>`);
    } else {
      res.status(400).send(`<pre>Failed to get access token:\n${JSON.stringify(tokenData, null, 2)}</pre>`);
    }
  } catch (err) {
    res.status(500).send('OAuth2 error: ' + err.message);
  }
});

app.listen(PORT, () => {
  console.log(`Backend listening on port ${PORT}`);
}); 