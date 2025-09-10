import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import Stripe from 'stripe';
dotenv.config();

console.log('SUPABASE_URL:', process.env.SUPABASE_URL);
console.log('SUPABASE_SERVICE_ROLE_KEY:', process.env.SUPABASE_SERVICE_ROLE_KEY);

const supabaseAdmin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: '2022-11-15',
}) : null;

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

function sanitizeDescription(input) {
  if (!input) return '';
  let desc = String(input);
  // Remove raw URLs and domain-like tokens
  desc = desc.replace(/https?:\/\/\S+/gi, '');
  desc = desc.replace(/\b([a-z0-9-]+\.)+[a-z]{2,}\b/gi, '');
  // Remove common CTA phrases about visiting/clicking for more info
  const ctaPatterns = [
    /visit\s+[^.\n]+/gi,
    /click( the)? link( below| above| in bio)?/gi,
    /learn more( at| here)?/gi,
    /read more( at| here)?/gi,
    /see more( at| here)?/gi,
    /for more info(rmation)?/gi
  ];
  ctaPatterns.forEach((re) => { desc = desc.replace(re, ''); });
  // Collapse multiple spaces
  desc = desc.replace(/\s{2,}/g, ' ').trim();
  // Ensure hashtags exist (>=3). If not, append some generic but safe hashtags
  const hashtags = (desc.match(/#[A-Za-z0-9_]+/g) || []).length;
  if (hashtags < 3) {
    const toAdd = ['#tips', '#guide', '#inspiration'].slice(0, 3 - hashtags).join(' ');
    desc = (desc + ' ' + toAdd).trim();
  }
  // Enforce 450 char limit
  if (desc.length > 450) desc = desc.slice(0, 450);
  return desc;
}

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

// --- Custom Templates CRUD (Supabase) ---
// Requires authenticated user (Bearer token)
async function requireUser(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !user) return res.status(401).json({ error: 'Unauthorized' });
  req.user = user;
  next();
}

// List templates for current user
app.get('/api/custom-templates', requireUser, async (req, res) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('custom_templates')
      .select('id,name,template_json,updated_at,created_at')
      .eq('user_id', req.user.id)
      .order('updated_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    const templates = (data || []).map(row => ({ id: row.id, name: row.name, ...row.template_json }));
    res.json({ templates });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Create or update a template
app.post('/api/custom-templates', requireUser, async (req, res) => {
  try {
    const tpl = req.body;
    if (!tpl || !tpl.id || !tpl.elements) {
      return res.status(400).json({ error: 'Invalid template payload' });
    }
    const row = {
      id: tpl.id,
      user_id: req.user.id,
      name: tpl.name || 'Untitled Template',
      template_json: tpl,
      updated_at: new Date().toISOString(),
    };
    const { error } = await supabaseAdmin
      .from('custom_templates')
      .upsert(row, { onConflict: 'id' });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Delete a template
app.delete('/api/custom-templates/:id', requireUser, async (req, res) => {
  try {
    const { id } = req.params;
    const { error } = await supabaseAdmin
      .from('custom_templates')
      .delete()
      .match({ id, user_id: req.user.id });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/export-pin (Puppeteer server-side rendering)
app.post('/api/export-pin', async (req, res) => {
  const { pinData, template, templateType, templateData } = req.body;
  console.log('[export-pin] Request received', { pinData: !!pinData, template });
  console.log('[export-pin] Template type:', typeof template);
  console.log('[export-pin] Template value:', template);
  console.log('[export-pin] Template meta:', { templateType, hasTemplateData: !!templateData });
  if (!pinData || !template) {
    console.error('[export-pin] Missing pinData or template');
    return res.status(400).json({ error: 'pinData and template are required' });
  }
  let browser;
  try {
    console.log('[export-pin] Launching Puppeteer...');
    browser = await puppeteer.launch({ 
      headless: 'new', 
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      timeout: 30000
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1000, height: 1500 });
    
    // Set up console log capture BEFORE navigating to the page
    page.on('console', msg => {
      const text = msg.text();
      console.log('[Puppeteer Console]', text);
      // Check for ExportPinPage loading
      if (text.includes('ExportPinPage component is loading')) {
        console.log('[export-pin] ✅ ExportPinPage component loaded successfully!');
      }
    });
    
    // Use environment variable for frontend base URL, fallback to localhost for local dev
    const FRONTEND_BASE_URL = process.env.FRONTEND_BASE_URL || 'http://localhost:3000';
    console.log('[export-pin] FRONTEND_BASE_URL from env:', process.env.FRONTEND_BASE_URL);
    console.log('[export-pin] Final FRONTEND_BASE_URL:', FRONTEND_BASE_URL);
    
    // Construct the URL with template parameters
    const params = new URLSearchParams();
    params.set('data', JSON.stringify(pinData));
    params.set('template', template);
    if (templateType) params.set('templateType', templateType);
    if (templateType === 'custom' && templateData) {
      // Pass full custom template JSON to the export page
      params.set('templateData', JSON.stringify(templateData));
    }
    const url = `${FRONTEND_BASE_URL}/export-pin?${params.toString()}`;
    console.log('[export-pin] Navigating to', url);
    console.log('[export-pin] Encoded template:', encodeURIComponent(template));
    
    // Add timeout and better error handling
    await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
    
    // Wait a bit more for React to hydrate
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // Capture page errors
    page.on('pageerror', error => {
      console.log('[Puppeteer Page Error]', error.message);
      console.log('[Puppeteer Page Error Stack]', error.stack);
    });
    
    // Capture network failures
    page.on('requestfailed', request => {
      console.log('[Puppeteer Request Failed]', request.url(), request.failure().errorText);
    });
    
    // Wait a bit for the page to fully load using setTimeout
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const pageTitle = await page.title();
    console.log('[export-pin] Page title:', pageTitle);
    
    // Get the page content to see if it's loading correctly
    const pageContent = await page.content();
    console.log('[export-pin] Page content length:', pageContent.length);
    
    // Check for React app indicators instead of component names
    const hasReactApp = pageContent.includes('react') || pageContent.includes('React');
    console.log('[export-pin] Page appears to be React app:', hasReactApp);
    
    // Debug: Check what's actually on the page
    const hasRootDiv = pageContent.includes('id="root"') || pageContent.includes('id=\'root\'');
    const hasScriptTags = pageContent.includes('<script');
    const hasCSV2Pin = pageContent.includes('CSV2Pin') || pageContent.includes('csv2pin');
    console.log('[export-pin] Page has root div:', hasRootDiv);
    console.log('[export-pin] Page has script tags:', hasScriptTags);
    console.log('[export-pin] Page mentions CSV2Pin:', hasCSV2Pin);
    
    // Show first 500 chars of page content for debugging
    console.log('[export-pin] Page content preview:', pageContent.substring(0, 500));
    
    // Test if JavaScript is working at all
    try {
      const jsTest = await page.evaluate(() => {
        return {
          hasWindow: typeof window !== 'undefined',
          hasDocument: typeof document !== 'undefined',
          hasReact: typeof window !== 'undefined' && window.React,
          hasReactDOM: typeof window !== 'undefined' && window.ReactDOM,
          userAgent: navigator.userAgent,
          scriptCount: document.querySelectorAll('script').length,
          scriptSources: Array.from(document.querySelectorAll('script')).map(s => s.src || 'inline').slice(0, 5)
        };
      });
      console.log('[export-pin] JavaScript execution test:', jsTest);
    } catch (e) {
      console.log('[export-pin] JavaScript execution failed:', e.message);
    }
    
    // Check if React is loading by waiting longer and checking again
    console.log('[export-pin] Waiting additional 5 seconds for React to load...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    try {
      const reactTest = await page.evaluate(() => {
        return {
          hasReact: typeof window !== 'undefined' && window.React,
          hasReactDOM: typeof window !== 'undefined' && window.ReactDOM,
          hasExportDebug: typeof window !== 'undefined' && window.exportDebug,
          rootElement: document.getElementById('root')?.innerHTML?.length || 0,
          isExportPageWorking: typeof window !== 'undefined' && window.exportDebug && window.exportDebug.templateKey
        };
      });
      console.log('[export-pin] React loading test after wait:', reactTest);
      
      if (reactTest.isExportPageWorking) {
        console.log('[export-pin] ✅ ExportPinPage is working correctly!');
      } else {
        console.log('[export-pin] ❌ ExportPinPage is not working properly');
      }
    } catch (e) {
      console.log('[export-pin] React test failed:', e.message);
    }
    
    const templateReceived = await page.evaluate(() => {
      if (window.exportDebug) {
        return window.exportDebug;
      }
      return null;
    });
    console.log('[export-pin] Template debug info:', templateReceived);
    
    // Check if the template component is rendering
    const templateComponent = await page.evaluate(() => {
      const templateDiv = document.querySelector('div');
      if (templateDiv) {
        return {
          className: templateDiv.className,
          style: templateDiv.style.cssText,
          children: templateDiv.children.length
        };
      }
      return null;
    });
    console.log('[export-pin] Template component info:', templateComponent);
    
    // Ensure all images are fully loaded before screenshot (important for multi-image templates)
    try {
      await page.waitForFunction(() => {
        const imgs = Array.from(document.images || []);
        if (imgs.length === 0) return true;
        return imgs.every((img) => img.complete && img.naturalWidth > 0);
      }, { timeout: 10000 });
      console.log('[export-pin] All images reported loaded.');
    } catch (e) {
      console.log('[export-pin] Proceeding without full image load confirmation:', e.message);
    }

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

// POST /api/generate-field (requires login; credits handled on frontend)
app.post('/api/generate-field', requireUser, async (req, res) => {
  const { content, type } = req.body; // type: 'title' or 'description'
  if (!content || !type) return res.status(400).json({ error: 'Missing content or type' });
  const prompt = type === 'title'
    ? `Write a concise, curiosity-driven Pinterest pin title (max 100 characters) for this content. The title should make people want to click to learn more. It should include emotional triggers, urgency, or questions where possible. Avoid generic phrases and focus on being unique and compelling. Only return the title, nothing else. Do not include any quotes or special characters in your response:\n${content}`
    : `Write an engaging Pinterest pin description (max 450 characters) for this content. The description should explain the benefit or insight the user will get by clicking. Avoid phrases like "+visit site+", "+click the link+", or adding URLs. Include 4–6 relevant hashtags at the end. Only return the description, nothing else:\n${content}`;
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
      result = result.replace(/["'`~!@#$%^&*()_+=\[\]{}|;:<>\/?]+/g, '').slice(0, 100);
    }
    if (type === 'description') result = sanitizeDescription(result);
    res.json({ result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Analyze image with OpenAI Vision to extract text and generate metadata (requires login)
app.post('/api/analyze-image', requireUser, async (req, res) => {
  try {
    const { imageUrl, urlHint } = req.body;
    if (!imageUrl) return res.status(400).json({ error: 'Missing imageUrl' });

    const systemPrompt = `You are helping generate Pinterest pin metadata. First, read any visible text in the image (OCR). Then propose a compelling title (<=100 chars) and an engaging description (<=450 chars) suitable for Pinterest. The description must include 4–6 relevant hashtags at the end. Do not include URLs or phrases like \"visit example.com\", \"click the link\", or similar calls to visit a site. If a destination URL context is provided, use it only to infer keywords, but never include the URL or a CTA. Return JSON with keys: extractedText, title, description. Do not include markdown, code fences, or commentary.`;

    const userPrompt = `Image URL: ${imageUrl}\n${urlHint ? `Destination URL (context): ${urlHint}` : ''}`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: systemPrompt },
        {
          role: 'user',
          content: [
            { type: 'text', text: userPrompt },
            { type: 'image_url', image_url: { url: imageUrl } }
          ]
        }
      ],
      temperature: 0.4,
      max_tokens: 600
    });

    const raw = response.choices?.[0]?.message?.content?.trim() || '';

    // Attempt to parse JSON; if model returned plain text, wrap it
    let extracted = { extractedText: '', title: '', description: '' };
    try {
      const maybe = JSON.parse(raw);
      extracted.extractedText = String(maybe.extractedText || '').slice(0, 2000);
      extracted.title = String(maybe.title || '').slice(0, 100);
      extracted.description = sanitizeDescription(String(maybe.description || ''));
    } catch (_) {
      // Fallback heuristic: split lines
      const lines = raw.split('\n').map(s => s.trim()).filter(Boolean);
      extracted.extractedText = lines.join(' ').slice(0, 2000);
      extracted.title = (lines[0] || '').slice(0, 100);
      extracted.description = sanitizeDescription(lines.slice(1).join(' '));
    }

    // Final cleanup for title
    extracted.title = extracted.title.replace(/["'`~!@#$%^&*()_+=\[\]{}|;:<>\/?]+/g, '').slice(0, 100);

    return res.json(extracted);
  } catch (err) {
    console.error('analyze-image error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// CSV save endpoint for all registered users
app.post('/api/csv', async (req, res) => {
  // Save CSV logic here (not implemented)
  res.json({ message: 'CSV saved (available to all registered users)' });
});

// --- Pinterest OAuth2 Integration ---
// Redirect user to Pinterest OAuth2
app.get('/api/pinterest/login', (req, res) => {
  console.log('--- Pinterest OAuth Login Initiated ---');
  console.log('client_id:', process.env.PINTEREST_CLIENT_ID);
  console.log('redirect_uri:', process.env.PINTEREST_REDIRECT_URI);
  console.log('scope:', 'pins:write boards:read boards:write pins:read user_accounts:read');
  const params = new URLSearchParams({
    client_id: process.env.PINTEREST_CLIENT_ID,
    redirect_uri: process.env.PINTEREST_REDIRECT_URI,
    response_type: 'code',
    scope: 'pins:write boards:read boards:write pins:read user_accounts:read', // added user_accounts:read
    state: 'secureRandomState123', // TODO: Use a real random state for security
  });
  const redirectUrl = `https://www.pinterest.com/oauth/?${params.toString()}`;
  console.log('Redirecting to:', redirectUrl);
  res.redirect(redirectUrl);
});


async function exchangePinterestCodeForToken(code, redirectUri) {
  console.log('redirectUri used in token exchange:', redirectUri);
  
  // Try Method 1: Basic Auth (Confidential Client approach)
  const basicAuth = Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString('base64');
  
  const params = new URLSearchParams({
    grant_type: 'authorization_code',
    code: code,
    redirect_uri: redirectUri
  });

  console.log('--- Exchanging Pinterest Code for Token (Method 1: Basic Auth) ---');
  console.log('Request body:', params.toString());

  try {
    let response = await fetch('https://api.pinterest.com/v5/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': `Basic ${basicAuth}`,
      },
      body: params.toString(),
    });

    let text = await response.text();
    let result;
    try { 
      result = JSON.parse(text); 
    } catch { 
      result = { error: 'Invalid JSON response', response: text }; 
    }

    console.log('Pinterest token endpoint response (Method 1):', response.status, result);

    // If Method 1 fails, try Method 2: Include credentials in body
    if (!response.ok && response.status === 400) {
      console.log('--- Trying Method 2: Credentials in body ---');
      
      const paramsWithCreds = new URLSearchParams({
        grant_type: 'authorization_code',
        code: code,
        client_id: process.env.PINTEREST_CLIENT_ID,
        client_secret: process.env.PINTEREST_CLIENT_SECRET,
        redirect_uri: redirectUri
      });

      response = await fetch('https://api.pinterest.com/v5/oauth/token', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json',
        },
        body: paramsWithCreds.toString(),
      });

      text = await response.text();
      try { 
        result = JSON.parse(text); 
      } catch { 
        result = { error: 'Invalid JSON response', response: text }; 
      }

      console.log('Pinterest token endpoint response (Method 2):', response.status, result);
    }

    if (!response.ok) {
      throw new Error(`Pinterest API error: ${response.status} - ${JSON.stringify(result)}`);
    }

    return result;
  } catch (error) {
    console.error('Error in exchangePinterestCodeForToken:', error);
    throw error;
  }
}


// Handle Pinterest OAuth2 callback
app.get('/api/pinterest/callback', async (req, res) => {
  const { code, state } = req.query;
  console.log('--- Pinterest OAuth Callback ---');
  console.log('Received code:', code);
  console.log('Received state:', state);
  if (!code) return res.status(400).send('Missing code');
  // Redirect to frontend with code for user association (use env FRONTEND_URL)
  const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:3000';
  res.redirect(`${FRONTEND_URL.replace(/\/$/, '')}/pinterest/finish?code=${code}`);
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
      // Enforce plan limits
      const { data: profile } = await supabaseAdmin
        .from('profiles')
        .select('plan_type')
        .eq('id', user.id)
        .single();
      const planType = (profile?.plan_type || 'free');
      const { data: existing } = await supabaseAdmin
        .from('pinterest_accounts')
        .select('id')
        .eq('user_id', user.id);
      const count = Array.isArray(existing) ? existing.length : 0;
      const planLimits = { free: 1, creator: 3, pro: Infinity, agency: Infinity };
      const limit = planLimits[planType] ?? 1;
      if (count >= limit) {
        return res.status(403).json({ error: `Plan limit reached. Your plan (${planType}) allows ${limit === Infinity ? 'unlimited' : limit} account(s).` });
      }

      // Fetch account info for labeling
      let accountName = '';
      try {
        const accRes = await fetch('https://api.pinterest.com/v5/user_account', {
          headers: { 'Authorization': `Bearer ${tokenData.access_token}`, 'Accept': 'application/json' }
        });
        const acc = await accRes.json();
        accountName = acc?.username || acc?.profile?.username || 'Pinterest Account';
      } catch (e) {
        console.warn('Failed to fetch Pinterest account info:', e?.message || e);
      }
      // Save token to pinterest_accounts
      const { error: insertError } = await supabaseAdmin
        .from('pinterest_accounts')
        .insert({ user_id: user.id, access_token: tokenData.access_token, account_name: accountName });
      if (insertError) {
        console.error('Error saving pinterest account:', insertError);
      }
      return res.json({ access_token: tokenData.access_token, account_name: accountName });
    } else {
      return res.status(400).json({ error: tokenData });
    }
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// List Pinterest accounts for current user
app.get('/api/pinterest/accounts', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });
  const { data, error } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, account_name, created_at')
    .eq('user_id', user.id)
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ accounts: data || [] });
});

function extractAccountId(req) {
  return req.query.account_id || req.body?.account_id || null;
}

async function getPinterestAccessTokenForUser(userId, accountId) {
  if (!accountId) {
    // Fallback to single-token in profiles for backwards compatibility
    const { data: profile } = await supabaseAdmin
    .from('profiles')
    .select('pinterest_access_token')
      .eq('id', userId)
    .single();
    return profile?.pinterest_access_token || null;
  }
  const { data: account } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('access_token')
    .eq('id', accountId)
    .eq('user_id', userId)
    .single();
  return account?.access_token || null;
}

app.get('/api/pinterest/boards', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const accountId = extractAccountId(req);
  const accessToken = await getPinterestAccessTokenForUser(user.id, accountId);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user.' });
  }

  // Fetch boards from Pinterest API
  const pinterestRes = await fetch('https://api.pinterest.com/v5/boards', {
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  });
  const boards = await pinterestRes.json();
  res.json({ boards: boards.items || boards.data || [] });
});

app.post('/api/pinterest/create-pin', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { image_url, title, description, board_id, link, account_id } = req.body;
  if (!image_url || !title || !description || !board_id) {
    return res.status(400).json({ error: 'Missing required fields.' });
  }

  const accessToken = await getPinterestAccessTokenForUser(user.id, account_id);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user/account.' });
  }

  try {
    const requestBody = {
      board_id,
      title,
      description,
      media_source: {
        source_type: 'image_url',
        url: image_url,
      },
      link: link || undefined,
    };
    
    console.log('Pinterest API request:', {
      board_id,
      title: title.substring(0, 50) + '...',
      description: description.substring(0, 100) + '...',
      image_url: image_url.substring(0, 100) + '...',
      link: link || 'none'
    });
    
    const pinterestRes = await fetch('https://api.pinterest.com/v5/pins', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });

    const pinData = await pinterestRes.json();
    if (pinterestRes.ok) {
      return res.json(pinData);
    } else {
      console.error('Pinterest API error:', pinData);
      return res.status(400).json({ 
        error: {
          message: pinData.message || pinData.error || 'Pinterest API error',
          code: pinData.code || pinterestRes.status,
          details: pinData
        }
      });
    }
  } catch (error) {
    return res.status(400).json({ 
      error: { 
        code: 2, 
        message: `Pinterest API error: ${error.message}`, 
        status: 'failure' 
      } 
    });
  }
});

// Debug endpoint to check Pinterest connection status
app.get('/api/pinterest/status', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  // Load accounts
  const { data: accounts, error: accError } = await supabaseAdmin
    .from('pinterest_accounts')
    .select('id, account_name, access_token');
  const hasToken = Array.isArray(accounts) && accounts.some(a => !!a.access_token);
  let tokenValid = false;
  let pinterestError = null;

  if (hasToken) {
    try {
      const anyToken = accounts.find(a => a.access_token)?.access_token;
      const testRes = await fetch('https://api.pinterest.com/v5/user_account', {
        headers: {
          'Authorization': `Bearer ${anyToken}`,
          'Content-Type': 'application/json',
        },
      });
      if (testRes.ok) tokenValid = true; else pinterestError = await testRes.json();
    } catch (error) {
      pinterestError = { message: error.message };
    }
  }

  res.json({
    user_id: user.id,
    has_accounts: hasToken,
    accounts_count: Array.isArray(accounts) ? accounts.length : 0,
    token_valid: tokenValid,
    pinterest_error: pinterestError,
    profile_error: accError
  });
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

app.get('/api/pinterest/boards', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const accountId = extractAccountId(req);
  const accessToken = await getPinterestAccessTokenForUser(user.id, accountId);
  if (!accessToken) {
    return res.status(400).json({ error: 'No Pinterest access token found for user.' });
  }

  // Fetch boards from Pinterest API
  const pinterestRes = await fetch('https://api.pinterest.com/v5/boards', {
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  });
  const boards = await pinterestRes.json();
  res.json({ boards: boards.items || boards.data || [] });
});


// Stripe checkout session endpoint
app.post('/api/create-checkout-session', async (req, res) => {
  if (!stripe) {
    return res.status(500).json({ error: 'Stripe not configured' });
  }
  
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { planType } = req.body;
  
  // Define plan configurations
  const plans = {
    'creator': {
      name: 'Creator Plan',
      price: 1500, // $15.00 in cents
      credits: 1000,
      planType: 'creator'
    },
    'pro': {
      name: 'Pro Plan',
      price: 2500, // $25.00 in cents
      credits: 3000,
      planType: 'pro'
    },
    'agency': {
      name: 'Agency Plan',
      price: 5600, // $56.00 in cents
      credits: 7500,
      planType: 'agency'
    }
  };

  const plan = plans[planType];
  if (!plan) {
    return res.status(400).json({ error: 'Invalid plan type' });
  }

  try {
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [
        {
          price_data: {
            currency: 'usd',
            product_data: {
              name: plan.name,
              description: `${plan.credits} credits per month`,
            },
            unit_amount: plan.price,
            recurring: {
              interval: 'month',
            },
          },
          quantity: 1,
        },
      ],
      mode: 'subscription',
      success_url: `${process.env.FRONTEND_URL || 'https://csv2pin.com'}/payment-success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.FRONTEND_URL || 'https://csv2pin.com'}/pricing`,
      client_reference_id: user.id,
      metadata: {
        userId: user.id,
        planType: plan.planType,
        credits: plan.credits.toString()
      }
    });



    res.json({ sessionId: session.id });
  } catch (error) {
    console.error('Error creating checkout session:', error);
    res.status(500).json({ error: 'Failed to create checkout session' });
  }
});


// One-time credits top-up checkout session endpoint
app.post('/api/create-credits-session', async (req, res) => {
  if (!stripe) {
    return res.status(500).json({ error: 'Stripe not configured' });
  }

  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });
  const token = authHeader.split(' ')[1];
  const { data: { user }, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !user) return res.status(401).json({ error: 'Unauthorized' });

  const { creditsPack, couponCode } = req.body; // '250' | '500' | '1000' | '3000', optional coupon
  const packs = {
    '250': { name: 'Top-up 250 credits', amount: 600, credits: 250 },   // $6.00
    '500': { name: 'Top-up 500 credits', amount: 1000, credits: 500 },  // $10.00
    '1000': { name: 'Top-up 1000 credits', amount: 1800, credits: 1000 }, // $18.00
    '3000': { name: 'Top-up 3000 credits', amount: 3600, credits: 3000 }, // $36.00
  };
  const pack = packs[String(creditsPack)];
  if (!pack) return res.status(400).json({ error: 'Invalid credits pack' });

  try {
    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      payment_method_types: ['card'],
      line_items: [
        {
          price_data: {
            currency: 'usd',
            product_data: { name: pack.name },
            unit_amount: pack.amount,
          },
          quantity: 1,
        },
      ],
      success_url: `${process.env.FRONTEND_URL || 'https://csv2pin.com'}/payment-success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.FRONTEND_URL || 'https://csv2pin.com'}/pricing`,
      client_reference_id: user.id,
      metadata: {
        userId: user.id,
        type: 'topup',
        credits: String(pack.credits),
      },
      discounts: (couponCode && couponCode.trim()) ? [{ coupon: couponCode.trim() }] : undefined,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error('Error creating credits checkout session:', error);
    console.error('Error details:', {
      message: error.message,
      type: error.type,
      code: error.code,
      param: error.param,
      creditsPack,
      couponCode,
      userId: user.id
    });
    res.status(500).json({ 
      error: 'Failed to create checkout session',
      details: error.message 
    });
  }
});



// Stripe webhook endpoint
app.post('/api/stripe-webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!stripe) {
    return res.status(500).json({ error: 'Stripe not configured' });
  }
  
  const sig = req.headers['stripe-signature'];
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;

  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    console.error('Webhook signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  // Handle the event
  switch (event.type) {
    case 'checkout.session.completed':
      const session = event.data.object;
      await handleCheckoutSessionCompleted(session);
      // Handle one-time credits top-up
      if (session?.metadata?.type === 'topup' && session?.metadata?.userId && session?.metadata?.credits) {
        const userId = session.metadata.userId;
        const addCredits = parseInt(session.metadata.credits, 10) || 0;
        if (addCredits > 0) {
          try {
            const { data: profile } = await supabaseAdmin
              .from('profiles')
              .select('credits_remaining')
              .eq('id', userId)
              .single();
            const current = profile?.credits_remaining || 0;
            const newCredits = current + addCredits;
            await supabaseAdmin
              .from('profiles')
              .update({ credits_remaining: newCredits })
              .eq('id', userId);
          } catch (e) {
            console.error('Error applying top-up credits:', e);
          }
        }
      }
      break;
    case 'invoice.payment_succeeded':
      const invoice = event.data.object;
      await handleInvoicePaymentSucceeded(invoice);
      break;
    case 'customer.subscription.deleted':
      const subscription = event.data.object;
      await handleSubscriptionDeleted(subscription);
      break;
    default:
      console.log(`Unhandled event type ${event.type}`);
  }

  res.json({ received: true });
});

async function handleCheckoutSessionCompleted(session) {
  if (!stripe) return;
  
  const userId = session.metadata.userId;
  const planType = session.metadata.planType;
  const credits = parseInt(session.metadata.credits);

  try {
    // Update user profile with new plan and credits
    const { error } = await supabaseAdmin
      .from('profiles')
      .update({
        plan_type: planType,
        credits_remaining: credits,
        is_pro: true,
        stripe_customer_id: session.customer,
        stripe_subscription_id: session.subscription
      })
      .eq('id', userId);

    if (error) {
      console.error('Error updating user profile:', error);
    } else {
      console.log(`User ${userId} upgraded to ${planType} plan`);
    }
  } catch (error) {
    console.error('Error handling checkout session completed:', error);
  }
}

async function handleInvoicePaymentSucceeded(invoice) {
  if (!stripe) return;
  
  const subscriptionId = invoice.subscription;
  
  try {
    // Get subscription details
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    const customerId = subscription.customer;
    
    // Find user by Stripe customer ID
    const { data: profile, error } = await supabaseAdmin
      .from('profiles')
      .select('id, plan_type')
      .eq('stripe_customer_id', customerId)
      .single();

    if (error || !profile) {
      console.error('User not found for customer ID:', customerId);
      return;
    }

    // Define credits for each plan
    const planCredits = {
      'creator': 500,
      'pro': 1500,
      'agency': 5000
    };

    const credits = planCredits[profile.plan_type] || 0;

    // Reset credits for the new month
    const { error: updateError } = await supabaseAdmin
      .from('profiles')
      .update({ credits_remaining: credits })
      .eq('id', profile.id);

    if (updateError) {
      console.error('Error updating credits:', updateError);
    } else {
      console.log(`Credits reset for user ${profile.id} to ${credits}`);
    }
  } catch (error) {
    console.error('Error handling invoice payment succeeded:', error);
  }
}

async function handleSubscriptionDeleted(subscription) {
  if (!stripe) return;
  
  const customerId = subscription.customer;
  
  try {
    // Find user by Stripe customer ID
    const { data: profile, error } = await supabaseAdmin
      .from('profiles')
      .select('id')
      .eq('stripe_customer_id', customerId)
      .single();

    if (error || !profile) {
      console.error('User not found for customer ID:', customerId);
      return;
    }

    // Downgrade user to free plan
    const { error: updateError } = await supabaseAdmin
      .from('profiles')
      .update({
        plan_type: 'free',
        credits_remaining: 50,
        is_pro: false,
        stripe_subscription_id: null
      })
      .eq('id', profile.id);

    if (updateError) {
      console.error('Error downgrading user:', updateError);
    } else {
      console.log(`User ${profile.id} downgraded to free plan`);
    }
  } catch (error) {
    console.error('Error handling subscription deleted:', error);
  }
}

// Test endpoint
app.get('/api/test', (req, res) => {
  res.json({ message: 'Backend is working' });
});

// WordPress API endpoints
app.post('/api/wordpress/test-connection', async (req, res) => {
  const { siteUrl, username, appPassword } = req.body;
  
  if (!siteUrl || !username || !appPassword) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    // Clean up the site URL
    const cleanSiteUrl = siteUrl.replace(/\/$/, ''); // Remove trailing slash
    
    console.log('Testing connection to:', cleanSiteUrl);
    console.log('Username:', username);
    
    const auth = Buffer.from(`${username}:${appPassword}`).toString('base64');
    const response = await fetch(`${cleanSiteUrl}/wp-json/wp/v2/posts?per_page=1&status=publish`, {
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (response.ok) {
      const posts = await response.json();
      console.log('Connection test - posts found:', posts.length);
      res.json({ 
        success: true, 
        message: 'Connection successful',
        postsFound: posts.length
      });
    } else {
      const errorData = await response.json().catch(() => ({}));
      console.log('Connection test failed:', errorData);
      res.status(400).json({ 
        error: 'Invalid credentials or site URL',
        details: errorData
      });
    }
  } catch (error) {
    console.error('WordPress connection error:', error);
    res.status(500).json({ error: 'Connection failed. Please check your site URL and credentials.' });
  }
});

app.post('/api/wordpress/fetch-posts', async (req, res) => {
  const { siteUrl, username, appPassword } = req.body;
  
  if (!siteUrl || !username || !appPassword) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    const cleanSiteUrl = siteUrl.replace(/\/$/, '');
    const auth = Buffer.from(`${username}:${appPassword}`).toString('base64');
    
    console.log('Fetching posts from:', `${cleanSiteUrl}/wp-json/wp/v2/posts`);
    console.log('Using auth for user:', username);
    
    // First, let's test the basic posts endpoint
    const testResponse = await fetch(`${cleanSiteUrl}/wp-json/wp/v2/posts?per_page=1`, {
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      }
    });
    
    console.log('Test response status:', testResponse.status);
    console.log('Test response headers:', Object.fromEntries(testResponse.headers.entries()));
    
    if (!testResponse.ok) {
      const errorText = await testResponse.text();
      console.log('Test response error:', errorText);
      return res.status(400).json({ 
        error: 'Failed to fetch posts',
        details: { status: testResponse.status, body: errorText }
      });
    }
    
    // Now fetch posts with more details - try without status filter first
    const response = await fetch(`${cleanSiteUrl}/wp-json/wp/v2/posts?per_page=50&_embed`, {
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      return res.status(400).json({ 
        error: 'Failed to fetch posts',
        details: errorData
      });
    }
    
    const posts = await response.json();
    
    console.log('Raw posts response:', JSON.stringify(posts, null, 2));
    console.log('Number of posts found:', posts.length);
    console.log('Posts structure:', posts.length > 0 ? Object.keys(posts[0]) : 'No posts');
    
    // Filter to only published posts if we got any posts
    const publishedPosts = posts.filter(post => post.status === 'publish');
    console.log('Published posts found:', publishedPosts.length);
    
    // Transform WordPress posts to our format - use filtered posts
    const transformedPosts = publishedPosts.map(post => {
      // Extract featured image URL if available
      let featuredImageUrl = '';
      if (post._embedded && post._embedded['wp:featuredmedia'] && post._embedded['wp:featuredmedia'][0]) {
        featuredImageUrl = post._embedded['wp:featuredmedia'][0].source_url;
      }
      
      // Clean up content by removing HTML tags
      const cleanContent = post.content.rendered
        .replace(/<[^>]*>/g, '') // Remove HTML tags
        .replace(/\s+/g, ' ') // Replace multiple spaces with single space
        .trim();
      
      const cleanExcerpt = post.excerpt.rendered
        .replace(/<[^>]*>/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      
      return {
        id: post.id,
        title: post.title.rendered,
        content: cleanContent,
        excerpt: cleanExcerpt,
        featured_image_url: featuredImageUrl,
        link: post.link,
        date: post.date,
        author: post.author,
        categories: post.categories.join(', ')
      };
    });
    
    res.json({ posts: transformedPosts });
  } catch (error) {
    console.error('WordPress fetch posts error:', error);
    res.status(500).json({ error: 'Failed to fetch posts' });
  }
});

app.listen(PORT, () => {
  console.log(`Backend listening on port ${PORT}`);
}); 