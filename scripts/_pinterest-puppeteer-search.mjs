import puppeteer from 'puppeteer';

const query = process.argv[2] || 'bathroom organizer';
const url = `https://www.pinterest.com/search/pins/?q=${encodeURIComponent(query)}`;

let browser;
try {
  browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
  const page = await browser.newPage();
  const urls = [];

  page.on('response', (resp) => {
    const u = resp.url();
    if (u.includes('pinterest.com/resource/')) urls.push(u.split('?')[0].slice(-60));
  });

  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await new Promise((r) => setTimeout(r, 5000));
  console.log([...new Set(urls)].join('\n'));
} finally {
  if (browser) await browser.close();
}
