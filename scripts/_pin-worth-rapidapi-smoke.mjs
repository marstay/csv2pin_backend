import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { fetchPinterestSearchSnapshot } from '../src/pinterestSearchSnapshot.js';
import { fetchRapidApiSuggestions } from '../src/pinterestRapidApi.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const keyword = process.argv[2] || 'bathroom organizer';

const suggestions = await fetchRapidApiSuggestions(keyword.split(' ')[0]);
console.log('suggestions', suggestions.slice(0, 5));

const snapshot = await fetchPinterestSearchSnapshot(keyword, { getAccessToken: async () => null });
console.log('snapshot available', snapshot.available);
console.log('source', snapshot.source);
console.log('pins', snapshot.stats?.pinsInSample);
console.log('competition', snapshot.stats?.competitionLevel);
console.log('first pin', snapshot.pins?.[0]);
