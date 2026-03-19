/**
 * Strategic Pin System – content understanding, strategy planning, and diversity check.
 * See docs/STRATEGIC_PIN_SYSTEM_DESIGN.md
 */

// Strategy → layoutId mapping (from design doc Section 8)
const STRATEGY_LAYOUT_MAP = {
  curiosity_hook: ['curiosity_shock', 'question_style', 'viral_curiosity'],
  list_value: ['timeline_infographic', 'step_cards_3', 'grid_3_images', 'grid_4_images', 'stacked_strips'],
  lifestyle: ['cozy_baking', 'minimal_elegant', 'stacked_strips'],
  clean_authority: ['clean_appetizing', 'minimal_typography'],
  mistake_warning: ['curiosity_shock', 'clumpy_fix'],
  transformation: ['before_after', 'offset_collage_3', 'grid_4_images'],
  wildcard: ['circle_cluster_4', 'offset_collage_3', 'grid_3_images', 'stacked_strips'],
};

// Niche-specific strategy mixes (from design doc Section 13)
const NICHE_MIXES = {
  default: { curiosity_hook: 3, list_value: 2, lifestyle: 2, clean_authority: 1, wildcard: 1, transformation: 1 },
  recipe: { curiosity_hook: 2, list_value: 3, lifestyle: 3, transformation: 1, clean_authority: 1 },
  finance: { curiosity_hook: 3, list_value: 2, mistake_warning: 2, clean_authority: 1, wildcard: 1, transformation: 1 },
  travel: { curiosity_hook: 2, list_value: 3, lifestyle: 3, wildcard: 1, transformation: 1 },
  self_improvement: { curiosity_hook: 3, lifestyle: 3, transformation: 2, list_value: 1, wildcard: 1 },
  product_review: { curiosity_hook: 2, list_value: 3, transformation: 2, clean_authority: 2, wildcard: 1 },
};

// Strategy-specific copy rules for LLM (from design doc Section 12)
const STRATEGY_COPY_RULES = {
  curiosity_hook: {
    goal: 'clicks',
    rules: `Goal: Maximize clicks using curiosity.
- Create an open loop (do NOT reveal full value)
- Use emotional or surprising language
- Make the user feel they must click
- Avoid generic phrases
Overlay text: Short and intriguing (5–8 words)
Image prompt: High contrast, strong focal point, close-up or dramatic subject`,
  },
  list_value: {
    goal: 'saves',
    rules: `Goal: Maximize saves.
- Use numbers (e.g. 5, 7, 10)
- Clearly communicate value
- Make it feel actionable and useful
Overlay text: Include number + benefit
Image prompt: Structured composition, multiple elements or sections, clean and organized layout`,
  },
  lifestyle: {
    goal: 'engagement',
    rules: `Goal: Increase engagement and relatability.
- Use emotional or aspirational tone
- Show real-life usage or scenario
- Avoid aggressive marketing language
Overlay text: Soft, natural phrasing
Image prompt: Real-life setting, natural lighting, human presence if relevant`,
  },
  clean_authority: {
    goal: 'trust',
    rules: `Goal: Build trust and clarity.
- Be direct and useful
- No hype or exaggeration
- Focus on clarity
Overlay text: Straightforward headline
Image prompt: Minimal design, soft tones, plenty of whitespace`,
  },
  mistake_warning: {
    goal: 'clicks',
    rules: `Goal: Trigger urgency and clicks.
- Highlight a mistake or risk
- Use strong phrasing (e.g. "stop", "avoid")
- Create tension
Overlay text: Direct and bold
Image prompt: High contrast, emphasis on text, slightly dramatic tone
IMPORTANT CONSISTENCY:
- If the title or overlay uses words like "mistakes", "doing this wrong", or "things to stop", each item or step MUST be phrased as a mistake or wrong action to avoid (not as a correct tip).
- Do NOT mix a "mistakes" title with positive step-by-step instructions. Either:
  - Phrase each line as a mistake (e.g. "Keeping your fridge too warm"), or
  - Reframe the title to talk about "tips/steps/ways" if the content is positive recommendations.`,
  },
  transformation: {
    goal: 'clicks',
    rules: `Goal: Show clear outcome or result.
- Use before/after framing
- Emphasize improvement or change
Overlay text: "From X to Y" or similar
Image prompt: Split composition or contrast, clearly show difference`,
  },
  wildcard: {
    goal: 'experimental',
    rules: `Goal: Explore a unique or unexpected angle.
- Use a creative or contrarian idea
- Avoid typical phrasing
Overlay text: Bold or unusual
Image prompt: Visually distinct from other pins, creative composition`,
  },
};

/**
 * Enrich article data with LLM-derived content profile.
 * @param {Object} articleData - { title, description, domain, keyword }
 * @param {Object} openai - OpenAI client
 * @returns {Promise<Object>} contentProfile: { topic, content_type, audience, core_value, possible_angles }
 */
async function enrichContentProfile(articleData, openai) {
  const title = articleData?.title || '';
  const description = articleData?.description || '';
  const keyword = articleData?.keyword || '';

  if (!title && !description) {
    return {
      topic: keyword || 'General topic',
      content_type: 'informational',
      audience: 'general readers',
      core_value: title || 'Key insights',
      possible_angles: [],
      niche: 'default',
    };
  }

  const prompt = `Analyze this article metadata and return JSON only (no markdown).

TITLE: ${title}
DESCRIPTION: ${description.slice(0, 500)}
KEYWORD: ${keyword}

Return a JSON object with these exact keys:
- topic: short phrase (what the article is about)
- content_type: one of "listicle", "how_to", "informational", "recipe", "review", "inspirational", "finance", "travel", "self_improvement", "product_review"
- audience: who it's for (e.g. "home cooks", "budget-conscious readers")
- core_value: main benefit in one sentence
- possible_angles: array of 3-5 different angles or hooks for Pinterest pins (short phrases)
- niche: one of "default", "recipe", "finance", "travel", "self_improvement", "product_review" (pick the best fit for content_type)
- emotional_intensity: one of "low", "medium", "high" (how emotionally charged or urgent the content feels)
- visual_potential: one of "low", "high" (e.g. recipe/travel = high; finance/tips = low)

Examples: listicle → default; recipe/food → recipe; money/SEO/business → finance; travel/destinations → travel; productivity/mindset → self_improvement; product review → product_review.`;

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 400,
      temperature: 0.3,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      return {
        topic: parsed.topic || title || keyword || 'General topic',
        content_type: parsed.content_type || 'informational',
        audience: parsed.audience || 'general readers',
        core_value: parsed.core_value || description?.slice(0, 100) || title,
        possible_angles: Array.isArray(parsed.possible_angles) ? parsed.possible_angles : [],
        niche: parsed.niche || 'default',
        emotional_intensity: ['low', 'medium', 'high'].includes(parsed.emotional_intensity) ? parsed.emotional_intensity : 'medium',
        visual_potential: ['low', 'high'].includes(parsed.visual_potential) ? parsed.visual_potential : 'low',
      };
    }
  } catch (e) {
    console.warn('enrichContentProfile error:', e.message || e);
  }

  return {
    topic: title || keyword || 'General topic',
    content_type: 'informational',
    audience: 'general readers',
    core_value: description?.slice(0, 100) || title,
    possible_angles: [],
    niche: 'default',
    emotional_intensity: 'medium',
    visual_potential: 'low',
  };
}

/**
 * Strategy → CTR/Save score heuristics for pin ranking.
 */
const STRATEGY_SCORES = {
  curiosity_hook: { ctr: 90, save: 40 },
  mistake_warning: { ctr: 85, save: 30 },
  transformation: { ctr: 80, save: 50 },
  list_value: { ctr: 60, save: 90 },
  lifestyle: { ctr: 65, save: 70 },
  clean_authority: { ctr: 45, save: 75 },
  wildcard: { ctr: 70, save: 55 },
};

// Layouts that are clearly multi-image or multi-panel
const MULTI_IMAGE_LAYOUTS = new Set([
  'grid_3_images',
  'grid_4_images',
  'stacked_strips',
  'offset_collage_3',
  'circle_cluster_4',
  'step_cards_3',
  'timeline_infographic',
  'before_after',
]);

/**
 * Build strategy mix with dynamic weighting based on content profile.
 * @param {Object} contentProfile - from enrichContentProfile
 * @returns {Object} strategy mix { curiosity_hook: n, list_value: n, ... }
 */
function getWeightedMix(contentProfile) {
  const base = NICHE_MIXES[contentProfile.niche] || NICHE_MIXES.default;
  const mix = { ...base };
  const ct = contentProfile.content_type || 'informational';
  const emotional = contentProfile.emotional_intensity || 'medium';
  const visual = contentProfile.visual_potential || 'low';

  // Listicle → boost list_value, reduce clean
  if (ct === 'listicle') {
    mix.list_value = Math.min(4, (mix.list_value || 0) + 1);
    mix.clean_authority = Math.max(0, (mix.clean_authority || 0) - 1);
  }

  // How-to → boost list_value
  if (ct === 'how_to') {
    mix.list_value = Math.min(4, (mix.list_value || 0) + 1);
  }

  // Inspirational → boost lifestyle
  if (ct === 'inspirational') {
    mix.lifestyle = Math.min(4, (mix.lifestyle || 0) + 1);
  }

  // High visual potential → boost lifestyle
  if (visual === 'high') {
    mix.lifestyle = Math.min(4, (mix.lifestyle || 0) + 1);
  }

  // High emotional intensity → boost curiosity, mistake_warning
  if (emotional === 'high') {
    mix.curiosity_hook = Math.min(4, (mix.curiosity_hook || 0) + 1);
    mix.mistake_warning = Math.min(2, (mix.mistake_warning || 0) + 1);
  }

  // Low emotional → boost clean_authority
  if (emotional === 'low') {
    mix.clean_authority = Math.min(2, (mix.clean_authority || 0) + 1);
  }

  return mix;
}

/**
 * Build strategy plan from content profile.
 * @param {Object} contentProfile - from enrichContentProfile
 * @param {number} count - desired number of pins (default 10)
 * @returns {Array<{ strategy, goal, layoutId }>}
 */
function planStrategies(contentProfile, count = 10) {
  const mix = getWeightedMix(contentProfile);
  const plan = [];

  for (const [strategy, num] of Object.entries(mix)) {
    const layouts = STRATEGY_LAYOUT_MAP[strategy];
    if (!layouts) continue;
    const goal = STRATEGY_COPY_RULES[strategy]?.goal || 'clicks';
    for (let i = 0; i < num && plan.length < count; i++) {
      const layoutId = layouts[i % layouts.length];
      plan.push({ strategy, goal, layoutId });
    }
  }

  // If we're short, fill with default mix
  while (plan.length < count) {
    const defaultPlan = planStrategies({ ...contentProfile, niche: 'default' }, count - plan.length);
    for (const p of defaultPlan) {
      if (plan.length >= count) break;
      plan.push(p);
    }
    if (plan.length === 0) break;
  }

  // Ensure at least a couple of multi-image layouts are present when possible
  let multiCount = plan.filter((p) => MULTI_IMAGE_LAYOUTS.has(p.layoutId)).length;
  const desiredMulti = Math.min(2, count);
  if (multiCount < desiredMulti) {
    // Prefer to add list_value or transformation strategies that map to multi-image layouts
    const candidateStrategies = ['list_value', 'transformation', 'wildcard', 'lifestyle'];
    while (multiCount < desiredMulti && plan.length < count) {
      let added = false;
      for (const strat of candidateStrategies) {
        const layouts = STRATEGY_LAYOUT_MAP[strat];
        if (!layouts) continue;
        const multiLayout = layouts.find((id) => MULTI_IMAGE_LAYOUTS.has(id));
        if (!multiLayout) continue;
        const goal = STRATEGY_COPY_RULES[strat]?.goal || 'clicks';
        plan.push({ strategy: strat, goal, layoutId: multiLayout });
        multiCount += 1;
        added = true;
        break;
      }
      if (!added) break;
    }
  }

  return plan.slice(0, count);
}

/**
 * Get strategy-specific copy guidance for overlay text generation.
 */
function getStrategyCopyGuidance(strategy) {
  return STRATEGY_COPY_RULES[strategy]?.rules || STRATEGY_COPY_RULES.curiosity_hook.rules;
}

/**
 * Simple word overlap similarity (0-1).
 */
function textSimilarity(a, b) {
  if (!a || !b) return 0;
  const wordsA = new Set(String(a).toLowerCase().replace(/[^\w\s]/g, '').split(/\s+/).filter(Boolean));
  const wordsB = new Set(String(b).toLowerCase().replace(/[^\w\s]/g, '').split(/\s+/).filter(Boolean));
  if (wordsA.size === 0 || wordsB.size === 0) return 0;
  let overlap = 0;
  for (const w of wordsA) {
    if (wordsB.has(w)) overlap++;
  }
  return overlap / Math.min(wordsA.size, wordsB.size);
}

/**
 * Detect hook type from headline.
 */
function getHookType(text) {
  const t = (text || '').toLowerCase();
  if (/\?/.test(t) || /^(how|what|why|can|does|is|are|will|should)\b/.test(t)) return 'question';
  if (/\b(stop|avoid|don't|never|warning|mistake)\b/.test(t)) return 'warning';
  if (/\b(\d+)\s*(tips|steps|ways|ideas|methods)\b/.test(t)) return 'list';
  if (/\b(before|after|transform|change)\b/.test(t)) return 'transformation';
  return 'statement';
}

/**
 * Extract leading number from text (e.g. "5 Tips" -> "5", "10 Ways" -> "10").
 */
function getLeadingNumber(text) {
  const m = (text || '').match(/^(\d+)\s*(?:tips?|steps?|ways?|ideas?|methods?)/i);
  return m ? m[1] : null;
}

/**
 * Check diversity of pins – remove near-duplicates. Stricter rules to fix repetition.
 * @param {Array} pins - array of { title, overlayText, strategy, ... }
 * @returns {Array} pins with duplicates removed
 */
function checkDiversity(pins) {
  if (!pins || pins.length <= 1) return pins;

  const normalize = (s) =>
    (s || '')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .replace(/[^\w\s]/g, '')
      .trim();

  const getFirstWords = (s, n = 4) =>
    normalize(s)
      .split(/\s+/)
      .slice(0, n)
      .join(' ');

  const result = [];
  for (const pin of pins) {
    const headline = pin.overlayText?.headline || pin.title || '';
    const title = pin.title || '';
    const combined = `${headline} ${title}`.trim();
    const key = normalize(headline).slice(0, 60);
    const hookType = getHookType(headline);
    const firstWords = getFirstWords(headline);
    const leadingNum = getLeadingNumber(headline) || getLeadingNumber(title);

    let isDuplicate = false;
    for (const existing of result) {
      const existingHeadline = existing.overlayText?.headline || existing.title || '';
      const existingTitle = existing.title || '';
      const existingCombined = `${existingHeadline} ${existingTitle}`.trim();
      const existingFirstWords = getFirstWords(existingHeadline);
      const existingLeadingNum = getLeadingNumber(existingHeadline) || getLeadingNumber(existingTitle);

      if (normalize(existingHeadline).slice(0, 60) === key) {
        isDuplicate = true;
        break;
      }
      if (textSimilarity(headline, existingHeadline) > 0.5) {
        isDuplicate = true;
        break;
      }
      if (textSimilarity(combined, existingCombined) > 0.55) {
        isDuplicate = true;
        break;
      }
      if (getHookType(existingHeadline) === hookType && existingFirstWords === firstWords) {
        isDuplicate = true;
        break;
      }
      if (pin.strategy === existing.strategy && leadingNum && leadingNum === existingLeadingNum) {
        isDuplicate = true;
        break;
      }
    }
    if (!isDuplicate) result.push(pin);
  }

  return result;
}

/**
 * Score and rank pins by CTR/save potential. Best pins first.
 * @param {Array} pins - array of { strategy, goal, ... }
 * @returns {Array} pins sorted by combined score (best first)
 */
function rankPins(pins) {
  if (!pins || pins.length <= 1) return pins;
  const scored = pins.map((pin) => {
    const s = STRATEGY_SCORES[pin.strategy] || { ctr: 50, save: 50 };
    const combined = (s.ctr * 0.6 + s.save * 0.4);
    return { ...pin, _ctr_score: s.ctr, _save_score: s.save, _combined: combined };
  });
  scored.sort((a, b) => (b._combined || 0) - (a._combined || 0));
  return scored.map(({ _ctr_score, _save_score, _combined, ...p }) => ({ ...p, ctr_score: _ctr_score, save_score: _save_score }));
}

/** Strategy → "Why this works" copy for in-product trust. */
const STRATEGY_REASONS = {
  curiosity_hook: 'Uses curiosity gap to increase clicks',
  list_value: 'Numbers and clear value drive saves',
  lifestyle: 'Emotional, relatable content boosts engagement',
  clean_authority: 'Trust and clarity encourage action',
  mistake_warning: 'Urgency and risk trigger clicks',
  transformation: 'Before/after framing drives interest',
  wildcard: 'Unexpected angles stand out in the feed',
};

function getStrategyReason(strategy) {
  return STRATEGY_REASONS[strategy] || 'Optimized for Pinterest performance';
}

const LAYOUT_NUMBER_RULES = {
  step_cards_3: 'CRITICAL: This layout shows exactly 3 steps. Use "3" in the title (e.g. "3 Steps to...", "3 Tips for...") and overlay (Step 1, Step 2, Step 3). Return step_count: 3.',
  timeline_infographic: 'CRITICAL: This layout shows exactly 5 steps. Use "5" in the title (e.g. "5 Steps to...", "5 Tips...", "5 Ways to...") and overlay. Return step_count: 5. Title, overlay, and image MUST all show exactly 5 steps.',
  grid_3_images: 'CRITICAL: This layout shows exactly 3 images. Use "3" in the title (e.g. "3 Ways to...", "3 Ideas for..."). Return step_count: 3.',
  grid_4_images: 'CRITICAL: This layout shows exactly 4 images. Use "4" in the title (e.g. "4 Tips...", "4 Methods..."). Return step_count: 4.',
};

/** Ensure numbers before Steps/Tips/Ways/etc match stepCount (title/overlay consistency with image). */
function normalizeNumberInText(text, stepCount) {
  if (!text || typeof stepCount !== 'number') return text;
  return String(text).replace(/\b\d+\s*(Steps?|Tips?|Ways?|Ideas?|Methods?)/gi, `${stepCount} $1`);
}

const ANGLE_OPTIONS = ['mistake', 'beginner', 'advanced', 'time-saving', 'emotional', 'secret', 'warning', 'benefit'];

/**
 * Pick an angle based on strategy, content profile, and already used angles.
 * Keeps variation while staying intentional.
 */
function pickAngle(strategy, contentProfile, usedAngles = []) {
  const basePoolByStrategy = {
    curiosity_hook: ['secret', 'mistake', 'warning', 'emotional'],
    list_value: ['benefit', 'beginner', 'time-saving'],
    lifestyle: ['emotional', 'benefit'],
    clean_authority: ['advanced', 'benefit'],
    mistake_warning: ['mistake', 'warning', 'secret'],
    transformation: ['benefit', 'advanced', 'time-saving'],
    wildcard: ['secret', 'emotional', 'warning'],
  };

  let pool = basePoolByStrategy[strategy] || ANGLE_OPTIONS;

  if (contentProfile?.emotional_intensity === 'high') {
    const emotionalFavored = ['emotional', 'mistake', 'secret', 'warning'];
    pool = pool.filter((a) => emotionalFavored.includes(a)).concat(pool);
  }

  const unused = pool.filter((a) => !usedAngles.includes(a));
  const candidates = unused.length > 0 ? unused : pool;

  const chosen = candidates.find((a) => ANGLE_OPTIONS.includes(a)) || ANGLE_OPTIONS[0];
  return chosen;
}

const BASE_TEMPLATE = `Generate a Pinterest pin.

ARTICLE:
{{article_summary}}

KEY IDEAS (use these concretely in copy and visuals):
{{article_key_ideas}}

TARGET KEYWORD:
{{keyword}}

STRATEGY:
{{strategy}}
{{layout_rule}}

ANGLE (use this perspective): {{suggested_angle}}
- mistake: "Most people get this wrong"
- beginner: "If you're new to..."
- advanced: "Pro tip", "Expert-level"
- time-saving: "5 minutes", "Quick hack"
- emotional: "I was shocked", "Game-changer"
- secret: "Hidden trick", "Nobody talks about"
- warning: "Stop doing X", "Avoid this"
- benefit: "Get more X", "Save money"

Follow the strategy rules below. Use the angle to give a distinct PERSPECTIVE - not just different wording.

Also return:
- reason: one short sentence (max 80 chars) explaining why THIS pin works for Pinterest (e.g. "Creates curiosity without revealing the answer", "Numbers signal clear value for saves")

Return JSON only (no markdown) with these exact keys:
- title: catchy pin title (max 100 chars)
- overlay_headline: main on-image text (max 60 chars, 5-10 words)
- overlay_subheadline: supporting on-image line (max 80 chars, optional)
- description: pin description with 4-6 hashtags at end (max 450 chars)
- hashtags: array of 10-20 relevant hashtags
- image_prompt_hint: short hint for image composition (1-2 sentences, used with layout)
- step_count: (optional) number of steps/images to show - MUST match the number used in title and overlay
- angle: one of mistake, beginner, advanced, time-saving, emotional, secret, warning, benefit
- reason: one short sentence why this pin works for Pinterest (max 80 chars)

IMPORTANT:
- Ensure semantic consistency between the title and the type of items you imply in the copy.
- If the title uses "mistakes", "things you're doing wrong", or similar, the content (overlay and implied steps/items) MUST describe mistakes or wrong behaviors to avoid, not correct tips.
- If the content is positive recommendations (tips/steps/ways), then the title should use positive framing (tips/steps/ways) instead of "mistakes".
No markdown, no code fences.`;

/**
 * Extract 3–5 key ideas from an article summary.
 * @param {string} articleSummary
 * @param {Object} openai
 * @returns {Promise<string[]>}
 */
async function extractArticleKeyIdeas(articleSummary, openai) {
  const text = (articleSummary || '').trim();
  if (!text) return [];

  const prompt = `You are helping generate Pinterest pins for an article.

ARTICLE SUMMARY:
${text.slice(0, 1500)}

From this summary, extract the 3–5 most important concrete ideas, problems, solutions, or entities that should appear in visuals and headlines.

Return JSON only (no markdown) with this exact shape:
{ "key_ideas": ["idea 1", "idea 2", "idea 3"] }`;

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 250,
      temperature: 0.4,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      if (Array.isArray(parsed.key_ideas)) {
        return parsed.key_ideas
          .map((s) => String(s).trim())
          .filter(Boolean)
          .slice(0, 5);
      }
    }
  } catch (e) {
    console.warn('extractArticleKeyIdeas error:', e.message || e);
  }

  return [];
}

/**
 * Generate full pin metadata (title, overlay, description, hashtags, image hint) using strategy rules.
 * @param {Object} params
 * @param {string} params.articleSummary
 * @param {string} params.keyword
 * @param {string} params.strategy
 * @param {string} [params.layoutId] - layout ID for number consistency (step_count)
 * @param {string[]} [params.keyIdeas] - optional list of key ideas for this article
 * @param {Object} openai
 * @returns {Promise<Object>} { title, overlay_headline, overlay_subheadline, description, hashtags, image_prompt_hint, step_count }
 */
async function generateStrategicPinMetadata(
  { articleSummary, keyword, strategy, layoutId, suggestedAngle, keyIdeas },
  openai
) {
  const rules = STRATEGY_COPY_RULES[strategy]?.rules || STRATEGY_COPY_RULES.curiosity_hook.rules;
  const strategyLabel = strategy.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  const layoutRule = LAYOUT_NUMBER_RULES[layoutId] ? `\nLAYOUT (${layoutId}):\n${LAYOUT_NUMBER_RULES[layoutId]}` : '';
  const angle = ANGLE_OPTIONS.includes(suggestedAngle) ? suggestedAngle : ANGLE_OPTIONS[0];

  const ideasList = Array.isArray(keyIdeas) && keyIdeas.length
    ? keyIdeas.map((s) => `- ${s}`).join('\n')
    : '';

  const prompt = BASE_TEMPLATE
    .replace('{{article_summary}}', articleSummary.slice(0, 600))
    .replace('{{article_key_ideas}}', ideasList || '- (not provided)')
    .replace('{{keyword}}', keyword || '')
    .replace('{{strategy}}', strategyLabel)
    .replace('{{layout_rule}}', layoutRule)
    .replace('{{suggested_angle}}', angle)
    + `\n\nSTRATEGY RULES:\n${rules}\n\nReturn JSON only.`;

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 500,
      temperature: 0.8,
    });
    const raw = completion.choices?.[0]?.message?.content?.trim() || '';
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      const hashtags = Array.isArray(parsed.hashtags) ? parsed.hashtags : [];
      const layoutDefaults = { step_cards_3: 3, grid_3_images: 3, grid_4_images: 4, timeline_infographic: 5 };
      const stepCount = typeof parsed.step_count === 'number' ? parsed.step_count : (layoutId ? layoutDefaults[layoutId] : null);
      let title = (parsed.title || '').slice(0, 100).trim();
      let overlay_headline = (parsed.overlay_headline || parsed.overlay_text || '').slice(0, 120).trim();
      if (typeof stepCount === 'number') {
        title = normalizeNumberInText(title, stepCount);
        overlay_headline = normalizeNumberInText(overlay_headline, stepCount);
      }
      const angle = ANGLE_OPTIONS.includes(parsed.angle) ? parsed.angle : (ANGLE_OPTIONS.includes(suggestedAngle) ? suggestedAngle : ANGLE_OPTIONS[0]);
      const reason = (parsed.reason || '').slice(0, 100).trim() || getStrategyReason(strategy);
      return {
        title,
        overlay_headline,
        overlay_subheadline: (parsed.overlay_subheadline || '').slice(0, 140).trim(),
        description: (parsed.description || '').slice(0, 450).trim(),
        hashtags: hashtags.slice(0, 20),
        image_prompt_hint: (parsed.image_prompt_hint || '').slice(0, 300).trim(),
        step_count: stepCount ?? parsed.step_count,
        angle,
        reason,
      };
    }
  } catch (e) {
    console.warn('generateStrategicPinMetadata error:', e.message || e);
  }

  return {
    title: articleSummary.slice(0, 80) || 'Pinterest pin',
    overlay_headline: keyword || 'Click to learn more',
    overlay_subheadline: '',
    description: articleSummary.slice(0, 400) || '',
    hashtags: [],
    image_prompt_hint: 'High quality Pinterest pin, clear focal point',
    step_count: { step_cards_3: 3, grid_3_images: 3, grid_4_images: 4, timeline_infographic: 5 }[layoutId] ?? null,
    angle: 'benefit',
    reason: getStrategyReason(strategy),
  };
}

export {
  enrichContentProfile,
  planStrategies,
  getStrategyCopyGuidance,
  getStrategyReason,
  checkDiversity,
  rankPins,
  generateStrategicPinMetadata,
  extractArticleKeyIdeas,
  pickAngle,
  STRATEGY_LAYOUT_MAP,
  STRATEGY_COPY_RULES,
  NICHE_MIXES,
};
