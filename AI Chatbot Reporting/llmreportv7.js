import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import OpenAI from 'openai';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { createObjectCsvWriter } from 'csv-writer';
import axios from 'axios';

// Load environment variables
dotenv.config();

// ── allow override via CLI: `node llmreportv7.js brand=TD` ────────────
// ── allow override via CLI (handles spaces):
//    node llmreportv7.js brand=Tim Hortons
//    node llmreportv7.js brand="Tim Hortons"
//    node llmreportv7.js --brand "Tim Hortons"
let brandFromCli = null;
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];

  if (a.startsWith('brand=')) {
    brandFromCli = a.slice(6);
    for (let j = i + 1; j < process.argv.length; j++) {
      const next = process.argv[j];
      if (next.startsWith('-') || next.includes('=')) break;
      brandFromCli += ' ' + next;
      i = j;
    }
    break;
  }

  if (a === '--brand') {
    if (i + 1 < process.argv.length) {
      brandFromCli = process.argv[++i];
      while (i + 1 < process.argv.length &&
             !process.argv[i + 1].startsWith('-') &&
             !process.argv[i + 1].includes('=')) {
        brandFromCli += ' ' + process.argv[++i];
      }
    }
    break;
  }
}
if (brandFromCli) {
  process.env.BRAND = brandFromCli.trim().replace(/^["']|["']$/g, '');
}
// Resolve __dirname in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ── load config.json manually ───────────────────────────

const configPath = path.join(__dirname, 'config.json');
const config     = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
// --- helpers (use function declarations so they hoist) ---
function slugLower(s = '') {
  return s
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^A-Za-z0-9]+/g, '')
    .toLowerCase();
}

function pascalKey(s = '') {
  return s
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean)
    .map(w => w[0].toUpperCase() + w.slice(1).toLowerCase())
    .join('');
}

function resolveBrandKey(name, cfg = config) {
  const target = slugLower(String(name || ''));
  const keys = new Set([
    ...Object.keys(cfg.inputFiles || {}),
    ...Object.keys(cfg.domains || {}),
    ...Object.keys(cfg.competitors || {}),
    ...(cfg.brands || []),
  ]);
  for (const k of keys) {
    if (slugLower(k) === target) return k;
  }
  const k = pascalKey(String(name || ''));
  return k || String(name || '').replace(/\s+/g, '');
}
const LOCATION   = (config.location && String(config.location).trim())
  || (process.env.LOCATION && String(process.env.LOCATION).trim())
  || 'Canada';

// ── Perplexity SDK ─────────────────────────────────────
// import Perplexity from 'perplexity‑sdk/dist/index.js';
// you can keep it simple or get the enum from 'perplexity-sdk'
// import { ChatCompletionsPostRequestModelEnum } from 'perplexity-sdk';

// line 17: create an output folder if it doesn’t already exist
const OUTPUT_DIR = path.join(__dirname, 'output');
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// Capture a single timestamp for this run
const RUN_AT = new Date().toISOString();
// Unique, filesystem-safe timestamp for filenames
const TS     = new Date().toISOString().replace(/[-:TZ]/g,'').slice(0,14);
//const BRAND  = process.env.BRAND || config.defaultBrand;
//const BRANDS = [
//  BRAND,
//  ...(config.competitors[BRAND] || [])
//];

// Providers and models
const PROVIDERS    = ['chatgpt', 'gemini','perplexity'];
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-pro-1.5-flash';
const PERPLEXITY_MODEL = process.env.PERPLEXITY_MODEL || 'pplx-7b-chat';

// Gemini endpoint
const GEMINI_ENDPOINT = 'https://generativelanguage.googleapis.com/v1';

// Brands, CTR curve, and domains
// Allow spaces in brand names — resolve config key for lookups
const DISPLAY_BRAND = process.env.BRAND || config.defaultBrand;
const BRAND_SLUG = slugLower(DISPLAY_BRAND);

const BRAND_KEY = resolveBrandKey(DISPLAY_BRAND);
const COMP_KEYS = (config.competitors[BRAND_KEY] || []);
const BRANDS = [
  DISPLAY_BRAND,
  ...COMP_KEYS.map(k => (config.brandDisplayNames && config.brandDisplayNames[k]) || k)
];
const CTR = config.ctr;

// Domains: compute per brand name (display → key → domain)
const domainFor = (name) => {
  const key = resolveBrandKey(name);
  return (config.domains && config.domains[key]) || '';
};

// Initialize OpenAI client
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
// Initialize Perplexity client
//const perplexity = new Perplexity({ apiKey: process.env.PERPLEXITY_API_KEY }).client();

// Locale enforcement prompt
const LOCALE_PROMPT =
   `You are an AI assistant. From now on, assume the user is located in ${LOCATION} ` +
  `and tailor all examples, regulations, pricing, date formats (MM/DD/YYYY), currency (local), etc. accordingly.`;

// Unified LLM caller
async function askLLM(provider, system, user) {
  if (provider === 'chatgpt') {
    const resp = await openai.chat.completions.create({
      model: process.env.LLM_MODEL || 'gpt-4o-mini',
      messages: [
        { role: 'system', content: LOCALE_PROMPT },
        { role: 'system', content: system },
        { role: 'user',   content: user }
      ],
      temperature: 0.7
    });
    return resp.choices[0].message.content.trim();

  } else if (provider === 'gemini') {
    const url = `${GEMINI_ENDPOINT}/models/${GEMINI_MODEL}:generateContent?key=${process.env.GEMINI_API_KEY}`;
    const combined = `${LOCALE_PROMPT}\n${system}\n${user}`;
    const payload = {
      contents: [{ role: 'user', parts: [{ text: combined }] }],
      generationConfig: { temperature: 0.7, candidateCount: 1 }
    };
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const text = await response.text();
    let raw;
    try {
      raw = JSON.parse(text);
    } catch (err) {
      console.error('❌ Failed to parse Gemini JSON, response text:', text);
      return '';
    }
    if (!response.ok) {
      console.error('❌ Gemini HTTP error:', response.status, raw);
      return '';
    }
    return raw.candidates?.[0]?.content?.parts?.[0]?.text?.trim() || '';

  } else if (provider === 'perplexity') {
// stub for dummy key
     if (process.env.PERPLEXITY_API_KEY?.startsWith('dummy')) {
       return `🧪 DUMMY RESPONSE for: ${user}`;
     }
     try {
       const resp = await axios.post(
         'https://api.perplexity.ai/chat',             // verify Perplexity’s REST endpoint
         {
           api_key: process.env.PERPLEXITY_API_KEY,
           model:   process.env.PERPLEXITY_MODEL || 'pplx-7b-chat',
           messages: [
             { role: 'system', content: LOCALE_PROMPT },
             { role: 'system', content: system },
             { role: 'user',   content: user }
           ]
         }
       );
       return resp.data.choices?.[0]?.message?.content.trim() || '';
     } catch (err) {
       console.warn('Perplexity HTTP error:', err.message);
       return '';
     }
   } else {
     throw new Error(`Unknown provider: ${provider}`);
   }
 }

async function getBrandSentiments(provider, answer, brandList) {
  const system = `You are a strict sentiment rater. Rate the sentiment expressed ABOUT EACH brand in the list,
based solely on the PASSAGE below. Return ONLY a minified JSON object whose KEYS match the brand names exactly and
whose VALUES are integers in [-100,100]:
100 = very positive endorsement; 0 = neutral/no opinion; -100 = very negative. If the passage does not express any
opinion about a brand, use 0. Do not include extra keys.`;
  const user = `BRANDS: ${brandList.join(', ')}\nPASSAGE:\n${answer}`;

  const raw = await askLLM(provider, system, user);
  try {
    const obj = JSON.parse(raw);
    const out = {};
    for (const b of brandList) {
      const v = Math.round(Number(obj[b]));
      out[b] = Number.isFinite(v) ? Math.max(-100, Math.min(100, v)) : 0;
    }
    return out;
  } catch {
    console.warn('Sentiment JSON parse failed; defaulting to 0s. Raw:', raw);
    return Object.fromEntries(brandList.map(b => [b, 0]));
  }
}


// Extract links by brand domain
function extractLinks(text) {
  const urls = Array.from(text.matchAll(/https?:\/\/[^")]+/g), m => m[0]);
  const byBrand = {};
  BRANDS.forEach(b => {
  const d = domainFor(b);
  byBrand[b] = d ? urls.filter(u => u.includes(d)).join('; ') : '';
});
  return byBrand;
}

// Parse metrics (v4 logic)
function parseMetrics(answer) {
  const mentions = {};
  BRANDS.forEach(b => {
    mentions[b] = (answer.match(new RegExp(`\\b${b}\\b`, 'gi')) || []).length;
  });
  const positions = {};
  BRANDS.forEach(b => {
    const pos = answer.search(new RegExp(`\\b${b}\\b`, 'i'));
    positions[b] = pos < 0 ? Infinity : pos;
  });
  const sorted = [...BRANDS].sort((a, b) => positions[a] - positions[b]);
  const ranks = {};
  sorted.forEach((b, i) => { ranks[b] = positions[b] === Infinity ? 10 : i + 1; });
  const sov = {};
  BRANDS.forEach(b => { sov[b] = (CTR[ranks[b]] || 0) * 100; });
  const links = extractLinks(answer);
  return BRANDS.map(b => ({ brand: b, mentions: mentions[b], rank: ranks[b], sov: sov[b], links: links[b] }));
}

// Main execution
(async () => {
  // Reset DB
  const DB_PATH = path.join(OUTPUT_DIR, 'llmreport.db');
  const db = await open({ filename: DB_PATH, driver: sqlite3.Database });
  await db.exec(fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf-8'));
  try {
	await db.exec(`ALTER TABLE metrics ADD COLUMN sentiment REAL DEFAULT 0`);
  } catch (e) {
  // ignore if already exists
  }

  // Load tasks from input file
//  const lines = fs.readFileSync(path.join(__dirname, 'llmreportinput.txt'), 'utf-8').split(/\r?\n/);

// pick the right input file for this BRAND (fall back to default)
  const inputFile = config.inputFiles[BRAND_KEY] || config.inputFile;
  const lines = fs.readFileSync(path.join(__dirname, inputFile), 'utf-8').split(/\r?\n/);
  const tasks = [];
  let current = null;
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    const kw = line.match(/^\[(.+)\]\s*-\s*(.+)$/);
    const qm = line.match(/^\[(.+)\]$/);
    if (kw) {
      if (current) tasks.push(current);
      current = { keyword: kw[2].trim(), questions: [] };
    } else if (qm && current) {
      current.questions.push(qm[1].trim());
    }
  }
  if (current) tasks.push(current);

  // Process each provider
  for (const provider of PROVIDERS) {
  console.log(`\n=== Running for provider: ${provider} ===`);
  const txtLines = [];
  const csvRecords = [];

  // one per-provider run prefix
  const filePrefix = `${BRAND_SLUG}-${provider}-${TS}`;

  for (const { keyword, questions } of tasks) {
    console.log(`Processing keyword: ${keyword}`);

    // MSV
    const msvAns = await askLLM(
      provider,
      'You are a data assistant. Provide the approximate monthly search volume (integer only) in ' + LOCATION + ' for the term:',
      keyword
    );
    const MSV = parseInt(msvAns.replace(/\D/g, ''), 10) || 0;
    await db.run(
      `INSERT INTO raw_responses(provider,keyword,question,raw_answer,run_at) VALUES(?,?,?,?,?)`,
      [provider, keyword, 'MSV', msvAns, RUN_AT]
    );
    txtLines.push(`[${keyword}]`, `MSV: ${MSV}`);

    // init per-keyword accumulators
    const perf = { mentions: {}, ranks: {}, SOVs: {}, sentiments: {} };
    BRANDS.forEach(b => { perf.mentions[b] = 0; perf.ranks[b] = []; perf.SOVs[b] = []; perf.sentiments[b] = []; });

    // per-question answers + metrics + sentiment
    for (const q of questions) {
      console.log(`  → Processing question: ${q}`);
      const ans = await askLLM(
        provider,
        'You are a helpful assistant based in ' + LOCATION + '. Answer concisely and include any relevant links.',
        q
      );
      txtLines.push(`[${q}]`, ans, '');

      await db.run(
        `INSERT INTO raw_responses(provider,keyword,question,raw_answer,run_at) VALUES(?,?,?,?,?)`,
        [provider, keyword, q, ans, RUN_AT]
      );

      const metrics = parseMetrics(ans);
      const sentMap = await getBrandSentiments(provider, ans, BRANDS);

      metrics.forEach(m => {
        const s = Number(sentMap[m.brand]) || 0;
        perf.mentions[m.brand]  += m.mentions;
        perf.ranks[m.brand].push(m.rank);
        perf.SOVs[m.brand].push(m.sov);
        perf.sentiments[m.brand].push(s);

        db.run(
          `INSERT INTO metrics(provider,keyword,question,brand,mentions,rank,sov,sentiment,links,run_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
          [provider, keyword, q, m.brand, m.mentions, m.rank, m.sov, s, m.links, RUN_AT]
        );
      });

      // CSV row (per question)
      const row = { Keyword: keyword, Question: q, MSV };
      metrics.forEach(m => {
        row[`${m.brand}_Mentions`]   = m.mentions;
        row[`${m.brand}_Rank`]       = m.rank;
        row[`${m.brand}_SOV`]        = `${m.sov.toFixed(1)}%`;
        row[`${m.brand}_Sentiment`]  = Math.round(Number(sentMap[m.brand]) || 0); // −100..+100 integer
        row[`${m.brand}_Links`]      = m.links;
      });
      csvRecords.push(row);
    } // ← closes questions loop

    // Keyword-level “Keyword Performance”
    const perfRec = { Keyword: keyword, Question: 'Keyword Performance', MSV };
    BRANDS.forEach(b => {
      const avgRank = perf.ranks[b].length
        ? perf.ranks[b].reduce((a, v) => a + v, 0) / perf.ranks[b].length
        : 10;
      const avgSov = perf.SOVs[b].length
        ? perf.SOVs[b].reduce((a, v) => a + v, 0) / perf.SOVs[b].length
        : 0;
      const avgSent = perf.sentiments[b].length
        ? perf.sentiments[b].reduce((a, v) => a + v, 0) / perf.sentiments[b].length
        : 0;

      perfRec[`${b}_Mentions`]  = perf.mentions[b];
      perfRec[`${b}_Rank`]      = avgRank.toFixed(1);
      perfRec[`${b}_SOV`]       = `${avgSov.toFixed(1)}%`;
      perfRec[`${b}_Sentiment`] = Math.round(avgSent); // −100..+100
      perfRec[`${b}_Links`]     = '';

      db.run(
        `INSERT INTO metrics(provider,keyword,question,brand,mentions,rank,sov,sentiment,links,run_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
        [provider, keyword, 'Keyword Performance', b, perf.mentions[b], avgRank, avgSov, avgSent, '', RUN_AT]
      );
    });
    csvRecords.push(perfRec);
    txtLines.push('-'.repeat(50));
  } // ← closes tasks loop

  // write files once per provider
  const csvWriter = createObjectCsvWriter({
    path: path.join(OUTPUT_DIR, `${filePrefix}.csv`),
    header: [
      { id: 'Keyword', title: 'Keyword' },
      { id: 'Question', title: 'Question' },
      { id: 'MSV',      title: 'MSV' },
      ...BRANDS.flatMap(b => ([
        { id: `${b}_Mentions`,  title: `${b} Mentions` },
        { id: `${b}_Rank`,      title: `${b} Rank` },
        { id: `${b}_SOV`,       title: `${b} SOV` },
        { id: `${b}_Sentiment`, title: `${b} Sentiment` },
        { id: `${b}_Links`,     title: `${b} Links` }
      ]))
    ]
  });
  await csvWriter.writeRecords(csvRecords);
  fs.writeFileSync(path.join(OUTPUT_DIR, `${filePrefix}.txt`), txtLines.join('\n'), 'utf-8');
  console.log(`✅ Outputs for ${provider} complete.`);
} // ← closes provider loop

await db.close();
console.log('\n🎉 All providers processed.');
})();
