// opportunity_miner.js
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import OpenAI from 'openai';
import { createObjectCsvWriter } from 'csv-writer';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const CONFIG_PATH = path.join(__dirname, 'config.json');
if (!fs.existsSync(CONFIG_PATH)) {
  console.error('Missing config.json next to opportunity_miner.js');
  process.exit(1);
}
const CONFIG = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf-8'));

// ---------- brand helpers ----------
const slugLower = (s='') =>
  s.normalize('NFKD').replace(/[\u0300-\u036f]/g,'').replace(/[^A-Za-z0-9]+/g,'').toLowerCase();

const pascalKey = (s='') =>
  s.split(/[^A-Za-z0-9]+/).filter(Boolean)
   .map(w=>w[0].toUpperCase()+w.slice(1).toLowerCase()).join('');

const resolveBrandKey = (name, cfg=CONFIG) => {
  const target = slugLower(String(name||''));
  const keys = new Set([
    ...Object.keys(cfg.inputFiles || {}),
    ...Object.keys(cfg.domains || {}),
    ...Object.keys(cfg.competitors || {}),
    ...(cfg.brands || [])
  ]);
  for (const k of keys) {
    const norm = k.normalize('NFKD').replace(/[\u0300-\u036f]/g,'').replace(/[^A-Za-z0-9]+/g,'').toLowerCase();
    if (norm === target) return k;
  }
  const k = pascalKey(String(name||''));
  return k || String(name||'').replace(/\s+/g,'');
};

const brandVariants = (name) => {
  const disp = String(name||'');
  const key  = resolveBrandKey(disp);
  const compact = disp.replace(/[^A-Za-z0-9]+/g,'');
  return { disp, key, compact };
};

// ---- URL helpers (avoid homepage fallbacks, clean candidates) ----
function normalizeUrl(u='') {
  try { return new URL(u).toString().replace(/\/+$/,'/'); } catch { return ''; }
}
function isHttp(u=''){ return /^https?:\/\//i.test(u); }
function isRootUrl(u='') {
  try {
    const url = new URL(u);
    return (url.pathname === '/' || url.pathname === '') && !url.search && !url.hash;
  } catch { return false; }
}
function uniq(arr){ return Array.from(new Set(arr.filter(Boolean))); }
function splitLinks(s=''){
  // metrics.links are '; ' separated; also accept whitespace
  return uniq(
    String(s).split(/;\s*|\s+/).map(x=>x.trim()).filter(x=>isHttp(x)).map(normalizeUrl)
  );
}
function filterBrandCandidates(links=[], brandDomain=''){
  const domain = (brandDomain||'').replace(/\/+$/,'');
  return links.filter(u => {
    if (!isHttp(u)) return false;
    if (isRootUrl(u)) return false;                 // drop homepage
    if (domain && !u.includes(domain)) return false;// keep only own domain
    return true;
  });
}

function getHostname(u=''){ try { return new URL(u).hostname.toLowerCase(); } catch { return ''; } }
function domainMatches(u='', domain=''){
  if (!u || !domain) return false;
  const hu = getHostname(u);
  const hd = domain.replace(/^https?:\/\//,'').replace(/\/.*$/,'').toLowerCase();
  return hu === hd || hu.endsWith(`.${hd}`);
}



// ---------- CLI args ----------
let brandArg = null, providerArg = 'chatgpt', dateArg = null;
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === '--brand' && process.argv[i+1]) { brandArg = process.argv[++i]; continue; }
  if (a.startsWith('brand=')) { brandArg = a.slice(6); continue; }
  if (a === '--provider' && process.argv[i+1]) { providerArg = process.argv[++i]; continue; }
  if (a.startsWith('provider=')) { providerArg = a.slice(9); continue; }
  if (a === '--date' && process.argv[i+1]) { dateArg = process.argv[++i]; continue; }
  if (a.startsWith('date=')) { dateArg = a.slice(5); continue; }
}
if (!brandArg) {
  console.error('Usage: node opportunity_miner.js --brand "Your Brand" [--provider chatgpt] [--date <ISO>]');
  process.exit(1);
}
const { disp: BRAND_DISP, key: BRAND_KEY, compact: BRAND_COMPACT } = brandVariants(brandArg);

// ---------- paths ----------
const DB_MAIN_PATH = path.join(__dirname, 'output', 'llmreport.db');      // existing DB
const DB_OPP_PATH  = path.join(__dirname, 'output', 'opportunities.db');  // NEW DB
const MODEL        = process.env.LLM_MODEL || 'gpt-4o-mini';
const openai       = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ---------- db helpers ----------
async function openDb(dbPath) {
  return open({ filename: dbPath, driver: sqlite3.Database });
}

async function ensureOppSchema(oppDb) {
  await oppDb.exec(`
    CREATE TABLE IF NOT EXISTS opportunity_insights (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      brand TEXT NOT NULL,
      provider TEXT NOT NULL,
      run_date TEXT NOT NULL,
      keyword TEXT NOT NULL,
      prompt TEXT NOT NULL,
      opportunity_type TEXT NOT NULL CHECK(opportunity_type IN ('Ranking','Mention')),
      content_update_type TEXT NOT NULL CHECK(content_update_type IN ('Optimization','Net New')),
      suggested_brand_url TEXT NOT NULL DEFAULT '',
      example_competitor_url TEXT NOT NULL DEFAULT '',
      msv INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE (brand, provider, run_date, keyword, prompt)
    );
    CREATE INDEX IF NOT EXISTS idx_opp_brand_date ON opportunity_insights (brand, provider, run_date);
    CREATE INDEX IF NOT EXISTS idx_opp_type ON opportunity_insights (opportunity_type, content_update_type);
  `);
}

async function metricsHasMSV(mainDb) {
  const cols = await mainDb.all(`PRAGMA table_info(metrics)`);
  return cols.some(c => c.name.toLowerCase() === 'msv');
}

async function getMSV(mainDb, provider, runDate, keyword) {
  // Prefer metrics.msv if present; otherwise try a keywords table; else null
  if (await metricsHasMSV(mainDb)) {
    const r = await mainDb.get(
      `SELECT MAX(msv) AS msv
         FROM metrics
        WHERE provider = ? AND run_at = ? AND keyword = ?`,
      [provider, runDate, keyword]
    );
    if (r && Number.isFinite(r.msv)) return r.msv;
  }
  // fallback: keywords(msv) if exists
  try {
    const r2 = await mainDb.get(
      `SELECT msv FROM keywords WHERE keyword = ? LIMIT 1`,
      [keyword]
    );
    if (r2 && Number.isFinite(r2.msv)) return r2.msv;
  } catch { /* table may not exist */ }
  return null;
}

async function pickLatestRun(mainDb) {
  if (dateArg) return dateArg;
  const rows = await mainDb.all(
    `SELECT DISTINCT run_at
       FROM metrics
      WHERE provider = ?
        AND (brand = ? OR brand = ?)
      ORDER BY run_at DESC`,
    [providerArg, BRAND_DISP, BRAND_COMPACT]
  );
  if (!rows.length) throw new Error(`No runs found for ${BRAND_DISP} (${providerArg}).`);
  return rows[0].run_at;
}

async function getRankingOpps(db, date) {
  const rows = await db.all(
    `SELECT m1.keyword, m1.question
       FROM metrics m1
      WHERE m1.provider = ?
        AND m1.run_at   = ?
        AND (m1.brand = ? OR m1.brand = ?)
        AND m1.rank     < 10
        AND EXISTS (
          SELECT 1 FROM metrics m2
           WHERE m2.provider = m1.provider
             AND m2.run_at   = m1.run_at
             AND m2.question = m1.question
             AND m2.brand    != m1.brand
             AND m2.rank     < m1.rank
        )
        AND m1.question != 'Keyword Performance'
      ORDER BY m1.keyword, m1.question`,
    [providerArg, date, BRAND_DISP, BRAND_COMPACT]
  );
  return rows.map(r => ({ ...r, opportunityType: 'Ranking' }));
}

async function getMentionOpps(db, date) {
  const rows = await db.all(
    `SELECT m1.keyword, m1.question
       FROM metrics m1
  JOIN metrics AS m2
         ON m2.provider = m1.provider
        AND m2.run_at   = m1.run_at
        AND m2.keyword  = m1.keyword
        AND m2.question = m1.question
        AND m2.mentions > 0
        AND m2.brand   != ? AND m2.brand != ?
      WHERE m1.provider = ? AND m1.run_at = ?
        AND (m1.brand = ? OR m1.brand = ?)
        AND m1.mentions = 0
        AND m1.question != 'Keyword Performance'
      GROUP BY m1.keyword, m1.question
      ORDER BY m1.keyword, m1.question`,
    [BRAND_DISP, BRAND_COMPACT, providerArg, date, BRAND_DISP, BRAND_COMPACT]
  );
  return rows.map(r => ({ ...r, opportunityType: 'Mention' }));
}

async function getRawAnswer(db, date, keyword, question) {
  const row = await db.get(
    `SELECT raw_answer
       FROM raw_responses
      WHERE provider = ?
        AND run_at   = ?
        AND keyword  = ?
        AND question = ?`,
    [providerArg, date, keyword, question]
  );
  return row?.raw_answer || '';
}

async function getLinksFor(db, date, keyword, question, brandName) {
  const row = await db.get(
    `SELECT links
       FROM metrics
      WHERE provider = ?
        AND run_at   = ?
        AND keyword  = ?
        AND question = ?
        AND brand    = ?
      LIMIT 1`,
    [providerArg, date, keyword, question, brandName]
  );
  return row?.links || '';
}

async function getTopCompetitorLinks(db, date, keyword, question, excludeBrand) {
  const row = await db.get(
    `SELECT brand, links
       FROM metrics
      WHERE provider = ?
        AND run_at   = ?
        AND keyword  = ?
        AND question = ?
        AND brand   != ?
      ORDER BY rank ASC
      LIMIT 1`,
    [providerArg, date, keyword, question, excludeBrand]
  );
  return { brand: row?.brand || '', links: row?.links || '' };
}

async function classifyWithLLM({ prompt, rawAnswer, brandDomain, location }) {
  const system = `You are an SEO strategist working in ${location}.
Return ONLY one JSON object exactly like:
{
  "contentUpdateType": "Optimization" | "Net New",
  "suggestedBrandUrl": "string",     // a real URL on the brand's domain or ""
  "exampleCompetitorUrl": "string"   // a real competitor URL (not the brand) or ""
}
Research rules:
- Try to recall or infer the most relevant EXISTING page on the brand's domain (${brandDomain}) that answers the prompt.
- If you cannot find a clearly relevant page on the brand's domain, set contentUpdateType = "Net New" and suggestedBrandUrl = "".
- For the competitor URL, return a credible page from a major competitor that answers the prompt. Do NOT use the brand's domain.
- NEVER invent obviously fake paths, NEVER return a plain homepage or domain-only URL unless the homepage uniquely answers the query.
- Prefer deep URLs that directly address the query intent.`;

  const userPayload = {
    prompt,
    rawAnswerSnippet: String(rawAnswer||'').slice(0, 4000),
    brandDomain
  };

  const resp = await openai.chat.completions.create({
    model: MODEL,
    temperature: 0.2,
    messages: [
      { role: 'system', content: system },
      { role: 'user',   content: JSON.stringify(userPayload) }
    ]
  });

  let out = { contentUpdateType: 'Net New', suggestedBrandUrl: '', exampleCompetitorUrl: '' };
  try {
    const parsed = JSON.parse(resp.choices[0].message.content.trim());
    out = {
      contentUpdateType: (parsed.contentUpdateType === 'Optimization' ? 'Optimization' : 'Net New'),
      suggestedBrandUrl: String(parsed.suggestedBrandUrl||'').trim(),
      exampleCompetitorUrl: String(parsed.exampleCompetitorUrl||'').trim()
    };
  } catch { /* keep defaults */ }

  // ---- Post-guards (brand URL must be brand domain & not homepage) ----
  if (out.contentUpdateType === 'Optimization') {
    const okDomain = domainMatches(out.suggestedBrandUrl, brandDomain);
    if (!okDomain || isRootUrl(out.suggestedBrandUrl) || !isHttp(out.suggestedBrandUrl)) {
      out.contentUpdateType = 'Net New';
      out.suggestedBrandUrl = '';
    }
  } else {
    out.suggestedBrandUrl = '';
  }

  // ---- Competitor URL must NOT be brand domain or homepage ----
  if (!out.exampleCompetitorUrl || !isHttp(out.exampleCompetitorUrl)
      || domainMatches(out.exampleCompetitorUrl, brandDomain)
      || isRootUrl(out.exampleCompetitorUrl)) {
    out.exampleCompetitorUrl = '';
  }

  return out;
}

const nowStamp = () => new Date().toISOString().replace(/[-:TZ]/g,'').slice(0,14);

async function main() {
  const mainDb = await openDb(DB_MAIN_PATH);
  const oppDb  = await openDb(DB_OPP_PATH);
  await ensureOppSchema(oppDb);

  const runDate    = dateArg || await pickLatestRun(mainDb);
  const location   = CONFIG.location || 'Canada';
  const brandDomain = (CONFIG.domains && CONFIG.domains[BRAND_KEY]) || '';

  const ranking = await getRankingOpps(mainDb, runDate);
  const mention = await getMentionOpps(mainDb, runDate);
  const all     = [...ranking, ...mention];

  const results = [];
  for (const row of all) {
    const { keyword, question, opportunityType } = row;

    const [rawAnswer, brandLinks1, brandLinks2, topComp] = await Promise.all([
      getRawAnswer(mainDb, runDate, keyword, question),
      getLinksFor(mainDb, runDate, keyword, question, BRAND_DISP),
      getLinksFor(mainDb, runDate, keyword, question, BRAND_COMPACT),
      getTopCompetitorLinks(mainDb, runDate, keyword, question, BRAND_DISP)
    ]);

	const llm = await classifyWithLLM({
  prompt: question,
  rawAnswer,
  brandDomain,
  location
});

    const msv = await getMSV(mainDb, providerArg, runDate, keyword);

    const rec = {
      brand: BRAND_DISP,
      provider: providerArg,
      run_date: runDate,
      keyword,
      prompt: question,
      opportunity_type: opportunityType,                              // Ranking | Mention
      content_update_type: llm.contentUpdateType || 'Optimization',   // Optimization | Net New
      suggested_brand_url: llm.suggestedBrandUrl || '',
      example_competitor_url: llm.exampleCompetitorUrl || '',
      msv
    };

    // UPSERT into opportunities.db
    await oppDb.run(
      `INSERT INTO opportunity_insights
       (brand, provider, run_date, keyword, prompt, opportunity_type, content_update_type,
        suggested_brand_url, example_competitor_url, msv)
       VALUES (?,?,?,?,?,?,?,?,?,?)
       ON CONFLICT(brand, provider, run_date, keyword, prompt)
       DO UPDATE SET
         opportunity_type = excluded.opportunity_type,
         content_update_type = excluded.content_update_type,
         suggested_brand_url = excluded.suggested_brand_url,
         example_competitor_url = excluded.example_competitor_url,
         msv = excluded.msv`,
      [rec.brand, rec.provider, rec.run_date, rec.keyword, rec.prompt, rec.opportunity_type,
       rec.content_update_type, rec.suggested_brand_url, rec.example_competitor_url, rec.msv]
    );

    results.push({
      Prompt: rec.prompt,
      OpportunityType: rec.opportunity_type,
      ContentUpdateType: rec.content_update_type,
      SuggestedExistingUrl: rec.suggested_brand_url,
      ExampleCompetitorUrl: rec.example_competitor_url,
      Keyword: rec.keyword,
      MSV: rec.msv ?? '',
      Provider: rec.provider,
      RunDate: rec.run_date
    });
  }

  // CSV output (still useful)
  const outDir = path.join(__dirname, 'output');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `${slugLower(BRAND_DISP)}-opportunities-${nowStamp()}.csv`);
  const csvWriter = createObjectCsvWriter({
    path: outPath,
    header: [
      { id: 'Prompt',               title: 'Prompt' },
      { id: 'OpportunityType',      title: 'Opportunity Type' },
      { id: 'ContentUpdateType',    title: 'Content Update Type' },
      { id: 'SuggestedExistingUrl', title: 'Suggested existing URL for optimization' },
      { id: 'ExampleCompetitorUrl', title: 'Example URL from competitor for net new content' },
      { id: 'Keyword',              title: 'Keyword' },
      { id: 'MSV',                  title: 'MSV' },
      { id: 'Provider',             title: 'Provider' },
      { id: 'RunDate',              title: 'Run Date' }
    ]
  });
  await csvWriter.writeRecords(results);

  console.log(`✅ Stored ${results.length} rows in opportunities.db and wrote CSV:\n${outPath}`);
  await mainDb.close();
  await oppDb.close();
}

main().catch(err => {
  console.error('❌ Error:', err);
  process.exit(1);
});
