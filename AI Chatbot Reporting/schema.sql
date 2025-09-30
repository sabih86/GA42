-- schema.sql

-- Table to store raw LLM responses
CREATE TABLE IF NOT EXISTS raw_responses (
id INTEGER PRIMARY KEY AUTOINCREMENT,
provider TEXT NOT NULL,
keyword TEXT NOT NULL,
question TEXT NOT NULL,
raw_answer TEXT,
run_at TEXT NOT NULL
);

-- Table to store parsed metrics
CREATE TABLE IF NOT EXISTS metrics (
id INTEGER PRIMARY KEY AUTOINCREMENT,
provider TEXT NOT NULL,
keyword TEXT NOT NULL,
question TEXT NOT NULL,
brand TEXT NOT NULL,
mentions INTEGER NOT NULL,
rank INTEGER NOT NULL,
sov REAL NOT NULL,
sentiment REAL NOT NULL DEFAULT 0,
links TEXT,
run_at TEXT NOT NULL
);

-- Optional indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_responses_run_at ON raw_responses(run_at);
CREATE INDEX IF NOT EXISTS idx_metrics_run_at ON metrics(run_at);

