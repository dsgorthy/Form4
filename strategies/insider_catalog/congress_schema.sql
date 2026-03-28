CREATE TABLE IF NOT EXISTS politicians (
    politician_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    chamber TEXT NOT NULL,  -- 'House' or 'Senate'
    state TEXT,
    party TEXT,  -- 'D', 'R', 'I'
    district TEXT,
    committees TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(name_normalized, chamber)
);

CREATE TABLE IF NOT EXISTS congress_trades (
    congress_trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    politician_id INTEGER NOT NULL REFERENCES politicians(politician_id),
    ticker TEXT NOT NULL,
    company TEXT,
    asset_type TEXT NOT NULL DEFAULT 'stock',
    trade_type TEXT NOT NULL,  -- 'buy', 'sell', 'exchange'
    trade_date TEXT NOT NULL,
    trade_date_start TEXT,
    trade_date_end TEXT,
    value_low INTEGER,
    value_high INTEGER,
    value_estimate INTEGER,
    filing_date TEXT,
    filing_delay_days INTEGER,
    owner TEXT,  -- 'Self', 'Spouse', 'Joint', 'Child'
    report_url TEXT,
    source TEXT NOT NULL DEFAULT 'senate_watcher',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(politician_id, ticker, trade_type, trade_date, value_low)
);

CREATE INDEX IF NOT EXISTS idx_congress_trades_ticker ON congress_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_congress_trades_politician ON congress_trades(politician_id);
CREATE INDEX IF NOT EXISTS idx_congress_trades_date ON congress_trades(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_congress_trades_filing ON congress_trades(filing_date DESC);
