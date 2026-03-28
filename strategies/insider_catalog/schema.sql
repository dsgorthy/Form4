-- Insider Catalog — SQLite Schema
-- Tracks every insider, their trades, and computed track records.
-- Enables "follow proven insiders" strategy and enriches cluster buy signals.

-- ─── Core: who is the insider ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS insiders (
    insider_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,           -- full name as filed (e.g. "Benioff Marc")
    name_normalized TEXT    NOT NULL,           -- lowercase, stripped for dedup matching
    cik             TEXT,                       -- SEC CIK if available
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(name_normalized, cik)
);

CREATE INDEX IF NOT EXISTS idx_insiders_name ON insiders(name_normalized);
CREATE INDEX IF NOT EXISTS idx_insiders_cik  ON insiders(cik);


-- ─── Every Form 4 transaction tied to an insider ─────────────────────────────

CREATE TABLE IF NOT EXISTS trades (
    trade_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    insider_id      INTEGER NOT NULL REFERENCES insiders(insider_id),
    ticker          TEXT    NOT NULL,
    company         TEXT,
    title           TEXT,                       -- role at time of trade (CEO, CFO, Dir, 10%, etc.)
    trade_type      TEXT    NOT NULL,           -- 'buy' or 'sell'
    trade_date      TEXT    NOT NULL,           -- YYYY-MM-DD actual transaction date
    filing_date     TEXT    NOT NULL,           -- YYYY-MM-DD SEC filing date
    price           REAL    NOT NULL,
    qty             INTEGER NOT NULL,
    value           REAL    NOT NULL,           -- price * qty (always positive)
    is_csuite       INTEGER NOT NULL DEFAULT 0, -- 1 if C-suite at time of trade
    title_weight    REAL    NOT NULL DEFAULT 1.0,
    source          TEXT    NOT NULL DEFAULT 'edgar_bulk', -- 'edgar_bulk', 'edgar_live', 'openinsider'
    accession       TEXT,                       -- SEC accession number for dedup
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(insider_id, ticker, trade_date, trade_type, value)
);

CREATE INDEX IF NOT EXISTS idx_trades_insider   ON trades(insider_id);
CREATE INDEX IF NOT EXISTS idx_trades_ticker    ON trades(ticker);
CREATE INDEX IF NOT EXISTS idx_trades_date      ON trades(trade_date);
CREATE INDEX IF NOT EXISTS idx_trades_filing    ON trades(filing_date);
CREATE INDEX IF NOT EXISTS idx_trades_type      ON trades(trade_type);
CREATE INDEX IF NOT EXISTS idx_trades_accession ON trades(accession);


-- ─── New columns on trades (populated by Phase A re-parse + Phase B classify) ─
-- These are added via migrate_schema() ALTER TABLE for existing DBs.
-- Listed here for documentation; CREATE TABLE above has the original columns.
--
-- filing_key             TEXT    -- Pre-computed: accession if not null, else trade_date (for GROUP BY)
-- trans_code             TEXT    -- NONDERIV_TRANS.TRANS_CODE (P/S/F/M/A/G/V/X)
-- trans_acquired_disp    TEXT    -- NONDERIV_TRANS.TRANS_ACQUIRED_DISP_CD (A/D)
-- direct_indirect        TEXT    -- NONDERIV_TRANS.DIRECT_INDIRECT_OWNERSHIP (D/I)
-- shares_owned_after     REAL    -- NONDERIV_TRANS.SHRS_OWND_FOLWNG_TRANS
-- value_owned_after      REAL    -- NONDERIV_TRANS.VALU_OWND_FOLWNG_TRANS
-- nature_of_ownership    TEXT    -- NONDERIV_TRANS.NATURE_OF_OWNERSHIP
-- equity_swap            INTEGER -- NONDERIV_TRANS.EQUITY_SWAP_INVOLVED
-- is_10b5_1              INTEGER -- SUBMISSION.AFF10B5ONE
-- security_title         TEXT    -- NONDERIV_TRANS.SECURITY_TITLE
-- deemed_execution_date  TEXT    -- NONDERIV_TRANS.DEEMED_EXECUTION_DATE
-- trans_form_type        TEXT    -- NONDERIV_TRANS.TRANS_FORM_TYPE
-- rptowner_cik           TEXT    -- REPORTINGOWNER.RPTOWNERCIK
-- signal_quality         REAL    -- Computed in Phase B (0.0-1.0)
-- signal_category        TEXT    -- Computed in Phase B
-- is_routine             INTEGER -- Computed in Phase B (0/1)


-- ─── Derivative transactions (option exercises, RSU vesting, etc.) ───────────

CREATE TABLE IF NOT EXISTS derivative_trades (
    deriv_trade_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    insider_id          INTEGER NOT NULL REFERENCES insiders(insider_id),
    ticker              TEXT    NOT NULL,
    company             TEXT,
    title               TEXT,
    trans_code           TEXT,              -- F/M/A/G/V/X
    trans_acquired_disp  TEXT,              -- A/D
    trade_date          TEXT    NOT NULL,
    filing_date         TEXT    NOT NULL,
    security_title      TEXT,              -- e.g. "Employee Stock Option"
    exercise_price      REAL,
    expiration_date     TEXT,
    trans_shares        REAL,
    trans_price_per_share REAL,
    trans_total_value   REAL,
    underlying_title    TEXT,              -- e.g. "Common Stock"
    underlying_shares   REAL,
    underlying_value    REAL,
    shares_owned_after  REAL,
    value_owned_after   REAL,
    direct_indirect     TEXT,
    nature_of_ownership TEXT,
    equity_swap         INTEGER,
    is_10b5_1           INTEGER,
    deemed_execution_date TEXT,
    trans_form_type     TEXT,
    rptowner_cik        TEXT,
    accession           TEXT,
    source              TEXT    NOT NULL DEFAULT 'edgar_bulk',
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(insider_id, ticker, trade_date, trans_code, exercise_price, trans_shares)
);

CREATE INDEX IF NOT EXISTS idx_deriv_insider  ON derivative_trades(insider_id);
CREATE INDEX IF NOT EXISTS idx_deriv_ticker   ON derivative_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_deriv_date     ON derivative_trades(trade_date);
CREATE INDEX IF NOT EXISTS idx_deriv_code     ON derivative_trades(trans_code);
CREATE INDEX IF NOT EXISTS idx_deriv_accession ON derivative_trades(accession);


-- ─── Filing footnotes (10b5-1 disclosures, restrictions, etc.) ──────────────

CREATE TABLE IF NOT EXISTS filing_footnotes (
    footnote_id_pk      INTEGER PRIMARY KEY AUTOINCREMENT,
    accession           TEXT    NOT NULL,
    footnote_id         TEXT    NOT NULL,   -- e.g. "F1", "F2"
    footnote_text       TEXT,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(accession, footnote_id)
);

CREATE INDEX IF NOT EXISTS idx_footnotes_accession ON filing_footnotes(accession);


-- ─── Non-derivative holdings (end-of-period per filing) ─────────────────────

CREATE TABLE IF NOT EXISTS nonderiv_holdings (
    holding_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    accession           TEXT    NOT NULL,
    insider_id          INTEGER REFERENCES insiders(insider_id),
    ticker              TEXT,
    security_title      TEXT,
    shares_owned        REAL,
    value_owned         REAL,
    direct_indirect     TEXT,
    nature_of_ownership TEXT,
    trans_form_type     TEXT,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(accession, security_title, direct_indirect)
);

CREATE INDEX IF NOT EXISTS idx_holdings_accession ON nonderiv_holdings(accession);
CREATE INDEX IF NOT EXISTS idx_holdings_insider   ON nonderiv_holdings(insider_id);
CREATE INDEX IF NOT EXISTS idx_holdings_ticker    ON nonderiv_holdings(ticker);


-- ─── Forward returns: what happened after each trade ─────────────────────────

CREATE TABLE IF NOT EXISTS trade_returns (
    trade_id        INTEGER PRIMARY KEY REFERENCES trades(trade_id),
    entry_price     REAL,                      -- price on entry_date (T+1 open or trade_date close)
    exit_price_7d   REAL,                      -- price 7 trading days after entry
    exit_price_30d  REAL,                      -- price 30 calendar days after entry
    exit_price_90d  REAL,                      -- price 90 calendar days after entry
    return_7d       REAL,                      -- (exit - entry) / entry
    return_30d      REAL,
    return_90d      REAL,
    spy_return_7d   REAL,                      -- SPY return over same period
    spy_return_30d  REAL,
    spy_return_90d  REAL,
    abnormal_7d     REAL,                      -- return_7d - spy_return_7d
    abnormal_30d    REAL,
    abnormal_90d    REAL,
    computed_at     TEXT    NOT NULL DEFAULT (datetime('now'))
);


-- ─── Aggregated track record per insider (refreshed periodically) ────────────

CREATE TABLE IF NOT EXISTS insider_track_records (
    insider_id      INTEGER PRIMARY KEY REFERENCES insiders(insider_id),

    -- Buy-side stats (our primary signal)
    buy_count       INTEGER NOT NULL DEFAULT 0,
    buy_win_rate_7d REAL,                      -- % of buys with positive 7d return
    buy_avg_return_7d   REAL,                  -- mean 7d return across all buys
    buy_median_return_7d REAL,
    buy_avg_abnormal_7d REAL,                  -- mean 7d abnormal return (vs SPY)
    buy_win_rate_30d REAL,
    buy_avg_return_30d  REAL,
    buy_win_rate_90d REAL,
    buy_avg_return_90d  REAL,
    buy_total_value REAL   NOT NULL DEFAULT 0, -- lifetime $ bought
    buy_first_date  TEXT,
    buy_last_date   TEXT,

    -- Sell-side stats (for v2 put leg)
    sell_count      INTEGER NOT NULL DEFAULT 0,
    sell_win_rate_7d REAL,                     -- % of sells followed by 7d price decline
    sell_avg_return_7d  REAL,
    sell_total_value REAL   NOT NULL DEFAULT 0,
    sell_first_date TEXT,
    sell_last_date  TEXT,

    -- Composite score (CEOWatcher-inspired 0-3 scale)
    score           REAL,                      -- 0.0 to 3.0
    score_tier      INTEGER,                   -- 0, 1 (top 33%), 2 (top 20%), 3 (top 7%)
    percentile      REAL,                      -- percentile rank among all insiders with 3+ trades

    -- Metadata
    primary_title   TEXT,                      -- most frequent title across trades
    primary_ticker  TEXT,                      -- ticker they trade most
    n_tickers       INTEGER NOT NULL DEFAULT 0, -- how many different companies
    computed_at     TEXT    NOT NULL DEFAULT (datetime('now'))
);


-- ─── Insider <-> Company association (many-to-many) ──────────────────────────

CREATE TABLE IF NOT EXISTS insider_companies (
    insider_id      INTEGER NOT NULL REFERENCES insiders(insider_id),
    ticker          TEXT    NOT NULL,
    company         TEXT,
    title           TEXT,                      -- most recent title at this company
    trade_count     INTEGER NOT NULL DEFAULT 0,
    total_value     REAL    NOT NULL DEFAULT 0,
    first_trade     TEXT,
    last_trade      TEXT,

    PRIMARY KEY (insider_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_ic_ticker ON insider_companies(ticker);


-- ─── Signals: when we decide to trade alongside a proven insider ─────────────

CREATE TABLE IF NOT EXISTS solo_insider_signals (
    signal_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    insider_id      INTEGER NOT NULL REFERENCES insiders(insider_id),
    trade_id        INTEGER NOT NULL REFERENCES trades(trade_id),
    ticker          TEXT    NOT NULL,
    signal_date     TEXT    NOT NULL,           -- date signal was generated
    insider_score   REAL    NOT NULL,           -- score at time of signal
    insider_tier    INTEGER NOT NULL,           -- tier at time of signal
    buy_count_at_signal INTEGER NOT NULL,       -- how many prior buys at signal time
    win_rate_at_signal  REAL NOT NULL,          -- win rate at signal time
    avg_return_at_signal REAL NOT NULL,         -- avg return at signal time
    trade_value     REAL    NOT NULL,           -- $ value of triggering trade
    status          TEXT    NOT NULL DEFAULT 'pending', -- pending, entered, skipped, expired
    skip_reason     TEXT,
    entry_price     REAL,
    entry_date      TEXT,
    exit_price      REAL,
    exit_date       TEXT,
    exit_reason     TEXT,                       -- time_exit, stop_loss, profit_target
    pnl             REAL,
    pnl_pct         REAL,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_solo_signals_date   ON solo_insider_signals(signal_date);
CREATE INDEX IF NOT EXISTS idx_solo_signals_status ON solo_insider_signals(status);


-- ─── Entity resolution: link entity insiders to controlling individuals ────

CREATE TABLE IF NOT EXISTS insider_groups (
    group_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    primary_insider_id INTEGER NOT NULL REFERENCES insiders(insider_id),
    group_name      TEXT    NOT NULL,
    confidence      REAL    NOT NULL DEFAULT 1.0,
    method          TEXT    NOT NULL,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_groups_primary ON insider_groups(primary_insider_id);

CREATE TABLE IF NOT EXISTS insider_group_members (
    group_id        INTEGER NOT NULL REFERENCES insider_groups(group_id),
    insider_id      INTEGER NOT NULL REFERENCES insiders(insider_id),
    is_primary      INTEGER NOT NULL DEFAULT 0,
    is_entity       INTEGER NOT NULL DEFAULT 0,
    relationship    TEXT,
    PRIMARY KEY (group_id, insider_id)
);
CREATE INDEX IF NOT EXISTS idx_group_members_insider ON insider_group_members(insider_id);


-- ─── Views for quick querying ────────────────────────────────────────────────

-- Top proven insiders (buy side, 3+ trades, positive track record)
CREATE VIEW IF NOT EXISTS v_proven_buyers AS
SELECT
    i.insider_id,
    i.name,
    tr.buy_count,
    tr.buy_win_rate_7d,
    tr.buy_avg_return_7d,
    tr.buy_avg_abnormal_7d,
    tr.buy_total_value,
    tr.score,
    tr.score_tier,
    tr.percentile,
    tr.primary_title,
    tr.primary_ticker,
    tr.n_tickers,
    tr.buy_first_date,
    tr.buy_last_date
FROM insiders i
JOIN insider_track_records tr ON i.insider_id = tr.insider_id
WHERE tr.buy_count >= 3
  AND tr.buy_win_rate_7d > 0.5
  AND tr.buy_avg_return_7d > 0
ORDER BY tr.score DESC;

-- Recent trades by proven insiders (for the "follow" strategy)
CREATE VIEW IF NOT EXISTS v_recent_proven_trades AS
SELECT
    t.trade_id,
    i.name AS insider_name,
    t.ticker,
    t.company,
    t.title,
    t.trade_date,
    t.filing_date,
    t.value,
    tr.score,
    tr.score_tier,
    tr.buy_count,
    tr.buy_win_rate_7d,
    tr.buy_avg_return_7d
FROM trades t
JOIN insiders i ON t.insider_id = i.insider_id
JOIN insider_track_records tr ON i.insider_id = tr.insider_id
WHERE t.trade_type = 'buy'
  AND t.filing_date >= date('now', '-7 days')
  AND tr.score_tier >= 2
ORDER BY t.filing_date DESC, tr.score DESC;


-- ─── Per-insider-per-ticker point-in-time scores (Phase C) ─────────────────

CREATE TABLE IF NOT EXISTS insider_ticker_scores (
    insider_id          INTEGER NOT NULL,
    ticker              TEXT    NOT NULL,
    as_of_date          TEXT    NOT NULL,

    -- Ticker-specific
    ticker_trade_count  INTEGER,
    ticker_win_rate_7d  REAL,
    ticker_avg_abnormal_7d REAL,
    ticker_score        REAL,              -- 0-3 scale

    -- Global (all tickers for this insider)
    global_trade_count  INTEGER,
    global_win_rate_7d  REAL,
    global_avg_abnormal_7d REAL,
    global_score        REAL,              -- 0-3 scale

    -- Blended
    blended_score       REAL,              -- weighted blend
    role_at_ticker      TEXT,
    role_weight         REAL,
    is_primary_company  INTEGER DEFAULT 0,
    sufficient_data     INTEGER DEFAULT 0,

    PRIMARY KEY (insider_id, ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_its_insider ON insider_ticker_scores(insider_id);
CREATE INDEX IF NOT EXISTS idx_its_ticker  ON insider_ticker_scores(ticker);
CREATE INDEX IF NOT EXISTS idx_its_date    ON insider_ticker_scores(as_of_date);
CREATE INDEX IF NOT EXISTS idx_its_blended ON insider_ticker_scores(blended_score);


-- ─── Score evolution tracking ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_history (
    score_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    insider_id          INTEGER NOT NULL,
    ticker              TEXT    NOT NULL,
    as_of_date          TEXT    NOT NULL,
    trigger_trade_id    INTEGER,
    blended_score       REAL,
    global_score        REAL,
    ticker_score        REAL,
    trade_count         INTEGER,
    computed_at         TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sh_insider_ticker ON score_history(insider_id, ticker);
CREATE INDEX IF NOT EXISTS idx_sh_date           ON score_history(as_of_date);


-- ─── Trade context facts (descriptive annotations per trade) ──────────────

CREATE TABLE IF NOT EXISTS trade_context (
    context_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id     INTEGER NOT NULL REFERENCES trades(trade_id),
    context_type TEXT    NOT NULL,
    context_text TEXT,                -- Pre-rendered string (NULL if needs live rendering)
    sort_order   INTEGER NOT NULL DEFAULT 0,
    metadata     TEXT    NOT NULL,    -- JSON structured data (always populated)
    computed_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_id, context_type)
);

CREATE INDEX IF NOT EXISTS idx_tc_trade ON trade_context(trade_id);
CREATE INDEX IF NOT EXISTS idx_tc_type  ON trade_context(context_type);


-- ─── Trade signal tagging (CEOWatcher-inspired taxonomy) ──────────────────

CREATE TABLE IF NOT EXISTS trade_signals (
    signal_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id     INTEGER NOT NULL REFERENCES trades(trade_id),
    signal_type  TEXT    NOT NULL,              -- e.g. buying_the_dip, first_time_buyer
    signal_label TEXT    NOT NULL,              -- human-readable label
    signal_class TEXT    NOT NULL DEFAULT 'bullish',  -- bullish, bearish, noise, neutral
    confidence   REAL    NOT NULL DEFAULT 1.0,  -- 0.0-1.0
    metadata     TEXT,                          -- JSON context
    computed_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(trade_id, signal_type)
);

CREATE INDEX IF NOT EXISTS idx_ts_trade ON trade_signals(trade_id);
CREATE INDEX IF NOT EXISTS idx_ts_type  ON trade_signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_ts_class ON trade_signals(signal_class);
