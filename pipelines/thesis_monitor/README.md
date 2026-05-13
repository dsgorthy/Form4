# Thesis Monitor

Daily monitoring service for the two May 13 2026 investment theses:
1. **Iran / Strait of Hormuz oil convexity** (~$16k of $25k)
2. **Data center supply chain** (~$8.3k of $25k)

Runs on Mac Studio via launchd, weekdays post-close. Uses GLM-4.7-flash via local Ollama for summarization. Sends an email brief via Resend (reuses `api.email`).

## Architecture

```
launchd (1:30 PM PT, weekdays)
   ↓
monitor.py
   ├── positions.py    → reads positions.yaml
   ├── prices.py       → yfinance (no key needed)
   ├── macro.py        → FRED (FRED_API_KEY in .env)
   ├── news.py         → Finnhub + NewsAPI (FINNHUB_API_KEY, NEWSAPI_KEY in .env)
   ├── stops.py        → hardcoded stops from the May 13 PDFs
   ├── ollama_client.py → POST http://localhost:11434 (Studio Ollama)
   └── email_sender.py → reuses api.email.send_email (Resend)
```

## CLI

```bash
# After entering fills with your broker:
thesis-add-fill FRO 165 36.85 --thesis oil
thesis-add-fill FRO 6 2.50 --thesis oil --option \
    --strike 45 --expiry 2027-01-15 --side call
thesis-add-fill MP 30 65.40 --thesis data_center

# Check what's tracked
thesis-list

# Drop a fill (use index from `thesis-list`)
thesis-remove 3
```

## Running the monitor

```bash
# Dry-run (prints to stdout, no email)
python3 -m pipelines.thesis_monitor.monitor --dry-run

# Real run (sends email)
python3 -m pipelines.thesis_monitor.monitor
```

## API keys needed (in `.env`)

- `FRED_API_KEY` — already set ✓
- `RESEND_API_KEY` — already set ✓
- `FINNHUB_API_KEY` — sign up free at https://finnhub.io/dashboard
- `NEWSAPI_KEY` — sign up free at https://newsapi.org/account

If `FINNHUB_API_KEY` or `NEWSAPI_KEY` are missing, the monitor still runs — the
news block just shows "no news retrieved." Add them when you want richer briefs.

## Stops monitored

| Thesis | Stop | Action |
|---|---|---|
| Oil | FRO < $30 | Exit equity leg |
| Oil | Brent < $90 (1 day; watch 10 consecutive) | Close options leg |
| Oil | Time stop Feb 13 2027 | Mechanical exit |
| Data center | MP < $49 (-25%) | Exit MP shares |
| Data center | COPX < $74 (-20%) | Exit COPX shares |

Stop trips show in the email subject (⚠) and at top of the message body.

## Override model

```bash
THESIS_MONITOR_MODEL=qwen2.5:72b-instruct-q4_K_M python3 -m pipelines.thesis_monitor.monitor
```

Default is `glm-4.7-flash:latest`.
