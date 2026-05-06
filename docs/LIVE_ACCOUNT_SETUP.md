# Live Alpaca Account — Setup

Steps to take **today** so the account is funded and credentials are in place by Day 14. ACH funding takes 1-3 business days; everything else is fast.

## 1. Open the live trading account

1. Sign in at <https://alpaca.markets>.
2. The default account on signup is **paper**. Click *Account* → *Brokerage Account* → "Open a Live Trading Account".
3. KYC: SSN, address, employment, Form W-9, brokerage agreement. Takes ~10 min if all docs are at hand.
4. Wait for approval — usually <24h, sometimes same-day.

## 2. Fund $10,000 (start this Day 1)

ACH from a linked bank account. Settles in 1-3 business days. Account → *Banking* → *ACH Deposit*. Don't wire — wires cost $25 and don't settle materially faster for this size.

While waiting, the rest of the steps can run in parallel.

## 3. Generate live API credentials

Account → *API Keys* → *Generate New Key* → copy the key + secret. **You only see the secret once.** Save it directly to Studio.

## 4. Install credentials on Studio

```bash
ssh derekg@100.78.9.66
nano ~/.config/form4/secrets.env  # the secret-bearing file already exists
```

Add two lines (replace placeholders):

```
ALPACA_API_KEY_QUALITY_MOMENTUM_LIVE=PKxxxxxxxxxxxxxxxx
ALPACA_API_SECRET_QUALITY_MOMENTUM_LIVE=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Make sure file mode is `600`:

```bash
chmod 600 ~/.config/form4/secrets.env
```

The plist wrappers source this file at runtime; no plist edits required.

## 5. Configure the SMS alert gateway

In `~/trading-framework/.env` on Studio (already-existing file), set:

```
CRITICAL_ALERT_SMS_TO=5551234567@vtext.com   # your-number@your-carrier-gateway
CRITICAL_ALERT_EMAIL_TO=derek.gorthy@gmail.com
DAILY_DIGEST_TO=derek.gorthy@gmail.com
```

Common carrier gateways (replace `5551234567` with your number, no dashes):

| Carrier | Address |
|---|---|
| Verizon | `5551234567@vtext.com` |
| AT&T | `5551234567@txt.att.net` |
| T-Mobile | `5551234567@tmomail.net` |
| Google Fi | `5551234567@msg.fi.google.com` |

## 6. Verify credentials (read-only — no orders)

```bash
ssh derekg@100.78.9.66
cd ~/trading-framework
python3 scripts/verify_live_creds.py --strategy quality_momentum --min-equity 9500
```

Expected output once funding settles:

```
=== Live credential check: quality_momentum ===
  ok           : True
  reason       : all checks pass
  equity       : $10,000.00
  portfolio    : $10,000.00
  cash         : $10,000.00
  positions    : 0
```

If `ok: False` and reason is `equity ... < min`, the ACH deposit hasn't fully settled yet. Re-run after settlement.

## 7. Test the SMS path

```bash
ssh derekg@100.78.9.66
cd ~/trading-framework
python3 -m framework.alerts.sms test "live trading SMS path verified"
```

You should receive an SMS within 1-5 minutes. The same path fires automatically on every `alert.critical(...)` call from the runners and probes.

## 8. (Day 14 only) Enable the live runner

Don't run this until the pre-flight validator returns all-GREEN. See `docs/LIVE_LAUNCH.md` for the full procedure.

```bash
ssh derekg@100.78.9.66
cp ~/trading-framework/scripts/launchd/com.openclaw.quality-momentum-live.plist \
   ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/com.openclaw.quality-momentum-live.plist
launchctl list | grep quality-momentum-live   # should show a PID > 0
```

## Halt switch

If anything looks wrong at any time:

```bash
~/.local/bin/studio halt-trading quality_momentum
```

This sets `TRADING_HALTED_QUALITY_MOMENTUM=true` in `.env` and restarts the runner. Both paper and live runners pick up the halt — entries stop, exits keep firing so safety-outs still work.

Resume with `studio resume-trading quality_momentum`.
