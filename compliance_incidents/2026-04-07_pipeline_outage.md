# Compliance Incident — 2026-04-07 Pipeline Outage

**Status:** Closed (no customer disclosure required)
**Reviewed by:** derek (sole operator)
**Date opened:** 2026-05-01
**Date closed:** 2026-05-02

## Summary

A 21-day silent outage of three live paper trading strategies (full incident detail in `docs/postmortems/2026-04-07_21d_silent_outage.md`) impacted the trading-decision pipeline between 2026-04-07 and 2026-04-28. No customer-facing material misrepresentation occurred; remediation and rebuild plan are tracked in this repo.

## Customer-impact assessment

| Surface | Was it stale during outage? | Did it misrepresent? |
|---|---|---|
| form4.app dashboard "live performance" YTD | Showed values current to 2026-04-06; froze | No — the displayed performance was correct at the time it was last updated. Performance values do not move while no orders fire, so what was shown remained accurate. |
| form4.app "today's signals" / "recent insider activity" | Showed values current to 2026-04-06; froze | No — date stamps on each item reflected when they were filed, not "as of today." Users could see the dates were old. |
| form4.app portfolio overlay | Showed last-known position state | No — the underlying paper accounts were genuinely in that state. |
| API endpoints (/api/v1/*) | Returned stale data with stale dates | No — clients can read the dates. |
| Email notifications / Telegram alerts | None fired (no events to notify on) | No — silence is correct given no events occurred. |
| Marketing claims ("Sharpe 1.18", strategy descriptions) | Static — not affected | No — these are research-validated metrics, not live performance claims. |

**Conclusion:** Per the 2026-05-02 reliability rebuild planning session, **internal-only postmortem suffices.** No proactive disclosure required because no surface presented stale data as fresh.

## Compensating controls being added

The reliability rebuild (`~/.claude/plans/here-s-the-housing-thanks-cached-sifakis.md`) makes this class of failure structurally impossible going forward:

- **Phase 2 #2.4 — order_audit + trade_decision_audit tables.** Every order placed and every signal scanned writes a row to PG with full provenance: signal inputs, conviction, config_yaml_sha, decision rationale. Customer-facing claims will be reconcilable to this audit log within 24 hours of any future incident.
- **Phase 2 #2.6 — Alpaca↔DB position reconciliation, daily.** Detects orphan positions, missing positions, size mismatches. The 2026-04-15 BW/KOS orphan re-track was manual; this becomes automated.
- **Phase 3 #3.1 — Trade-or-halt automation.** Runners refuse entries when freshness SLO is breached. Pre-outage system traded on (zero) stale data; post-rebuild system halts and alerts.

## Future incidents

If a future incident affects customer-facing live data, the disposition framework is:

| Incident shape | Required disclosure |
|---|---|
| Stale data presented as fresh (e.g., yesterday's signals shown without dates) | Proactive: in-app banner + email to affected users + public changelog |
| Stale data with dates clearly visible | Public changelog only |
| Internal-only impact (research, backtests, internal dashboards) | Internal postmortem only |
| Material misrepresentation in marketing or performance numbers | Coordinate with sidequestgroup.com legal counsel |

The decision matrix above is the documented standard. Future incidents must explicitly map to a row before resolution.

## References

- Postmortem: `docs/postmortems/2026-04-07_21d_silent_outage.md`
- Rebuild plan: `~/.claude/plans/here-s-the-housing-thanks-cached-sifakis.md`
- Original audit transcript: ChatGPT/Claude session 2026-05-01–02 (in agent memory)

## Sign-off

derek — 2026-05-02. Internal-only postmortem suffices. No further customer outreach planned.
