"""Low-float momentum scanner — OBSERVATION-ONLY research tool.

Surfaces and scores catalyst-driven low-float small/micro-cap "runners"
(the gap-and-go universe traded by services like trademomentum.org) so we
can (a) replicate the candidate list without paying for a scanner and
(b) log candidates + their intraday continuation-vs-fade path to build the
point-in-time dataset needed to honestly test whether the long-breakout
rule has any net-of-cost edge.

IMPORTANT: A multi-source literature review (2026-06-16) REJECTED the premise
that a naive long-only intraday micro-cap breakout has positive net-of-cost
expectancy for retail (intraday reversal is stronger for small caps; the
MAX/lottery effect concentrates here; micro-cap spreads dwarf any edge). This
module exists to OBSERVE and COLLECT DATA, not to assert tradeability, and it
places NO orders. See pipelines/momentum_scanner/DESIGN.md.
"""
