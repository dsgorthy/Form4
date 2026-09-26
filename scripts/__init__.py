"""Operational and research scripts.

A package only so research tools can import each other's shipped helpers
instead of re-implementing them: `signal_cells` takes its episode chaining from
`exit_horizon_study.to_episodes` and its clustered bootstrap from
`signal_screen._clustered_t`. A second copy of episode logic is not a style
problem — a fixed calendar bucket in place of gap-based chaining inflated
episode counts 13.8% and reversed two headline findings.
"""
