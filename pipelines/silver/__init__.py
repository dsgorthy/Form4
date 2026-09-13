"""Silver: the faithful, judged parse of Bronze's SEC submissions.

    parse.py   unwrap the SGML submission and parse the ownershipDocument
               EXACTLY as filed -- nothing dropped, nothing coerced
    build.py   bronze rows with no receipt -> silver.form4_transaction
    assess.py  price_quality, as a separate pass, from a per-ticker price band
    parity.py  silver vs trades, the report Derek reads before any cutover

Design: docs/silver_layer_model.md, docs/silver_build_plan.md.
"""
