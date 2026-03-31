from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends

from api.auth import UserContext, get_current_user

router = APIRouter(prefix="/api/v1/data-quality", tags=["data-quality"])

REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "reports"


@router.get("")
def get_data_quality_report(user: UserContext = Depends(get_current_user)) -> dict:
    """Serve the latest data quality report."""
    report_path = REPORTS_DIR / "data_quality.json"
    if not report_path.exists():
        return {"error": "No data quality report found. Run: python3 strategies/insider_catalog/data_quality.py"}

    with open(report_path) as f:
        return json.load(f)
