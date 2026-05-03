"""Single source of truth for bulk-data storage locations.

Today (no array): paths point under the repo's data/ directory or insider_study/data/.
After the Areca array mounts on Studio: set FORM4_DATA_ROOT=/Volumes/data/form4
and every consumer flips automatically.

Usage:
    from config.storage_paths import paths

    out_dir = paths.equity_1min / "AAPL"
    out_dir.mkdir(parents=True, exist_ok=True)

The module is read-only — paths are computed once at import. To override the
root, set FORM4_DATA_ROOT in the environment before importing.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _resolve_root() -> Path:
    """Decide where Form4 bulk data lives.

    Precedence:
      1. FORM4_DATA_ROOT environment variable (post-array deployment)
      2. /Volumes/data/form4 if it exists (auto-detected post-mount)
      3. Repo-relative data/ fallback (current Mini-only state)
    """
    env_root = os.getenv("FORM4_DATA_ROOT")
    if env_root:
        return Path(env_root)

    array_root = Path("/Volumes/data/form4")
    if array_root.exists():
        return array_root

    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data"


@dataclass(frozen=True)
class StoragePaths:
    """Named paths for every Form4 bulk dataset.

    All paths are derived from `root`. They are not auto-created — call
    `.ensure()` to mkdir all of them, or mkdir individually as needed.
    """
    root: Path

    @property
    def equity(self) -> Path:
        return self.root / "equity"

    @property
    def equity_1min(self) -> Path:
        """Phase 1 #1 — full-universe 1-min OHLCV Parquet."""
        return self.equity / "1min"

    @property
    def equity_daily(self) -> Path:
        """Existing daily prices; consolidated location for the array."""
        return self.equity / "daily"

    @property
    def equity_taq(self) -> Path:
        """Phase 2 #7 — event-windowed trades + NBBO quotes Parquet."""
        return self.equity / "taq"

    @property
    def options(self) -> Path:
        return self.root / "options"

    @property
    def options_eod(self) -> Path:
        """Phase 1 #3 — ThetaData EOD options chains."""
        return self.options / "eod"

    @property
    def options_1min(self) -> Path:
        """Phase 2 #6 — ThetaData 1-min options chains."""
        return self.options / "1min"

    @property
    def options_cache(self) -> Path:
        """Pull-layer cache (theta_cache.db). Not a data source."""
        return self.options / "cache"

    @property
    def edgar(self) -> Path:
        return self.root / "edgar"

    @property
    def edgar_filings(self) -> Path:
        """Phase 1 #2 — raw filing HTML/text by accession."""
        return self.edgar / "filings"

    @property
    def edgar_indexes(self) -> Path:
        """Quarterly form.idx / company.idx caches."""
        return self.edgar / "indexes"

    @property
    def fundamentals(self) -> Path:
        """Phase 3 #11 — XBRL-parsed financials."""
        return self.root / "fundamentals"

    @property
    def news(self) -> Path:
        """Phase 3 #8 — news corpus (raw + parsed)."""
        return self.root / "news"

    @property
    def transcripts(self) -> Path:
        """Phase 3 #9 — earnings call transcripts."""
        return self.root / "transcripts"

    @property
    def short_metrics(self) -> Path:
        """Phase 1 #4 — FINRA Reg SHO + borrow rates."""
        return self.root / "short_metrics"

    @property
    def etf_data(self) -> Path:
        """Phase 3 #10 — ETF holdings + flows."""
        return self.root / "etf"

    @property
    def manifest(self) -> Path:
        """Per-dataset progress tracking."""
        return self.root / "manifest"

    def ensure(self) -> None:
        """Create every directory if missing. Idempotent."""
        for path in [
            self.root,
            self.equity, self.equity_1min, self.equity_daily, self.equity_taq,
            self.options, self.options_eod, self.options_1min, self.options_cache,
            self.edgar, self.edgar_filings, self.edgar_indexes,
            self.fundamentals, self.news, self.transcripts,
            self.short_metrics, self.etf_data, self.manifest,
        ]:
            path.mkdir(parents=True, exist_ok=True)

    def describe(self) -> str:
        """Pretty-print all paths and whether they exist."""
        rows = []
        for name in [
            "root", "equity_1min", "equity_daily", "equity_taq",
            "options_eod", "options_1min", "options_cache",
            "edgar_filings", "edgar_indexes",
            "fundamentals", "news", "transcripts",
            "short_metrics", "etf_data", "manifest",
        ]:
            p = getattr(self, name)
            mark = "✓" if p.exists() else "·"
            rows.append(f"  {mark} {name:18s} {p}")
        return f"FORM4_DATA_ROOT = {self.root}\n" + "\n".join(rows)


paths = StoragePaths(root=_resolve_root())


if __name__ == "__main__":
    print(paths.describe())
