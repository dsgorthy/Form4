"""
Parquet storage utilities for minute-level market data.

Handles saving/loading minute bars as Parquet files and daily summaries
as JSON. Directory layout:

    {raw_dir}/{SYMBOL}/{YYYY-MM-DD}.parquet   — minute bars
    {raw_dir}/daily_summaries/{YYYY-MM-DD}.json — daily summary dicts
    {options_dir}/{YYYY-MM-DD}.parquet         — options bars
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

DateLike = Union[str, date]


def _normalize_date(d: DateLike) -> str:
    """Convert a date-like value to a 'YYYY-MM-DD' string."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d.isoformat()
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d").date().isoformat()
    raise TypeError(f"Expected str or datetime.date, got {type(d).__name__}")


class DataStorage:
    """Read/write interface for minute-bar Parquet files and daily summaries."""

    def __init__(
        self,
        raw_dir: Optional[Union[str, Path]] = None,
        processed_dir: Optional[Union[str, Path]] = None,
        options_dir: Optional[Union[str, Path]] = None,
        base_dir: Optional[Union[str, Path]] = None,
        extra_raw_dirs: Optional[list] = None,
    ):
        """
        Parameters
        ----------
        raw_dir : path, optional
            Primary directory for raw minute bars. Defaults to {base_dir}/data/raw.
        processed_dir : path, optional
            Directory for processed data. Defaults to {base_dir}/data/processed.
        options_dir : path, optional
            Directory for options bars. Defaults to {base_dir}/data/options.
        base_dir : path, optional
            Project base directory. Defaults to the trading-framework root.
            Only used when raw_dir/processed_dir/options_dir are not provided.
        extra_raw_dirs : list of paths, optional
            Additional raw data directories searched in order after raw_dir.
            Useful when data is split across multiple sources (e.g. spy-0dte
            has 2024+ data; trading-framework has 2020-2023 data).
        """
        if base_dir is None:
            base_dir = Path(__file__).parent.parent.parent
        else:
            base_dir = Path(base_dir)

        self.raw_dir = Path(raw_dir) if raw_dir else base_dir / "data" / "raw"
        self.processed_dir = Path(processed_dir) if processed_dir else base_dir / "data" / "processed"
        self.options_dir = Path(options_dir) if options_dir else base_dir / "data" / "options"

        # Additional raw dirs searched after raw_dir (first match wins)
        self._extra_raw_dirs: list = [Path(d) for d in (extra_raw_dirs or [])]

    # ── Minute bars ──────────────────────────────────────────────

    def _bars_path(self, symbol: str, date_str: str) -> Optional[Path]:
        """Return the first existing path for symbol/date across all raw dirs."""
        candidates = [self.raw_dir] + self._extra_raw_dirs
        for d in candidates:
            p = d / symbol.upper() / f"{date_str}.parquet"
            if p.exists():
                return p
        return None

    def save_minute_bars(self, symbol: str, dt: DateLike, df: pd.DataFrame) -> Path:
        date_str = _normalize_date(dt)
        path = self.raw_dir / symbol.upper() / f"{date_str}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, engine="pyarrow", index=True)
        logger.info("Saved %d bars for %s on %s -> %s", len(df), symbol.upper(), date_str, path)
        return path

    def load_minute_bars(self, symbol: str, dt: DateLike) -> Optional[pd.DataFrame]:
        date_str = _normalize_date(dt)
        path = self._bars_path(symbol, date_str)
        if path is None:
            logger.debug("No data found for %s on %s in any raw dir", symbol.upper(), date_str)
            return None
        df = pd.read_parquet(path, engine="pyarrow")
        logger.debug("Loaded %d bars for %s on %s from %s", len(df), symbol.upper(), date_str, path)
        return df

    def load_minute_bars_range(
        self, symbol: str, start_date: DateLike, end_date: DateLike
    ) -> pd.DataFrame:
        start_str = _normalize_date(start_date)
        end_str = _normalize_date(end_date)
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_str, "%Y-%m-%d").date()

        if start_dt > end_dt:
            raise ValueError(f"start_date ({start_str}) must be <= end_date ({end_str})")

        frames = []
        available = self.get_available_dates(symbol)
        available_set = set(available)

        import pandas as pd
        current = start_dt
        one_day = pd.Timedelta(days=1)
        while current <= end_dt:
            ds = current.isoformat()
            if ds in available_set:
                df = self.load_minute_bars(symbol, ds)
                if df is not None and not df.empty:
                    frames.append(df)
            current += one_day

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames)
        combined.sort_index(inplace=True)
        return combined

    def has_date(self, symbol: str, dt: DateLike) -> bool:
        return self._bars_path(symbol, _normalize_date(dt)) is not None

    def get_available_dates(self, symbol: str) -> list:
        dates = set()
        for d in [self.raw_dir] + self._extra_raw_dirs:
            symbol_dir = d / symbol.upper()
            if symbol_dir.is_dir():
                dates.update(p.stem for p in symbol_dir.glob("*.parquet") if p.is_file())
        return sorted(dates)

    # ── Daily summaries ──────────────────────────────────────────

    def _summary_path(self, date_str: str) -> Path:
        return self.raw_dir / "daily_summaries" / f"{date_str}.json"

    def save_daily_summary(self, dt: DateLike, summary_dict: dict) -> Path:
        date_str = _normalize_date(dt)
        path = self._summary_path(date_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary_dict, f, indent=2, default=str)
        return path

    def load_daily_summary(self, dt: DateLike) -> Optional[dict]:
        date_str = _normalize_date(dt)
        path = self._summary_path(date_str)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ── Options bars ─────────────────────────────────────────────

    def _options_path(self, date_str: str) -> Path:
        return self.options_dir / f"{date_str}.parquet"

    def save_options_day(self, dt: DateLike, df: pd.DataFrame) -> Path:
        date_str = _normalize_date(dt)
        path = self._options_path(date_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, engine="pyarrow", index=True)
        logger.info("Saved options day %s: %d symbols, %d bars -> %s",
                    date_str, len(df.columns), len(df), path)
        return path

    def load_options_day(self, dt: DateLike) -> Optional[pd.DataFrame]:
        date_str = _normalize_date(dt)
        path = self._options_path(date_str)
        if not path.exists():
            return None
        return pd.read_parquet(path, engine="pyarrow")

    def has_options_day(self, dt: DateLike) -> bool:
        return self._options_path(_normalize_date(dt)).exists()

    def get_available_options_dates(self) -> list:
        if not self.options_dir.is_dir():
            return []
        return sorted(p.stem for p in self.options_dir.glob("*.parquet") if p.is_file())
