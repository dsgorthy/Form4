"""
Volatility engine for the trading framework.

Estimates implied volatility from a vol-proxy ETF (default: VIXY) and
provides utilities for scaling, skew, and term-structure adjustments.
No framework config imports — all parameters via constructor.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class VolEngine:
    """
    Estimate and manipulate implied volatility for option pricing.

    The engine converts a vol-proxy ETF price (e.g., VIXY) into an
    annualized IV estimate, applies skew adjustments for OTM strikes,
    and accounts for intraday time decay.

    Parameters
    ----------
    scaling_factor : float
        Multiplier to convert vol-proxy level to annualized IV.
        Default 1.3 calibrated for VIXY -> SPY IV relationship.
    vix_vixy_ratio : float
        Approximate VIX / VIXY price ratio for cross-referencing.
    skew_slope : float
        IV skew per unit of moneyness (|log(K/S)|).
        Puts typically have higher IV than calls (negative skew).
    trading_day_hours : float
        Hours in a trading day used for intraday time calculations.
    vol_proxy_symbol : str
        The ETF symbol used as the vol proxy (default "VIXY").
    """

    def __init__(
        self,
        scaling_factor: float = 1.3,
        vix_vixy_ratio: float = 0.8,
        skew_slope: float = 0.12,
        trading_day_hours: float = 6.5,
        vol_proxy_symbol: str = "VIXY",  # NEW: configurable vol proxy
    ) -> None:
        self.scaling_factor = scaling_factor
        self.vix_vixy_ratio = vix_vixy_ratio
        self.skew_slope = skew_slope
        self.trading_day_hours = trading_day_hours
        self.vol_proxy_symbol = vol_proxy_symbol

    def estimate_iv(self, vol_proxy_price: float) -> float:
        """
        Convert vol-proxy ETF price to annualized implied volatility.

        Parameters
        ----------
        vol_proxy_price : float
            Current price of the vol-proxy ETF (e.g., VIXY at ~$14).

        Returns
        -------
        float
            Annualized IV as a decimal (e.g., 0.18 for 18%).
        """
        if vol_proxy_price <= 0:
            logger.warning("vol_proxy_price <= 0 (%s); returning default IV=0.20", vol_proxy_price)
            return 0.20
        # VIXY at ~14 -> VIX ~18 -> IV ~18%
        # Scale: proxy * ratio * scaling_factor / 100
        raw_vix_equiv = vol_proxy_price * self.vix_vixy_ratio * self.scaling_factor
        return raw_vix_equiv / 100.0

    def estimate_iv_from_vix(self, vix_level: float) -> float:
        """
        Convert a VIX level directly to annualized IV.

        Parameters
        ----------
        vix_level : float
            VIX index level (e.g., 18.5).

        Returns
        -------
        float
            Annualized IV as a decimal.
        """
        return vix_level / 100.0

    def skew_adjusted_iv(
        self,
        base_iv: float,
        spot: float,
        strike: float,
        option_type: str = "call",
    ) -> float:
        """
        Apply volatility skew adjustment for OTM/ITM strikes.

        Uses a simple linear skew model. Puts trade at a premium
        (negative skew), calls at a slight discount.

        Parameters
        ----------
        base_iv : float
            ATM implied volatility (annualized decimal).
        spot : float
            Current underlying price.
        strike : float
            Option strike price.
        option_type : str
            "call" or "put".

        Returns
        -------
        float
            Skew-adjusted IV (always positive).
        """
        if spot <= 0 or strike <= 0:
            return base_iv

        moneyness = math.log(strike / spot)  # log(K/S)

        # Puts: higher IV for OTM (moneyness < 0), calls: slight discount
        if option_type.lower() == "put":
            # OTM puts (K < S) -> moneyness < 0 -> add skew
            adjustment = -self.skew_slope * moneyness
        else:
            # OTM calls (K > S) -> moneyness > 0 -> slight reduction
            adjustment = self.skew_slope * moneyness * 0.3

        adjusted = base_iv + adjustment
        return max(0.01, adjusted)  # floor at 1%

    def intraday_time_to_expiry(
        self,
        entry_time: str,
        expiry_time: str = "16:00",
    ) -> float:
        """
        Calculate time to expiry in years for intraday options.

        Parameters
        ----------
        entry_time : str
            Current time as "HH:MM" (Eastern).
        expiry_time : str
            Option expiry time as "HH:MM" (default "16:00" for 0DTE).

        Returns
        -------
        float
            Time to expiry in years (fraction).
        """
        def _parse(t: str) -> float:
            h, m = map(int, t.split(":"))
            return h + m / 60.0

        entry_hours = _parse(entry_time)
        expiry_hours = _parse(expiry_time)
        minutes_remaining = max(0.0, (expiry_hours - entry_hours) * 60.0)
        # Convert minutes to years: minutes / (365 * 24 * 60)
        return minutes_remaining / (365.0 * 24.0 * 60.0)

    def get_vol_proxy_bar(
        self,
        bars: dict,
        timeframe: str = "1Min",
    ) -> Optional[pd.Series]:
        """
        Extract the most recent vol-proxy bar from the bars dict.

        Parameters
        ----------
        bars : dict
            Keyed "SYMBOL_TIMEFRAME" (standard framework bars dict).
        timeframe : str
            Which timeframe to look up (default "1Min").

        Returns
        -------
        pd.Series or None
            Last bar row for the vol proxy, or None if not present.
        """
        key = f"{self.vol_proxy_symbol}_{timeframe}"
        df = bars.get(key)
        if df is None or df.empty:
            logger.debug("No bars for vol proxy %s", key)
            return None
        return df.iloc[-1]

    def current_iv(
        self,
        bars: dict,
        timeframe: str = "1Min",
        fallback_iv: float = 0.20,
    ) -> float:
        """
        Estimate current IV from the vol proxy in the bars dict.

        Parameters
        ----------
        bars : dict
            Standard framework bars dict.
        timeframe : str
            Timeframe to use for vol proxy lookup.
        fallback_iv : float
            IV to return if vol proxy data is unavailable.

        Returns
        -------
        float
            Annualized IV estimate.
        """
        bar = self.get_vol_proxy_bar(bars, timeframe)
        if bar is None:
            logger.warning(
                "Vol proxy %s not in bars; using fallback IV=%.2f",
                self.vol_proxy_symbol, fallback_iv,
            )
            return fallback_iv
        price = bar.get("close", bar.iloc[-1] if hasattr(bar, "iloc") else fallback_iv)
        return self.estimate_iv(float(price))

    def get_iv_for_strike(
        self,
        vixy_price: float,
        strike: float,
        spot: float,
        option_type: str,
        time_to_expiry_years: Optional[float] = None,
    ) -> float:
        """
        End-to-end IV estimate for a specific strike.

        Combines estimate_iv (VIXY → base IV) and skew_adjusted_iv
        (strike-level adjustment). time_to_expiry_years is accepted for
        API compatibility but term-structure scaling is handled via
        the BlackScholes pricing layer.
        """
        base_iv = self.estimate_iv(vixy_price)
        return self.skew_adjusted_iv(base_iv, spot, strike, option_type)

    def apply_skew(
        self,
        base_iv: float,
        strike: float,
        spot: float,
        option_type: str,
    ) -> float:
        """Alias for skew_adjusted_iv (original spy-0dte API name)."""
        return self.skew_adjusted_iv(base_iv, spot, strike, option_type)

    def expected_move(
        self,
        spot: float,
        iv: float,
        time_fraction: float,
    ) -> float:
        """
        Expected 1-sigma move of the underlying over time_fraction of a year.

        Parameters
        ----------
        spot : float
            Current underlying price.
        iv : float
            Annualized implied volatility (decimal).
        time_fraction : float
            Time horizon as a fraction of a year.

        Returns
        -------
        float
            Expected 1-sigma move in price units (dollars).
        """
        if spot <= 0 or iv <= 0 or time_fraction <= 0:
            return 0.0
        return spot * iv * math.sqrt(time_fraction)
