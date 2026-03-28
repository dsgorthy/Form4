"""
Black-Scholes option pricing engine for the trading framework.

Provides a class-based interface with price, greeks, and implied volatility
calculations. No external framework dependencies — pure scipy/math.
"""

import math
import warnings
from typing import Optional

from scipy.stats import norm


class BlackScholes:
    """
    Black-Scholes option pricing model.

    All methods are static — instantiate or call directly.

    Parameters used throughout
    --------------------------
    S : float
        Underlying spot price.
    K : float
        Strike price.
    T : float
        Time to expiration in years (e.g., 31 minutes = 31 / (365 * 24 * 60)).
    r : float
        Continuous risk-free rate (annualized, e.g., 0.0525 for 5.25%).
    sigma : float
        Implied / historical volatility (annualized, e.g., 0.18 for 18%).
    option_type : str
        "call" or "put" (case-insensitive).
    """

    @staticmethod
    def _d1_d2(S: float, K: float, T: float, r: float, sigma: float):
        """Compute d1 and d2 for the B-S formula."""
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return None, None
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
        return d1, d2

    @staticmethod
    def price(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str = "call",
    ) -> float:
        """
        Black-Scholes option price.

        At expiry (T <= 0), returns intrinsic value.

        Returns
        -------
        float
            Option price (per share, not per contract).
        """
        ot = option_type.lower()
        if T <= 0:
            if ot == "call":
                return max(0.0, S - K)
            else:
                return max(0.0, K - S)

        d1, d2 = BlackScholes._d1_d2(S, K, T, r, sigma)
        if d1 is None:
            return max(0.0, S - K) if ot == "call" else max(0.0, K - S)

        if ot == "call":
            return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
        else:
            return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    @staticmethod
    def delta(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str = "call",
    ) -> float:
        """
        Option delta — sensitivity of price to underlying move.

        Returns
        -------
        float
            Delta in [-1, 1]. Calls: (0, 1). Puts: (-1, 0).
        """
        ot = option_type.lower()
        if T <= 0:
            if ot == "call":
                return 1.0 if S > K else (0.5 if S == K else 0.0)
            else:
                return -1.0 if S < K else (-0.5 if S == K else 0.0)

        d1, _ = BlackScholes._d1_d2(S, K, T, r, sigma)
        if d1 is None:
            return 0.0

        if ot == "call":
            return norm.cdf(d1)
        else:
            return -norm.cdf(-d1)

    @staticmethod
    def gamma(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
    ) -> float:
        """
        Option gamma — rate of change of delta per $1 move in underlying.
        Same for calls and puts.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0.0

        d1, _ = BlackScholes._d1_d2(S, K, T, r, sigma)
        if d1 is None:
            return 0.0

        return norm.pdf(d1) / (S * sigma * math.sqrt(T))

    @staticmethod
    def theta(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str = "call",
    ) -> float:
        """
        Option theta — daily time decay (price change per calendar day).

        Returns a negative value for long options (time decay hurts buyers).
        Divided by 365 to convert from per-year to per-day.
        """
        ot = option_type.lower()
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0.0

        d1, d2 = BlackScholes._d1_d2(S, K, T, r, sigma)
        if d1 is None:
            return 0.0

        sqrt_T = math.sqrt(T)
        term1 = -(S * norm.pdf(d1) * sigma) / (2.0 * sqrt_T)
        discount = K * math.exp(-r * T)

        if ot == "call":
            term2 = -r * discount * norm.cdf(d2)
        else:
            term2 = r * discount * norm.cdf(-d2)

        return (term1 + term2) / 365.0

    @staticmethod
    def vega(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
    ) -> float:
        """
        Option vega — price change per 1% move in implied volatility.
        Same for calls and puts. Divided by 100 for per-1%-vol convention.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0.0

        d1, _ = BlackScholes._d1_d2(S, K, T, r, sigma)
        if d1 is None:
            return 0.0

        return S * norm.pdf(d1) * math.sqrt(T) / 100.0

    @staticmethod
    def rho(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str = "call",
    ) -> float:
        """
        Option rho — price change per 1% change in risk-free rate.
        Divided by 100 for per-1%-rate convention.
        """
        ot = option_type.lower()
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0.0

        d1, d2 = BlackScholes._d1_d2(S, K, T, r, sigma)
        if d1 is None:
            return 0.0

        discount = K * math.exp(-r * T)
        if ot == "call":
            return discount * T * norm.cdf(d2) / 100.0
        else:
            return -discount * T * norm.cdf(-d2) / 100.0

    @staticmethod
    def all_greeks(
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str = "call",
    ) -> dict:
        """
        Compute all greeks in a single call.

        Returns
        -------
        dict with keys: price, delta, gamma, theta, vega, rho
        """
        return {
            "price": BlackScholes.price(S, K, T, r, sigma, option_type),
            "delta": BlackScholes.delta(S, K, T, r, sigma, option_type),
            "gamma": BlackScholes.gamma(S, K, T, r, sigma),
            "theta": BlackScholes.theta(S, K, T, r, sigma, option_type),
            "vega": BlackScholes.vega(S, K, T, r, sigma),
            "rho": BlackScholes.rho(S, K, T, r, sigma, option_type),
        }

    @staticmethod
    def implied_vol(
        market_price: float,
        S: float,
        K: float,
        T: float,
        r: float,
        option_type: str = "call",
        tol: float = 1e-6,
        max_iter: int = 200,
    ) -> Optional[float]:
        """
        Compute implied volatility via bisection search.

        Parameters
        ----------
        market_price : float
            Observed market price of the option.
        tol : float
            Convergence tolerance on the vol estimate.
        max_iter : int
            Maximum bisection iterations.

        Returns
        -------
        float or None
            Implied volatility if converged, else None.
        """
        if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
            return None

        intrinsic = BlackScholes.price(S, K, 0, r, 0.0001, option_type)
        if market_price < intrinsic - tol:
            warnings.warn(
                f"market_price ({market_price:.4f}) < intrinsic ({intrinsic:.4f}); "
                "implied vol undefined"
            )
            return None

        lo, hi = 1e-6, 20.0  # sigma search bounds [0.0001%, 2000%]

        for _ in range(max_iter):
            mid = (lo + hi) / 2.0
            price_mid = BlackScholes.price(S, K, T, r, mid, option_type)
            diff = price_mid - market_price

            if abs(diff) < tol:
                return mid

            if diff > 0:
                hi = mid
            else:
                lo = mid

            if (hi - lo) < tol:
                return (lo + hi) / 2.0

        warnings.warn("implied_vol: bisection did not converge")
        return (lo + hi) / 2.0
