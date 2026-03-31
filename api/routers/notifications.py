from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import UserContext
from api.db import get_db
from api.email import verify_unsubscribe_token
from api.gating import require_pro
from api.id_encoding import decode_notification_id, encode_notification_id
from api.notifications_db import get_notifications_db

router = APIRouter(prefix="/api/v1/notifications", tags=["notifications"])

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class PreferencesUpdate(BaseModel):
    email_enabled: Optional[bool] = None
    in_app_enabled: Optional[bool] = None
    email_frequency: Optional[str] = Field(None, pattern="^(realtime|daily)$")
    high_value_filing: Optional[bool] = None
    cluster_formation: Optional[bool] = None
    activity_spike: Optional[bool] = None
    congress_convergence: Optional[bool] = None
    watchlist_activity: Optional[bool] = None
    min_trade_value: Optional[float] = Field(None, ge=0)
    min_insider_tier: Optional[int] = Field(None, ge=1, le=5)


class WatchlistAdd(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BOOL_PREF_FIELDS = [
    "email_enabled",
    "in_app_enabled",
    "high_value_filing",
    "cluster_formation",
    "activity_spike",

    "congress_convergence",
    "watchlist_activity",
]


def _ensure_preferences(conn, user_id: str) -> dict:
    """Return preferences for user, creating defaults if none exist."""
    row = conn.execute(
        "SELECT * FROM notification_preferences WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        "INSERT INTO notification_preferences (user_id) VALUES (?)",
        (user_id,),
    )
    conn.commit()
    return dict(
        conn.execute(
            "SELECT * FROM notification_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    )


def _format_prefs(row: dict) -> dict:
    """Convert integer booleans to Python bools for JSON response."""
    result = dict(row)
    for field in BOOL_PREF_FIELDS:
        if field in result:
            result[field] = bool(result[field])
    return result


# ---------------------------------------------------------------------------
# Notification endpoints
# ---------------------------------------------------------------------------


@router.get("")
def list_notifications(
    user: UserContext = Depends(require_pro),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    unread_only: bool = Query(default=False),
) -> dict:
    """List in-app notifications, newest first."""
    with get_notifications_db() as conn:
        where = "WHERE user_id = ?"
        params: list = [user.user_id]
        if unread_only:
            where += " AND is_read = 0"

        total = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM notifications {where}",
            params,
        ).fetchone()["cnt"]

        rows = conn.execute(
            f"""SELECT id, event_type, title, body, ticker, is_read, created_at
                FROM notifications {where}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

    items = [dict(r) for r in rows]
    for item in items:
        if item.get("id") is not None:
            item["id"] = encode_notification_id(item["id"])
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }


@router.get("/unread-count")
def unread_count(user: UserContext = Depends(require_pro)) -> dict:
    """Return count of unread notifications for badge display."""
    with get_notifications_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM notifications WHERE user_id = ? AND is_read = 0",
            (user.user_id,),
        ).fetchone()
    return {"count": row["cnt"]}


@router.post("/{notification_id}/read")
def mark_read(notification_id: str, user: UserContext = Depends(require_pro)) -> dict:
    """Mark a single notification as read."""
    raw_id = decode_notification_id(notification_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Notification not found")
    with get_notifications_db() as conn:
        result = conn.execute(
            "UPDATE notifications SET is_read = 1 WHERE id = ? AND user_id = ?",
            (raw_id, user.user_id),
        )
        conn.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Notification not found")
    return {"ok": True}


@router.post("/read-all")
def mark_all_read(user: UserContext = Depends(require_pro)) -> dict:
    """Mark all notifications as read."""
    with get_notifications_db() as conn:
        result = conn.execute(
            "UPDATE notifications SET is_read = 1 WHERE user_id = ? AND is_read = 0",
            (user.user_id,),
        )
        conn.commit()
    return {"ok": True, "updated": result.rowcount}


# ---------------------------------------------------------------------------
# Preference endpoints
# ---------------------------------------------------------------------------


@router.get("/preferences")
def get_preferences(user: UserContext = Depends(require_pro)) -> dict:
    """Get notification preferences, auto-creating defaults on first access."""
    with get_notifications_db() as conn:
        prefs = _ensure_preferences(conn, user.user_id)
    return _format_prefs(prefs)


@router.put("/preferences")
def update_preferences(
    body: PreferencesUpdate,
    user: UserContext = Depends(require_pro),
) -> dict:
    """Update notification preferences."""
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Convert bools to ints for SQLite
    for field in BOOL_PREF_FIELDS:
        if field in updates:
            updates[field] = int(updates[field])

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values())

    with get_notifications_db() as conn:
        _ensure_preferences(conn, user.user_id)
        conn.execute(
            f"UPDATE notification_preferences SET {set_clause}, updated_at = datetime('now') WHERE user_id = ?",
            values + [user.user_id],
        )
        conn.commit()
        prefs = dict(
            conn.execute(
                "SELECT * FROM notification_preferences WHERE user_id = ?",
                (user.user_id,),
            ).fetchone()
        )
    return _format_prefs(prefs)


# ---------------------------------------------------------------------------
# Watchlist endpoints
# ---------------------------------------------------------------------------


@router.get("/watchlist")
def get_watchlist(user: UserContext = Depends(require_pro)) -> dict:
    """List watched tickers."""
    with get_notifications_db() as conn:
        rows = conn.execute(
            "SELECT ticker, added_at FROM watchlist WHERE user_id = ? ORDER BY added_at DESC",
            (user.user_id,),
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@router.post("/watchlist")
def add_to_watchlist(
    body: WatchlistAdd,
    user: UserContext = Depends(require_pro),
) -> dict:
    """Add a ticker to the watchlist."""
    ticker = body.ticker.upper().strip()
    # Validate ticker exists in our data
    with get_db() as iconn:
        exists = iconn.execute(
            "SELECT 1 FROM trades WHERE ticker = ? LIMIT 1",
            (ticker,),
        ).fetchone()
    if not exists:
        raise HTTPException(status_code=400, detail=f"Ticker '{ticker}' not found in our database")
    with get_notifications_db() as conn:
        # Cap at 25 tickers
        count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM watchlist WHERE user_id = ?",
            (user.user_id,),
        ).fetchone()["cnt"]
        if count >= 25:
            raise HTTPException(
                status_code=400,
                detail="Watchlist limited to 25 tickers. Remove one first.",
            )
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (user_id, ticker) VALUES (?, ?)",
            (user.user_id, ticker),
        )
        conn.commit()
    return {"ok": True, "ticker": ticker}


@router.delete("/watchlist/{ticker}")
def remove_from_watchlist(ticker: str, user: UserContext = Depends(require_pro)) -> dict:
    """Remove a ticker from the watchlist."""
    ticker = ticker.upper().strip()
    with get_notifications_db() as conn:
        result = conn.execute(
            "DELETE FROM watchlist WHERE user_id = ? AND ticker = ?",
            (user.user_id, ticker),
        )
        conn.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Ticker not in watchlist")
    return {"ok": True, "ticker": ticker}


# ---------------------------------------------------------------------------
# Unsubscribe (public, token-authenticated)
# ---------------------------------------------------------------------------


@router.get("/unsubscribe")
def unsubscribe(user_id: str = Query(...), token: str = Query(...)) -> dict:
    """One-click email unsubscribe via signed token URL."""
    if not verify_unsubscribe_token(user_id, token):
        raise HTTPException(status_code=403, detail="Invalid unsubscribe link")
    with get_notifications_db() as conn:
        conn.execute(
            "UPDATE notification_preferences SET email_enabled = 0, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()
    return {"ok": True, "message": "Email notifications disabled. You can re-enable them in settings."}
